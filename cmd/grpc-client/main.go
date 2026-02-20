package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"go-mongodb-sharding-poc/internal/config"
	"go-mongodb-sharding-poc/internal/loadbalancer"
	pb "go-mongodb-sharding-poc/proto/sharding/v1"
)

const database = "sharding_poc"
const collection = "grpc_demo"

func main() {
	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg := config.Load()

	log.Println("gRPC Client Demo (Client-Side Load Balancing)")
	log.Println("")

	// Connect using client-side load balancing.
	//
	// LOCAL (static resolver):
	//   target = "static:///localhost:50051,localhost:50052"
	//   Resolves a fixed list — each RPC round-robins across them.
	//
	// KUBERNETES (DNS resolver):
	//   target = "dns:///grpc-server-headless.sharding-poc.svc.cluster.local:50051"
	//   Resolves headless service to individual pod IPs, re-resolves every 30s
	//   to pick up scale events.
	//
	// Unlike the old GRPCPool that opened 4 TCP connections to one address,
	// this creates separate HTTP/2 connections to each resolved endpoint and
	// distributes individual RPCs across them via round-robin.
	target := cfg.GRPCTarget
	conn, err := loadbalancer.NewClientConn(target)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	log.Printf("Target: %s", target)
	log.Printf("Policy: round_robin + health_check")

	// All demos share one client — the balancer distributes RPCs internally
	client := pb.NewShardingServiceClient(conn)

	// Demo 1: Unary InsertDocument
	log.Println("")
	log.Println("=== Demo 1: Unary InsertDocument ===")

	doc := bson.M{
		"_id":    "grpc_test_001",
		"name":   "Alice",
		"region": "US",
		"email":  "alice@example.com",
	}
	payload, _ := bson.Marshal(doc)

	insertResp, err := client.InsertDocument(ctx, &pb.InsertRequest{
		Document: &pb.Document{
			Id:         "grpc_test_001",
			Database:   database,
			Collection: collection,
			Payload:    payload,
		},
	})
	if err != nil {
		log.Printf("  [ERROR] InsertDocument: %v", err)
	} else {
		log.Printf("  Inserted: id=%s latency=%dµs", insertResp.InsertedId, insertResp.LatencyUs)
	}

	// Demo 2: Unary QueryDocuments
	log.Println("")
	log.Println("=== Demo 2: Unary QueryDocuments ===")

	filter, _ := bson.Marshal(bson.M{"_id": "grpc_test_001"})

	queryResp, err := client.QueryDocuments(ctx, &pb.QueryRequest{
		Database:   database,
		Collection: collection,
		Filter:     filter,
		Limit:      10,
	})
	if err != nil {
		log.Printf("  [ERROR] QueryDocuments: %v", err)
	} else {
		log.Printf("  Found: %d documents (total=%d) latency=%dµs",
			len(queryResp.Documents), queryResp.TotalCount, queryResp.LatencyUs)
		for _, d := range queryResp.Documents {
			log.Printf("    id=%s payload=%d bytes", d.Id, len(d.Payload))
		}
	}

	// Demo 3: Client-streaming BulkInsert
	// Each batch send picks a different backend via round-robin
	log.Println("")
	log.Println("=== Demo 3: Client-Streaming BulkInsert ===")
	log.Println("Sending 5 batches of 1,000 documents...")

	bulkStream, err := client.BulkInsert(ctx)
	if err != nil {
		log.Printf("  [ERROR] BulkInsert stream: %v", err)
	} else {
		for batch := 0; batch < 5; batch++ {
			docs := make([][]byte, 0, 1000)
			for i := 0; i < 1000; i++ {
				idx := batch*1000 + i
				d := bson.M{
					"_id":      fmt.Sprintf("bulk_%06d", idx),
					"batch":    batch,
					"index":    idx,
					"category": fmt.Sprintf("cat_%d", idx%10),
					"data":     fmt.Sprintf("payload-%d", idx),
				}
				raw, _ := bson.Marshal(d)
				docs = append(docs, raw)
			}

			if err := bulkStream.Send(&pb.BulkInsertRequest{
				Database:    database,
				Collection:  collection,
				Documents:   docs,
				BatchNumber: int32(batch + 1),
			}); err != nil {
				log.Printf("  [ERROR] send batch %d: %v", batch+1, err)
				break
			}
			log.Printf("  Sent batch %d (%d docs)", batch+1, len(docs))
		}

		bulkResp, err := bulkStream.CloseAndRecv()
		if err != nil {
			log.Printf("  [ERROR] BulkInsert response: %v", err)
		} else {
			log.Printf("  Result: %d inserted in %d batches, latency=%dµs",
				bulkResp.TotalInserted, bulkResp.BatchesReceived, bulkResp.TotalLatencyUs)
		}
	}

	// Demo 4: Bidirectional Streaming WatchUpdates
	log.Println("")
	log.Println("=== Demo 4: Bidi-Streaming WatchUpdates ===")
	log.Println("Starting change stream watcher (5 second window)...")

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()

	watchStream, err := client.WatchUpdates(watchCtx)
	if err != nil {
		log.Printf("  [ERROR] WatchUpdates stream: %v", err)
	} else {
		if err := watchStream.Send(&pb.WatchRequest{
			Database:        database,
			Collection:      collection,
			OperationFilter: pb.WatchRequest_INSERT,
		}); err != nil {
			log.Printf("  [ERROR] send watch request: %v", err)
		} else {
			log.Println("  Watch filter sent: INSERT operations only")
			log.Println("  Listening for events (5s)...")

			eventCount := 0
			for {
				event, err := watchStream.Recv()
				if err != nil {
					break
				}
				eventCount++
				log.Printf("    Event: op=%s id=%s payload=%d bytes",
					event.Operation, event.DocumentId, len(event.FullDocument))
				if eventCount >= 10 {
					break
				}
			}
			log.Printf("  Received %d events", eventCount)
		}
	}

	// Demo 5: Parallel RPCs to demonstrate round-robin distribution
	log.Println("")
	log.Println("=== Demo 5: Parallel RPCs (Round-Robin Distribution) ===")
	log.Println("Sending 20 InsertDocument RPCs — each hits a different backend pod")

	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("lb_test_%03d", i)
		d := bson.M{"_id": id, "seq": i, "purpose": "load_balance_demo"}
		raw, _ := bson.Marshal(d)

		resp, err := client.InsertDocument(ctx, &pb.InsertRequest{
			Document: &pb.Document{
				Id: id, Database: database, Collection: collection, Payload: raw,
			},
		})
		if err != nil {
			log.Printf("  [%02d] ERROR: %v", i, err)
		} else {
			log.Printf("  [%02d] id=%s latency=%dµs", i, resp.InsertedId, resp.LatencyUs)
		}
	}

	log.Println("")
	log.Println("gRPC client demo complete")
	os.Exit(0)
}

// Legacy GRPCPool has been replaced by client-side load balancing.
// The round-robin balancer + DNS/static resolver distributes RPCs across
// all backend pods automatically, without maintaining separate connections manually.
// See internal/loadbalancer/ for implementation.
