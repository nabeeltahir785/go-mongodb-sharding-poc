package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"go-mongodb-sharding-poc/internal/config"
	"go-mongodb-sharding-poc/internal/grpcserver"
	"go-mongodb-sharding-poc/internal/loadbalancer"
	pb "go-mongodb-sharding-poc/proto/sharding/v1"
)

const grpcPort = ":50051"

func main() {
	log.SetFlags(log.Ltime)

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// MongoDB connection pool monitor — logs creation/close events to detect churn
	poolMonitor := &event.PoolMonitor{
		Event: func(e *event.PoolEvent) {
			switch e.Type {
			case event.ConnectionCreated:
				log.Printf("[pool] connection created (addr=%s)", e.Address)
			case event.ConnectionClosed:
				log.Printf("[pool] connection closed (addr=%s reason=%s)", e.Address, e.Reason)
			case event.PoolReady:
				log.Printf("[pool] pool ready (addr=%s)", e.Address)
			}
		},
	}

	// Connect to both mongos routers for load distribution
	mongosAddrs := strings.Join(cfg.MongosHosts, ",")
	uri := "mongodb://" + cfg.AdminUser + ":" + cfg.AdminPassword + "@" + mongosAddrs + "/?authSource=admin"

	mongoOpts := options.Client().
		ApplyURI(uri).
		SetMinPoolSize(100).                        // Pre-warm 100 connections — eliminates latency spikes
		SetMaxPoolSize(500).                        // Headroom for traffic bursts
		SetMaxConnIdleTime(5 * time.Minute).        // Reclaim stale connections
		SetCompressors([]string{"zstd", "snappy"}). // Compress wire protocol traffic
		SetTimeout(30 * time.Second).
		SetPoolMonitor(poolMonitor)

	mongoClient, err := mongo.Connect(ctx, mongoOpts)
	if err != nil {
		log.Fatalf("MongoDB connect: %v", err)
	}
	if err := mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("MongoDB ping: %v", err)
	}
	log.Println("Connected to MongoDB sharded cluster")
	log.Printf("  mongos routers: %s", mongosAddrs)
	log.Printf("  pool: min=100 max=500 idle_timeout=5m compressors=zstd,snappy")

	// gRPC server with high-throughput options
	grpcServer := grpc.NewServer(
		// Allow thousands of concurrent RPCs over a single TCP connection
		grpc.MaxConcurrentStreams(5000),
		// 16MB max message size for large bulk payloads
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
		// Keepalive: server-side enforcement to prevent stale connections
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     5 * time.Minute,  // Close idle connections after 5m
			MaxConnectionAge:      30 * time.Minute, // Force reconnect every 30m (rebalance)
			MaxConnectionAgeGrace: 10 * time.Second, // Grace period for in-flight RPCs
			Time:                  1 * time.Minute,  // Ping clients every 60s
			Timeout:               20 * time.Second, // Wait 20s for ping response
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second, // Minimum time between client pings
			PermitWithoutStream: true,             // Allow pings even without active streams
		}),
	)

	shardingServer := grpcserver.NewServer(mongoClient)
	pb.RegisterShardingServiceServer(grpcServer, shardingServer)
	reflection.Register(grpcServer)

	// Health checking — enables client-side LB to detect unhealthy pods
	// and stop routing RPCs to them automatically
	loadbalancer.RegisterHealthServer(grpcServer)

	// Listen
	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("listen %s: %v", grpcPort, err)
	}

	log.Printf("gRPC server listening on %s", grpcPort)
	log.Println("  MaxConcurrentStreams=5000 MaxMsgSize=16MB")
	log.Println("  Keepalive: idle=5m age=30m ping=60s")
	log.Println("  Health: grpc.health.v1 registered (client-side LB support)")
	log.Println("RPCs: InsertDocument, QueryDocuments, BulkInsert, WatchUpdates")

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down gRPC server...")
		grpcServer.GracefulStop()
		mongoClient.Disconnect(context.Background())
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
