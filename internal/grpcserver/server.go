package grpcserver

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "go-mongodb-sharding-poc/proto/sharding/v1"
)

// Server implements the ShardingService gRPC server.
type Server struct {
	pb.UnimplementedShardingServiceServer
	client *mongo.Client
}

// NewServer creates a new gRPC server backed by the given MongoDB client.
func NewServer(client *mongo.Client) *Server {
	return &Server{client: client}
}

// InsertDocument handles single document insertion (unary RPC).
func (s *Server) InsertDocument(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	start := time.Now()

	if req.Document == nil {
		return nil, status.Error(codes.InvalidArgument, "document required")
	}

	doc, err := ProtoDocumentToBSON(req.Document)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid document: %v", err)
	}

	db := req.Document.Database
	coll := req.Document.Collection
	if db == "" || coll == "" {
		return nil, status.Error(codes.InvalidArgument, "database and collection required")
	}

	result, err := s.client.Database(db).Collection(coll).InsertOne(ctx, doc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert: %v", err)
	}

	insertedID := fmt.Sprintf("%v", result.InsertedID)
	log.Printf("gRPC InsertDocument: %s.%s id=%s latency=%dµs", db, coll, insertedID, MicrosecondsSince(start))

	return &pb.InsertResponse{
		InsertedId: insertedID,
		LatencyUs:  MicrosecondsSince(start),
	}, nil
}

// QueryDocuments handles document queries (unary RPC).
func (s *Server) QueryDocuments(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	start := time.Now()

	if req.Database == "" || req.Collection == "" {
		return nil, status.Error(codes.InvalidArgument, "database and collection required")
	}

	filter, err := BSONFilterFromBytes(req.Filter)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid filter: %v", err)
	}

	findOpts := options.Find()
	if req.Limit > 0 {
		findOpts.SetLimit(int64(req.Limit))
	}
	if req.Skip > 0 {
		findOpts.SetSkip(int64(req.Skip))
	}

	coll := s.client.Database(req.Database).Collection(req.Collection)

	cursor, err := coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "find: %v", err)
	}
	defer cursor.Close(ctx)

	var documents []*pb.Document
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		protoDoc, err := BSONToProtoDocument(doc, req.Collection, req.Database)
		if err != nil {
			continue
		}
		documents = append(documents, protoDoc)
	}

	totalCount, _ := coll.CountDocuments(ctx, filter)

	log.Printf("gRPC QueryDocuments: %s.%s returned=%d total=%d latency=%dµs",
		req.Database, req.Collection, len(documents), totalCount, MicrosecondsSince(start))

	return &pb.QueryResponse{
		Documents:  documents,
		TotalCount: totalCount,
		LatencyUs:  MicrosecondsSince(start),
	}, nil
}

// BulkInsert handles client-streaming bulk document insertion.
// Uses bson.Raw zero-copy path: gRPC bytes → bson.Raw → InsertMany.
// This skips deserialization to bson.M, eliminating allocation overhead.
func (s *Server) BulkInsert(stream grpc.ClientStreamingServer[pb.BulkInsertRequest, pb.BulkInsertResponse]) error {
	start := time.Now()
	var totalInserted int64
	var batchesReceived int32
	perShard := make(map[string]int64)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv: %v", err)
		}

		if req.Database == "" || req.Collection == "" {
			return status.Error(codes.InvalidArgument, "database and collection required")
		}

		// Zero-copy: wrap raw BSON bytes directly as bson.Raw
		// Avoids bson.Unmarshal → bson.M → InsertMany marshal cycle
		docs := make([]interface{}, 0, len(req.Documents))
		for _, raw := range req.Documents {
			docs = append(docs, bson.Raw(raw))
		}

		if len(docs) == 0 {
			continue
		}

		// Unordered bulk insert: allows MongoDB to process shards in parallel
		// without waiting for the previous write to finish
		result, err := s.client.Database(req.Database).Collection(req.Collection).InsertMany(
			stream.Context(), docs, options.InsertMany().SetOrdered(false))
		if err != nil {
			log.Printf("gRPC BulkInsert batch %d: %v", req.BatchNumber, err)
		}

		inserted := int64(len(docs))
		if result != nil {
			inserted = int64(len(result.InsertedIDs))
		}

		totalInserted += inserted
		batchesReceived++

		log.Printf("gRPC BulkInsert batch %d: %d docs (zero-copy)", req.BatchNumber, inserted)
	}

	log.Printf("gRPC BulkInsert complete: %d docs in %d batches, latency=%dµs",
		totalInserted, batchesReceived, MicrosecondsSince(start))

	return stream.SendAndClose(&pb.BulkInsertResponse{
		TotalInserted:   totalInserted,
		BatchesReceived: batchesReceived,
		TotalLatencyUs:  MicrosecondsSince(start),
		PerShardCount:   perShard,
	})
}

// WatchUpdates handles bidirectional streaming for real-time change events.
// Client sends watch filters; server streams matching MongoDB change stream events.
func (s *Server) WatchUpdates(stream grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchEvent]) error {
	// Receive the initial watch request
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "recv watch request: %v", err)
	}

	if req.Database == "" || req.Collection == "" {
		return status.Error(codes.InvalidArgument, "database and collection required")
	}

	// Build change stream pipeline
	pipeline := mongo.Pipeline{}
	if req.OperationFilter != pb.WatchRequest_ALL {
		opType := operationTypeString(req.OperationFilter)
		if opType != "" {
			pipeline = append(pipeline, bson.D{
				{Key: "$match", Value: bson.D{
					{Key: "operationType", Value: opType},
				}},
			})
		}
	}

	// Open change stream
	coll := s.client.Database(req.Database).Collection(req.Collection)
	cs, err := coll.Watch(stream.Context(), pipeline)
	if err != nil {
		return status.Errorf(codes.Internal, "watch: %v", err)
	}
	defer cs.Close(stream.Context())

	log.Printf("gRPC WatchUpdates: streaming %s.%s (filter=%s)",
		req.Database, req.Collection, req.OperationFilter)

	// Stream change events
	for cs.Next(stream.Context()) {
		var event bson.M
		if err := cs.Decode(&event); err != nil {
			continue
		}

		watchEvent := changeEventToProto(event, req.Collection)
		if err := stream.Send(watchEvent); err != nil {
			return err
		}
	}

	return nil
}

// operationTypeString maps protobuf enum to MongoDB change stream operation type.
func operationTypeString(op pb.WatchRequest_Operation) string {
	switch op {
	case pb.WatchRequest_INSERT:
		return "insert"
	case pb.WatchRequest_UPDATE:
		return "update"
	case pb.WatchRequest_DELETE:
		return "delete"
	case pb.WatchRequest_REPLACE:
		return "replace"
	default:
		return ""
	}
}

// changeEventToProto converts a MongoDB change stream event to a protobuf WatchEvent.
func changeEventToProto(event bson.M, collection string) *pb.WatchEvent {
	we := &pb.WatchEvent{
		Collection: collection,
	}

	if op, ok := event["operationType"].(string); ok {
		we.Operation = op
	}

	if docKey, ok := event["documentKey"].(bson.M); ok {
		if id, ok := docKey["_id"]; ok {
			we.DocumentId = fmt.Sprintf("%v", id)
		}
	}

	if fullDoc, ok := event["fullDocument"].(bson.M); ok {
		if raw, err := bson.Marshal(fullDoc); err == nil {
			we.FullDocument = raw
		}
	}

	we.TimestampMs = time.Now().UnixMilli()

	return we
}
