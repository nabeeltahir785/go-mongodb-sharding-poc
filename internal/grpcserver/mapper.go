package grpcserver

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	pb "go-mongodb-sharding-poc/proto/sharding/v1"
)

// BSONToProtoDocument converts a BSON document to a Protobuf Document.
// Uses bson.Marshal for the payload (zero-reflection, byte-level serialization).
func BSONToProtoDocument(doc bson.M, collection, database string) (*pb.Document, error) {
	// Extract _id
	id := ""
	if oid, ok := doc["_id"].(primitive.ObjectID); ok {
		id = oid.Hex()
	} else if sid, ok := doc["_id"].(string); ok {
		id = sid
	} else if doc["_id"] != nil {
		id = fmt.Sprintf("%v", doc["_id"])
	}

	// Marshal full document to BSON bytes (avoids UTF-8 encoding overhead)
	payload, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal bson: %w", err)
	}

	return &pb.Document{
		Id:         id,
		Collection: collection,
		Database:   database,
		Payload:    payload,
	}, nil
}

// ProtoDocumentToBSON converts a Protobuf Document back to a BSON document.
// Direct unmarshal from bytes — no reflection.
func ProtoDocumentToBSON(doc *pb.Document) (bson.M, error) {
	if len(doc.Payload) > 0 {
		var result bson.M
		if err := bson.Unmarshal(doc.Payload, &result); err != nil {
			return nil, fmt.Errorf("unmarshal bson payload: %w", err)
		}
		return result, nil
	}

	// Fallback: construct from metadata
	result := bson.M{}
	if doc.Id != "" {
		result["_id"] = doc.Id
	}
	for k, v := range doc.Metadata {
		result[k] = v
	}
	return result, nil
}

// RawBSONToProtoPayload wraps raw BSON bytes for protobuf transport.
// This is the fastest path — zero conversion.
func RawBSONToProtoPayload(raw []byte) []byte {
	return raw
}

// RawToInsertable wraps raw BSON bytes as bson.Raw for direct MongoDB insertion.
// This enables the zero-copy flow: gRPC bytes → bson.Raw → InsertMany
// without deserializing to bson.M intermediary maps.
func RawToInsertable(raw []byte) bson.Raw {
	return bson.Raw(raw)
}

// BSONFilterFromBytes deserializes a protobuf bytes field to a BSON filter.
func BSONFilterFromBytes(data []byte) (bson.M, error) {
	if len(data) == 0 {
		return bson.M{}, nil
	}
	var filter bson.M
	if err := bson.Unmarshal(data, &filter); err != nil {
		return nil, fmt.Errorf("unmarshal filter: %w", err)
	}
	return filter, nil
}

// BSONFilterToBytes serializes a BSON filter to bytes for protobuf transport.
func BSONFilterToBytes(filter bson.M) ([]byte, error) {
	if filter == nil {
		return nil, nil
	}
	return bson.Marshal(filter)
}

// MicrosecondsSince returns microseconds elapsed since the given time.
// Used for latency reporting in gRPC responses.
func MicrosecondsSince(start time.Time) int64 {
	return time.Since(start).Microseconds()
}
