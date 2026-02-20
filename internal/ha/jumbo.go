package ha

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const jumboCollection = "jumbo_analysis"
const jumboDocCount = 30000

// RunJumboChunkAnalysis demonstrates how low-cardinality shard keys create
// unmovable "jumbo" chunks and provides diagnostic analysis.
func RunJumboChunkAnalysis(ctx context.Context, adminClient, appClient *mongo.Client, db string) error {
	log.Println("=== Jumbo Chunk Analysis ===")
	log.Println("Goal: Identify unmovable chunks caused by low-cardinality shard keys")
	log.Println("")

	// Drop and create collection with low-cardinality shard key
	appClient.Database(db).Collection(jumboCollection).Drop(ctx)

	shardKey := bson.D{{Key: "status", Value: 1}}
	appClient.Database(db).Collection(jumboCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: shardKey,
	})

	ns := db + "." + jumboCollection
	var shardResult bson.M
	if err := adminClient.Database("admin").RunCommand(ctx, bson.D{
		{Key: "shardCollection", Value: ns},
		{Key: "key", Value: shardKey},
	}).Decode(&shardResult); err != nil {
		return fmt.Errorf("shard collection: %w", err)
	}
	log.Println("Shard key: { status: 1 } — ONLY 3 possible values (low cardinality)")
	log.Println("  This is a deliberately bad shard key to demonstrate jumbo chunks")

	// Insert data with only 3 status values
	log.Println("")
	log.Printf("Inserting %d documents with only 3 status values...", jumboDocCount)
	statuses := []string{"active", "inactive", "pending"}
	coll := appClient.Database(db).Collection(jumboCollection)
	batchSize := 1000

	for i := 0; i < jumboDocCount; i += batchSize {
		end := i + batchSize
		if end > jumboDocCount {
			end = jumboDocCount
		}
		docs := make([]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			docs = append(docs, bson.M{
				"status":  statuses[j%3],
				"user_id": fmt.Sprintf("user_%08d", j),
				"email":   fmt.Sprintf("user%d@example.com", j),
				"data":    fmt.Sprintf("payload-%d-%s", j, strings.Repeat("x", 100)),
			})
		}
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("insert at %d: %w", i, err)
		}
	}
	log.Printf("  [OK] %d documents inserted", jumboDocCount)

	// Per-status distribution
	log.Println("")
	log.Println("Document distribution by status:")
	for _, s := range statuses {
		count, _ := coll.CountDocuments(ctx, bson.M{"status": s})
		log.Printf("    %-10s %d docs", s, count)
	}

	// Analyze chunks
	log.Println("")
	log.Println("Chunk analysis:")
	chunks, err := getChunksForNamespace(ctx, adminClient, ns)
	if err != nil {
		log.Printf("  [WARN] chunk query: %v", err)
	} else {
		log.Printf("  Total chunks: %d", len(chunks))
		for i, chunk := range chunks {
			log.Printf("    Chunk %d: shard=%s min=%v max=%v",
				i+1, chunk.Shard, formatBound(chunk.Min), formatBound(chunk.Max))
		}
	}

	// Attempt to move a chunk to prove it fails (jumbo)
	log.Println("")
	log.Println("Attempting moveChunk to prove jumbo chunk restriction...")
	if len(chunks) > 0 {
		// Find which shards have chunks
		sourceShard := chunks[0].Shard
		targetShard := findDifferentShard(ctx, adminClient, sourceShard)

		if targetShard != "" {
			moveErr := attemptMoveChunk(ctx, adminClient, ns, chunks[0].Min, targetShard)
			if moveErr != nil {
				log.Printf("  [EXPECTED] moveChunk failed: %v", moveErr)
				log.Println("  Jumbo chunks cannot be moved because the shard key range")
				log.Println("  contains too many documents with the same key value")
			} else {
				log.Println("  [OK] moveChunk succeeded (chunk was small enough)")
			}
		} else {
			log.Println("  [SKIP] Could not identify target shard")
		}
	}

	// Diagnostic report
	log.Println("")
	log.Println("JUMBO CHUNK DIAGNOSTIC REPORT")
	log.Println("")
	log.Println("  Problem: Low-cardinality shard key { status: 1 }")
	log.Printf("  Cardinality: %d unique values for %d documents", len(statuses), jumboDocCount)
	log.Printf("  Ratio: %.0f docs per unique key value", float64(jumboDocCount)/float64(len(statuses)))
	log.Println("")
	log.Println("  Why this is bad:")
	log.Println("    - MongoDB cannot split a chunk below the shard key granularity")
	log.Println("    - With only 3 values, maximum 3 chunks can exist")
	log.Println("    - Each chunk contains ~10,000 docs (far above normal)")
	log.Println("    - These chunks become 'jumbo' and cannot be migrated")
	log.Println("")
	log.Println("  Recommendations:")
	log.Println("    1. Use high-cardinality shard keys (e.g., user_id, _id)")
	log.Println("    2. Use compound keys: { status: 1, user_id: 1 }")
	log.Println("    3. Use hashed sharding for monotonic keys")
	log.Println("    4. Ensure cardinality >> number of shards")

	log.Println("")
	log.Println("Result: Jumbo chunk behavior analyzed and diagnosed")
	log.Println("")
	return nil
}

// chunkDoc represents a chunk from config.chunks.
type chunkDoc struct {
	Shard string
	Min   bson.D
	Max   bson.D
}

// getChunksForNamespace queries config.chunks for a namespace.
func getChunksForNamespace(ctx context.Context, client *mongo.Client, ns string) ([]chunkDoc, error) {
	// Try by namespace first
	chunks, err := queryChunks(ctx, client, bson.M{"ns": ns})
	if err == nil && len(chunks) > 0 {
		return chunks, nil
	}

	// Fallback: lookup by UUID (MongoDB 7.0+)
	var collDoc bson.M
	err = client.Database("config").Collection("collections").FindOne(ctx, bson.M{"_id": ns}).Decode(&collDoc)
	if err != nil {
		return nil, fmt.Errorf("lookup collection: %w", err)
	}

	uuid, ok := collDoc["uuid"]
	if !ok {
		return nil, fmt.Errorf("no uuid for %s", ns)
	}

	return queryChunks(ctx, client, bson.M{"uuid": uuid})
}

// queryChunks runs a find on config.chunks with the given filter.
func queryChunks(ctx context.Context, client *mongo.Client, filter bson.M) ([]chunkDoc, error) {
	cursor, err := client.Database("config").Collection("chunks").Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var chunks []chunkDoc
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		chunk := chunkDoc{}
		if s, ok := doc["shard"].(string); ok {
			chunk.Shard = s
		}
		if m, ok := doc["min"].(bson.D); ok {
			chunk.Min = m
		}
		if m, ok := doc["max"].(bson.D); ok {
			chunk.Max = m
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// formatBound formats a chunk boundary for display.
func formatBound(bound bson.D) string {
	if len(bound) == 0 {
		return "{}"
	}
	parts := make([]string, 0, len(bound))
	for _, elem := range bound {
		parts = append(parts, fmt.Sprintf("%s: %v", elem.Key, elem.Value))
	}
	return "{ " + strings.Join(parts, ", ") + " }"
}

// findDifferentShard returns a shard name that differs from the given one.
func findDifferentShard(ctx context.Context, client *mongo.Client, excludeShard string) string {
	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "listShards", Value: 1},
	}).Decode(&result); err != nil {
		return ""
	}

	if shards, ok := result["shards"].(bson.A); ok {
		for _, s := range shards {
			if m, ok := s.(bson.M); ok {
				if id, ok := m["_id"].(string); ok && id != excludeShard {
					return id
				}
			}
		}
	}
	return ""
}

// attemptMoveChunk tries to move a chunk to the target shard.
func attemptMoveChunk(ctx context.Context, client *mongo.Client, ns string, min bson.D, toShard string) error {
	cmd := bson.D{
		{Key: "moveChunk", Value: ns},
		{Key: "find", Value: min},
		{Key: "to", Value: toShard},
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, cmd).Decode(&result); err != nil {
		return err
	}
	return nil
}
