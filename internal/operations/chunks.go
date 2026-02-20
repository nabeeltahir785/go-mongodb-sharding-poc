package operations

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const chunkLabCollection = "chunk_lab"
const jumboDocCount = 50000

// ChunkInfo holds chunk details for a collection.
type ChunkInfo struct {
	Namespace  string
	TotalCount int64
	PerShard   map[string]int64
}

// RunChunkLab demonstrates chunk monitoring, jumbo chunk simulation, and manual split.
func RunChunkLab(ctx context.Context, adminClient, appClient *mongo.Client, db string) error {
	log.Println("=== Chunk Management Lab ===")
	log.Println("Goal: Monitor chunks, simulate jumbo chunk, manual split")
	log.Println("")

	// Drop and recreate collection with ranged sharding on category
	appClient.Database(db).Collection(chunkLabCollection).Drop(ctx)

	shardKey := bson.D{{Key: "category", Value: 1}, {Key: "item_id", Value: 1}}
	appClient.Database(db).Collection(chunkLabCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: shardKey,
	})

	ns := db + "." + chunkLabCollection
	cmd := bson.D{
		{Key: "shardCollection", Value: ns},
		{Key: "key", Value: shardKey},
	}
	var shardResult bson.M
	if err := adminClient.Database("admin").RunCommand(ctx, cmd).Decode(&shardResult); err != nil {
		return fmt.Errorf("shard collection: %w", err)
	}
	log.Printf("Sharded collection: %s { category: 1, item_id: 1 }", ns)

	// Show initial chunk state
	log.Println("")
	log.Println("Initial chunk state:")
	info, err := GetChunkInfo(ctx, adminClient, ns)
	if err != nil {
		log.Printf("  [WARN] chunk info: %v", err)
	} else {
		PrintChunkReport(info)
	}

	// Simulate jumbo chunk: insert 50K docs with identical category to create hotspot
	log.Println("")
	log.Printf("Simulating jumbo chunk: inserting %d docs with category='hotspot'...", jumboDocCount)
	coll := appClient.Database(db).Collection(chunkLabCollection)
	batchSize := 1000

	for i := 0; i < jumboDocCount; i += batchSize {
		end := i + batchSize
		if end > jumboDocCount {
			end = jumboDocCount
		}
		docs := make([]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			docs = append(docs, bson.M{
				"category": "hotspot",
				"item_id":  fmt.Sprintf("ITEM-%08d", j),
				"data":     fmt.Sprintf("payload-%d-padding-to-increase-document-size-%s", j, strings.Repeat("x", 200)),
			})
		}
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("bulk insert at %d: %w", i, err)
		}
	}
	log.Printf("  [OK] Inserted %d documents into category='hotspot'", jumboDocCount)

	// Also insert some distributed data for contrast
	log.Println("Inserting 5,000 distributed docs across 10 categories...")
	for i := 0; i < 5000; i += batchSize {
		end := i + batchSize
		if end > 5000 {
			end = 5000
		}
		docs := make([]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			docs = append(docs, bson.M{
				"category": fmt.Sprintf("cat_%02d", j%10),
				"item_id":  fmt.Sprintf("DIST-%08d", j),
				"data":     fmt.Sprintf("distributed-payload-%d", j),
			})
		}
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("distributed insert at %d: %w", i, err)
		}
	}
	log.Println("  [OK] Distributed documents inserted")

	// Show chunk state after heavy insert
	log.Println("")
	log.Println("Chunk state after jumbo simulation:")
	info, err = GetChunkInfo(ctx, adminClient, ns)
	if err != nil {
		log.Printf("  [WARN] chunk info: %v", err)
	} else {
		PrintChunkReport(info)
	}

	// Attempt manual split on the hotspot chunk
	log.Println("")
	log.Println("Attempting manual split on hotspot chunk...")
	splitPoint := bson.D{
		{Key: "category", Value: "hotspot"},
		{Key: "item_id", Value: fmt.Sprintf("ITEM-%08d", jumboDocCount/2)},
	}
	if err := ManualSplitChunk(ctx, adminClient, ns, splitPoint); err != nil {
		log.Printf("  [WARN] Manual split: %v", err)
		log.Println("  This can happen if the chunk was already auto-split by MongoDB")
	} else {
		log.Println("  [OK] Manual split succeeded")
	}

	// Show final chunk state
	log.Println("")
	log.Println("Chunk state after manual split:")
	info, err = GetChunkInfo(ctx, adminClient, ns)
	if err != nil {
		log.Printf("  [WARN] chunk info: %v", err)
	} else {
		PrintChunkReport(info)
	}

	log.Println("")
	log.Println("Result: Demonstrated chunk monitoring, jumbo simulation, and manual split")
	log.Println("")
	return nil
}

// GetChunkInfo queries config.chunks to get chunk distribution for a namespace.
func GetChunkInfo(ctx context.Context, client *mongo.Client, ns string) (*ChunkInfo, error) {
	info := &ChunkInfo{
		Namespace: ns,
		PerShard:  make(map[string]int64),
	}

	// Aggregate chunks per shard
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "ns", Value: ns}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$shard"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}

	cursor, err := client.Database("config").Collection("chunks").Aggregate(ctx, pipeline)
	if err != nil {
		// Try uuid-based namespace lookup (MongoDB 7.0+)
		return getChunkInfoByUUID(ctx, client, ns)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		shard := ""
		if v, ok := doc["_id"].(string); ok {
			shard = v
		}
		count := int64(0)
		switch v := doc["count"].(type) {
		case int32:
			count = int64(v)
		case int64:
			count = v
		case float64:
			count = int64(v)
		}
		if shard != "" {
			info.PerShard[shard] = count
			info.TotalCount += count
		}
	}

	if info.TotalCount == 0 {
		return getChunkInfoByUUID(ctx, client, ns)
	}

	return info, nil
}

// getChunkInfoByUUID handles MongoDB 7.0+ where chunks use uuid instead of ns.
func getChunkInfoByUUID(ctx context.Context, client *mongo.Client, ns string) (*ChunkInfo, error) {
	info := &ChunkInfo{
		Namespace: ns,
		PerShard:  make(map[string]int64),
	}

	// Look up the collection UUID from config.collections
	var collDoc bson.M
	err := client.Database("config").Collection("collections").FindOne(ctx, bson.M{"_id": ns}).Decode(&collDoc)
	if err != nil {
		return info, fmt.Errorf("lookup collection uuid: %w", err)
	}

	uuid, ok := collDoc["uuid"]
	if !ok {
		return info, fmt.Errorf("no uuid for %s", ns)
	}

	// Query chunks by uuid
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "uuid", Value: uuid}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$shard"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}

	cursor, err := client.Database("config").Collection("chunks").Aggregate(ctx, pipeline)
	if err != nil {
		return info, fmt.Errorf("aggregate chunks by uuid: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		shard := ""
		if v, ok := doc["_id"].(string); ok {
			shard = v
		}
		count := int64(0)
		switch v := doc["count"].(type) {
		case int32:
			count = int64(v)
		case int64:
			count = v
		case float64:
			count = int64(v)
		}
		if shard != "" {
			info.PerShard[shard] = count
			info.TotalCount += count
		}
	}

	return info, nil
}

// PrintChunkReport logs a formatted chunk distribution report.
func PrintChunkReport(info *ChunkInfo) {
	log.Printf("  Namespace: %s", info.Namespace)
	log.Printf("  Total chunks: %d", info.TotalCount)
	for shard, count := range info.PerShard {
		pct := float64(0)
		if info.TotalCount > 0 {
			pct = float64(count) / float64(info.TotalCount) * 100
		}
		log.Printf("    %-12s %3d chunks (%.1f%%)", shard, count, pct)
	}
}

// ManualSplitChunk splits a chunk at the given point.
func ManualSplitChunk(ctx context.Context, client *mongo.Client, ns string, splitPoint bson.D) error {
	cmd := bson.D{
		{Key: "split", Value: ns},
		{Key: "middle", Value: splitPoint},
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, cmd).Decode(&result); err != nil {
		return fmt.Errorf("split %s: %w", ns, err)
	}
	return nil
}
