package sharding

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const hashedCollection = "users_hashed"
const hashedDocCount = 10000

// RunHashedDemo demonstrates hashed sharding for even write distribution.
// Uses sequential _id values to show that hashing eliminates hotspots
// on monotonically increasing keys.
func RunHashedDemo(ctx context.Context, adminClient, appClient *mongo.Client, db string) error {
	log.Println("=== Hashed Sharding Demo ===")
	log.Println("Goal: Even write distribution despite monotonic _id")

	DropCollection(ctx, appClient, db, hashedCollection)

	// Create hashed shard key on _id
	if err := ShardCollectionHashed(ctx, adminClient, db, hashedCollection, "_id"); err != nil {
		return fmt.Errorf("shard collection: %w", err)
	}
	log.Println("Shard key: { _id: 'hashed' }")

	// Insert documents with sequential IDs
	log.Printf("Inserting %d documents with sequential IDs...", hashedDocCount)
	docs := make([]interface{}, hashedDocCount)
	for i := 0; i < hashedDocCount; i++ {
		docs[i] = bson.M{
			"_id":      fmt.Sprintf("user_%06d", i),
			"username": fmt.Sprintf("user%d", i),
			"email":    fmt.Sprintf("user%d@example.com", i),
			"age":      20 + (i % 50),
		}
	}

	if err := batchInsert(ctx, appClient, db, hashedCollection, docs); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	// Analyze distribution
	dist, err := GetShardDistribution(ctx, adminClient, db, hashedCollection)
	if err != nil {
		return fmt.Errorf("distribution: %w", err)
	}

	PrintDistribution(dist)
	log.Println("Result: Documents are evenly spread despite sequential keys")
	log.Println("")
	return nil
}

// batchInsert inserts documents in batches of 1000.
func batchInsert(ctx context.Context, client *mongo.Client, db, coll string, docs []interface{}) error {
	collection := client.Database(db).Collection(coll)
	batchSize := 1000

	for i := 0; i < len(docs); i += batchSize {
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		if _, err := collection.InsertMany(ctx, docs[i:end]); err != nil {
			return err
		}
	}
	return nil
}
