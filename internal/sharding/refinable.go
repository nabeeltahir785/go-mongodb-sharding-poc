package sharding

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const refinableCollection = "products_refinable"
const refinableDocCount = 5000
const categoryCount = 10

// RunRefinableDemo demonstrates refining an existing shard key.
// Starts with { category: 1 }, inserts data, then refines to
// { category: 1, sku: 1 } to further subdivide chunks without resharding.
func RunRefinableDemo(ctx context.Context, adminClient, appClient *mongo.Client, db string) error {
	log.Println("=== Refinable Shard Key Demo ===")
	log.Println("Goal: Add suffix to shard key without full reshard")

	DropCollection(ctx, appClient, db, refinableCollection)

	// Start with a simple shard key
	initialKey := bson.D{{Key: "category", Value: 1}}
	if err := ShardCollection(ctx, adminClient, db, refinableCollection, initialKey); err != nil {
		return fmt.Errorf("shard collection: %w", err)
	}
	log.Println("Initial shard key: { category: 1 }")

	// Create supporting index for the refined key (must exist before refine)
	refinedKey := bson.D{
		{Key: "category", Value: 1},
		{Key: "sku", Value: 1},
	}
	appClient.Database(db).Collection(refinableCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: refinedKey,
	})

	// Insert products across categories
	log.Printf("Inserting %d products across %d categories...", refinableDocCount, categoryCount)
	categories := []string{
		"electronics", "clothing", "books", "home", "sports",
		"toys", "food", "automotive", "health", "garden",
	}

	docs := make([]interface{}, refinableDocCount)
	for i := 0; i < refinableDocCount; i++ {
		docs[i] = bson.M{
			"category": categories[i%categoryCount],
			"sku":      fmt.Sprintf("SKU-%06d", i),
			"name":     fmt.Sprintf("Product %d", i),
			"price":    float64(5 + (i % 200)),
		}
	}

	if err := batchInsert(ctx, appClient, db, refinableCollection, docs); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	// Show distribution before refinement
	log.Println("Distribution BEFORE refinement:")
	distBefore, err := GetShardDistribution(ctx, adminClient, db, refinableCollection)
	if err != nil {
		return fmt.Errorf("distribution before: %w", err)
	}
	PrintDistribution(distBefore)

	// Refine the shard key
	log.Println("Refining shard key to { category: 1, sku: 1 }...")
	if err := RefineShardKey(ctx, adminClient, db, refinableCollection, refinedKey); err != nil {
		return fmt.Errorf("refine key: %w", err)
	}
	log.Println("Shard key refined successfully")

	// Show distribution after refinement
	log.Println("Distribution AFTER refinement:")
	distAfter, err := GetShardDistribution(ctx, adminClient, db, refinableCollection)
	if err != nil {
		return fmt.Errorf("distribution after: %w", err)
	}
	PrintDistribution(distAfter)

	log.Println("Result: Key refined without full reshard operation")
	log.Println("")
	return nil
}
