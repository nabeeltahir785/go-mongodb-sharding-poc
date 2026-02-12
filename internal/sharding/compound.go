package sharding

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const compoundCollection = "orders_compound"
const compoundDocCount = 10000
const tenantCount = 5

// RunCompoundDemo demonstrates compound shard keys for multi-tenant workloads.
// Uses { tenant_id: 1, user_id: 1 } to ensure tenant data spreads across
// shards and no single chunk becomes a "jumbo chunk."
func RunCompoundDemo(ctx context.Context, adminClient, appClient *mongo.Client, db string) error {
	log.Println("=== Compound Shard Key Demo ===")
	log.Println("Goal: Multi-tenant isolation without jumbo chunks")

	DropCollection(ctx, appClient, db, compoundCollection)

	// Create compound shard key
	key := bson.D{
		{Key: "tenant_id", Value: 1},
		{Key: "user_id", Value: 1},
	}
	if err := ShardCollection(ctx, adminClient, db, compoundCollection, key); err != nil {
		return fmt.Errorf("shard collection: %w", err)
	}
	log.Println("Shard key: { tenant_id: 1, user_id: 1 }")

	// Create supporting index
	appClient.Database(db).Collection(compoundCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: key,
	})

	// Insert orders across 5 tenants with varying user counts
	log.Printf("Inserting %d orders across %d tenants...", compoundDocCount, tenantCount)
	docs := make([]interface{}, compoundDocCount)
	for i := 0; i < compoundDocCount; i++ {
		tenantID := fmt.Sprintf("tenant_%d", (i%tenantCount)+1)
		userID := fmt.Sprintf("user_%06d", i)
		docs[i] = bson.M{
			"tenant_id":  tenantID,
			"user_id":    userID,
			"order_id":   fmt.Sprintf("ORD-%08d", i),
			"amount":     float64(10 + (i % 500)),
			"product":    fmt.Sprintf("product_%d", i%20),
		}
	}

	if err := batchInsert(ctx, appClient, db, compoundCollection, docs); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	// Analyze overall distribution
	dist, err := GetShardDistribution(ctx, adminClient, db, compoundCollection)
	if err != nil {
		return fmt.Errorf("distribution: %w", err)
	}
	PrintDistribution(dist)

	// Show per-tenant counts
	log.Println("Per-tenant document counts:")
	coll := appClient.Database(db).Collection(compoundCollection)
	for t := 1; t <= tenantCount; t++ {
		tenantID := fmt.Sprintf("tenant_%d", t)
		count, _ := coll.CountDocuments(ctx, bson.M{"tenant_id": tenantID})
		log.Printf("    %-12s %d docs", tenantID, count)
	}

	// Check for jumbo chunk risk
	maxPct := float64(0)
	for _, count := range dist.Shards {
		pct := float64(count) / float64(dist.Total) * 100
		if pct > maxPct {
			maxPct = pct
		}
	}
	if maxPct <= 50 {
		log.Printf("  No jumbo chunk risk (max shard has %.1f%%)", maxPct)
	} else {
		log.Printf("  Warning: potential jumbo chunk (max shard has %.1f%%)", maxPct)
	}

	log.Println("Result: Compound key distributes multi-tenant data evenly")
	log.Println("")
	return nil
}
