package sharding

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const rangedCollection = "events_ranged"
const rangedDocCount = 10000

// RunRangedDemo demonstrates ranged sharding for query locality.
// Uses last_login_date as the shard key so date-range queries
// target only the relevant shard instead of scatter-gathering.
func RunRangedDemo(ctx context.Context, adminClient, appClient *mongo.Client, db string) error {
	log.Println("=== Ranged Sharding Demo ===")
	log.Println("Goal: Date-range queries hit only the relevant shard")

	DropCollection(ctx, appClient, db, rangedCollection)

	// Create ranged shard key on last_login_date
	if err := ShardCollection(ctx, adminClient, db, rangedCollection, bson.D{{Key: "last_login_date", Value: 1}}); err != nil {
		return fmt.Errorf("shard collection: %w", err)
	}
	log.Println("Shard key: { last_login_date: 1 }")

	// Create index for the shard key
	appClient.Database(db).Collection(rangedCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "last_login_date", Value: 1}},
	})

	// Insert documents spread over 12 months
	log.Printf("Inserting %d events across 12 months...", rangedDocCount)
	baseDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	docs := make([]interface{}, rangedDocCount)
	for i := 0; i < rangedDocCount; i++ {
		dayOffset := i % 365
		docs[i] = bson.M{
			"last_login_date": baseDate.AddDate(0, 0, dayOffset),
			"user_id":         fmt.Sprintf("user_%06d", i),
			"event_type":      "login",
			"ip_address":      fmt.Sprintf("192.168.%d.%d", (i/256)%256, i%256),
		}
	}

	if err := batchInsert(ctx, appClient, db, rangedCollection, docs); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	// Analyze distribution
	dist, err := GetShardDistribution(ctx, adminClient, db, rangedCollection)
	if err != nil {
		return fmt.Errorf("distribution: %w", err)
	}
	PrintDistribution(dist)

	// Run a targeted date-range query
	log.Println("Running date-range query (Jan 2025 only)...")
	filter := bson.D{
		{Key: "last_login_date", Value: bson.D{
			{Key: "$gte", Value: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
			{Key: "$lt", Value: time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)},
		}},
	}

	shards, err := ExplainQuery(ctx, adminClient, db, rangedCollection, filter)
	if err != nil {
		log.Printf("  Explain: %v", err)
	} else {
		log.Printf("  Targeted shards: %v (fewer = better locality)", shards)
	}

	log.Println("Result: Range queries avoid scatter-gather")
	log.Println("")
	return nil
}
