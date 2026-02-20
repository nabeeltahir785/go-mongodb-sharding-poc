package operations

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const hedgedCollection = "hedged_reads_test"
const hedgedQueryCount = 20

// RunHedgedReadsLab demonstrates hedged reads for latency reduction.
// Compares query latencies with standard reads vs hedged reads.
func RunHedgedReadsLab(ctx context.Context, host, user, password, db string) error {
	log.Println("=== Hedged Reads Lab ===")
	log.Println("Goal: Reduce read latency by querying multiple replicas")
	log.Println("")

	// Standard client (no hedging)
	standardURI := fmt.Sprintf("mongodb://%s:%s@%s/?authSource=admin", user, password, host)
	standardClient, err := mongo.Connect(ctx, options.Client().
		ApplyURI(standardURI).
		SetTimeout(30*time.Second).
		SetReadPreference(readpref.Nearest()))
	if err != nil {
		return fmt.Errorf("connect standard: %w", err)
	}
	defer standardClient.Disconnect(ctx)

	// Hedged client (sends reads to multiple replicas)
	hedgedPref, err := readpref.New(readpref.NearestMode, readpref.WithHedgeEnabled(true))
	if err != nil {
		return fmt.Errorf("create hedged readpref: %w", err)
	}

	hedgedClient, err := mongo.Connect(ctx, options.Client().
		ApplyURI(standardURI).
		SetTimeout(30*time.Second).
		SetReadPreference(hedgedPref))
	if err != nil {
		return fmt.Errorf("connect hedged: %w", err)
	}
	defer hedgedClient.Disconnect(ctx)

	// Seed test data
	log.Println("Seeding test data...")
	coll := standardClient.Database(db).Collection(hedgedCollection)
	coll.Drop(ctx)

	docs := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		docs[i] = bson.M{
			"_id":       fmt.Sprintf("doc_%04d", i),
			"value":     i,
			"category":  fmt.Sprintf("cat_%d", i%10),
			"timestamp": time.Now().UTC(),
			"payload":   fmt.Sprintf("test-data-%d", i),
		}
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("seed data: %w", err)
	}
	log.Println("  [OK] 1,000 test documents inserted")

	// Benchmark standard reads
	log.Println("")
	log.Println("Running standard reads (nearest, no hedging)...")
	standardLatencies := benchmarkReads(ctx, standardClient, db, hedgedCollection)
	standardAvg := avgDuration(standardLatencies)
	log.Printf("  %d queries, avg latency: %v", len(standardLatencies), standardAvg)

	// Benchmark hedged reads
	log.Println("")
	log.Println("Running hedged reads (nearest, hedging enabled)...")
	hedgedLatencies := benchmarkReads(ctx, hedgedClient, db, hedgedCollection)
	hedgedAvg := avgDuration(hedgedLatencies)
	log.Printf("  %d queries, avg latency: %v", len(hedgedLatencies), hedgedAvg)

	// Report
	log.Println("")
	log.Println("HEDGED READS COMPARISON")
	log.Printf("  Standard avg:  %v", standardAvg)
	log.Printf("  Hedged avg:    %v", hedgedAvg)

	if hedgedAvg < standardAvg {
		improvement := float64(standardAvg-hedgedAvg) / float64(standardAvg) * 100
		log.Printf("  Improvement:   %.1f%% faster with hedged reads", improvement)
	} else {
		log.Println("  Note: Hedged reads overhead may exceed benefit on local/low-latency clusters")
		log.Println("  In production with network jitter, hedged reads reduce tail latency (p99)")
	}

	// Explain hedged reads behavior
	log.Println("")
	log.Println("How hedged reads work:")
	log.Println("  1. mongos sends the read to the preferred replica")
	log.Println("  2. After a short delay, it sends the same read to another replica")
	log.Println("  3. The first response to arrive is used, the other is discarded")
	log.Println("  4. This reduces tail latency (p95/p99) in production environments")

	// Cleanup
	coll.Drop(ctx)

	log.Println("")
	log.Println("Result: Hedged reads configured and benchmarked")
	log.Println("")
	return nil
}

// benchmarkReads runs a series of find queries and returns per-query latencies.
func benchmarkReads(ctx context.Context, client *mongo.Client, db, collection string) []time.Duration {
	coll := client.Database(db).Collection(collection)
	latencies := make([]time.Duration, 0, hedgedQueryCount)

	for i := 0; i < hedgedQueryCount; i++ {
		filter := bson.M{"category": fmt.Sprintf("cat_%d", i%10)}

		start := time.Now()
		cursor, err := coll.Find(ctx, filter)
		if err != nil {
			log.Printf("    query %d error: %v", i, err)
			continue
		}
		// Drain cursor to measure full read
		var results []bson.M
		cursor.All(ctx, &results)
		elapsed := time.Since(start)

		latencies = append(latencies, elapsed)
	}

	return latencies
}

// avgDuration computes the average of a duration slice.
func avgDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	total := time.Duration(0)
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}
