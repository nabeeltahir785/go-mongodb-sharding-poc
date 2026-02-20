package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go-mongodb-sharding-poc/internal/config"
)

const (
	database   = "sharding_poc"
	collection = "throughput_bench"
)

func main() {
	log.SetFlags(log.Ltime)

	cfg := config.Load()
	ctx := context.Background()

	log.Println("Phase 7: Throughput & Latency Benchmark")
	log.Println("========================================")

	// Connect with production-grade pool settings
	mongosAddrs := strings.Join(cfg.MongosHosts, ",")
	uri := "mongodb://" + cfg.AdminUser + ":" + cfg.AdminPassword + "@" + mongosAddrs + "/?authSource=admin"

	mongoOpts := options.Client().
		ApplyURI(uri).
		SetMinPoolSize(100).
		SetMaxPoolSize(500).
		SetMaxConnIdleTime(5 * time.Minute).
		SetCompressors([]string{"zstd", "snappy"}).
		SetTimeout(30 * time.Second)

	client, err := mongo.Connect(ctx, mongoOpts)
	if err != nil {
		log.Fatalf("MongoDB connect: %v", err)
	}
	defer client.Disconnect(ctx)

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("MongoDB ping: %v", err)
	}

	log.Printf("Connected to %s (pool: min=100 max=500)", mongosAddrs)

	// Clean up from previous runs
	coll := client.Database(database).Collection(collection)
	coll.Drop(ctx)

	log.Println("")

	// Benchmark 1: Concurrent Bulk Insert
	runBulkInsertBenchmark(ctx, coll)

	log.Println("")

	// Benchmark 2: Mixed Read/Write
	runMixedBenchmark(ctx, coll)

	log.Println("")
	log.Println("Benchmark complete")
	os.Exit(0)
}

// runBulkInsertBenchmark tests concurrent unordered bulk inserts.
// 8 goroutines × 10 batches × 1,000 docs = 80,000 inserts.
func runBulkInsertBenchmark(ctx context.Context, coll *mongo.Collection) {
	log.Println("=== Benchmark 1: Concurrent Bulk Insert ===")
	log.Println("8 goroutines × 10 batches × 1,000 docs = 80,000 inserts")

	goroutines := 8
	batchesPerWorker := 10
	docsPerBatch := 1000

	var totalOps atomic.Int64
	var mu sync.Mutex
	var allLatencies []time.Duration

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var workerLatencies []time.Duration

			for batch := 0; batch < batchesPerWorker; batch++ {
				docs := make([]interface{}, 0, docsPerBatch)
				for i := 0; i < docsPerBatch; i++ {
					idx := workerID*batchesPerWorker*docsPerBatch + batch*docsPerBatch + i
					doc := bson.M{
						"_id":       fmt.Sprintf("bench_%08d", idx),
						"worker":    workerID,
						"batch":     batch,
						"index":     idx,
						"category":  fmt.Sprintf("cat_%d", idx%50),
						"value":     rand.Float64() * 10000,
						"timestamp": time.Now(),
						"data":      fmt.Sprintf("payload-data-for-document-%d", idx),
					}
					docs = append(docs, doc)
				}

				batchStart := time.Now()
				_, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
				batchLatency := time.Since(batchStart)
				workerLatencies = append(workerLatencies, batchLatency)

				if err != nil {
					log.Printf("  worker %d batch %d: %v", workerID, batch, err)
				}
				totalOps.Add(int64(docsPerBatch))
			}

			mu.Lock()
			allLatencies = append(allLatencies, workerLatencies...)
			mu.Unlock()
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Calculate metrics
	ops := totalOps.Load()
	opsPerSec := float64(ops) / elapsed.Seconds()
	dailyCapacity := opsPerSec * 86400

	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	p50 := allLatencies[len(allLatencies)/2]
	p95 := allLatencies[int(float64(len(allLatencies))*0.95)]
	p99 := allLatencies[int(float64(len(allLatencies))*0.99)]

	log.Println("")
	log.Println("--- Bulk Insert Results ---")
	log.Printf("  Total ops:       %d", ops)
	log.Printf("  Elapsed:         %v", elapsed.Round(time.Millisecond))
	log.Printf("  Throughput:      %.0f ops/sec", opsPerSec)
	log.Printf("  Daily capacity:  %.1fM ops/day", dailyCapacity/1_000_000)
	log.Printf("  Batch latency p50: %v", p50.Round(time.Millisecond))
	log.Printf("  Batch latency p95: %v", p95.Round(time.Millisecond))
	log.Printf("  Batch latency p99: %v", p99.Round(time.Millisecond))

	if dailyCapacity >= 30_000_000 {
		log.Println("  [PASS] Exceeds 30M ops/day target")
	} else {
		log.Printf("  [INFO] %.1fM/30M ops/day (%.0f%% of target)", dailyCapacity/1_000_000, (dailyCapacity/30_000_000)*100)
	}
}

// runMixedBenchmark tests sustained mixed reads + writes (70/30 split).
// 4 goroutines running for 10 seconds.
func runMixedBenchmark(ctx context.Context, coll *mongo.Collection) {
	log.Println("=== Benchmark 2: Mixed Read/Write (70% write, 30% read) ===")
	log.Println("4 goroutines × 10 seconds")

	goroutines := 4
	duration := 10 * time.Second

	var writeOps atomic.Int64
	var readOps atomic.Int64
	var mu sync.Mutex
	var writeLatencies []time.Duration
	var readLatencies []time.Duration

	start := time.Now()
	deadline := start.Add(duration)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var localWriteLatencies []time.Duration
			var localReadLatencies []time.Duration
			opCounter := 0

			for time.Now().Before(deadline) {
				opCounter++
				isWrite := rand.Float64() < 0.7

				if isWrite {
					doc := bson.M{
						"_id":       fmt.Sprintf("mixed_%d_%d", workerID, opCounter),
						"worker":    workerID,
						"op":        opCounter,
						"category":  fmt.Sprintf("cat_%d", opCounter%50),
						"value":     rand.Float64() * 10000,
						"timestamp": time.Now(),
					}

					opStart := time.Now()
					_, err := coll.InsertOne(ctx, doc)
					lat := time.Since(opStart)
					localWriteLatencies = append(localWriteLatencies, lat)

					if err != nil {
						continue
					}
					writeOps.Add(1)
				} else {
					filter := bson.M{"category": fmt.Sprintf("cat_%d", rand.Intn(50))}

					opStart := time.Now()
					cursor, err := coll.Find(ctx, filter, options.Find().SetLimit(10))
					lat := time.Since(opStart)
					localReadLatencies = append(localReadLatencies, lat)

					if err != nil {
						continue
					}
					cursor.Close(ctx)
					readOps.Add(1)
				}
			}

			mu.Lock()
			writeLatencies = append(writeLatencies, localWriteLatencies...)
			readLatencies = append(readLatencies, localReadLatencies...)
			mu.Unlock()
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	writes := writeOps.Load()
	reads := readOps.Load()
	totalOps := writes + reads
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	dailyCapacity := opsPerSec * 86400

	log.Println("")
	log.Println("--- Mixed Benchmark Results ---")
	log.Printf("  Total ops:       %d (writes=%d reads=%d)", totalOps, writes, reads)
	log.Printf("  Elapsed:         %v", elapsed.Round(time.Millisecond))
	log.Printf("  Throughput:      %.0f ops/sec", opsPerSec)
	log.Printf("  Daily capacity:  %.1fM ops/day", dailyCapacity/1_000_000)

	if len(writeLatencies) > 1 {
		sort.Slice(writeLatencies, func(i, j int) bool { return writeLatencies[i] < writeLatencies[j] })
		wp50 := writeLatencies[len(writeLatencies)/2]
		wp95 := writeLatencies[int(float64(len(writeLatencies))*0.95)]
		log.Printf("  Write latency p50: %v", wp50.Round(time.Microsecond))
		log.Printf("  Write latency p95: %v", wp95.Round(time.Microsecond))
	}

	if len(readLatencies) > 1 {
		sort.Slice(readLatencies, func(i, j int) bool { return readLatencies[i] < readLatencies[j] })
		rp50 := readLatencies[len(readLatencies)/2]
		rp95 := readLatencies[int(float64(len(readLatencies))*0.95)]
		log.Printf("  Read latency  p50: %v", rp50.Round(time.Microsecond))
		log.Printf("  Read latency  p95: %v", rp95.Round(time.Microsecond))
	}

	if dailyCapacity >= 30_000_000 {
		log.Println("  [PASS] Exceeds 30M ops/day target")
	} else {
		log.Printf("  [INFO] %.1fM/30M ops/day (%.0f%% of target)", dailyCapacity/1_000_000, (dailyCapacity/30_000_000)*100)
	}
}
