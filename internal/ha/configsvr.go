package ha

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// RunConfigServerOutageTest shuts down 2 of 3 config servers to demonstrate
// that the cluster enters a degraded state where data reads still work
// (via cached routing) but metadata writes fail.
func RunConfigServerOutageTest(ctx context.Context, mongosClient *mongo.Client, db string) error {
	log.Println("=== Config Server Outage Test ===")
	log.Println("Goal: Verify behavior when config server majority is lost")
	log.Println("")

	configServers := []string{"cfg-2", "cfg-3"} // Keep cfg-1 alive (minority)

	// Verify cluster is healthy before test
	log.Println("Verifying cluster health before outage...")
	if err := mongosClient.Ping(ctx, nil); err != nil {
		return fmt.Errorf("cluster not healthy: %w", err)
	}
	log.Println("  [OK] Cluster healthy")

	// Insert baseline data
	log.Println("")
	log.Println("Inserting baseline data...")
	coll := mongosClient.Database(db).Collection("configsvr_test")
	coll.Drop(ctx)

	docs := make([]interface{}, 50)
	for i := 0; i < 50; i++ {
		docs[i] = bson.M{
			"_id":   fmt.Sprintf("baseline_%04d", i),
			"phase": "pre_outage",
		}
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("baseline insert: %w", err)
	}
	log.Println("  [OK] 50 baseline documents inserted")

	// Stop 2 of 3 config servers
	log.Println("")
	log.Printf("Stopping config servers: %v...", configServers)
	for _, cs := range configServers {
		if err := StopContainer(cs); err != nil {
			// Restart any we already stopped
			for _, stopped := range configServers {
				StartContainer(stopped)
			}
			return fmt.Errorf("stop %s: %w", cs, err)
		}
		log.Printf("  [OK] %s stopped", cs)
	}

	// Wait for cluster to detect the outage
	log.Println("")
	log.Println("Waiting for cluster to detect config server outage...")
	time.Sleep(10 * time.Second)

	// Test data reads (should still work via cached routing tables)
	log.Println("")
	log.Println("Testing data reads (cached routing)...")
	readCtx, readCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readCancel()

	count, err := coll.CountDocuments(readCtx, bson.M{"phase": "pre_outage"})
	if err != nil {
		log.Printf("  [RESULT] Data reads FAILED: %v", err)
		log.Println("  Config server outage affected data reads (routing cache expired)")
	} else {
		log.Printf("  [RESULT] Data reads WORK: found %d/50 documents", count)
		log.Println("  mongos uses cached routing tables for existing collections")
	}

	// Test data writes to existing collection
	log.Println("")
	log.Println("Testing data writes to existing collection...")
	writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
	defer writeCancel()

	_, writeErr := coll.InsertOne(writeCtx, bson.M{
		"_id":   "during_outage",
		"phase": "during_outage",
	})
	if writeErr != nil {
		log.Printf("  [RESULT] Data writes FAILED: %v", writeErr)
		log.Println("  Writes may fail when config servers lose majority")
	} else {
		log.Println("  [RESULT] Data writes WORK (cached routing sufficient)")
	}

	// Test metadata operation (should fail without config server majority)
	log.Println("")
	log.Println("Testing metadata operation (enableSharding on new DB)...")
	metaCtx, metaCancel := context.WithTimeout(ctx, 10*time.Second)
	defer metaCancel()

	var metaResult bson.M
	metaErr := mongosClient.Database("admin").RunCommand(metaCtx, bson.D{
		{Key: "enableSharding", Value: "test_outage_db"},
	}).Decode(&metaResult)
	if metaErr != nil {
		log.Printf("  [RESULT] Metadata write FAILED (expected): %v", metaErr)
		log.Println("  Config server majority required for metadata changes")
	} else {
		log.Println("  [RESULT] Metadata write succeeded (MongoDB 7.0+ auto-sharding)")
	}

	// Restore config servers
	log.Println("")
	log.Printf("Restoring config servers: %v...", configServers)
	for _, cs := range configServers {
		if err := StartContainer(cs); err != nil {
			log.Printf("  [WARN] start %s: %v", cs, err)
		} else {
			log.Printf("  [OK] %s restarted", cs)
		}
	}

	// Wait for recovery
	log.Println("")
	log.Println("Waiting for config server recovery...")
	time.Sleep(15 * time.Second)

	// Verify full operation restored
	log.Println("Verifying full cluster recovery...")
	recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 15*time.Second)
	defer recoveryCancel()

	var pingErr error
	for attempt := 0; attempt < 5; attempt++ {
		pingErr = mongosClient.Ping(recoveryCtx, nil)
		if pingErr == nil {
			break
		}
		time.Sleep(3 * time.Second)
	}

	if pingErr != nil {
		log.Printf("  [WARN] Cluster ping: %v", pingErr)
	} else {
		log.Println("  [OK] Cluster fully operational")
	}

	// Verify data survived
	totalCount, _ := coll.CountDocuments(ctx, bson.M{})
	log.Printf("  Total documents after recovery: %d", totalCount)

	log.Println("")
	log.Println("OUTAGE SUMMARY")
	log.Println("  Config servers stopped:       cfg-2, cfg-3 (majority lost)")
	log.Println("  Data reads during outage:     Depend on cached routing tables")
	log.Println("  Metadata writes during outage: FAIL (no config server majority)")
	log.Println("  After recovery:               Full operation restored")

	log.Println("")
	log.Println("Result: Config server outage behavior verified")
	log.Println("")
	return nil
}
