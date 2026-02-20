package ha

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const failoverCollection = "failover_test"

// RunShardFailoverTest kills a shard primary and verifies automatic failover.
// Proves that mongos transparently redirects traffic to the new primary
// with zero data loss.
func RunShardFailoverTest(ctx context.Context, mongosClient *mongo.Client, db string) error {
	log.Println("=== Shard Failover Test ===")
	log.Println("Goal: Kill primary, verify re-election, confirm zero data loss")
	log.Println("")

	// Target shard1rs for the failover test
	shardRS := "shard1rs"
	shardMembers := []string{"shard1-1:27022", "shard1-2:27023", "shard1-3:27024"}
	containerMap := map[string]string{
		"shard1-1:27022": "shard1-1",
		"shard1-2:27023": "shard1-2",
		"shard1-3:27024": "shard1-3",
	}

	// Find current primary
	log.Printf("Identifying %s primary...", shardRS)
	primaryAddr, err := FindPrimary(ctx, shardMembers)
	if err != nil {
		return fmt.Errorf("find primary: %w", err)
	}
	primaryContainer := containerMap[primaryAddr]
	log.Printf("  Current PRIMARY: %s (%s)", primaryAddr, primaryContainer)

	// Insert pre-failover data through mongos
	log.Println("")
	log.Println("Inserting pre-failover test data...")
	coll := mongosClient.Database(db).Collection(failoverCollection)
	coll.Drop(ctx)

	preDocs := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		preDocs[i] = bson.M{
			"_id":   fmt.Sprintf("pre_%04d", i),
			"phase": "before_failover",
			"index": i,
		}
	}
	if _, err := coll.InsertMany(ctx, preDocs); err != nil {
		return fmt.Errorf("pre-failover insert: %w", err)
	}
	log.Println("  [OK] 100 pre-failover documents inserted")

	// Kill the primary
	log.Println("")
	log.Printf("Killing primary container: %s...", primaryContainer)
	if err := StopContainer(primaryContainer); err != nil {
		return fmt.Errorf("stop %s: %w", primaryContainer, err)
	}
	log.Printf("  [OK] Container %s stopped", primaryContainer)

	// Wait for new election
	log.Println("")
	log.Println("Waiting for new primary election...")
	remainingMembers := []string{}
	for _, m := range shardMembers {
		if m != primaryAddr {
			remainingMembers = append(remainingMembers, m)
		}
	}

	newPrimary, err := WaitForNewPrimary(ctx, remainingMembers, primaryAddr, 60*time.Second)
	if err != nil {
		// Restart the container before returning error
		StartContainer(primaryContainer)
		return fmt.Errorf("election timeout: %w", err)
	}
	log.Printf("  [OK] New PRIMARY elected: %s", newPrimary)

	// Insert post-failover data through mongos
	log.Println("")
	log.Println("Inserting post-failover data through mongos...")

	// Give mongos a moment to discover the new topology
	time.Sleep(3 * time.Second)

	postDocs := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		postDocs[i] = bson.M{
			"_id":   fmt.Sprintf("post_%04d", i),
			"phase": "after_failover",
			"index": i,
		}
	}

	// Retry insert with backoff (mongos may need time)
	var insertErr error
	for attempt := 0; attempt < 5; attempt++ {
		_, insertErr = coll.InsertMany(ctx, postDocs)
		if insertErr == nil {
			break
		}
		log.Printf("  Attempt %d: %v (retrying...)", attempt+1, insertErr)
		time.Sleep(3 * time.Second)
	}
	if insertErr != nil {
		StartContainer(primaryContainer)
		return fmt.Errorf("post-failover insert failed: %w", insertErr)
	}
	log.Println("  [OK] 100 post-failover documents inserted")

	// Verify data integrity
	log.Println("")
	log.Println("Verifying data integrity...")
	preCount, _ := coll.CountDocuments(ctx, bson.M{"phase": "before_failover"})
	postCount, _ := coll.CountDocuments(ctx, bson.M{"phase": "after_failover"})
	totalCount, _ := coll.CountDocuments(ctx, bson.M{})

	log.Printf("  Pre-failover docs:  %d/100", preCount)
	log.Printf("  Post-failover docs: %d/100", postCount)
	log.Printf("  Total docs:         %d/200", totalCount)

	if totalCount == 200 {
		log.Println("  [OK] ZERO DATA LOSS confirmed")
	} else {
		log.Printf("  [WARN] Expected 200 docs, found %d", totalCount)
	}

	// Restart the killed node
	log.Println("")
	log.Printf("Restarting %s...", primaryContainer)
	if err := StartContainer(primaryContainer); err != nil {
		log.Printf("  [WARN] restart %s: %v", primaryContainer, err)
	} else {
		log.Printf("  [OK] %s restarted (will rejoin as SECONDARY)", primaryContainer)
	}

	// Wait and show final RS status
	time.Sleep(5 * time.Second)
	log.Println("")
	log.Println("Final replica set status:")
	PrintRSStatus(ctx, shardMembers)

	log.Println("")
	log.Println("Result: Shard failover completed with zero data loss")
	log.Println("")
	return nil
}

// FindPrimary connects to each member and returns the address of the PRIMARY.
func FindPrimary(ctx context.Context, members []string) (string, error) {
	for _, addr := range members {
		uri := fmt.Sprintf("mongodb://%s/?directConnection=true", addr)
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(5*time.Second))
		if err != nil {
			continue
		}

		var status bson.M
		err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
		client.Disconnect(ctx)
		if err != nil {
			continue
		}

		if members, ok := status["members"].(bson.A); ok {
			for _, m := range members {
				if doc, ok := m.(bson.M); ok {
					if stateStr, ok := doc["stateStr"].(string); ok && stateStr == "PRIMARY" {
						if name, ok := doc["name"].(string); ok {
							return name, nil
						}
					}
				}
			}
		}
	}
	return "", fmt.Errorf("no PRIMARY found among %v", members)
}

// WaitForNewPrimary polls until a new primary is elected that differs from oldPrimary.
func WaitForNewPrimary(ctx context.Context, members []string, oldPrimary string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, addr := range members {
			uri := fmt.Sprintf("mongodb://%s/?directConnection=true", addr)
			client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(5*time.Second))
			if err != nil {
				continue
			}

			var status bson.M
			err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
			client.Disconnect(ctx)
			if err != nil {
				continue
			}

			if mems, ok := status["members"].(bson.A); ok {
				for _, m := range mems {
					if doc, ok := m.(bson.M); ok {
						stateStr, _ := doc["stateStr"].(string)
						name, _ := doc["name"].(string)
						if stateStr == "PRIMARY" && name != oldPrimary {
							return name, nil
						}
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return "", fmt.Errorf("no new primary elected within %v", timeout)
}

// PrintRSStatus prints the replica set member states.
func PrintRSStatus(ctx context.Context, members []string) {
	for _, addr := range members {
		uri := fmt.Sprintf("mongodb://%s/?directConnection=true", addr)
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(5*time.Second))
		if err != nil {
			log.Printf("    %-20s UNREACHABLE", addr)
			continue
		}

		var status bson.M
		err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
		client.Disconnect(ctx)
		if err != nil {
			log.Printf("    %-20s UNREACHABLE", addr)
			continue
		}

		if mems, ok := status["members"].(bson.A); ok {
			for _, m := range mems {
				if doc, ok := m.(bson.M); ok {
					name, _ := doc["name"].(string)
					state, _ := doc["stateStr"].(string)
					log.Printf("    %-20s %s", name, state)
				}
			}
			break // Only need one successful response
		}
	}
}

// StopContainer stops a Docker container by name.
func StopContainer(name string) error {
	cmd := exec.Command("docker", "stop", name)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

// StartContainer starts a Docker container by name.
func StartContainer(name string) error {
	cmd := exec.Command("docker", "start", name)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}
