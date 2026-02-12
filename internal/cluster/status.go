package cluster

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ClusterStatus holds a snapshot of the sharded cluster state.
type ClusterStatus struct {
	Shards    []ShardInfo
	Balancer  BalancerInfo
	Databases []DatabaseInfo
}

// ShardInfo represents one registered shard.
type ShardInfo struct {
	ID    string
	Host  string
	State int
}

// BalancerInfo represents the balancer state.
type BalancerInfo struct {
	Enabled bool
}

// DatabaseInfo represents a database in the cluster.
type DatabaseInfo struct {
	Name string
}

// GetClusterStatus fetches shard, balancer, and database info from mongos.
func GetClusterStatus(ctx context.Context, client *mongo.Client) (*ClusterStatus, error) {
	status := &ClusterStatus{}

	// Fetch registered shards
	var shardsResult bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&shardsResult); err != nil {
		return nil, fmt.Errorf("listShards: %w", err)
	}
	if shards, ok := shardsResult["shards"].(bson.A); ok {
		for _, s := range shards {
			if m, ok := s.(bson.M); ok {
				status.Shards = append(status.Shards, ShardInfo{
					ID:    stringField(m, "_id"),
					Host:  stringField(m, "host"),
					State: intField(m, "state"),
				})
			}
		}
	}

	// Fetch balancer status
	var balResult bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "balancerStatus", Value: 1}}).Decode(&balResult); err == nil {
		if mode, ok := balResult["mode"].(string); ok {
			status.Balancer.Enabled = (mode == "full")
		}
	}

	// Fetch database list
	var dbResult bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}}).Decode(&dbResult); err == nil {
		if dbs, ok := dbResult["databases"].(bson.A); ok {
			for _, d := range dbs {
				if m, ok := d.(bson.M); ok {
					status.Databases = append(status.Databases, DatabaseInfo{
						Name: stringField(m, "name"),
					})
				}
			}
		}
	}

	return status, nil
}

// PrintClusterStatus prints a formatted cluster report.
func PrintClusterStatus(s *ClusterStatus) {
	log.Println("")
	log.Println("=== CLUSTER STATUS REPORT ===")
	log.Println("")

	log.Printf("  Shards: %d", len(s.Shards))
	for _, shard := range s.Shards {
		state := "ACTIVE"
		if shard.State != 1 {
			state = fmt.Sprintf("STATE(%d)", shard.State)
		}
		log.Printf("    %-12s %-8s %s", shard.ID, state, shard.Host)
	}

	log.Println("")
	balancer := "DISABLED"
	if s.Balancer.Enabled {
		balancer = "ENABLED"
	}
	log.Printf("  Balancer: %s", balancer)

	log.Println("")
	log.Printf("  Databases: %d", len(s.Databases))
	for _, db := range s.Databases {
		log.Printf("    %s", db.Name)
	}

	log.Println("")
	log.Println("=============================")
	log.Println("")
}

// VerifyCluster checks that all expected shards are registered and active.
func VerifyCluster(ctx context.Context, client *mongo.Client, expectedShards int) error {
	log.Println("[VERIFY] Running cluster checks...")

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&result); err != nil {
		return fmt.Errorf("listShards: %w", err)
	}

	shards, ok := result["shards"].(bson.A)
	if !ok {
		return fmt.Errorf("unexpected listShards format")
	}

	if len(shards) != expectedShards {
		return fmt.Errorf("expected %d shards, got %d", expectedShards, len(shards))
	}
	log.Printf("[VERIFY] Shard count: %d/%d", len(shards), expectedShards)

	for _, s := range shards {
		if m, ok := s.(bson.M); ok {
			id := stringField(m, "_id")
			state := intField(m, "state")
			if state != 1 {
				return fmt.Errorf("shard %s state=%d, expected 1", id, state)
			}
			log.Printf("[VERIFY] Shard '%s': ACTIVE", id)
		}
	}

	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("cluster ping: %w", err)
	}
	log.Println("[VERIFY] Connectivity: OK")
	log.Println("[VERIFY] All checks passed")
	return nil
}

// stringField safely extracts a string from a bson.M.
func stringField(m bson.M, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// intField safely extracts an int from a bson.M (handles int32/int64/float64).
func intField(m bson.M, key string) int {
	switch v := m[key].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

// truncate shortens a string and pads with spaces.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s + strings.Repeat(" ", max-len(s))
	}
	return s[:max-3] + "..."
}
