package sharding

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ShardDistribution holds document counts per shard for a collection.
type ShardDistribution struct {
	Collection string
	Shards     map[string]int64
	Total      int64
}

// GetShardDistribution returns how documents are distributed across shards.
func GetShardDistribution(ctx context.Context, client *mongo.Client, db, collection string) (*ShardDistribution, error) {
	dist := &ShardDistribution{
		Collection: collection,
		Shards:     make(map[string]int64),
	}

	// Use $collStats aggregation to get per-shard doc counts
	pipeline := mongo.Pipeline{
		{{Key: "$collStats", Value: bson.D{{Key: "storageStats", Value: bson.D{}}}}},
	}

	cursor, err := client.Database(db).Collection(collection).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("collStats for %s: %w", collection, err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		shard := stringVal(doc, "shard")
		count := int64(0)
		if stats, ok := doc["storageStats"].(bson.M); ok {
			count = intVal(stats, "count")
		}

		if shard != "" {
			dist.Shards[shard] = count
			dist.Total += count
		}
	}

	return dist, nil
}

// PrintDistribution logs a formatted distribution report.
func PrintDistribution(dist *ShardDistribution) {
	log.Printf("  Collection: %s (total: %d)", dist.Collection, dist.Total)
	for shard, count := range dist.Shards {
		pct := float64(0)
		if dist.Total > 0 {
			pct = float64(count) / float64(dist.Total) * 100
		}
		log.Printf("    %-12s %6d docs  (%.1f%%)", shard, count, pct)
	}
}

// ExplainQuery runs explain on a find query and returns targeted shard names.
func ExplainQuery(ctx context.Context, client *mongo.Client, db, collection string, filter bson.D) ([]string, error) {
	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: collection},
			{Key: "filter", Value: filter},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}

	var result bson.M
	if err := client.Database(db).RunCommand(ctx, cmd).Decode(&result); err != nil {
		return nil, fmt.Errorf("explain: %w", err)
	}

	return extractTargetedShards(result), nil
}

// ShardCollection creates a shard key on a collection via the admin command.
func ShardCollection(ctx context.Context, client *mongo.Client, db, collection string, key bson.D) error {
	ns := db + "." + collection
	cmd := bson.D{
		{Key: "shardCollection", Value: ns},
		{Key: "key", Value: key},
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, cmd).Decode(&result); err != nil {
		return fmt.Errorf("shardCollection %s: %w", ns, err)
	}
	return nil
}

// ShardCollectionHashed creates a hashed shard key on a collection.
func ShardCollectionHashed(ctx context.Context, client *mongo.Client, db, collection, field string) error {
	ns := db + "." + collection
	cmd := bson.D{
		{Key: "shardCollection", Value: ns},
		{Key: "key", Value: bson.D{{Key: field, Value: "hashed"}}},
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, cmd).Decode(&result); err != nil {
		return fmt.Errorf("shardCollection (hashed) %s: %w", ns, err)
	}
	return nil
}

// RefineShardKey adds a suffix field to an existing shard key.
func RefineShardKey(ctx context.Context, client *mongo.Client, db, collection string, newKey bson.D) error {
	ns := db + "." + collection
	cmd := bson.D{
		{Key: "refineCollectionShardKey", Value: ns},
		{Key: "key", Value: newKey},
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, cmd).Decode(&result); err != nil {
		return fmt.Errorf("refineCollectionShardKey %s: %w", ns, err)
	}
	return nil
}

// DropCollection drops a collection if it exists.
func DropCollection(ctx context.Context, client *mongo.Client, db, collection string) {
	client.Database(db).Collection(collection).Drop(ctx)
}

// extractTargetedShards pulls shard names from an explain result.
func extractTargetedShards(result bson.M) []string {
	var shards []string

	// Look in queryPlanner.winningPlan.shards
	if qp, ok := result["queryPlanner"].(bson.M); ok {
		if wp, ok := qp["winningPlan"].(bson.M); ok {
			if shardList, ok := wp["shards"].(bson.A); ok {
				for _, s := range shardList {
					if sm, ok := s.(bson.M); ok {
						if name := stringVal(sm, "shardName"); name != "" {
							shards = append(shards, name)
						}
					}
				}
			}
		}
	}

	return shards
}

func stringVal(m bson.M, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func intVal(m bson.M, key string) int64 {
	switch v := m[key].(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case int:
		return int64(v)
	default:
		return 0
	}
}
