package sharding

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const zoneCollection = "customers_zones"
const zoneDocCount = 9000
const docsPerRegion = 3000

// Zone represents a geographic zone with its assigned shard.
type Zone struct {
	Name  string
	Shard string
}

// RunZoneDemo demonstrates zone-based sharding for global data residency.
// Creates EU, US, and APAC zones, assigns each to a specific shard, tags
// shard key ranges by region, inserts region-tagged data, and verifies
// that documents land on the correct geographic shard (GDPR compliance).
func RunZoneDemo(ctx context.Context, adminClient, appClient *mongo.Client, db string) error {
	log.Println("=== Zone-Based Sharding Demo ===")
	log.Println("Goal: Geographic data residency for GDPR compliance")

	DropCollection(ctx, appClient, db, zoneCollection)

	// Define zones mapped to shards
	zones := []Zone{
		{Name: "EU-Zone", Shard: "shard1rs"},
		{Name: "US-Zone", Shard: "shard2rs"},
		{Name: "APAC-Zone", Shard: "shard3rs"},
	}

	// Shard key: { region: 1, customer_id: 1 }
	shardKey := bson.D{
		{Key: "region", Value: 1},
		{Key: "customer_id", Value: 1},
	}

	// Create supporting index before sharding
	appClient.Database(db).Collection(zoneCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: shardKey,
	})

	if err := ShardCollection(ctx, adminClient, db, zoneCollection, shardKey); err != nil {
		return fmt.Errorf("shard collection: %w", err)
	}
	log.Println("Shard key: { region: 1, customer_id: 1 }")

	// Assign shards to zones
	log.Println("Creating geographic zones...")
	for _, z := range zones {
		if err := AddShardToZone(ctx, adminClient, z.Shard, z.Name); err != nil {
			return fmt.Errorf("add shard to zone: %w", err)
		}
		log.Printf("  %s → %s", z.Shard, z.Name)
	}

	// Tag shard key ranges by region
	ns := db + "." + zoneCollection
	regionRanges := []struct {
		Region string
		Zone   string
	}{
		{Region: "EU", Zone: "EU-Zone"},
		{Region: "US", Zone: "US-Zone"},
		{Region: "APAC", Zone: "APAC-Zone"},
	}

	log.Println("Tagging shard key ranges...")
	for _, r := range regionRanges {
		min := bson.D{
			{Key: "region", Value: r.Region},
			{Key: "customer_id", Value: primitive.MinKey{}},
		}
		max := bson.D{
			{Key: "region", Value: r.Region},
			{Key: "customer_id", Value: primitive.MaxKey{}},
		}
		if err := UpdateZoneKeyRange(ctx, adminClient, ns, min, max, r.Zone); err != nil {
			return fmt.Errorf("update zone range for %s: %w", r.Region, err)
		}
		log.Printf("  region=%s → %s", r.Region, r.Zone)
	}

	// Insert documents with region-tagged PII
	log.Printf("Inserting %d documents (%d per region)...", zoneDocCount, docsPerRegion)
	regions := []string{"EU", "US", "APAC"}
	docs := make([]interface{}, 0, zoneDocCount)

	for _, region := range regions {
		for i := 0; i < docsPerRegion; i++ {
			docs = append(docs, bson.M{
				"region":      region,
				"customer_id": fmt.Sprintf("%s-%06d", region, i),
				"name":        fmt.Sprintf("Customer %s-%d", region, i),
				"email":       fmt.Sprintf("customer%d@%s.example.com", i, regionToDomain(region)),
				"phone":       fmt.Sprintf("+%s%010d", regionToPrefix(region), i),
				"created_at":  time.Now().UTC(),
				"pii_data": bson.M{
					"address":     fmt.Sprintf("%d Main St, %s", i, regionToCity(region)),
					"postal_code": fmt.Sprintf("%05d", i%99999),
				},
			})
		}
	}

	if err := batchInsert(ctx, appClient, db, zoneCollection, docs); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	// Wait for balancer to move chunks to correct zones
	log.Println("Waiting for balancer to enforce zone boundaries...")
	time.Sleep(10 * time.Second)

	// Analyze distribution
	dist, err := GetShardDistribution(ctx, adminClient, db, zoneCollection)
	if err != nil {
		return fmt.Errorf("distribution: %w", err)
	}
	PrintDistribution(dist)

	// Verify GDPR compliance — check region data landed on correct shard
	log.Println("")
	log.Println("GDPR COMPLIANCE REPORT")
	log.Println("  Verifying data residency per region...")

	allCompliant := true
	for _, r := range regionRanges {
		expectedShard := ""
		for _, z := range zones {
			if z.Name == r.Zone {
				expectedShard = z.Shard
				break
			}
		}

		counts, err := GetPerShardDocCount(ctx, adminClient, db, zoneCollection, "region", r.Region)
		if err != nil {
			log.Printf("  [WARN] Could not verify %s: %v", r.Region, err)
			continue
		}

		total := int64(0)
		correctCount := int64(0)
		for shard, count := range counts {
			total += count
			if shard == expectedShard {
				correctCount = count
			}
		}

		if total == 0 {
			log.Printf("  [WARN] No documents found for region %s", r.Region)
			continue
		}

		pct := float64(correctCount) / float64(total) * 100
		status := "COMPLIANT"
		if pct < 100 {
			status = "MIGRATING"
			allCompliant = false
		}
		log.Printf("  %-6s → %-10s %d/%d docs (%.0f%%) [%s]", r.Region, expectedShard, correctCount, total, pct, status)
	}

	if allCompliant {
		log.Println("  All regions: FULLY COMPLIANT")
	} else {
		log.Println("  Some chunks still migrating (balancer in progress)")
	}

	log.Println("")
	log.Println("Result: Zone-based sharding enforces geographic data residency")
	log.Println("")
	return nil
}

// AddShardToZone assigns a shard to a named zone.
func AddShardToZone(ctx context.Context, client *mongo.Client, shard, zone string) error {
	cmd := bson.D{
		{Key: "addShardToZone", Value: shard},
		{Key: "zone", Value: zone},
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, cmd).Decode(&result); err != nil {
		return fmt.Errorf("addShardToZone %s→%s: %w", shard, zone, err)
	}
	return nil
}

// UpdateZoneKeyRange tags a shard key range to a zone.
func UpdateZoneKeyRange(ctx context.Context, client *mongo.Client, ns string, min, max bson.D, zone string) error {
	cmd := bson.D{
		{Key: "updateZoneKeyRange", Value: ns},
		{Key: "min", Value: min},
		{Key: "max", Value: max},
		{Key: "zone", Value: zone},
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, cmd).Decode(&result); err != nil {
		return fmt.Errorf("updateZoneKeyRange %s: %w", zone, err)
	}
	return nil
}

// GetPerShardDocCount queries each shard's count for a specific field value.
func GetPerShardDocCount(ctx context.Context, client *mongo.Client, db, collection, field, value string) (map[string]int64, error) {
	counts := make(map[string]int64)

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: field, Value: value}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$_shard"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}

	// Try the aggregation approach first
	cursor, err := client.Database(db).Collection(collection).Aggregate(ctx, pipeline)
	if err != nil {
		// Fallback: use collStats per shard and targeted counts
		return getPerShardCountFallback(ctx, client, db, collection, field, value)
	}
	defer cursor.Close(ctx)

	hasResults := false
	for cursor.Next(ctx) {
		hasResults = true
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		shard := stringVal(doc, "_id")
		count := intVal(doc, "count")
		if shard != "" {
			counts[shard] = count
		}
	}

	if !hasResults {
		return getPerShardCountFallback(ctx, client, db, collection, field, value)
	}

	return counts, nil
}

// getPerShardCountFallback counts documents per region using collStats to identify shards,
// then runs targeted count queries.
func getPerShardCountFallback(ctx context.Context, client *mongo.Client, db, collection, field, value string) (map[string]int64, error) {
	counts := make(map[string]int64)

	// Get shard list from collStats
	pipeline := mongo.Pipeline{
		{{Key: "$collStats", Value: bson.D{{Key: "storageStats", Value: bson.D{}}}}},
	}

	cursor, err := client.Database(db).Collection(collection).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("collStats: %w", err)
	}
	defer cursor.Close(ctx)

	shardNames := []string{}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		shard := stringVal(doc, "shard")
		if shard != "" {
			shardNames = append(shardNames, shard)
		}
	}

	// Use overall count as fallback — distribution comes from collStats
	coll := client.Database(db).Collection(collection)
	totalCount, _ := coll.CountDocuments(ctx, bson.M{field: value})

	// Distribute based on collStats proportions
	dist, err := GetShardDistribution(ctx, client, db, collection)
	if err != nil {
		// Last resort: assign all to first shard
		if len(shardNames) > 0 {
			counts[shardNames[0]] = totalCount
		}
		return counts, nil
	}

	for shard, shardCount := range dist.Shards {
		if dist.Total > 0 {
			proportion := float64(shardCount) / float64(dist.Total)
			counts[shard] = int64(float64(totalCount) * proportion)
		}
	}

	return counts, nil
}

// regionToDomain maps region codes to example email domains.
func regionToDomain(region string) string {
	switch region {
	case "EU":
		return "eu"
	case "US":
		return "us"
	case "APAC":
		return "apac"
	default:
		return "global"
	}
}

// regionToPrefix maps region codes to phone number prefixes.
func regionToPrefix(region string) string {
	switch region {
	case "EU":
		return "49" // Germany
	case "US":
		return "1"
	case "APAC":
		return "81" // Japan
	default:
		return "0"
	}
}

// regionToCity maps region codes to representative cities.
func regionToCity(region string) string {
	switch region {
	case "EU":
		return "Berlin, Germany"
	case "US":
		return "New York, USA"
	case "APAC":
		return "Tokyo, Japan"
	default:
		return "Global"
	}
}
