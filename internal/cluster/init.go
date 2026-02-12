package cluster

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go-mongodb-sharding-poc/internal/config"
)

// InitReplicaSet runs rs.initiate() on the first member of the set.
func InitReplicaSet(ctx context.Context, rsName string, members []config.Member, isConfigSvr bool) error {
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", members[0].Addr())
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(30*time.Second))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", members[0].Addr(), err)
	}
	defer client.Disconnect(ctx)

	// Build member list
	memberDocs := bson.A{}
	for i, m := range members {
		memberDocs = append(memberDocs, bson.D{
			{Key: "_id", Value: i},
			{Key: "host", Value: m.Addr()},
		})
	}

	rsConfig := bson.D{
		{Key: "_id", Value: rsName},
		{Key: "members", Value: memberDocs},
	}
	if isConfigSvr {
		rsConfig = append(rsConfig, bson.E{Key: "configsvr", Value: true})
	}

	result := client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetInitiate", Value: rsConfig}})
	if result.Err() != nil {
		if containsAny(result.Err().Error(), "already initialized", "AlreadyInitialized") {
			log.Printf("[OK] Replica set '%s' already initialized", rsName)
			return nil
		}
		return fmt.Errorf("replSetInitiate %s: %w", rsName, result.Err())
	}

	log.Printf("[OK] Replica set '%s' initialized", rsName)
	return nil
}

// WaitForPrimary polls rs.status() until a PRIMARY is elected.
func WaitForPrimary(ctx context.Context, host string, timeout time.Duration) error {
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", host)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", host, err)
	}
	defer client.Disconnect(ctx)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hasPrimary(ctx, client) {
			log.Printf("[OK] PRIMARY elected on %s", host)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("timeout waiting for PRIMARY on %s", host)
}

// AddShard registers a shard replica set with the cluster via mongos.
func AddShard(ctx context.Context, mongosClient *mongo.Client, rsName string, members []config.Member) error {
	// Format: rsName/host1:port1,host2:port2,host3:port3
	addrs := make([]string, len(members))
	for i, m := range members {
		addrs[i] = m.Addr()
	}
	shardConn := rsName + "/" + strings.Join(addrs, ",")

	var result bson.M
	err := mongosClient.Database("admin").RunCommand(ctx, bson.D{{Key: "addShard", Value: shardConn}}).Decode(&result)
	if err != nil {
		if containsAny(err.Error(), "already", "E11000") {
			log.Printf("[OK] Shard '%s' already registered", rsName)
			return nil
		}
		return fmt.Errorf("addShard %s: %w", rsName, err)
	}

	log.Printf("[OK] Shard '%s' added to cluster", rsName)
	return nil
}

// EnableSharding enables sharding on a database.
// In MongoDB 7.0+ this is automatic, so errors are non-fatal.
func EnableSharding(ctx context.Context, mongosClient *mongo.Client, dbName string) error {
	var result bson.M
	err := mongosClient.Database("admin").RunCommand(ctx, bson.D{{Key: "enableSharding", Value: dbName}}).Decode(&result)
	if err != nil {
		log.Printf("[INFO] enableSharding '%s': %v (automatic in MongoDB 7.0+)", dbName, err)
		return nil
	}
	log.Printf("[OK] Sharding enabled on '%s'", dbName)
	return nil
}

// CreateAdminUser creates a root admin on a replica set primary.
func CreateAdminUser(ctx context.Context, host, user, password string) error {
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", host)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", host, err)
	}
	defer client.Disconnect(ctx)

	cmd := bson.D{
		{Key: "createUser", Value: user},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{
			bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}},
		}},
	}

	var result bson.M
	err = client.Database("admin").RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		if containsAny(err.Error(), "already exists", "UserAlreadyExists", "51003") {
			log.Printf("[OK] Admin user '%s' already exists on %s", user, host)
			return nil
		}
		return fmt.Errorf("createUser on %s: %w", host, err)
	}

	log.Printf("[OK] Admin user '%s' created on %s", user, host)
	return nil
}

// ConnectMongos connects to a single mongos with auth.
func ConnectMongos(ctx context.Context, host, user, password string) (*mongo.Client, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s/?authSource=admin", user, password, host)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(30*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connect to mongos %s: %w", host, err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("ping mongos %s: %w", host, err)
	}
	return client, nil
}

// ConnectMongosMulti connects to multiple mongos instances for failover.
func ConnectMongosMulti(ctx context.Context, hosts []string, user, password string) (*mongo.Client, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s/?authSource=admin", user, password, strings.Join(hosts, ","))
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(30*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connect to mongos cluster: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("ping mongos cluster: %w", err)
	}
	return client, nil
}

// WaitForHost blocks until a MongoDB host responds to ping.
func WaitForHost(ctx context.Context, host string, timeout time.Duration) error {
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true&serverSelectionTimeout=5000", host)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(5*time.Second))
		if err == nil {
			if pingErr := client.Ping(ctx, nil); pingErr == nil {
				client.Disconnect(ctx)
				return nil
			}
			client.Disconnect(ctx)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("timeout waiting for %s", host)
}

// hasPrimary checks if any member in the replica set is PRIMARY.
func hasPrimary(ctx context.Context, client *mongo.Client) bool {
	var status bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
	if err != nil {
		return false
	}
	members, ok := status["members"].(bson.A)
	if !ok {
		return false
	}
	for _, m := range members {
		if member, ok := m.(bson.M); ok {
			if state, ok := member["stateStr"].(string); ok && state == "PRIMARY" {
				return true
			}
		}
	}
	return false
}

// containsAny returns true if s contains any of the given substrings.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
