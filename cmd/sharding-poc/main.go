package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"go-mongodb-sharding-poc/internal/cluster"
	"go-mongodb-sharding-poc/internal/config"
	"go-mongodb-sharding-poc/internal/security"
)

func main() {
	log.SetFlags(log.Ltime)

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log.Println("MongoDB Sharding POC - Cluster Setup")

	waitForAllNodes(ctx, cfg)
	initAllReplicaSets(ctx, cfg)
	createAdminUsers(ctx, cfg)
	mongosClient := connectToMongos(ctx, cfg)
	defer mongosClient.Disconnect(ctx)
	registerShards(ctx, cfg, mongosClient)
	enableDatabaseSharding(ctx, cfg, mongosClient)
	createRBACUsers(ctx, cfg, mongosClient)
	verifyCluster(ctx, cfg, mongosClient)
	verifyRBAC(ctx, cfg)
	verifyMongosFailover(ctx, cfg)
	printConnectionInfo(cfg)

	os.Exit(0)
}

func waitForAllNodes(ctx context.Context, cfg *config.ClusterConfig) {
	log.Println("Waiting for all nodes...")
	for _, m := range cfg.ConfigRS.Members {
		must(cluster.WaitForHost(ctx, m.Addr(), 60*time.Second), m.Addr())
	}
	for _, shard := range cfg.Shards {
		for _, m := range shard.Members {
			must(cluster.WaitForHost(ctx, m.Addr(), 60*time.Second), m.Addr())
		}
	}
}

func initAllReplicaSets(ctx context.Context, cfg *config.ClusterConfig) {
	log.Println("Initializing config server replica set...")
	must(cluster.InitReplicaSet(ctx, cfg.ConfigRS.Name, cfg.ConfigRS.Members, true), "init "+cfg.ConfigRS.Name)
	must(cluster.WaitForPrimary(ctx, cfg.ConfigRS.Members[0].Addr(), 60*time.Second), "primary "+cfg.ConfigRS.Name)

	log.Println("Initializing shard replica sets...")
	for _, shard := range cfg.Shards {
		must(cluster.InitReplicaSet(ctx, shard.Name, shard.Members, false), "init "+shard.Name)
		must(cluster.WaitForPrimary(ctx, shard.Members[0].Addr(), 60*time.Second), "primary "+shard.Name)
	}
}

func createAdminUsers(ctx context.Context, cfg *config.ClusterConfig) {
	log.Println("Creating admin users...")
	must(cluster.CreateAdminUser(ctx, cfg.ConfigRS.Members[0].Addr(), cfg.AdminUser, cfg.AdminPassword), "admin on config")
	for _, shard := range cfg.Shards {
		must(cluster.CreateAdminUser(ctx, shard.Members[0].Addr(), cfg.AdminUser, cfg.AdminPassword), "admin on "+shard.Name)
	}
}

func connectToMongos(ctx context.Context, cfg *config.ClusterConfig) *mongo.Client {
	log.Println("Connecting to mongos...")
	for _, host := range cfg.MongosHosts {
		must(cluster.WaitForHost(ctx, host, 60*time.Second), "mongos "+host)
	}
	client, err := cluster.ConnectMongos(ctx, cfg.MongosHosts[0], cfg.AdminUser, cfg.AdminPassword)
	if err != nil {
		log.Fatalf("connect to mongos: %v", err)
	}
	return client
}

func registerShards(ctx context.Context, cfg *config.ClusterConfig, client *mongo.Client) {
	log.Println("Registering shards...")
	for _, shard := range cfg.Shards {
		must(cluster.AddShard(ctx, client, shard.Name, shard.Members), "addShard "+shard.Name)
	}
}

func enableDatabaseSharding(ctx context.Context, cfg *config.ClusterConfig, client *mongo.Client) {
	log.Println("Enabling sharding on database...")
	must(cluster.EnableSharding(ctx, client, cfg.AppDatabase), "enableSharding")
}

func createRBACUsers(ctx context.Context, cfg *config.ClusterConfig, client *mongo.Client) {
	log.Println("Creating RBAC users...")
	must(security.CreateAppUser(ctx, client, cfg.AppDatabase, cfg.AppUser, cfg.AppPassword), "create app user")
	must(security.CreateReadOnlyUser(ctx, client, cfg.AppDatabase, cfg.ReadOnlyUser, cfg.ReadOnlyPassword), "create read-only user")
}

func verifyCluster(ctx context.Context, cfg *config.ClusterConfig, client *mongo.Client) {
	log.Println("Verifying cluster...")
	must(cluster.VerifyCluster(ctx, client, len(cfg.Shards)), "cluster verification")

	status, err := cluster.GetClusterStatus(ctx, client)
	if err != nil {
		log.Printf("[WARN] status: %v", err)
		return
	}
	cluster.PrintClusterStatus(status)
}

func verifyRBAC(ctx context.Context, cfg *config.ClusterConfig) {
	log.Println("Verifying RBAC...")
	if err := security.VerifyAppUser(ctx, cfg.MongosHosts[0], cfg.AppDatabase, cfg.AppUser, cfg.AppPassword); err != nil {
		log.Printf("[WARN] app user: %v", err)
	}
	if err := security.VerifyReadOnlyUser(ctx, cfg.MongosHosts[0], cfg.AppDatabase, cfg.ReadOnlyUser, cfg.ReadOnlyPassword); err != nil {
		log.Printf("[WARN] read-only user: %v", err)
	}
}

func verifyMongosFailover(ctx context.Context, cfg *config.ClusterConfig) {
	log.Println("Testing multi-mongos failover...")
	client, err := cluster.ConnectMongosMulti(ctx, cfg.MongosHosts, cfg.AdminUser, cfg.AdminPassword)
	if err != nil {
		log.Printf("[WARN] multi-mongos: %v", err)
		return
	}
	defer client.Disconnect(ctx)

	if err := cluster.VerifyCluster(ctx, client, len(cfg.Shards)); err != nil {
		log.Printf("[WARN] multi-mongos verify: %v", err)
		return
	}
	log.Println("[OK] Multi-mongos failover works")
}

func printConnectionInfo(cfg *config.ClusterConfig) {
	fmt.Println("")
	fmt.Println("CLUSTER SETUP COMPLETE")
	fmt.Println("")
	fmt.Printf("  mongos-1:  mongodb://%s:%s@%s/?authSource=admin\n", cfg.AdminUser, cfg.AdminPassword, cfg.MongosHosts[0])
	fmt.Printf("  mongos-2:  mongodb://%s:%s@%s/?authSource=admin\n", cfg.AdminUser, cfg.AdminPassword, cfg.MongosHosts[1])
	fmt.Printf("  app user:  mongodb://%s:%s@%s/?authSource=%s\n", cfg.AppUser, cfg.AppPassword, cfg.MongosHosts[0], cfg.AppDatabase)
	fmt.Println("")
}

// must exits with a fatal log if err is non-nil.
func must(err error, msg string) {
	if err != nil {
		log.Fatalf("[FATAL] %s: %v", msg, err)
	}
}
