package main

import (
	"context"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go-mongodb-sharding-poc/internal/config"
	"go-mongodb-sharding-poc/internal/ha"
)

func main() {
	log.SetFlags(log.Ltime)

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	log.Println("MongoDB Sharding POC - HA Failure Scenario Labs")
	log.Println("")
	log.Println("WARNING: These tests will stop and start Docker containers.")
	log.Println("         All containers will be restored after each test.")
	log.Println("")

	adminClient := connectWithAuth(ctx, cfg.MongosHosts[0], cfg.AdminUser, cfg.AdminPassword, "admin")
	defer adminClient.Disconnect(ctx)

	appClient := connectWithAuth(ctx, cfg.MongosHosts[0], cfg.AppUser, cfg.AppPassword, cfg.AppDatabase)
	defer appClient.Disconnect(ctx)

	runLab("Shard Failover", func() error {
		return ha.RunShardFailoverTest(ctx, appClient, cfg.AppDatabase)
	})

	runLab("Config Server Outage", func() error {
		return ha.RunConfigServerOutageTest(ctx, appClient, cfg.AppDatabase)
	})

	runLab("Jumbo Chunk Analysis", func() error {
		return ha.RunJumboChunkAnalysis(ctx, adminClient, appClient, cfg.AppDatabase)
	})

	log.Println("All HA labs complete")
	os.Exit(0)
}

func connectWithAuth(ctx context.Context, host, user, password, authDB string) *mongo.Client {
	uri := "mongodb://" + user + ":" + password + "@" + host + "/?authSource=" + authDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTimeout(30*time.Second))
	if err != nil {
		log.Fatalf("connect as %s: %v", user, err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("ping as %s: %v", user, err)
	}
	return client
}

func runLab(name string, fn func() error) {
	if err := fn(); err != nil {
		log.Printf("[ERROR] %s lab failed: %v", name, err)
	}
}
