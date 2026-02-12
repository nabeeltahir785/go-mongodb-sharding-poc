package main

import (
	"context"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go-mongodb-sharding-poc/internal/config"
	"go-mongodb-sharding-poc/internal/sharding"
)

func main() {
	log.SetFlags(log.Ltime)

	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log.Println("MongoDB Sharding POC - Sharding Strategy Demos")

	adminClient := connectWithAuth(ctx, cfg.MongosHosts[0], cfg.AdminUser, cfg.AdminPassword, "admin")
	defer adminClient.Disconnect(ctx)

	appClient := connectWithAuth(ctx, cfg.MongosHosts[0], cfg.AppUser, cfg.AppPassword, cfg.AppDatabase)
	defer appClient.Disconnect(ctx)

	runDemo("Hashed", func() error {
		return sharding.RunHashedDemo(ctx, adminClient, appClient, cfg.AppDatabase)
	})

	runDemo("Ranged", func() error {
		return sharding.RunRangedDemo(ctx, adminClient, appClient, cfg.AppDatabase)
	})

	runDemo("Compound", func() error {
		return sharding.RunCompoundDemo(ctx, adminClient, appClient, cfg.AppDatabase)
	})

	runDemo("Refinable", func() error {
		return sharding.RunRefinableDemo(ctx, adminClient, appClient, cfg.AppDatabase)
	})

	log.Println("All demos complete")
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

func runDemo(name string, fn func() error) {
	if err := fn(); err != nil {
		log.Printf("[ERROR] %s demo failed: %v", name, err)
	}
}
