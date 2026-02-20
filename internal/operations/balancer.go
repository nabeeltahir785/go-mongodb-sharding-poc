package operations

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BalancerState holds the current balancer status.
type BalancerState struct {
	Mode       string
	InProgress bool
}

// RunBalancerLab demonstrates manual balancer control and maintenance windows.
func RunBalancerLab(ctx context.Context, client *mongo.Client) error {
	log.Println("=== Balancer Lab ===")
	log.Println("Goal: Manual balancer control and maintenance windows")
	log.Println("")

	// Show initial state
	state, err := GetBalancerStatus(ctx, client)
	if err != nil {
		return fmt.Errorf("initial status: %w", err)
	}
	log.Printf("  Initial state: mode=%s, migrating=%v", state.Mode, state.InProgress)

	// Stop the balancer
	log.Println("")
	log.Println("Stopping balancer...")
	if err := StopBalancer(ctx, client); err != nil {
		return fmt.Errorf("stop: %w", err)
	}

	state, err = GetBalancerStatus(ctx, client)
	if err != nil {
		return fmt.Errorf("status after stop: %w", err)
	}
	log.Printf("  After stop: mode=%s", state.Mode)

	// Set maintenance window (2:00 AM - 5:00 AM)
	log.Println("")
	log.Println("Configuring balancer window: 02:00 - 05:00 UTC...")
	if err := SetBalancerWindow(ctx, client, 2, 0, 5, 0); err != nil {
		return fmt.Errorf("set window: %w", err)
	}
	log.Println("  Window set: migrations only allowed between 02:00-05:00 UTC")
	log.Println("  This prevents performance degradation during peak hours")

	// Verify the window was set
	window, err := GetBalancerWindow(ctx, client)
	if err != nil {
		log.Printf("  [WARN] Could not read window: %v", err)
	} else {
		log.Printf("  Active window: start=%s, stop=%s", window.Start, window.Stop)
	}

	// Start the balancer back
	log.Println("")
	log.Println("Starting balancer...")
	if err := StartBalancer(ctx, client); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	state, err = GetBalancerStatus(ctx, client)
	if err != nil {
		return fmt.Errorf("status after start: %w", err)
	}
	log.Printf("  After start: mode=%s", state.Mode)

	// Clear window for other demos
	log.Println("")
	log.Println("Clearing balancer window (restoring 24/7 operation)...")
	if err := ClearBalancerWindow(ctx, client); err != nil {
		log.Printf("  [WARN] clear window: %v", err)
	}
	log.Println("  Balancer restored to full-time operation")

	log.Println("")
	log.Println("Result: Balancer manually controlled with maintenance window")
	log.Println("")
	return nil
}

// GetBalancerStatus returns the current balancer state.
func GetBalancerStatus(ctx context.Context, client *mongo.Client) (*BalancerState, error) {
	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "balancerStatus", Value: 1},
	}).Decode(&result); err != nil {
		return nil, fmt.Errorf("balancerStatus: %w", err)
	}

	state := &BalancerState{}
	if mode, ok := result["mode"].(string); ok {
		state.Mode = mode
	}
	if inProgress, ok := result["inBalancerRound"].(bool); ok {
		state.InProgress = inProgress
	}
	return state, nil
}

// StartBalancer manually starts the balancer.
func StartBalancer(ctx context.Context, client *mongo.Client) error {
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "balancerStart", Value: 1},
	}).Decode(&result)
	if err != nil {
		return fmt.Errorf("balancerStart: %w", err)
	}
	log.Println("  [OK] Balancer started")
	return nil
}

// StopBalancer manually stops the balancer.
func StopBalancer(ctx context.Context, client *mongo.Client) error {
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "balancerStop", Value: 1},
	}).Decode(&result)
	if err != nil {
		return fmt.Errorf("balancerStop: %w", err)
	}
	log.Println("  [OK] Balancer stopped")
	return nil
}

// BalancerWindow represents the active balancer time window.
type BalancerWindow struct {
	Start string
	Stop  string
}

// SetBalancerWindow restricts the balancer to run only during the specified UTC window.
func SetBalancerWindow(ctx context.Context, client *mongo.Client, startHour, startMin, stopHour, stopMin int) error {
	settings := client.Database("config").Collection("settings")

	filter := bson.M{"_id": "balancer"}
	update := bson.M{
		"$set": bson.M{
			"activeWindow": bson.M{
				"start": fmt.Sprintf("%02d:%02d", startHour, startMin),
				"stop":  fmt.Sprintf("%02d:%02d", stopHour, stopMin),
			},
		},
	}

	_, err := settings.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("set balancer window: %w", err)
	}
	return nil
}

// GetBalancerWindow reads the current balancer active window.
func GetBalancerWindow(ctx context.Context, client *mongo.Client) (*BalancerWindow, error) {
	var doc bson.M
	err := client.Database("config").Collection("settings").FindOne(ctx, bson.M{"_id": "balancer"}).Decode(&doc)
	if err != nil {
		return nil, fmt.Errorf("read balancer settings: %w", err)
	}

	window := &BalancerWindow{}
	if aw, ok := doc["activeWindow"].(bson.M); ok {
		if start, ok := aw["start"].(string); ok {
			window.Start = start
		}
		if stop, ok := aw["stop"].(string); ok {
			window.Stop = stop
		}
	}
	return window, nil
}

// ClearBalancerWindow removes the active window restriction.
func ClearBalancerWindow(ctx context.Context, client *mongo.Client) error {
	settings := client.Database("config").Collection("settings")

	_, err := settings.UpdateOne(ctx, bson.M{"_id": "balancer"}, bson.M{
		"$unset": bson.M{"activeWindow": ""},
	})
	if err != nil {
		return fmt.Errorf("clear balancer window: %w", err)
	}
	return nil
}
