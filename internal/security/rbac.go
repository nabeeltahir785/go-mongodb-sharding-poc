package security

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CreateAppUser creates a readWrite user on the given database.
func CreateAppUser(ctx context.Context, client *mongo.Client, db, user, pwd string) error {
	return createUser(ctx, client, db, user, pwd, "readWrite")
}

// CreateReadOnlyUser creates a read-only user on the given database.
func CreateReadOnlyUser(ctx context.Context, client *mongo.Client, db, user, pwd string) error {
	return createUser(ctx, client, db, user, pwd, "read")
}

// VerifyAppUser checks that the app user can insert and read.
func VerifyAppUser(ctx context.Context, host, db, user, pwd string) error {
	client, err := connectAs(ctx, host, db, user, pwd)
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)

	coll := client.Database(db).Collection("__rbac_test")
	if _, err := coll.InsertOne(ctx, bson.M{"test": true}); err != nil {
		return fmt.Errorf("write test as '%s': %w", user, err)
	}
	coll.Drop(ctx)

	log.Printf("[VERIFY] App user '%s' readWrite on '%s': OK", user, db)
	return nil
}

// VerifyReadOnlyUser checks that the read-only user cannot write.
func VerifyReadOnlyUser(ctx context.Context, host, db, user, pwd string) error {
	client, err := connectAs(ctx, host, db, user, pwd)
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)

	coll := client.Database(db).Collection("__rbac_test")
	_, err = coll.InsertOne(ctx, bson.M{"test": true})
	if err != nil {
		log.Printf("[VERIFY] Read-only user '%s' denied write: OK", user)
		return nil
	}

	coll.Drop(ctx)
	return fmt.Errorf("read-only user '%s' was able to write", user)
}

// createUser creates a user with the given role on a database.
func createUser(ctx context.Context, client *mongo.Client, db, user, pwd, role string) error {
	cmd := bson.D{
		{Key: "createUser", Value: user},
		{Key: "pwd", Value: pwd},
		{Key: "roles", Value: bson.A{
			bson.D{{Key: "role", Value: role}, {Key: "db", Value: db}},
		}},
	}

	var result bson.M
	err := client.Database(db).RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		if isUserExists(err) {
			log.Printf("[OK] User '%s' already exists on '%s'", user, db)
			return nil
		}
		return fmt.Errorf("create user '%s': %w", user, err)
	}

	log.Printf("[OK] User '%s' created with '%s' on '%s'", user, role, db)
	return nil
}

// connectAs creates a client authenticated as the given user.
func connectAs(ctx context.Context, host, authDB, user, pwd string) (*mongo.Client, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s/?authSource=%s", user, pwd, host, authDB)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect as '%s': %w", user, err)
	}
	return client, nil
}

// isUserExists checks if the error indicates the user already exists.
func isUserExists(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "already exists") ||
		strings.Contains(s, "UserAlreadyExists") ||
		strings.Contains(s, "51003")
}
