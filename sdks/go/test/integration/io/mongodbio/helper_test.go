package mongodbio

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/test/integration/internal/containers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	mongoImage = "mongo:6.0.3"
	mongoPort  = "27017"
)

func setUpTestContainer(ctx context.Context, t *testing.T) string {
	t.Helper()

	container := containers.NewContainer(
		ctx,
		t,
		mongoImage,
		containers.WithPorts([]string{mongoPort + "/tcp"}),
	)

	return containers.Port(ctx, t, container, mongoPort)
}

func objectIDFromHex(t *testing.T, hex string) primitive.ObjectID {
	t.Helper()

	id, err := primitive.ObjectIDFromHex(hex)
	if err != nil {
		t.Fatalf("error parsing hex string to primitive.ObjectID: %v", err)
	}

	return id
}

func newClient(ctx context.Context, t *testing.T, uri string) *mongo.Client {
	t.Helper()

	opts := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		t.Fatalf("error connecting to MongoDB: %v", err)
	}

	t.Cleanup(func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Fatalf("error disconnecting from MongoDB: %v", err)
		}
	})

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatalf("error pinging MongoDB: %v", err)
	}

	return client
}

func dropCollection(ctx context.Context, t *testing.T, collection *mongo.Collection) {
	t.Helper()

	if err := collection.Drop(ctx); err != nil {
		t.Fatalf("error dropping collection: %v", err)
	}
}

func readDocuments(
	ctx context.Context,
	t *testing.T,
	collection *mongo.Collection,
) []bson.M {
	t.Helper()

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		t.Fatalf("error finding documents: %v", err)
	}

	var documents []bson.M
	if err = cursor.All(ctx, &documents); err != nil {
		t.Fatalf("error decoding documents: %v", err)
	}

	return documents
}

func writeDocuments(
	ctx context.Context,
	t *testing.T,
	collection *mongo.Collection,
	documents []any,
) {
	t.Helper()

	if _, err := collection.InsertMany(ctx, documents); err != nil {
		t.Fatalf("error inserting documents: %v", err)
	}
}
