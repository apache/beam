package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"beam.apache.org/learning/tour-of-beam/backend/internal/fs_content"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	"cloud.google.com/go/datastore"
)

var repo storage.Iface

func init() {
	client, err := datastore.NewClient(context.Background(), "")
	if err != nil {
		log.Fatalf("new datastore client: %v", err)
	}
	repo = &storage.DatastoreDb{Client: client}
}

func main() {
	learningRoot := os.Getenv("TOB_LEARNING_ROOT")
	fmt.Printf("Parsing learning-content at %q\n", learningRoot)
	trees, err := fs_content.CollectLearningTree(learningRoot)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("collected %v sdks\n", len(trees))
	if err = repo.SaveContentTrees(context.Background(), trees); err != nil {
		log.Fatal(err)
	}
}
