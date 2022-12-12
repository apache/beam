// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"log"
	"os"

	"beam.apache.org/learning/tour-of-beam/backend/internal/fs_content"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	"cloud.google.com/go/datastore"
)

var (
	repo storage.Iface
	ctx  context.Context
)

func init() {
	ctx = context.Background()
	client, err := datastore.NewClient(ctx, "")
	if err != nil {
		log.Fatalf("new datastore client: %v", err)
	}
	repo = &storage.DatastoreDb{Client: client}
}

func main() {
	learningRoot := os.Getenv("TOB_LEARNING_ROOT")
	log.Printf("Parsing learning-content at %q\n", learningRoot)
	trees, err := fs_content.CollectLearningTree(learningRoot)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("collected %v sdks\n", len(trees))
	if err = repo.SaveContentTrees(ctx, trees); err != nil {
		log.Fatal(err)
	}
}
