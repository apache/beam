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

// Package mongodbio contains transforms for reading from and writing to MongoDB.
package mongodbio

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	bsonTag = "bson"
)

type mongoDBFn struct {
	URI        string
	Database   string
	Collection string
	client     *mongo.Client
	collection *mongo.Collection
}

func (fn *mongoDBFn) Setup(ctx context.Context) error {
	if fn.client == nil {
		client, err := newClient(ctx, fn.URI)
		if err != nil {
			return err
		}

		fn.client = client
	}

	fn.collection = fn.client.Database(fn.Database).Collection(fn.Collection)

	return nil
}

func newClient(ctx context.Context, uri string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("error connecting to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("error pinging MongoDB: %w", err)
	}

	return client, nil
}

func (fn *mongoDBFn) Teardown(ctx context.Context) error {
	if err := fn.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("error disconnecting from MongoDB: %w", err)
	}

	return nil
}

type documentID struct {
	ID any `bson:"_id"`
}

func findID(
	ctx context.Context,
	collection *mongo.Collection,
	filter any,
	order int,
	skip int64,
) (any, error) {
	opts := options.FindOne().
		SetProjection(bson.M{"_id": 1}).
		SetSort(bson.M{"_id": order}).
		SetSkip(skip)

	var docID documentID
	if err := collection.FindOne(ctx, filter, opts).Decode(&docID); err != nil {
		return nil, err
	}

	return docID.ID, nil
}
