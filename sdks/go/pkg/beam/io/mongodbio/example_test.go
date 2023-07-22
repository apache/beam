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

package mongodbio_test

import (
	"context"
	"log"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/mongodbio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func ExampleRead_default() {
	type Event struct {
		ID        primitive.ObjectID `bson:"_id"`
		Timestamp int64              `bson:"timestamp"`
		EventType int32              `bson:"event_type"`
	}

	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	col := mongodbio.Read(
		s,
		"mongodb://localhost:27017",
		"demo",
		"events",
		reflect.TypeOf(Event{}),
	)
	debug.Print(s, col)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func ExampleRead_options() {
	type Event struct {
		ID        primitive.ObjectID `bson:"_id"`
		Timestamp int64              `bson:"timestamp"`
		EventType int32              `bson:"event_type"`
	}

	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	col := mongodbio.Read(
		s,
		"mongodb://localhost:27017",
		"demo",
		"events",
		reflect.TypeOf(Event{}),
		mongodbio.WithReadBucketAuto(true),
		mongodbio.WithReadBundleSize(32*1024*1024),
		mongodbio.WithReadFilter(bson.M{"timestamp": bson.M{"$gt": 1640995200000}}),
	)
	debug.Print(s, col)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func ExampleWrite_default() {
	type Event struct {
		ID        primitive.ObjectID `bson:"_id"`
		Timestamp int64              `bson:"timestamp"`
		EventType int32              `bson:"event_type"`
	}

	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	input := []Event{
		{
			ID:        primitive.NewObjectIDFromTimestamp(time.UnixMilli(1640995200001)),
			Timestamp: 1640995200001,
			EventType: 1,
		},
		{
			ID:        primitive.NewObjectIDFromTimestamp(time.UnixMilli(1640995200002)),
			Timestamp: 1640995200002,
			EventType: 2,
		},
	}

	col := beam.CreateList(s, input)
	mongodbio.Write(s, "mongodb://localhost:27017", "demo", "events", col)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func ExampleWrite_options() {
	type Event struct {
		ID        primitive.ObjectID `bson:"_id"`
		Timestamp int64              `bson:"timestamp"`
		EventType int32              `bson:"event_type"`
	}

	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	input := []Event{
		{
			ID:        primitive.NewObjectIDFromTimestamp(time.UnixMilli(1640995200001)),
			Timestamp: 1640995200001,
			EventType: 1,
		},
		{
			ID:        primitive.NewObjectIDFromTimestamp(time.UnixMilli(1640995200002)),
			Timestamp: 1640995200002,
			EventType: 2,
		},
	}

	col := beam.CreateList(s, input)
	mongodbio.Write(
		s,
		"mongodb://localhost:27017",
		"demo",
		"events",
		col,
		mongodbio.WithWriteBatchSize(500),
		mongodbio.WithWriteOrdered(false),
	)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func ExampleWrite_generateID() {
	type Event struct {
		Timestamp int64 `bson:"timestamp"`
		EventType int32 `bson:"event_type"`
	}

	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	input := []Event{
		{
			Timestamp: 1640995200001,
			EventType: 1,
		},
		{
			Timestamp: 1640995200002,
			EventType: 1,
		},
	}

	col := beam.CreateList(s, input)
	ids := mongodbio.Write(s, "mongodb://localhost:27017", "demo", "events", col)
	debug.Print(s, ids)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
