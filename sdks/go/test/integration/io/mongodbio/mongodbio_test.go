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

package mongodbio

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/mongodbio"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*docWithObjectID)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*docWithStringID)(nil)).Elem())
}

type docWithObjectID struct {
	ID     primitive.ObjectID `bson:"_id"`
	Field1 int32              `bson:"field1"`
}

type docWithStringID struct {
	ID     string `bson:"_id"`
	Field1 int32  `bson:"field1"`
}

func TestMongoDBIO_Read(t *testing.T) {
	integration.CheckFilters(t)

	ctx := context.Background()
	port := setUpTestContainer(ctx, t)
	uri := fmt.Sprintf("mongodb://%s:%s", "localhost", port)

	tests := []struct {
		name    string
		input   []any
		t       reflect.Type
		options []mongodbio.ReadOptionFn
		want    []any
	}{
		{
			name: "Read documents from MongoDB with id of type primitive.ObjectID",
			input: []any{
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28620"), "field1": int32(0)},
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"), "field1": int32(1)},
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"), "field1": int32(2)},
			},
			t: reflect.TypeOf(docWithObjectID{}),
			want: []any{
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28620"), Field1: 0},
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"), Field1: 1},
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"), Field1: 2},
			},
		},
		{
			name: "Read documents from MongoDB with id of type string",
			input: []any{
				bson.M{"_id": "id01", "field1": int32(0)},
				bson.M{"_id": "id02", "field1": int32(1)},
				bson.M{"_id": "id03", "field1": int32(2)},
			},
			t: reflect.TypeOf(docWithStringID{}),
			want: []any{
				docWithStringID{ID: "id01", Field1: 0},
				docWithStringID{ID: "id02", Field1: 1},
				docWithStringID{ID: "id03", Field1: 2},
			},
		},
		{
			name: "Read documents from MongoDB where filter matches",
			input: []any{
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28620"), "field1": int32(0)},
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"), "field1": int32(1)},
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"), "field1": int32(2)},
			},
			t: reflect.TypeOf(docWithObjectID{}),
			options: []mongodbio.ReadOptionFn{
				mongodbio.WithReadFilter(bson.M{"field1": bson.M{"$gt": 0}}),
			},
			want: []any{
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"), Field1: 1},
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"), Field1: 2},
			},
		},
		{
			name: "Read documents from MongoDB with bucketAuto aggregation",
			input: []any{
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28620"), "field1": int32(0)},
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"), "field1": int32(1)},
				bson.M{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"), "field1": int32(2)},
			},
			t: reflect.TypeOf(docWithObjectID{}),
			options: []mongodbio.ReadOptionFn{
				mongodbio.WithReadBucketAuto(true),
			},
			want: []any{
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28620"), Field1: 0},
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"), Field1: 1},
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"), Field1: 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database := "db"
			collection := "coll"

			client := newClient(ctx, t, uri)
			mongoCollection := client.Database(database).Collection(collection)

			t.Cleanup(func() {
				dropCollection(ctx, t, mongoCollection)
			})

			writeDocuments(ctx, t, mongoCollection, tt.input)

			p, s := beam.NewPipelineWithRoot()

			got := mongodbio.Read(s, uri, database, collection, tt.t, tt.options...)

			passert.Equals(s, got, tt.want...)
			ptest.RunAndValidate(t, p)
		})
	}
}

func TestMongoDBIO_Write(t *testing.T) {
	integration.CheckFilters(t)

	ctx := context.Background()
	port := setUpTestContainer(ctx, t)
	uri := fmt.Sprintf("mongodb://%s:%s", "localhost", port)

	tests := []struct {
		name     string
		input    []any
		options  []mongodbio.WriteOptionFn
		wantIDs  []any
		wantDocs []bson.M
	}{
		{
			name: "Write documents to MongoDB with id of type primitive.ObjectID",
			input: []any{
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28620"), Field1: 0},
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"), Field1: 1},
				docWithObjectID{ID: objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"), Field1: 2},
			},
			wantIDs: []any{
				objectIDFromHex(t, "61cf9980dd2d24dc5cf28620"),
				objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"),
				objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"),
			},
			wantDocs: []bson.M{
				{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28620"), "field1": int32(0)},
				{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28621"), "field1": int32(1)},
				{"_id": objectIDFromHex(t, "61cf9980dd2d24dc5cf28622"), "field1": int32(2)},
			},
		},
		{
			name: "Write documents to MongoDB with id of type string",
			input: []any{
				docWithStringID{ID: "id01", Field1: 0},
				docWithStringID{ID: "id02", Field1: 1},
				docWithStringID{ID: "id03", Field1: 2},
			},
			wantIDs: []any{
				"id01",
				"id02",
				"id03",
			},
			wantDocs: []bson.M{
				{"_id": "id01", "field1": int32(0)},
				{"_id": "id02", "field1": int32(1)},
				{"_id": "id03", "field1": int32(2)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database := "db"
			collection := "coll"

			client := newClient(ctx, t, uri)
			mongoCollection := client.Database(database).Collection(collection)

			t.Cleanup(func() {
				dropCollection(ctx, t, mongoCollection)
			})

			p, s := beam.NewPipelineWithRoot()

			col := beam.CreateList(s, tt.input)
			gotIDs := mongodbio.Write(s, uri, database, collection, col, tt.options...)

			passert.Equals(s, gotIDs, tt.wantIDs...)
			ptest.RunAndValidate(t, p)

			if gotDocs := readDocuments(ctx, t, mongoCollection); !cmp.Equal(gotDocs, tt.wantDocs) {
				t.Errorf("readDocuments() = %v, want %v", gotDocs, tt.wantDocs)
			}
		})
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	ptest.MainRet(m)
}
