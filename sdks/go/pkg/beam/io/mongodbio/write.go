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
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/structx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultWriteBatchSize = 1000
	defaultWriteOrdered   = true
)

func init() {
	register.Function1x2(createIDFn)
	register.Emitter2[primitive.ObjectID, beam.Y]()

	register.DoFn3x0[context.Context, beam.Y, func(beam.X, beam.Y)](
		&extractIDFn{},
	)
	register.Emitter2[beam.X, beam.Y]()

	register.DoFn4x1[context.Context, beam.X, beam.Y, func(beam.X), error](
		&writeFn{},
	)
	register.Emitter1[primitive.ObjectID]()
}

// Write writes a PCollection<T> of a type T to MongoDB. T must be a struct with exported fields
// that should have a "bson" tag. If the struct has a field with the bson tag "_id", the value of
// that field will be used as the id of the document. Otherwise, a new id field of type
// primitive.ObjectID will be generated for each document. Write returns a PCollection<K> of the
// inserted id values with type K.
//
// The Write transform has the required parameters:
//   - s: the scope of the pipeline
//   - uri: the MongoDB connection string
//   - database: the MongoDB database to write to
//   - collection: the MongoDB collection to write to
//   - col: the PCollection to write to MongoDB
//
// The Write transform takes a variadic number of WriteOptionFn which can set the WriteOption
// fields:
//   - BatchSize: the number of documents to write in a single batch. Defaults to 1000
//   - Ordered: whether to execute the writes in order. Defaults to true
func Write(
	s beam.Scope,
	uri string,
	database string,
	collection string,
	col beam.PCollection,
	opts ...WriteOptionFn,
) beam.PCollection {
	s = s.Scope("mongodbio.Write")

	option := &WriteOption{
		BatchSize: defaultWriteBatchSize,
		Ordered:   defaultWriteOrdered,
	}

	for _, opt := range opts {
		if err := opt(option); err != nil {
			panic(fmt.Sprintf("mongodbio.Write: invalid option: %v", err))
		}
	}

	t := col.Type().Type()
	idIndex := structx.FieldIndexByTag(t, bsonTag, "_id")

	var keyed beam.PCollection

	if idIndex == -1 {
		pre := beam.ParDo(s, createIDFn, col)
		keyed = beam.Reshuffle(s, pre)
	} else {
		keyed = beam.ParDo(
			s,
			newExtractIDFn(idIndex),
			col,
			beam.TypeDefinition{Var: beam.XType, T: t.Field(idIndex).Type},
		)
	}

	return beam.ParDo(
		s,
		newWriteFn(uri, database, collection, option),
		keyed,
	)
}

func createIDFn(elem beam.Y) (primitive.ObjectID, beam.Y) {
	id := primitive.NewObjectID()
	return id, elem
}

type extractIDFn struct {
	IDIndex int
}

func newExtractIDFn(idIndex int) *extractIDFn {
	return &extractIDFn{
		IDIndex: idIndex,
	}
}

func (fn *extractIDFn) ProcessElement(
	_ context.Context,
	elem beam.Y,
	emit func(beam.X, beam.Y),
) {
	id := reflect.ValueOf(elem).Field(fn.IDIndex).Interface()
	emit(id, elem)
}

type writeFn struct {
	mongoDBFn
	BatchSize int64
	Ordered   bool
	models    []mongo.WriteModel
}

func newWriteFn(
	uri string,
	database string,
	collection string,
	option *WriteOption,
) *writeFn {
	return &writeFn{
		mongoDBFn: mongoDBFn{
			URI:        uri,
			Database:   database,
			Collection: collection,
		},
		BatchSize: option.BatchSize,
		Ordered:   option.Ordered,
	}
}

func (fn *writeFn) ProcessElement(
	ctx context.Context,
	key beam.X,
	value beam.Y,
	emit func(beam.X),
) error {
	model := mongo.NewReplaceOneModel().
		SetFilter(bson.M{"_id": key}).
		SetUpsert(true).
		SetReplacement(value)

	fn.models = append(fn.models, model)

	if len(fn.models) >= int(fn.BatchSize) {
		if err := fn.flush(ctx); err != nil {
			return err
		}
	}

	emit(key)

	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, _ func(beam.X)) error {
	if len(fn.models) > 0 {
		return fn.flush(ctx)
	}

	return nil
}

func (fn *writeFn) flush(ctx context.Context) error {
	opts := options.BulkWrite().SetOrdered(fn.Ordered)

	if _, err := fn.collection.BulkWrite(ctx, fn.models, opts); err != nil {
		return fmt.Errorf("error bulk writing to MongoDB: %w", err)
	}

	fn.models = nil

	return nil
}
