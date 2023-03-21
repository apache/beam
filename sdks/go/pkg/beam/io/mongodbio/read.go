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
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/structx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultReadBundleSize = 64 * 1024 * 1024
)

func init() {
	register.DoFn4x1[context.Context, *sdf.LockRTracker, []byte, func(beam.Y), error](
		&readFn{},
	)
	register.Emitter1[beam.Y]()
}

// Read reads a MongoDB collection and returns a PCollection<T> for a given type T. T must be a
// struct with exported fields that should have a "bson" tag. By default, the transform uses the
// MongoDB internal splitVector command to split the collection into bundles. The transform can be
// configured to use the $bucketAuto aggregation instead to support reading from MongoDB Atlas
// where the splitVector command is not allowed. This is enabled by passing the ReadOptionFn
// WithReadBucketAuto(true).
//
// The Read transform has the required parameters:
//   - s: the scope of the pipeline
//   - uri: the MongoDB connection string
//   - database: the MongoDB database to read from
//   - collection: the MongoDB collection to read from
//   - t: the type of the elements in the collection
//
// The Read transform takes a variadic number of ReadOptionFn which can set the ReadOption fields:
//   - BucketAuto: whether to use the bucketAuto aggregation to split the collection into bundles.
//     Defaults to false
//   - Filter: a bson.M map that is used to filter the documents in the collection. Defaults to nil,
//     which means no filter is applied
//   - BundleSize: the size in bytes to bundle the documents into when reading. Defaults to
//     64 * 1024 * 1024 (64 MB)
func Read(
	s beam.Scope,
	uri string,
	database string,
	collection string,
	t reflect.Type,
	opts ...ReadOptionFn,
) beam.PCollection {
	s = s.Scope("mongodbio.Read")

	option := &ReadOption{
		BundleSize: defaultReadBundleSize,
	}

	for _, opt := range opts {
		if err := opt(option); err != nil {
			panic(fmt.Sprintf("mongodbio.Read: invalid option: %v", err))
		}
	}

	imp := beam.Impulse(s)

	return beam.ParDo(
		s,
		newReadFn(uri, database, collection, t, option),
		imp,
		beam.TypeDefinition{Var: beam.YType, T: t},
	)
}

type readFn struct {
	mongoDBFn
	BucketAuto bool
	BundleSize int64
	Filter     []byte
	Type       beam.EncodedType
	filter     bson.M
	projection bson.D
}

func newReadFn(
	uri string,
	database string,
	collection string,
	t reflect.Type,
	option *ReadOption,
) *readFn {
	filter, err := encodeBSON[bson.M](option.Filter)
	if err != nil {
		panic(fmt.Sprintf("mongodbio.newReadFn: %v", err))
	}

	return &readFn{
		mongoDBFn: mongoDBFn{
			URI:        uri,
			Database:   database,
			Collection: collection,
		},
		BucketAuto: option.BucketAuto,
		BundleSize: option.BundleSize,
		Filter:     filter,
		Type:       beam.EncodedType{T: t},
	}
}

func (fn *readFn) Setup(ctx context.Context) error {
	var err error
	if err = fn.mongoDBFn.Setup(ctx); err != nil {
		return err
	}

	fn.filter, err = decodeBSON[bson.M](fn.Filter)
	if err != nil {
		return err
	}

	fn.projection = inferProjection(fn.Type.T, bsonTag)

	return nil
}

func inferProjection(t reflect.Type, tagKey string) bson.D {
	names := structx.InferFieldNames(t, tagKey)
	if len(names) == 0 {
		panic("mongodbio.inferProjection: no names to infer projection from")
	}

	projection := make(bson.D, len(names))

	for i, name := range names {
		projection[i] = bson.E{Key: name, Value: 1}
	}

	return projection
}

func (fn *readFn) CreateInitialRestriction(
	ctx context.Context,
	_ []byte,
) (idRangeRestriction, error) {
	if err := fn.Setup(ctx); err != nil {
		return idRangeRestriction{}, err
	}

	outerRange, err := findOuterIDRange(ctx, fn.collection, fn.filter)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Infof(
				ctx,
				"No documents in collection %s.%s match the provided filter",
				fn.Database,
				fn.Collection,
			)
			return idRangeRestriction{}, nil
		}

		return idRangeRestriction{}, err
	}

	return newIDRangeRestriction(
		ctx,
		fn.collection,
		outerRange,
		fn.filter,
	), nil
}

func findOuterIDRange(
	ctx context.Context,
	collection *mongo.Collection,
	filter bson.M,
) (idRange, error) {
	minID, err := findID(ctx, collection, filter, 1, 0)
	if err != nil {
		return idRange{}, err
	}

	maxID, err := findID(ctx, collection, filter, -1, 0)
	if err != nil {
		return idRange{}, err
	}

	outerRange := idRange{
		Min:          minID,
		MinInclusive: true,
		Max:          maxID,
		MaxInclusive: true,
	}

	return outerRange, nil
}

func (fn *readFn) SplitRestriction(
	ctx context.Context,
	_ []byte,
	rest idRangeRestriction,
) ([]idRangeRestriction, error) {
	if rest.Count == 0 {
		return []idRangeRestriction{rest}, nil
	}

	if err := fn.Setup(ctx); err != nil {
		return nil, err
	}

	splits, err := rest.SizedSplits(ctx, fn.collection, fn.BundleSize, fn.BucketAuto)
	if err != nil {
		return nil, err
	}

	return splits, nil
}

func (fn *readFn) CreateTracker(rest idRangeRestriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(newIDRangeTracker(rest, fn.collection))
}

func (fn *readFn) RestrictionSize(_ []byte, rest idRangeRestriction) float64 {
	return float64(rest.Count)
}

func (fn *readFn) ProcessElement(
	ctx context.Context,
	rt *sdf.LockRTracker,
	_ []byte,
	emit func(beam.Y),
) (err error) {
	rest := rt.GetRestriction().(idRangeRestriction)

	cursor, err := fn.getCursor(ctx, rest.Filter())
	if err != nil {
		return err
	}

	defer func() {
		closeErr := cursor.Close(ctx)

		if err != nil {
			if closeErr != nil {
				log.Errorf(ctx, "error closing cursor: %v", closeErr)
			}
			return
		}

		err = closeErr
	}()

	for cursor.Next(ctx) {
		id, value, err := decodeDocument(cursor, fn.Type.T)
		if err != nil {
			return err
		}

		result := cursorResult{nextID: id}
		if !rt.TryClaim(result) {
			return cursor.Err()
		}

		emit(value)
	}

	result := cursorResult{isExhausted: true}
	rt.TryClaim(result)

	return cursor.Err()
}

func (fn *readFn) getCursor(
	ctx context.Context,
	filter bson.M,
) (*mongo.Cursor, error) {
	opts := options.Find().
		SetProjection(fn.projection).
		SetSort(bson.M{"_id": 1})

	cursor, err := fn.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("error executing find command: %w", err)
	}

	return cursor, nil
}

func decodeDocument(cursor *mongo.Cursor, t reflect.Type) (id any, value any, err error) {
	var docID documentID
	if err := cursor.Decode(&docID); err != nil {
		return nil, nil, fmt.Errorf("error decoding document ID: %w", err)
	}

	out := reflect.New(t).Interface()
	if err := cursor.Decode(out); err != nil {
		return nil, nil, fmt.Errorf("error decoding document: %w", err)
	}

	value = reflect.ValueOf(out).Elem().Interface()

	return docID.ID, value, nil
}
