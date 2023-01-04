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
	"math"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/structx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	defaultReadBundleSize = 64 * 1024 * 1024

	minSplitVectorChunkSize = 1024 * 1024
	maxSplitVectorChunkSize = 1024 * 1024 * 1024

	maxBucketCount = math.MaxInt32
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(bson.M), error](&bucketAutoFn{})
	register.DoFn3x1[context.Context, []byte, func(bson.M), error](&splitVectorFn{})
	register.Emitter1[bson.M]()

	register.DoFn3x1[context.Context, bson.M, func(beam.Y), error](&readFn{})
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

	var bundled beam.PCollection

	if option.BucketAuto {
		bundled = beam.ParDo(s, newBucketAutoFn(uri, database, collection, option), imp)
	} else {
		bundled = beam.ParDo(s, newSplitVectorFn(uri, database, collection, option), imp)
	}

	return beam.ParDo(
		s,
		newReadFn(uri, database, collection, t, option),
		bundled,
		beam.TypeDefinition{Var: beam.YType, T: t},
	)
}

type bucketAutoFn struct {
	mongoDBFn
	BundleSize int64
}

func newBucketAutoFn(
	uri string,
	database string,
	collection string,
	option *ReadOption,
) *bucketAutoFn {
	return &bucketAutoFn{
		mongoDBFn: mongoDBFn{
			URI:        uri,
			Database:   database,
			Collection: collection,
		},
		BundleSize: option.BundleSize,
	}
}

func (fn *bucketAutoFn) ProcessElement(
	ctx context.Context,
	_ []byte,
	emit func(bson.M),
) error {
	collectionSize, err := fn.getCollectionSize(ctx)
	if err != nil {
		return err
	}

	if collectionSize == 0 {
		return nil
	}

	bucketCount := calculateBucketCount(collectionSize, fn.BundleSize)

	buckets, err := fn.getBuckets(ctx, bucketCount)
	if err != nil {
		return err
	}

	idFilters := idFiltersFromBuckets(buckets)

	for _, filter := range idFilters {
		emit(filter)
	}

	return nil
}

type collStats struct {
	Size int64 `bson:"size"`
}

func (fn *bucketAutoFn) getCollectionSize(ctx context.Context) (int64, error) {
	cmd := bson.M{"collStats": fn.Collection}
	opts := options.RunCmd().SetReadPreference(readpref.Primary())

	var stats collStats
	if err := fn.collection.Database().RunCommand(ctx, cmd, opts).Decode(&stats); err != nil {
		return 0, fmt.Errorf("error executing collStats command: %w", err)
	}

	return stats.Size, nil
}

func calculateBucketCount(collectionSize int64, bundleSize int64) int32 {
	if bundleSize < 0 {
		panic("monogdbio.calculateBucketCount: bundle size must be greater than 0")
	}

	count := collectionSize / bundleSize
	if collectionSize%bundleSize != 0 {
		count++
	}

	if count > int64(maxBucketCount) {
		count = maxBucketCount
	}

	return int32(count)
}

type bucket struct {
	ID minMax `bson:"_id"`
}

type minMax struct {
	Min any `bson:"min"`
	Max any `bson:"max"`
}

func (fn *bucketAutoFn) getBuckets(ctx context.Context, count int32) ([]bucket, error) {
	pipeline := mongo.Pipeline{bson.D{{
		Key: "$bucketAuto",
		Value: bson.M{
			"groupBy": "$_id",
			"buckets": count,
		},
	}}}

	cursor, err := fn.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("error executing bucketAuto aggregation: %w", err)
	}

	var buckets []bucket
	if err = cursor.All(ctx, &buckets); err != nil {
		return nil, fmt.Errorf("error decoding buckets: %w", err)
	}

	return buckets, nil
}

func idFiltersFromBuckets(buckets []bucket) []bson.M {
	idFilters := make([]bson.M, len(buckets))

	for i := 0; i < len(buckets); i++ {
		filter := bson.M{}

		if i != 0 {
			filter["$gt"] = buckets[i].ID.Min
		}

		if i != len(buckets)-1 {
			filter["$lte"] = buckets[i].ID.Max
		}

		if len(filter) == 0 {
			idFilters[i] = filter
		} else {
			idFilters[i] = bson.M{"_id": filter}
		}
	}

	return idFilters
}

type splitVectorFn struct {
	mongoDBFn
	BundleSize int64
}

func newSplitVectorFn(
	uri string,
	database string,
	collection string,
	option *ReadOption,
) *splitVectorFn {
	return &splitVectorFn{
		mongoDBFn: mongoDBFn{
			URI:        uri,
			Database:   database,
			Collection: collection,
		},
		BundleSize: option.BundleSize,
	}
}

func (fn *splitVectorFn) ProcessElement(
	ctx context.Context,
	_ []byte,
	emit func(bson.M),
) error {
	chunkSize := getChunkSize(fn.BundleSize)

	splitKeys, err := fn.getSplitKeys(ctx, chunkSize)
	if err != nil {
		return err
	}

	idFilters := idFiltersFromSplits(splitKeys)

	for _, filter := range idFilters {
		emit(filter)
	}

	return nil
}

func getChunkSize(bundleSize int64) int64 {
	var chunkSize int64

	if bundleSize < minSplitVectorChunkSize {
		chunkSize = minSplitVectorChunkSize
	} else if bundleSize > maxSplitVectorChunkSize {
		chunkSize = maxSplitVectorChunkSize
	} else {
		chunkSize = bundleSize
	}

	return chunkSize
}

type splitVector struct {
	SplitKeys []splitKey `bson:"splitKeys"`
}

type splitKey struct {
	ID any `bson:"_id"`
}

func (fn *splitVectorFn) getSplitKeys(ctx context.Context, chunkSize int64) ([]splitKey, error) {
	cmd := bson.D{
		{Key: "splitVector", Value: fmt.Sprintf("%s.%s", fn.Database, fn.Collection)},
		{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "maxChunkSizeBytes", Value: chunkSize},
	}

	opts := options.RunCmd().SetReadPreference(readpref.Primary())

	var vector splitVector
	if err := fn.collection.Database().RunCommand(ctx, cmd, opts).Decode(&vector); err != nil {
		return nil, fmt.Errorf("error executing splitVector command: %w", err)
	}

	return vector.SplitKeys, nil
}

func idFiltersFromSplits(splitKeys []splitKey) []bson.M {
	idFilters := make([]bson.M, len(splitKeys)+1)

	for i := 0; i < len(splitKeys)+1; i++ {
		filter := bson.M{}

		if i > 0 {
			filter["$gt"] = splitKeys[i-1].ID
		}

		if i < len(splitKeys) {
			filter["$lte"] = splitKeys[i].ID
		}

		if len(filter) == 0 {
			idFilters[i] = filter
		} else {
			idFilters[i] = bson.M{"_id": filter}
		}
	}

	return idFilters
}

type readFn struct {
	mongoDBFn
	Filter     []byte
	Type       beam.EncodedType
	projection bson.D
	filter     bson.M
}

func newReadFn(
	uri string,
	database string,
	collection string,
	t reflect.Type,
	option *ReadOption,
) *readFn {
	filter, err := encodeBSONMap(option.Filter)
	if err != nil {
		panic(fmt.Sprintf("mongodbio.newReadFn: %v", err))
	}

	return &readFn{
		mongoDBFn: mongoDBFn{
			URI:        uri,
			Database:   database,
			Collection: collection,
		},
		Filter: filter,
		Type:   beam.EncodedType{T: t},
	}
}

func (fn *readFn) Setup(ctx context.Context) error {
	if err := fn.mongoDBFn.Setup(ctx); err != nil {
		return err
	}

	filter, err := decodeBSONMap(fn.Filter)
	if err != nil {
		return err
	}

	fn.filter = filter
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

func (fn *readFn) ProcessElement(
	ctx context.Context,
	elem bson.M,
	emit func(beam.Y),
) (err error) {
	mergedFilter := mergeFilters(elem, fn.filter)

	cursor, err := fn.findDocuments(ctx, fn.projection, mergedFilter)
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
		value, err := decodeDocument(cursor, fn.Type.T)
		if err != nil {
			return err
		}

		emit(value)
	}

	return cursor.Err()
}

func mergeFilters(idFilter bson.M, customFilter bson.M) bson.M {
	if len(idFilter) == 0 {
		return customFilter
	}

	if len(customFilter) == 0 {
		return idFilter
	}

	return bson.M{
		"$and": []bson.M{idFilter, customFilter},
	}
}

func (fn *readFn) findDocuments(
	ctx context.Context,
	projection bson.D,
	filter bson.M,
) (*mongo.Cursor, error) {
	opts := options.Find().SetProjection(projection).SetAllowDiskUse(true)

	cursor, err := fn.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("error finding documents: %w", err)
	}

	return cursor, nil
}

func decodeDocument(cursor *mongo.Cursor, t reflect.Type) (any, error) {
	out := reflect.New(t).Interface()
	if err := cursor.Decode(out); err != nil {
		return nil, fmt.Errorf("error decoding document: %w", err)
	}

	value := reflect.ValueOf(out).Elem().Interface()

	return value, nil
}
