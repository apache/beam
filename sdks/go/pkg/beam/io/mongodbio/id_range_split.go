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

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	maxBucketCount          = math.MaxInt32
	minSplitVectorChunkSize = 1024 * 1024
	maxSplitVectorChunkSize = 1024 * 1024 * 1024
)

func bucketAutoSplits(
	ctx context.Context,
	collection *mongo.Collection,
	outerRange idRange,
	bundleSize int64,
) ([]idRange, error) {
	collSize, err := getCollectionSize(ctx, collection)
	if err != nil {
		return nil, err
	}

	bucketCount := calculateBucketCount(collSize, bundleSize)

	buckets, err := getBuckets(ctx, collection, outerRange.Filter(), bucketCount)
	if err != nil {
		return nil, err
	}

	return idRangesFromBuckets(buckets, outerRange), nil
}

func getCollectionSize(ctx context.Context, collection *mongo.Collection) (int64, error) {
	cmd := bson.M{"collStats": collection.Name()}
	opts := options.RunCmd().SetReadPreference(readpref.Primary())

	var stats struct {
		Size int64 `bson:"size"`
	}
	if err := collection.Database().RunCommand(ctx, cmd, opts).Decode(&stats); err != nil {
		return 0, fmt.Errorf("error executing collStats command: %w", err)
	}

	return stats.Size, nil
}

func calculateBucketCount(totalSize int64, bundleSize int64) int32 {
	if bundleSize < 0 {
		panic("monogdbio.calculateBucketCount: bundle size must be greater than 0")
	}

	count := totalSize / bundleSize
	if totalSize%bundleSize != 0 {
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

func getBuckets(
	ctx context.Context,
	collection *mongo.Collection,
	filter bson.M,
	count int32,
) ([]bucket, error) {
	pipeline := mongo.Pipeline{
		bson.D{{
			Key:   "$match",
			Value: filter,
		}},
		bson.D{{
			Key: "$bucketAuto",
			Value: bson.M{
				"groupBy": "$_id",
				"buckets": count,
			},
		}},
	}

	opts := options.Aggregate().SetAllowDiskUse(true)

	cursor, err := collection.Aggregate(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("error executing bucketAuto aggregation: %w", err)
	}

	var buckets []bucket
	if err := cursor.All(ctx, &buckets); err != nil {
		return nil, fmt.Errorf("error decoding buckets: %w", err)
	}

	return buckets, nil
}

func idRangesFromBuckets(buckets []bucket, outerRange idRange) []idRange {
	if len(buckets) == 0 {
		return nil
	}

	ranges := make([]idRange, len(buckets))

	for i := 0; i < len(buckets); i++ {
		subRange := idRange{}

		if i == 0 {
			subRange.MinInclusive = outerRange.MinInclusive
			subRange.Min = outerRange.Min
		} else {
			subRange.Min = buckets[i].ID.Min
			subRange.MinInclusive = true
		}

		if i == len(buckets)-1 {
			subRange.Max = outerRange.Max
			subRange.MaxInclusive = outerRange.MaxInclusive
		} else {
			subRange.Max = buckets[i].ID.Max
			subRange.MaxInclusive = false
		}

		ranges[i] = subRange
	}

	return ranges
}

func splitVectorSplits(
	ctx context.Context,
	collection *mongo.Collection,
	outerRange idRange,
	bundleSize int64,
) ([]idRange, error) {
	chunkSize := getChunkSize(bundleSize)

	splitKeys, err := getSplitKeys(ctx, collection, outerRange, chunkSize)
	if err != nil {
		return nil, err
	}

	return idRangesFromSplits(splitKeys, outerRange), nil
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

func getSplitKeys(
	ctx context.Context,
	collection *mongo.Collection,
	outerRange idRange,
	maxChunkSizeBytes int64,
) ([]documentID, error) {
	database := collection.Database()
	namespace := fmt.Sprintf("%s.%s", database.Name(), collection.Name())

	cmd := bson.D{
		{Key: "splitVector", Value: namespace},
		{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "min", Value: bson.D{{Key: "_id", Value: outerRange.Min}}},
		{Key: "max", Value: bson.D{{Key: "_id", Value: outerRange.Max}}},
		{Key: "maxChunkSizeBytes", Value: maxChunkSizeBytes},
	}

	opts := options.RunCmd().SetReadPreference(readpref.Primary())

	var result struct {
		SplitKeys []documentID `bson:"splitKeys"`
	}
	if err := database.RunCommand(ctx, cmd, opts).Decode(&result); err != nil {
		return nil, fmt.Errorf("error executing splitVector command: %w", err)
	}

	return result.SplitKeys, nil
}

func idRangesFromSplits(splitKeys []documentID, outerRange idRange) []idRange {
	subRanges := make([]idRange, len(splitKeys)+1)

	for i := 0; i < len(splitKeys)+1; i++ {
		subRange := idRange{}

		if i == 0 {
			subRange.Min = outerRange.Min
			subRange.MinInclusive = outerRange.MinInclusive
		} else {
			subRange.Min = splitKeys[i-1].ID
			subRange.MinInclusive = true
		}

		if i == len(splitKeys) {
			subRange.Max = outerRange.Max
			subRange.MaxInclusive = outerRange.MaxInclusive
		} else {
			subRange.Max = splitKeys[i].ID
			subRange.MaxInclusive = false
		}

		subRanges[i] = subRange
	}

	return subRanges
}
