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
	"math"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*idRangeRestriction)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*idRange)(nil)).Elem())
}

// idRangeRestriction represents a range of document IDs to read from MongoDB. IDRange holds
// information about the minimum and maximum IDs. CustomFilter is the custom filter to apply when
// reading from the collection. Count is the number of documents within the ID range that match the
// custom filter.
type idRangeRestriction struct {
	IDRange      idRange
	CustomFilter bson.M
	Count        int64
}

// newIDRangeRestriction creates a new idRangeRestriction and counts the documents within the ID
// range that match the custom filter.
func newIDRangeRestriction(
	ctx context.Context,
	collection *mongo.Collection,
	idRange idRange,
	filter bson.M,
) idRangeRestriction {
	mergedFilter := mergeFilters(idRange.Filter(), filter)

	count, err := collection.CountDocuments(ctx, mergedFilter)
	if err != nil {
		panic(err)
	}

	return idRangeRestriction{
		IDRange:      idRange,
		CustomFilter: filter,
		Count:        count,
	}
}

// Filter returns a bson.M filter based on the restriction's ID range and custom filter.
func (r idRangeRestriction) Filter() bson.M {
	idFilter := r.IDRange.Filter()
	return mergeFilters(idFilter, r.CustomFilter)
}

// mergeFilters merges the ID filter and the custom filter into a single bson.M filter.
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

// SizedSplits divides the restriction into sub-restrictions based on the desired bundle size in
// bytes.
func (r idRangeRestriction) SizedSplits(
	ctx context.Context,
	collection *mongo.Collection,
	bundleSize int64,
	useBucketAuto bool,
) ([]idRangeRestriction, error) {
	var idRanges []idRange
	var err error

	if useBucketAuto {
		idRanges, err = bucketAutoSplits(ctx, collection, r.IDRange, bundleSize)
	} else {
		idRanges, err = splitVectorSplits(ctx, collection, r.IDRange, bundleSize)
	}

	if err != nil {
		return nil, err
	}

	return restrictionsFromIDRanges(ctx, collection, idRanges, r.CustomFilter), err
}

// FractionSplits divides the restriction into a lower and higher ID sub-restriction based on the
// desired fraction of work the lower piece should be responsible for.
func (r idRangeRestriction) FractionSplits(
	ctx context.Context,
	collection *mongo.Collection,
	fraction float64,
) (lower, higher idRangeRestriction, err error) {
	skip := int64(math.Round(float64(r.Count) * fraction))

	splitID, err := findID(ctx, collection, r.Filter(), 1, skip)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return idRangeRestriction{}, idRangeRestriction{}, nil
		}

		return idRangeRestriction{}, idRangeRestriction{}, fmt.Errorf(
			"error finding document ID to split on: %w",
			err,
		)
	}

	lower = idRangeRestriction{
		IDRange: idRange{
			Min:          r.IDRange.Min,
			MinInclusive: r.IDRange.MinInclusive,
			Max:          splitID,
			MaxInclusive: false,
		},
		CustomFilter: r.CustomFilter,
		Count:        skip,
	}

	higher = idRangeRestriction{
		IDRange: idRange{
			Min:          splitID,
			MinInclusive: true,
			Max:          r.IDRange.Max,
			MaxInclusive: r.IDRange.MaxInclusive,
		},
		CustomFilter: r.CustomFilter,
		Count:        r.Count - skip,
	}

	return lower, higher, nil
}

// restrictionsFromIDRanges creates a slice of new restrictions based on the ID ranges.
func restrictionsFromIDRanges(
	ctx context.Context,
	collection *mongo.Collection,
	idRanges []idRange,
	customFilter bson.M,
) []idRangeRestriction {
	restrictions := make([]idRangeRestriction, len(idRanges))

	for i := 0; i < len(idRanges); i++ {
		rest := newIDRangeRestriction(
			ctx,
			collection,
			idRanges[i],
			customFilter,
		)
		restrictions[i] = rest
	}

	return restrictions
}

// idRange represents a range of document IDs in a MongoDB collection. It stores information about
// the minimum and maximum IDs, and whether they are inclusive or not.
type idRange struct {
	Min          any
	MinInclusive bool
	Max          any
	MaxInclusive bool
}

// Filter creates a bson.M filter representation of the idRange.
func (i idRange) Filter() bson.M {
	filter := make(bson.M, 2)

	if i.MinInclusive {
		filter["$gte"] = i.Min
	} else {
		filter["$gt"] = i.Min
	}

	if i.MaxInclusive {
		filter["$lte"] = i.Max
	} else {
		filter["$lt"] = i.Max
	}

	return bson.M{"_id": filter}
}
