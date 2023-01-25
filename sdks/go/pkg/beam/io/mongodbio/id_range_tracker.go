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
	"go.mongodb.org/mongo-driver/mongo"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*idRangeTracker)(nil)))
}

// idRangeTracker is a tracker of an idRangeRestriction.
type idRangeTracker struct {
	rest       idRangeRestriction
	collection *mongo.Collection
	claimed    int64
	claimedID  any
	stopped    bool
	err        error
}

// newIDRangeTracker creates a new idRangeTracker tracking the provided idRangeRestriction.
func newIDRangeTracker(rest idRangeRestriction, collection *mongo.Collection) *idRangeTracker {
	return &idRangeTracker{
		rest:       rest,
		collection: collection,
	}
}

// cursorResult holds information about the next document to process from MongoDB. nextID is the ID
// of the document. isExhausted is whether the cursor has been exhausted.
type cursorResult struct {
	nextID      any
	isExhausted bool
}

// TryClaim accepts a position representing a cursorResult of a document to read from MongoDB. The
// position is successfully claimed if the tracker has not yet completed the work within its
// restriction and the cursor has not been exhausted.
func (rt *idRangeTracker) TryClaim(pos any) (ok bool) {
	result, ok := pos.(cursorResult)
	if !ok {
		rt.err = fmt.Errorf("invalid pos type: %T", pos)
		return false
	}

	if rt.IsDone() {
		return false
	}

	if result.isExhausted {
		rt.stopped = true
		return false
	}

	rt.claimed++
	rt.claimedID = result.nextID

	return true
}

// GetError returns the error associated with the tracker, if any.
func (rt *idRangeTracker) GetError() error {
	return rt.err
}

// TrySplit splits the underlying restriction into a primary and residual restriction based on the
// fraction of remaining work the primary should be responsible for. The restriction may be modified
// as a result of the split. The primary is a copy of the tracker's restriction after the split.
// If the fraction is 1 or all work has already been claimed, returns the full restriction as the
// primary and nil as the residual. If the fraction is 0, stops the tracker, cuts off any remaining
// work from its underlying restriction, and returns a residual representing all remaining work.
// If the fraction is between 0 and 1, attempts to split the remaining work of the underlying
// restriction into two sub-restrictions based on the fraction and assigns them to the primary and
// residual respectively. Returns an error if the split cannot be performed.
func (rt *idRangeTracker) TrySplit(fraction float64) (primary, residual any, err error) {
	if fraction < 0 || fraction > 1 {
		return nil, nil, errors.New("fraction must be between 0 and 1")
	}

	done, remaining := rt.cutRestriction()

	if fraction == 1 || remaining.Count == 0 {
		return rt.rest, nil, nil
	}

	if fraction == 0 {
		rt.rest = done
		return rt.rest, remaining, nil
	}

	ctx := context.Background()

	primaryRem, resid, err := remaining.FractionSplits(ctx, rt.collection, fraction)
	if err != nil {
		return nil, nil, err
	}

	if resid.Count == 0 {
		return rt.rest, nil, nil
	}

	if primaryRem.Count == 0 {
		rt.rest = done
		return rt.rest, remaining, nil
	}

	rt.rest.IDRange.Max = primaryRem.IDRange.Max
	rt.rest.IDRange.MaxInclusive = primaryRem.IDRange.MaxInclusive
	rt.rest.Count -= resid.Count

	return rt.rest, resid, nil
}

// cutRestriction returns two restrictions: done represents the amount of work from the underlying
// restriction that has already been completed, and remaining represents the amount that remains to
// be processed. Does not modify the underlying restriction.
func (rt *idRangeTracker) cutRestriction() (done idRangeRestriction, remaining idRangeRestriction) {
	minRem := rt.claimedID
	minInclusiveRem := false
	maxInclusiveDone := true

	if minRem == nil {
		minRem = rt.rest.IDRange.Min
		minInclusiveRem = rt.rest.IDRange.MinInclusive
		maxInclusiveDone = false
	}

	done = idRangeRestriction{
		IDRange: idRange{
			Min:          rt.rest.IDRange.Min,
			MinInclusive: rt.rest.IDRange.MinInclusive,
			Max:          minRem,
			MaxInclusive: maxInclusiveDone,
		},
		CustomFilter: rt.rest.CustomFilter,
		Count:        rt.claimed,
	}

	remaining = idRangeRestriction{
		IDRange: idRange{
			Min:          minRem,
			MinInclusive: minInclusiveRem,
			Max:          rt.rest.IDRange.Max,
			MaxInclusive: rt.rest.IDRange.MaxInclusive,
		},
		CustomFilter: rt.rest.CustomFilter,
		Count:        rt.rest.Count - rt.claimed,
	}

	return done, remaining
}

// GetProgress returns the amount of done and remaining work, represented by the count of documents.
func (rt *idRangeTracker) GetProgress() (done float64, remaining float64) {
	done = float64(rt.claimed)
	remaining = float64(rt.rest.Count - rt.claimed)
	return
}

// IsDone returns true if all work within the tracker's restriction has been completed.
func (rt *idRangeTracker) IsDone() bool {
	return rt.stopped || rt.claimed == rt.rest.Count
}

// GetRestriction returns a copy of the restriction the tracker is tracking.
func (rt *idRangeTracker) GetRestriction() any {
	return rt.rest
}

// IsBounded returns whether the tracker is tracking a restriction with a finite amount of work.
func (*idRangeTracker) IsBounded() bool {
	return true
}
