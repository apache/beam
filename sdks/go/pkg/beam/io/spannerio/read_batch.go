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

package spannerio

import (
	"context"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"google.golang.org/api/iterator"
)

func init() {
	register.DoFn4x1[context.Context, *sdf.LockRTracker, partitionedRead, func(beam.X), error]((*readBatchFn)(nil))
	register.Emitter1[beam.X]()
}

type readBatchFn struct {
	spannerFn
	Type    beam.EncodedType
	Options queryOptions
}

func newReadBatchFn(db string, t reflect.Type, options queryOptions) *readBatchFn {
	return &readBatchFn{
		spannerFn: newSpannerFn(db),
		Type:      beam.EncodedType{T: t},
		Options:   options,
	}
}

func readBatch(s beam.Scope, db string, query string, t reflect.Type, options queryOptions) beam.PCollection {
	partitions := generatePartitions(s, db, query, t, options)

	s = s.Scope("spannerio.ReadBatch")

	return beam.ParDo(
		s,
		newReadBatchFn(db, t, options),
		partitions,
		beam.TypeDefinition{Var: beam.XType, T: t},
	)
}

func (f *readBatchFn) Setup(ctx context.Context) error {
	return f.spannerFn.Setup(ctx)
}

// CreateInitialRestriction creates an offset range restriction representing
// the number of rows in the partition..
func (f *readBatchFn) CreateInitialRestriction(read partitionedRead) offsetrange.Restriction {
	txn := f.client.BatchReadOnlyTransactionFromID(read.BatchTransactionId)
	iter := txn.Execute(context.Background(), read.Partition)
	defer iter.Stop()

	return offsetrange.Restriction{
		Start: 0,
		End:   iter.RowCount,
	}
}

const (
	blockSize = 10000
	tooSmall  = 100
)

// SplitRestriction splits each file restriction into blocks of a predetermined
// size, with some checks to avoid having small remainders.
func (f *readBatchFn) SplitRestriction(_ partitionedRead, rest offsetrange.Restriction) []offsetrange.Restriction {
	splits := rest.SizedSplits(blockSize)
	numSplits := len(splits)
	if numSplits > 1 {
		last := splits[numSplits-1]
		if last.End-last.Start <= tooSmall {
			// Last restriction is too small, so merge it with previous one.
			splits[numSplits-2].End = last.End
			splits = splits[:numSplits-1]
		}
	}
	return splits
}

// RestrictionSize returns the size of each restriction as its range.
func (f *readBatchFn) RestrictionSize(_ partitionedRead, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker creates sdf.LockRTrackers wrapping offsetRange.Trackers for
// each restriction.
func (f *readBatchFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

func (f *readBatchFn) Teardown() {
	f.spannerFn.Teardown()
}

func (f *readBatchFn) ProcessElement(ctx context.Context, rt *sdf.LockRTracker, read partitionedRead, emit func(beam.X)) error {
	rest := rt.GetRestriction().(offsetrange.Restriction)

	txn := f.client.BatchReadOnlyTransactionFromID(read.BatchTransactionId)
	iter := txn.Execute(ctx, read.Partition)
	defer iter.Stop()

	index := int64(0)
	for {
		if index == rest.Start {
			break
		}

		_, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}
	}

	for rt.TryClaim(index) {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		val := reflect.New(f.Type.T).Interface() // val : *T

		if err := row.ToStruct(val); err != nil {
			return err
		}

		emit(reflect.ValueOf(val).Elem().Interface()) // emit(*val)
	}

	return nil
}

// partitionedRead holds relevant partition information to support partitioned reading from Spanner.
type partitionedRead struct {
	BatchTransactionId spanner.BatchReadOnlyTransactionID `json:"batchTransactionId"` // The Spanner Batch Transaction Id
	Partition          *spanner.Partition                 `json:"partition"`          // The Spanner Partition to read from
}

// newPartitionedRead constructs a new PartitionedRead.
func newPartitionedRead(batchTransactionId spanner.BatchReadOnlyTransactionID, partition *spanner.Partition) partitionedRead {
	return partitionedRead{
		BatchTransactionId: batchTransactionId,
		Partition:          partition,
	}
}
