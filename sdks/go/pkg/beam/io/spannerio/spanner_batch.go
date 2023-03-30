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

// Package spannerio provides an API for reading and writing resouces to
// Google Spanner datastores.
package spannerio

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"google.golang.org/api/iterator"
	"reflect"
)

func init() {
	register.DoFn3x1[context.Context, *PartitionedRead, func(beam.X), error]((*queryBatchFn)(nil))
	register.Emitter1[beam.X]()
	register.DoFn3x1[context.Context, []byte, func(read *PartitionedRead), error]((*generatePartitionsFn)(nil))
	register.Emitter1[*PartitionedRead]()
}

// Options when Batch Querying
type queryBatchOptions struct {
	MaxPartitions  int64                  `json:"maxPartitions"`  // Maximum partitions
	TimestampBound spanner.TimestampBound `json:"timestampBound"` // The TimestampBound to use for batched reading
}

// UseMaxPartitions sets the maximum number of Partitions to split the query into
func UseMaxPartitions(maxPartitions int64) func(opts *queryBatchOptions) error {
	return func(opts *queryBatchOptions) error {
		opts.MaxPartitions = maxPartitions
		return nil
	}
}

// UseTimestampBound sets the TimestampBound to use when doing batched reads.
func UseTimestampBound(timestampBound spanner.TimestampBound) func(opts *queryBatchOptions) error {
	return func(opts *queryBatchOptions) error {
		opts.TimestampBound = timestampBound
		return nil
	}
}

type queryBatchFn struct {
	Db      *SpannerDatabase  `json:"db"`      // Spanner database
	Type    beam.EncodedType  `json:"type"`    // Type is the encoded schema type.
	Options queryBatchOptions `json:"options"` // Options specifies additional query execution options.
}

// QueryBatch executes a query using Spanners ability to do batched queries.
// The output must have a schema compatible with the given type, t. It returns a PCollection<t>.
func QueryBatch(s beam.Scope, db *SpannerDatabase, query string, t reflect.Type, options ...func(*queryBatchOptions) error) beam.PCollection {
	if db == nil {
		panic("spanner.QueryBatch no database provided!")
	}

	s = s.Scope("spanner.QueryBatch")

	opts := queryBatchOptions{}
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			panic(err)
		}
	}

	partitions := GeneratePartitions(s, db, query, options...)

	return beam.ParDo(
		s,
		&queryBatchFn{
			Db:      db,
			Type:    beam.EncodedType{T: t},
			Options: opts,
		},
		partitions,
		beam.TypeDefinition{Var: beam.XType, T: t},
	)
}

func (f *queryBatchFn) Setup(ctx context.Context) error {
	return f.Db.Setup(ctx)
}

func (f *queryBatchFn) Teardown() {
	f.Db.Close()
}

func (f *queryBatchFn) ProcessElement(ctx context.Context, read *PartitionedRead, emit func(beam.X)) error {
	txn := f.Db.Client.BatchReadOnlyTransactionFromID(read.BatchTransactionId)
	iter := txn.Execute(ctx, read.Partition)
	defer iter.Stop()

	for {
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

type generatePartitionsFn struct {
	Db      *SpannerDatabase  `json:"db"`      // Spanner database
	Query   string            `json:"query"`   // Table is the table identifier.
	Options queryBatchOptions `json:"options"` // Options specifies additional query execution options.
}

// PartitionedRead holds relevant partition information to support partitioned reading from Spanner.
type PartitionedRead struct {
	BatchTransactionId spanner.BatchReadOnlyTransactionID `json:"batchTransactionId"` // The Spanner Batch Transaction Id
	Partition          *spanner.Partition                 `json:"partition"`          // The Spanner Partition to read from
}

// NewPartitionedRead constructs a new PartitionedRead.
func NewPartitionedRead(batchTransactionId spanner.BatchReadOnlyTransactionID, partition *spanner.Partition) *PartitionedRead {
	return &PartitionedRead{
		BatchTransactionId: batchTransactionId,
		Partition:          partition,
	}
}

func (f *generatePartitionsFn) Setup(ctx context.Context) error {
	return f.Db.Setup(ctx)
}

func (f *generatePartitionsFn) Teardown() {
	f.Db.Close()
}

func partitionOptions(options queryBatchOptions) spanner.PartitionOptions {
	partitionOptions := spanner.PartitionOptions{}

	if options.MaxPartitions != 0 {
		partitionOptions.MaxPartitions = options.MaxPartitions
	}

	return partitionOptions
}

// GeneratePartitions generates read partitions to support batched reading from Spanner.
func GeneratePartitions(s beam.Scope, db *SpannerDatabase, query string, options ...func(*queryBatchOptions) error) beam.PCollection {
	if db == nil {
		panic("spanner.GeneratePartitions no database provided!")
	}

	s.Scope("spanner.GeneratePartitions")

	opts := queryBatchOptions{}
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			panic(err)
		}
	}

	imp := beam.Impulse(s)
	return beam.ParDo(
		s,
		&generatePartitionsFn{
			Db:      db,
			Query:   query,
			Options: opts,
		},
		imp,
	)
}

func (f *generatePartitionsFn) ProcessElement(ctx context.Context, _ []byte, emit func(*PartitionedRead)) error {
	txn, err := f.Db.Client.BatchReadOnlyTransaction(ctx, f.Options.TimestampBound)
	if err != nil {
		panic("spanner.QueryBatch: unable to create batch read only transaction: " + err.Error())
	}
	defer txn.Close()

	partitions, err := txn.PartitionQuery(ctx, spanner.Statement{SQL: f.Query}, partitionOptions(f.Options))
	if err != nil {
		panic(fmt.Sprintf("spanner.QueryBatch: unable to partition query: %v", err))
	}

	for _, p := range partitions {
		emit(NewPartitionedRead(txn.ID, p))
	}

	return nil
}
