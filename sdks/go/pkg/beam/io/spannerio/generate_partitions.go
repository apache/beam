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

// Package spannerio provides an API for reading and writing resources to
// Google Spanner datastores.
package spannerio

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(read *PartitionedRead), error]((*generatePartitionsFn)(nil))
	register.Emitter1[*PartitionedRead]()
}

type generatePartitionsFn struct {
	Db      *SpannerDatabase  `json:"db"`      // Spanner database
	Query   string            `json:"query"`   // Table is the table identifier.
	Options queryBatchOptions `json:"options"` // Options specifies additional query execution options.
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
