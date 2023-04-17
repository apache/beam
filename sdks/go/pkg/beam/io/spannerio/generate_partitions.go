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
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(read *PartitionedRead), error]((*generatePartitionsFn)(nil))
	register.Emitter1[*PartitionedRead]()
}

type generatePartitionsFn struct {
	spannerFn
	Query     string            `json:"query"`   // Table is the table identifier.
	Options   queryBatchOptions `json:"options"` // Options specifies additional query execution options.
	generator partitionGenerator
}

func (f *generatePartitionsFn) Setup(ctx context.Context) error {
	err := f.spannerFn.Setup(ctx)

	if f.generator == nil {
		f.generator = newPartitionGenerator(f.client)
	}

	return err
}

func (f *generatePartitionsFn) Teardown() {
	f.spannerFn.Teardown()
}

func partitionOptions(options queryBatchOptions) spanner.PartitionOptions {
	partitionOptions := spanner.PartitionOptions{}

	if options.MaxPartitions != 0 {
		partitionOptions.MaxPartitions = options.MaxPartitions
	}

	return partitionOptions
}

// GeneratePartitions generates read partitions to support batched reading from Spanner.
func GeneratePartitions(s beam.Scope, db string, query string, options ...func(*queryBatchOptions) error) beam.PCollection {
	s.Scope("spanner.GeneratePartitions")

	fn := newGeneratePartitionsFn(db, query, options...)
	return fn.generatePartitions(s)
}

func newGeneratePartitionsFn(
	db string,
	query string,
	options ...func(*queryBatchOptions) error,
) *generatePartitionsFn {
	if db == "" {
		panic("no database provided")
	}

	opts := queryBatchOptions{}
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			panic(err)
		}
	}

	return &generatePartitionsFn{
		spannerFn: newSpannerFn(db),
		Query:     query,
		Options:   opts,
	}
}

func (f *generatePartitionsFn) generatePartitions(s beam.Scope) beam.PCollection {
	imp := beam.Impulse(s)
	return beam.ParDo(s, f, imp)
}

func (f *generatePartitionsFn) ProcessElement(ctx context.Context, _ []byte, emit func(*PartitionedRead)) error {
	txnId, partitions := f.generator.generate(ctx, f.Options.TimestampBound, f.Query, partitionOptions(f.Options))

	for _, p := range partitions {
		emit(NewPartitionedRead(txnId, p))
	}

	return nil
}

type partitionGenerator interface {
	generate(
		ctx context.Context,
		tb spanner.TimestampBound,
		query string,
		opts spanner.PartitionOptions,
	) (spanner.BatchReadOnlyTransactionID, []*spanner.Partition)
}

type partitionGeneratorImpl struct {
	client *spanner.Client
}

func newPartitionGenerator(client *spanner.Client) partitionGenerator {
	return &partitionGeneratorImpl{client}
}

func (g *partitionGeneratorImpl) generate(
	ctx context.Context,
	tb spanner.TimestampBound,
	query string,
	opts spanner.PartitionOptions,
) (spanner.BatchReadOnlyTransactionID, []*spanner.Partition) {
	txn, err := g.client.BatchReadOnlyTransaction(ctx, tb)
	if err != nil {
		panic("unable to create batch read only transaction: " + err.Error())
	}
	defer txn.Close()

	partitions, err := txn.PartitionQuery(ctx, spanner.Statement{SQL: query}, opts)
	if err != nil {
		panic(fmt.Sprintf("unable to partition query: %v", err))
	}

	return txn.ID, partitions
}
