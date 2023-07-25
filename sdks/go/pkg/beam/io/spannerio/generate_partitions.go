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
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"reflect"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(read partitionedRead), error]((*generatePartitionsFn)(nil))
	register.Emitter1[*partitionedRead]()
}

type generatePartitionsFn struct {
	spannerFn
	Query   string       `json:"query"`   // Table is the table identifier.
	Options queryOptions `json:"options"` // Options specifies additional query execution options.
}

func generatePartitions(s beam.Scope, db string, query string, t reflect.Type, options queryOptions) beam.PCollection {
	s = s.Scope("spannerio.GeneratePartitions")

	imp := beam.Impulse(s)
	return beam.ParDo(s, newGeneratePartitionsFn(db, query, options), imp)
}

func (f *generatePartitionsFn) Setup(ctx context.Context) error {
	return f.spannerFn.Setup(ctx)
}

func (f *generatePartitionsFn) Teardown() {
	f.spannerFn.Teardown()
}

func partitionOptions(options queryOptions) spanner.PartitionOptions {
	partitionOptions := spanner.PartitionOptions{}

	if options.MaxPartitions != 0 {
		partitionOptions.MaxPartitions = options.MaxPartitions
	}

	return partitionOptions
}

func newGeneratePartitionsFn(
	db string,
	query string,
	options queryOptions,
) *generatePartitionsFn {
	if db == "" {
		panic("no database provided")
	}

	return &generatePartitionsFn{
		spannerFn: newSpannerFn(db),
		Query:     query,
		Options:   options,
	}
}

func (f *generatePartitionsFn) ProcessElement(ctx context.Context, _ []byte, emit func(partitionedRead)) error {
	txn, err := f.client.BatchReadOnlyTransaction(ctx, f.Options.TimestampBound)
	if err != nil {
		panic("unable to create batch read only transaction: " + err.Error())
	}
	defer txn.Close()

	mode := spannerpb.ExecuteSqlRequest_PROFILE

	partitions, err := txn.PartitionQueryWithOptions(ctx, spanner.Statement{SQL: f.Query}, partitionOptions(f.Options), spanner.QueryOptions{Mode: &mode})
	if err != nil {
		panic(fmt.Sprintf("unable to partition query: %v", err))
	}

	for _, p := range partitions {
		emit(newPartitionedRead(txn.ID, p))
	}

	return nil
}
