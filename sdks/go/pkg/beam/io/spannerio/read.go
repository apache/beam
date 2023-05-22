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
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/structx"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"google.golang.org/api/iterator"
)

// spannerTag is the struct tag key used to identify Spanner field names.
const (
	spannerTag = "spanner"
)

func init() {
	register.DoFn3x1[context.Context, []byte, func(beam.X), error]((*queryFn)(nil))
	register.Emitter1[beam.X]()
}

// Read reads all rows from the given spanner table. It returns a PCollection<t> for a given type T.
// T must be a struct with exported fields that have the "spanner" tag. If the
// table has more rows than t, then Read is implicitly a projection.
func Read(s beam.Scope, db string, table string, t reflect.Type) beam.PCollection {
	if db == "" {
		panic("no database provided!")
	}

	cols := strings.Join(structx.InferFieldNames(t, spannerTag), ",")

	return query(s, db, fmt.Sprintf("SELECT %v from %v", cols, table), t, newQueryOptions())
}

// Query executes a spanner query. It returns a PCollection<t> for a given type T. T must be a struct with exported
// fields that have the "spanner" tag. By default, the transform uses spanners partitioned read ability to split
// the results into bundles.
// If the underlying query is not root-partitionable you can disable batching via UseBatching.
func Query(s beam.Scope, db string, q string, t reflect.Type, options ...QueryOptionFn) beam.PCollection {
	queryOptions := newQueryOptions(options...)

	if db == "" {
		panic("no database provided!")
	}

	if queryOptions.Batching {
		return readBatch(s, db, q, t, queryOptions)
	} else {
		return query(s, db, q, t, queryOptions)
	}
}

func query(s beam.Scope, db string, query string, t reflect.Type, options queryOptions) beam.PCollection {
	s = s.Scope("spanner.Query")

	if options.TimestampBound != (spanner.TimestampBound{}) {
		panic("spannerio.Query: specifying timestamp bound for non-batched reads not currently supported.")
	}

	imp := beam.Impulse(s)

	return beam.ParDo(s, newQueryFn(db, query, t, options), imp, beam.TypeDefinition{Var: beam.XType, T: t})
}

type queryFn struct {
	spannerFn
	Query   string           `json:"query"`   // Table is the table identifier.
	Type    beam.EncodedType `json:"type"`    // Type is the encoded schema type.
	Options queryOptions     `json:"options"` // Options specifies additional query execution options.
}

func newQueryFn(
	db string,
	query string,
	t reflect.Type,
	options queryOptions,
) *queryFn {
	return &queryFn{spannerFn: newSpannerFn(db), Query: query, Type: beam.EncodedType{T: t}, Options: options}
}

func (f *queryFn) Setup(ctx context.Context) error {
	return f.spannerFn.Setup(ctx)
}

func (f *queryFn) Teardown() {
	f.spannerFn.Teardown()
}

func (f *queryFn) ProcessElement(ctx context.Context, _ []byte, emit func(beam.X)) error {
	stmt := spanner.Statement{SQL: f.Query}
	it := f.client.Single().Query(ctx, stmt)
	defer it.Stop()

	for {
		val := reflect.New(f.Type.T).Interface() // val : *T
		row, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		if err := row.ToStruct(val); err != nil {
			return err
		}

		emit(reflect.ValueOf(val).Elem().Interface()) // emit(*val)
	}
	return nil
}
