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

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn2x1[context.Context, beam.X, error]((*writeFn)(nil))
}

// WriteOptionsFn is a function that can be passed to Write to configure options for writing to spanner.
type WriteOptionsFn func(qo *writeOptions) error

type writeOptions struct {
	BatchSize int
}

// UseBatchSize explicitly sets the batch size per transaction for writes.
func UseBatchSize(batchSize int) WriteOptionsFn {
	return func(qo *writeOptions) error {
		qo.BatchSize = batchSize
		return nil
	}
}

// Write writes the elements of the given PCollection<T> to spanner. T is required
// to be the schema type.
func Write(s beam.Scope, db string, table string, col beam.PCollection, options ...WriteOptionsFn) {
	if db == "" {
		panic("no database provided!")
	}

	if typex.IsCoGBK(col.Type()) || typex.IsKV(col.Type()) {
		panic("unsupported collection type - only normal structs supported for writing.")
	}

	s = s.Scope("spanner.Write")

	beam.ParDo0(s, newWriteFn(db, table, col.Type().Type(), options...), col)
}

type writeFn struct {
	spannerFn
	Table     string           `json:"table"`   // The table to write to
	Type      beam.EncodedType `json:"type"`    // Type is the encoded schema type.
	Options   writeOptions     `json:"options"` // Spanner write options
	mutations []*spanner.Mutation
}

func newWriteFn(db string, table string, t reflect.Type, options ...WriteOptionsFn) *writeFn {
	writeOptions := writeOptions{
		BatchSize: 1000, // default
	}

	for _, opt := range options {
		if err := opt(&writeOptions); err != nil {
			panic(err)
		}
	}

	return &writeFn{spannerFn: newSpannerFn(db), Table: table, Type: beam.EncodedType{T: t}, Options: writeOptions}
}

func (f *writeFn) Setup(ctx context.Context) error {
	return f.spannerFn.Setup(ctx)
}

func (f *writeFn) Teardown() {
	f.spannerFn.Teardown()
}

func (f *writeFn) ProcessElement(ctx context.Context, value beam.X) error {
	mutation, err := spanner.InsertOrUpdateStruct(f.Table, value)
	if err != nil {
		return err
	}

	f.mutations = append(f.mutations, mutation)

	if len(f.mutations)+1 > f.Options.BatchSize {
		f.flush(ctx)
	}

	return nil
}

func (f *writeFn) FinishBundle(ctx context.Context) error {
	if len(f.mutations) > 0 {
		return f.flush(ctx)
	}

	return nil
}

func (f *writeFn) flush(ctx context.Context) error {
	_, err := f.client.Apply(ctx, f.mutations)
	if err != nil {
		return err
	}

	f.mutations = nil

	return nil
}
