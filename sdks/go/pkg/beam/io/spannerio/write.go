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
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn2x1[context.Context, beam.X, error]((*writeFn)(nil))
}

type writeOptions struct {
	BatchSize int
}

// UseBatchSize explicitly sets the batch size per transaction for writes
func UseBatchSize(batchSize int) func(qo *writeOptions) error {
	return func(qo *writeOptions) error {
		qo.BatchSize = batchSize
		return nil
	}
}

// Write writes the elements of the given PCollection<T> to spanner. T is required
// to be the schema type.
func Write(s beam.Scope, db *SpannerDatabase, table string, col beam.PCollection, options ...func(*writeOptions) error) {
	if db == nil {
		panic("spanner.Write no database provided!")
	}

	if typex.IsCoGBK(col.Type()) || typex.IsKV(col.Type()) {
		panic("Unsupported collection type - only normal structs supported for writing.")
	}

	s = s.Scope("spanner.Write")

	writeOptions := writeOptions{
		BatchSize: 1000, // default
	}

	for _, opt := range options {
		if err := opt(&writeOptions); err != nil {
			panic(err)
		}
	}

	t := col.Type().Type()

	beam.ParDo0(s, &writeFn{Db: db, Table: table, Type: beam.EncodedType{T: t}, Options: writeOptions}, col)
}

type writeFn struct {
	Db        *SpannerDatabase `json:"db"`      // Spanner database
	Table     string           `json:"table"`   // The table to write to
	Type      beam.EncodedType `json:"type"`    // Type is the encoded schema type.
	Options   writeOptions     `json:"options"` // Spanner write options
	mutations []*spanner.Mutation
}

func (f *writeFn) Setup(ctx context.Context) error {
	return f.Db.Setup(ctx)
}

func (f *writeFn) Teardown() {
	f.Db.Close()
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
	_, err := f.Db.Client.Apply(ctx, f.mutations)
	if err != nil {
		return err
	}

	f.mutations = nil

	return nil
}
