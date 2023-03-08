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
	"context"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/structx"
	"google.golang.org/api/iterator"
)

// spannerTag is the struct tag key used to identify Spanner field names.
const spannerTag = "spanner"

func init() {
	register.DoFn3x1[context.Context, []byte, func(beam.X), error]((*queryFn)(nil))
	register.Emitter1[beam.X]()
	register.DoFn3x1[context.Context, int, func(*beam.X) bool, error]((*writeFn)(nil))
	register.Iter1[beam.X]()
}

// Read reads all rows from the given table. The table must have a schema
// compatible with the given type, t, and Read returns a PCollection<t>. If the
// table has more rows than t, then Read is implicitly a projection.
func Read(s beam.Scope, database string, table string, t reflect.Type) beam.PCollection {
	s = s.Scope("spanner.Read")

	cols := strings.Join(structx.InferFieldNames(t, spannerTag), ",")

	return query(s, database, nil, fmt.Sprintf("SELECT %v from %v", cols, table), t)
}

// queryOptions represents additional options for executing a query.
type queryOptions struct {
}

// Query executes a query. The output must have a schema compatible with the given
// type, t. It returns a PCollection<t>.
// Note: Query will be executed on a single worker. Consider performance of query
// and if downstream splitting is required add beam.Reshuffle.
func Query(s beam.Scope, database string, q string, t reflect.Type, options ...func(*queryOptions) error) beam.PCollection {
	s = s.Scope("spanner.Query")
	return query(s, database, nil, q, t, options...)
}

// Entrypoint for testing with spanner client injectable.
func query(s beam.Scope, database string, client *spanner.Client, query string, t reflect.Type, options ...func(*queryOptions) error) beam.PCollection {
	queryOptions := queryOptions{}
	for _, opt := range options {
		if err := opt(&queryOptions); err != nil {
			panic(err)
		}
	}

	imp := beam.Impulse(s)
	return beam.ParDo(s, &queryFn{Database: database, Query: query, Type: beam.EncodedType{T: t}, Options: queryOptions, Client: client}, imp, beam.TypeDefinition{Var: beam.XType, T: t})
}

type queryFn struct {
	Database string           `json:"database"` // Database is the spanner connection string
	Query    string           `json:"query"`    // Table is the table identifier.
	Type     beam.EncodedType `json:"type"`     // Type is the encoded schema type.
	Options  queryOptions     `json:"options"`  // Options specifies additional query execution options.

	Client *spanner.Client // Spanner Client
}

func (f *queryFn) Setup() {
	if f.Client == nil {
		client, err := spanner.NewClient(context.Background(), f.Database)
		if err != nil {
			panic("Failed to initialise Spanner client: " + err.Error())
		}

		f.Client = client
	}
}

func (f *queryFn) Teardown() {
	if f.Client != nil {
		f.Client.Close()
	}
}

func (f *queryFn) ProcessElement(ctx context.Context, _ []byte, emit func(beam.X)) error {
	// todo: Use Batch Read

	stmt := spanner.Statement{SQL: f.Query}
	it := f.Client.Single().Query(ctx, stmt)
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
// Note: Writes occur against a single worker machine.
func Write(s beam.Scope, database string, table string, col beam.PCollection, options ...func(*writeOptions) error) {
	if typex.IsCoGBK(col.Type()) || typex.IsKV(col.Type()) {
		panic("Unsupported collection type - only normal structs supported for writing.")
	}

	s = s.Scope("spanner.Write")

	write(s, database, nil, table, col, options...)
}

// Entrypoint for testing with spanner client injectable.
func write(s beam.Scope, database string, client *spanner.Client, table string, col beam.PCollection, options ...func(*writeOptions) error) {
	writeOptions := writeOptions{
		BatchSize: 1000, // default
	}
	for _, opt := range options {
		if err := opt(&writeOptions); err != nil {
			panic(err)
		}
	}

	t := col.Type().Type()

	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFn{Database: database, Table: table, Type: beam.EncodedType{T: t}, Options: writeOptions, Client: client}, post)
}

type writeFn struct {
	Database string           `json:"database"` // Fully qualified identifier
	Table    string           `json:"table"`    // The table to write to
	Type     beam.EncodedType `json:"type"`     // Type is the encoded schema type.
	Options  writeOptions     `json:"options"`  // Spanner write options

	Client *spanner.Client // Spanner Client
}

func (f *writeFn) Setup() {
	if f.Client == nil {
		client, err := spanner.NewClient(context.Background(), f.Database)
		if err != nil {
			panic("Failed to initialise Spanner client: " + err.Error())
		}

		f.Client = client
	}
}

func (f *writeFn) Teardown() {
	if f.Client != nil {
		f.Client.Close()
	}
}

func (f *writeFn) ProcessElement(ctx context.Context, _ int, iter func(*beam.X) bool) error {
	var mutations []*spanner.Mutation

	var val beam.X
	for iter(&val) {
		mutation, err := spanner.InsertOrUpdateStruct(f.Table, val)
		if err != nil {
			return err
		}

		mutations = append(mutations, mutation)

		if len(mutations)+1 > f.Options.BatchSize {
			_, err := f.Client.Apply(ctx, mutations)
			if err != nil {
				return err
			}

			mutations = nil
		}
	}

	if mutations != nil {
		_, err := f.Client.Apply(ctx, mutations)
		if err != nil {
			return err
		}
	}

	return nil
}
