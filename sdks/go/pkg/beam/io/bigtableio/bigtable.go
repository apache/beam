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

// Package bigtableio provides transformations and utilities to interact with
// Google Bigtable. See also: https://cloud.google.com/bigtable/docs
package bigtableio

import (
	"context"
	"fmt"
	"hash/fnv"
	"reflect"

	"cloud.google.com/go/bigtable"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[context.Context, int, func(*Mutation) bool, error](&writeFn{})
	register.DoFn3x1[context.Context, int, func(*Mutation) bool, error](&writeBatchFn{})
	register.Iter1[*Mutation]()
}

// Mutation represents a necessary serializable wrapper analogue
// to bigtable.Mutation containing a rowKey and the operations to be applied.
type Mutation struct {
	RowKey string
	Ops    []Operation

	// optional custom beam.GroupByKey key, default is a fixed key of 1.
	GroupKey string
}

// Operation represents a raw change to be applied within a Mutation.
type Operation struct {
	Family string
	Column string
	Ts     bigtable.Timestamp
	Value  []byte
}

// NewMutation returns a new *Mutation, analogue to bigtable.NewMutation().
func NewMutation(rowKey string) *Mutation {
	return &Mutation{RowKey: rowKey}
}

// Set sets a value in a specified column, with the given timestamp,
// analogue to bigtable.Mutation.Set().
// The timestamp will be truncated to millisecond granularity.
// A timestamp of ServerTime means to use the server timestamp.
func (m *Mutation) Set(family, column string, ts bigtable.Timestamp, value []byte) {
	m.Ops = append(m.Ops, Operation{Family: family, Column: column, Ts: ts, Value: value})
}

// WithGroupKey sets a custom group key to be utilised by beam.GroupByKey.
func (m *Mutation) WithGroupKey(key string) *Mutation {
	m.GroupKey = key
	return m
}

// Write writes the elements of the given PCollection<bigtableio.Mutation> to bigtable.
func Write(s beam.Scope, project, instanceID, table string, col beam.PCollection) {
	t := col.Type().Type()
	err := mustBeBigtableioMutation(t)
	if err != nil {
		panic(err)
	}

	s = s.Scope("bigtable.Write")

	pre := beam.ParDo(s, addGroupKeyFn, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFn{Project: project, InstanceID: instanceID, TableName: table, Type: beam.EncodedType{T: t}}, post)
}

// WriteBatch writes the elements of the given PCollection<bigtableio.Mutation>
// to bigtable using bigtable.ApplyBulk().
// For the underlying bigtable.ApplyBulk function to work properly
// the maximum number of operations per bigtableio.Mutation of the input
// PCollection must not be greater than 100,000. For more information
// see https://cloud.google.com/bigtable/docs/writes#batch for more.
func WriteBatch(s beam.Scope, project, instanceID, table string, col beam.PCollection) {
	t := col.Type().Type()
	err := mustBeBigtableioMutation(t)
	if err != nil {
		panic(err)
	}

	s = s.Scope("bigtable.WriteBatch")

	pre := beam.ParDo(s, addGroupKeyFn, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeBatchFn{Project: project, InstanceID: instanceID, TableName: table, Type: beam.EncodedType{T: t}}, post)
}

func addGroupKeyFn(mutation Mutation) (int, Mutation) {
	if mutation.GroupKey != "" {
		return hashStringToInt(mutation.GroupKey), mutation
	}
	return 1, mutation
}

func hashStringToInt(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func mustBeBigtableioMutation(t reflect.Type) error {
	if t != reflect.TypeOf(Mutation{}) {
		return fmt.Errorf("type must be bigtableio.Mutation but is: %v", t)
	}
	return nil
}

type writeFn struct {
	// Project is the project
	Project string `json:"project"`
	// InstanceID is the bigtable instanceID
	InstanceID string `json:"instanceId"`
	// Client is the bigtable.Client
	client *bigtable.Client `json:"-"`
	// TableName is the qualified table identifier.
	TableName string `json:"tableName"`
	// Table is a bigtable.Table instance with an eventual open connection
	table *bigtable.Table `json:"-"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
}

func (f *writeFn) Setup(ctx context.Context) error {
	var err error
	f.client, err = bigtable.NewClient(ctx, f.Project, f.InstanceID)
	if err != nil {
		return fmt.Errorf("could not create data operations client: %v", err)
	}

	f.table = f.client.Open(f.TableName)
	return nil
}

func (f *writeFn) Teardown() error {
	if err := f.client.Close(); err != nil {
		return fmt.Errorf("could not close data operations client: %v", err)
	}
	return nil
}

func (f *writeFn) ProcessElement(ctx context.Context, key int, values func(*Mutation) bool) error {

	var mutation Mutation
	for values(&mutation) {

		err := validateMutation(mutation)
		if err != nil {
			return fmt.Errorf("invalid bigtableio.Mutation: %s", err)
		}

		err = f.table.Apply(ctx, mutation.RowKey, getBigtableMutation(mutation))
		if err != nil {
			return fmt.Errorf("could not apply mutation for row key='%s': %v", mutation.RowKey, err)
		}

	}

	return nil
}

type writeBatchFn struct {
	// Project is the project
	Project string `json:"project"`
	// InstanceID is the bigtable instanceID
	InstanceID string `json:"instanceId"`
	// Client is the bigtable.Client
	client *bigtable.Client `json:"-"`
	// TableName is the qualified table identifier.
	TableName string `json:"tableName"`
	// Table is a bigtable.Table instance with an eventual open connection
	table *bigtable.Table `json:"-"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
}

func (f *writeBatchFn) Setup(ctx context.Context) error {
	var err error
	f.client, err = bigtable.NewClient(ctx, f.Project, f.InstanceID)
	if err != nil {
		return fmt.Errorf("could not create data operations client: %v", err)
	}

	f.table = f.client.Open(f.TableName)
	return nil
}

func (f *writeBatchFn) Teardown() error {
	if err := f.client.Close(); err != nil {
		return fmt.Errorf("could not close data operations client: %v", err)
	}
	return nil
}

func (f *writeBatchFn) ProcessElement(ctx context.Context, key int, values func(*Mutation) bool) error {

	var rowKeysInBatch []string
	var mutationsInBatch []*bigtable.Mutation

	// opsAddedToBatch is used to make sure that one batch does not include more than 100000 operations/mutations
	opsAddedToBatch := 0

	var mutation Mutation
	for values(&mutation) {

		err := validateMutation(mutation)
		if err != nil {
			return fmt.Errorf("invalid bigtableio.Mutation: %s", err)
		}

		opsInMutation := len(mutation.Ops)

		if (opsAddedToBatch + opsInMutation) > 100000 {
			err := tryApplyBulk(f.table.ApplyBulk(ctx, rowKeysInBatch, mutationsInBatch))
			if err != nil {
				return err
			}

			rowKeysInBatch = nil
			mutationsInBatch = nil
			opsAddedToBatch = 0
		}

		rowKeysInBatch = append(rowKeysInBatch, mutation.RowKey)
		mutationsInBatch = append(mutationsInBatch, getBigtableMutation(mutation))
		opsAddedToBatch += len(mutation.Ops)

	}

	if len(rowKeysInBatch) != 0 && len(mutationsInBatch) != 0 {
		err := tryApplyBulk(f.table.ApplyBulk(ctx, rowKeysInBatch, mutationsInBatch))
		if err != nil {
			return err
		}
	}

	return nil
}

func validateMutation(mutation Mutation) error {
	if len(mutation.Ops) > 100000 {
		return fmt.Errorf("one instance of bigtableio.Mutation must not have more than 100,000 operations/mutations, see https://cloud.google.com/bigtable/docs/writes#batch")
	}
	return nil
}

func tryApplyBulk(errs []error, processErr error) error {
	if processErr != nil {
		return fmt.Errorf("bulk apply procces failed: %v", processErr)
	}
	for _, err := range errs {
		if err != nil {
			return fmt.Errorf("could not apply mutation: %v", err)
		}
	}
	return nil
}

func getBigtableMutation(mutation Mutation) *bigtable.Mutation {
	bigtableMutation := bigtable.NewMutation()
	for _, m := range mutation.Ops {
		bigtableMutation.Set(m.Family, m.Column, m.Ts, m.Value)
	}
	return bigtableMutation
}
