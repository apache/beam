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
)

func init() {
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*writeBatchFn)(nil)).Elem())
}

// BigtableioMutation represents a bigtable mutation containing a rowKey and the mutation to be applied
type BigtableioMutation struct {
	RowKey   string
	Mutations []Mutation

	// optional custom beam.GroupByKey key, default is fixed key 1
	GroupKey string
}

// Mutation represents a raw mutation within a BigtableioMutation
type Mutation struct {
	Family string
	Column string
	Ts bigtable.Timestamp
	Value []byte
}

// NewMutation returns a new *BigtableioMutation
func NewMutation(rowKey string) *BigtableioMutation {
	return &BigtableioMutation{RowKey: rowKey}
}

// Set sets a value in a specified column, with the given timestamp.
// The timestamp will be truncated to millisecond granularity.
// A timestamp of ServerTime means to use the server timestamp.
func (bigtableioMutation *BigtableioMutation) Set(family, column string, ts bigtable.Timestamp, value []byte) {
	bigtableioMutation.Mutations = append(bigtableioMutation.Mutations, Mutation{Family: family, Column: column, Ts: ts, Value: value})
}

// GroupKey sets a custom group key to be handed to beam.GroupByKey
func (bigtableioMutation *BigtableioMutation) WithGroupKey(key string) *BigtableioMutation {
	bigtableioMutation.GroupKey = key
	return bigtableioMutation
}

// Write writes the elements of the given PCollection<bigtableio.BigtableioMutation> to bigtable.
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

// WriteBatch writes the elements of the given PCollection<bigtableio.BigtableioMutation> using ApplyBulk to bigtable.
// For the underlying bigtable.ApplyBulk function to work properly the maximum number of mutations per BigtableioMutation (maxMutationsPerRow) must be given.
// This is necessary due to the maximum amount of mutations allowed per bulk operation (100,000), see https://cloud.google.com/bigtable/docs/writes#batch for more.
func WriteBatch(s beam.Scope, project, instanceID, table string, maxMutationsPerRow uint, col beam.PCollection) {
	t := col.Type().Type()
	err := mustBeBigtableioMutation(t)
	if err != nil {
		panic(err)
	}

	if maxMutationsPerRow == 0 {
		panic("maxMutationsPerRow must not be 0")
	}

	if maxMutationsPerRow > 100000 {
		panic("maxMutationsPerRow must not be greater than 100,000, see https://cloud.google.com/bigtable/docs/writes#batch")
	}

	s = s.Scope("bigtable.WriteBatch")

	pre := beam.ParDo(s, addGroupKeyFn, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeBatchFn{Project: project, InstanceID: instanceID, TableName: table, MaxMutationsPerRow: maxMutationsPerRow, Type: beam.EncodedType{T: t}}, post)
}

func addGroupKeyFn(bigtableioMutation BigtableioMutation) (int, BigtableioMutation) {
	if bigtableioMutation.GroupKey != "" {
		return hashStringToInt(bigtableioMutation.GroupKey), bigtableioMutation
	}
	return 1, bigtableioMutation
}

func hashStringToInt(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func mustBeBigtableioMutation(t reflect.Type) error {
	if t != reflect.TypeOf(BigtableioMutation{}) {
		return fmt.Errorf("type must be bigtableio.BigtableioMutation but is: %v", t)
	}
	return nil
}

type writeFn struct {
	// Project is the project
	Project string `json:"project"`
	// InstanceID is the bigtable instanceID
	InstanceID string `json:"instanceId"`
	// Client is the bigtable.Client
	Client *bigtable.Client `json:"client"`
	// TableName is the qualified table identifier.
	TableName string `json:"tableName"`
	// Table is a bigtable.Table instance with an eventual open connection
	Table *bigtable.Table `json:"table"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
}

func (f *writeFn) StartBundle() error {
	var err error
	f.Client, err = bigtable.NewClient(context.Background(), f.Project, f.InstanceID)
	if err != nil {
		return fmt.Errorf("could not create data operations client: %v", err)
	}

	f.Table = f.Client.Open(f.TableName)
	return nil
}

func (f *writeFn) FinishBundle() error {
	if err := f.Client.Close(); err != nil {
		return fmt.Errorf("could not close data operations client: %v", err)
	}
	return nil
}

func (f *writeFn) ProcessElement(key int, values func(*BigtableioMutation) bool) error {

	var bigtableioMutation BigtableioMutation
	for values(&bigtableioMutation) {
		
		err := f.Table.Apply(context.Background(), bigtableioMutation.RowKey, getBigtableMutation(bigtableioMutation))
		if err != nil {
			return fmt.Errorf("could not apply mutation for row key='%s': %v", bigtableioMutation.RowKey, err)
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
	Client *bigtable.Client `json:"client"`
	// TableName is the qualified table identifier.
	TableName string `json:"tableName"`
	// Table is a bigtable.Table instance with an eventual open connection
	Table *bigtable.Table `json:"table"`
	// Indicator of how many mutations exist per row at max
	MaxMutationsPerRow uint `json:"mutationsPerRow"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
}

func (f *writeBatchFn) StartBundle() error {
	var err error
	f.Client, err = bigtable.NewClient(context.Background(), f.Project, f.InstanceID)
	if err != nil {
		return fmt.Errorf("could not create data operations client: %v", err)
	}

	f.Table = f.Client.Open(f.TableName)
	return nil
}

func (f *writeBatchFn) FinishBundle() error {
	if err := f.Client.Close(); err != nil {
		return fmt.Errorf("could not close data operations client: %v", err)
	}
	return nil
}

func (f *writeBatchFn) ProcessElement(key int, values func(*BigtableioMutation) bool) error {
	maxMutationsPerBatch := 100000 / int(f.MaxMutationsPerRow)

	var rowKeys []string
	var mutations []*bigtable.Mutation

	mutationsAdded := 0

	var bigtableioMutation BigtableioMutation
	for values(&bigtableioMutation) {

		rowKeys = append(rowKeys, bigtableioMutation.RowKey)
		mutations = append(mutations, getBigtableMutation(bigtableioMutation))
		mutationsAdded += int(f.MaxMutationsPerRow)

		if (mutationsAdded + int(f.MaxMutationsPerRow)) > maxMutationsPerBatch {
			err := tryApplyBulk(f.Table.ApplyBulk(context.Background(), rowKeys, mutations))
			if err != nil {
				return err
			}

			rowKeys = nil
			mutations = nil
			mutationsAdded = 0
		}
	}

	if len(rowKeys) != 0 && len(mutations) != 0 {
		err := tryApplyBulk(f.Table.ApplyBulk(context.Background(), rowKeys, mutations))
		if err != nil {
			return err
		}
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

func getBigtableMutation(bigtableioMutation BigtableioMutation) *bigtable.Mutation {
	mutation := bigtable.NewMutation()
	for _, m := range bigtableioMutation.Mutations {
		mutation.Set(m.Family, m.Column, m.Ts, m.Value)
	}
	return mutation
}
