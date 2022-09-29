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
	register.DoFn2x1[int, func(*Mutation) bool, error](&writeFn{})
	register.Iter1[*Mutation]()
	register.DoFn2x1[int, func(*Mutation) bool, error](&writeBatchFn{})
	register.Iter1[*Mutation]()
}

// Mutation represents a necessary serializable wrapper analogue to bigtable.Mutation containing a rowKey and the operations to be applied
type Mutation struct {
	RowKey   string
	Ops []Operation

	// optional custom beam.GroupByKey key, default is a fixed key of 1
	GroupKey string
}

// Operation represents a raw change to be applied within a Mutation
type Operation struct {
	Family string
	Column string
	Ts bigtable.Timestamp
	Value []byte
}

// NewMutation returns a new *Mutation, analogue to bigtable.NewMutation()
func NewMutation(rowKey string) *Mutation {
	return &Mutation{RowKey: rowKey}
}

// Set sets a value in a specified column, with the given timestamp, analogue to bigtable.Mutation.Set().
// The timestamp will be truncated to millisecond granularity.
// A timestamp of ServerTime means to use the server timestamp.
func (m *Mutation) Set(family, column string, ts bigtable.Timestamp, value []byte) {
	m.Ops = append(m.Ops, Operation{Family: family, Column: column, Ts: ts, Value: value})
}

// GroupKey sets a custom group key to be utilised by beam.GroupByKey
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

// WriteBatch writes the elements of the given PCollection<bigtableio.Mutation> using ApplyBulk to bigtable.
// For the underlying bigtable.ApplyBulk function to work properly the maximum number of mutations per bigtableio.Mutation (maxMutationsPerRow) must be given.
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

func (f *writeFn) ProcessElement(key int, values func(*Mutation) bool) error {

	var mutation Mutation
	for values(&mutation) {
		
		err := f.Table.Apply(context.Background(), mutation.RowKey, getBigtableMutation(mutation))
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

func (f *writeBatchFn) ProcessElement(key int, values func(*Mutation) bool) error {
	maxMutationsPerBatch := 100000 / int(f.MaxMutationsPerRow)

	var rowKeys []string
	var mutations []*bigtable.Mutation

	mutationsAdded := 0

	var mutation Mutation
	for values(&mutation) {

		rowKeys = append(rowKeys, mutation.RowKey)
		mutations = append(mutations, getBigtableMutation(mutation))
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

func getBigtableMutation(mutation Mutation) *bigtable.Mutation {
	bigtableMutation := bigtable.NewMutation()
	for _, m := range mutation.Ops {
		bigtableMutation.Set(m.Family, m.Column, m.Ts, m.Value)
	}
	return bigtableMutation
}

