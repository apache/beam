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

// RowKeyMutationPair represents a planned row-mutation containing a rowKey and the mutation to be applied
type RowKeyMutationPair struct {
	rowKey   string
	mutation *bigtable.Mutation

	// optional custom beam.GroupByKey key, default is fixed key 1
	groupKey string
}

// NewRowKeyMutationPair returns a new *RowKeyMutationPair
func NewRowKeyMutationPair(rowKey string, mutation *bigtable.Mutation) *RowKeyMutationPair {
	return &RowKeyMutationPair{rowKey: rowKey, mutation: mutation}
}

// GroupKey sets a custom group key to be handed to beam.GroupByKey
func (rowKeyMutationPair *RowKeyMutationPair) WithGroupKey(key string) *RowKeyMutationPair {
	rowKeyMutationPair.groupKey = key
	return rowKeyMutationPair
}

// Write writes the elements of the given PCollection<bigtableio.RowKeyMutationPair> to bigtable.
func Write(s beam.Scope, project, instanceID, table string, col beam.PCollection) {
	t := col.Type().Type()
	mustBeRowKeyMutationPair(t)

	s = s.Scope("bigtable.Write")

	pre := beam.ParDo(s, addGroupKeyFn, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFn{Project: project, InstanceID: instanceID, TableName: table, Type: beam.EncodedType{T: t}}, post)
}

// WriteBatch writes the elements of the given PCollection<bigtableio.RowKeyMutationPair> using ApplyBulk to bigtable.
// For the underlying bigtable.ApplyBulk function to work properly the maximum number of mutations per RowKeyMutationPair (maxMutationsPerRow) must be given.
// This is necessary due to the maximum amount of mutations allowed per bulk operation (100,000), see https://cloud.google.com/bigtable/docs/writes#batch for more.
func WriteBatch(s beam.Scope, project, instanceID, table string, maxMutationsPerRow uint, col beam.PCollection) {
	t := col.Type().Type()
	mustBeRowKeyMutationPair(t)

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

func addGroupKeyFn(rowKeyMutationPair RowKeyMutationPair) (int, RowKeyMutationPair) {
	if rowKeyMutationPair.groupKey != "" {
		return hashStringToInt(rowKeyMutationPair.groupKey), rowKeyMutationPair
	}
	return 1, rowKeyMutationPair
}

func hashStringToInt(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func mustBeRowKeyMutationPair(t reflect.Type) {
	if t != reflect.TypeOf(RowKeyMutationPair{}) {
		panic(fmt.Sprintf("type must be bigtableio.KeyMutationPair, but is: %v", t))
	}
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

func (f *writeFn) ProcessElement(key int, values func(*RowKeyMutationPair) bool) error {
	var rowKeyMutationPair RowKeyMutationPair
	for values(&rowKeyMutationPair) {
		err := f.Table.Apply(context.Background(), rowKeyMutationPair.rowKey, rowKeyMutationPair.mutation)
		if err != nil {
			return fmt.Errorf("could not apply mutation for row key='%s': %v", rowKeyMutationPair.rowKey, err)
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

func (f *writeBatchFn) ProcessElement(key int, values func(*RowKeyMutationPair) bool) error {
	maxMutationsPerBatch := 100000 / int(f.MaxMutationsPerRow)

	var rowKeys []string
	var mutations []*bigtable.Mutation

	mutationsAdded := 0

	var rowKeyMutationPair RowKeyMutationPair
	for values(&rowKeyMutationPair) {

		rowKeys = append(rowKeys, rowKeyMutationPair.rowKey)
		mutations = append(mutations, rowKeyMutationPair.mutation)
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
