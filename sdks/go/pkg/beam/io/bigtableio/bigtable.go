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
	"reflect"

	"cloud.google.com/go/bigtable"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)).Elem())
}

type KeyMutationPair struct {
	Key      string
	Mutation bigtable.Mutation
}

// Write writes the elements of the given PCollection<bigtableio.KeyMutationPair> to bigtable.
func Write(s beam.Scope, project, instanceID, table string, col beam.PCollection) {
	t := col.Type().Type()
	mustBeKeyMutationPair(t)

	s = s.Scope("bigtable.Write")

	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFn{Project: project, InstanceID: instanceID, TableName: table, Type: beam.EncodedType{T: t}}, post)
}

func mustBeKeyMutationPair(t reflect.Type) {
	if t != reflect.TypeOf(KeyMutationPair{}) {
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

func (f *writeFn) ProcessElement(ctx context.Context, keyMutationPair KeyMutationPair) error {
	err := f.Table.Apply(ctx, keyMutationPair.Key, &keyMutationPair.Mutation)
	if err != nil{
		return fmt.Errorf("could not apply mutation for key='%s': %v", keyMutationPair.Key, err)
	}
	return nil
}
