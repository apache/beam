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

package bigqueryio

import (
	"reflect"
	"testing"
)

func TestNewQualifiedTableName(t *testing.T) {
	tests := []struct {
		Name string
		Exp  QualifiedTableName
	}{
		{"a:b.c", QualifiedTableName{Project: "a", Dataset: "b", Table: "c"}},
		{"foo.com:a:b.c", QualifiedTableName{Project: "foo.com:a", Dataset: "b", Table: "c"}},
	}

	for _, test := range tests {
		actual, err := NewQualifiedTableName(test.Name)
		if err != nil {
			t.Errorf("NewQualifiedTableName(%v) failed: %v", test.Name, err)
		}
		if actual != test.Exp {
			t.Errorf("NewQualifiedTableName(%v) = %v, want %v", test.Name, actual, test.Exp)
		}
	}
}

func Test_constructSelectStatement(t *testing.T) {
	t.Run("Statement with columns inferred from struct fields", func(t *testing.T) {
		typ := reflect.TypeOf(struct {
			Col1 string `bigquery:"col1"`
			Col2 string `bigquery:"col2,nullable"`
			Col3 string `bigquery:",nullable"`
			Col4 string
			Col5 string `other:"col5"`
			Col6 string `bigquery:"-"`
			col7 string
		}{})
		tagKey := "bigquery"
		table := "test_table"
		want := "SELECT col1, col2, Col3, Col4, Col5 FROM [test_table]"

		if got := constructSelectStatement(typ, tagKey, table); got != want {
			t.Errorf("constructSelectStatement() = %v, want %v", got, want)
		}
	})
}

func Test_constructSelectStatementPanic(t *testing.T) {
	t.Run("Panic for no columns", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("constructSelectStatement() does not panic")
			}
		}()

		typ := reflect.TypeOf(struct{}{})
		tagKey := "bigquery"
		table := "test_table"

		constructSelectStatement(typ, tagKey, table)
	})
}
