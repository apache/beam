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
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"google.golang.org/api/option"
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

func Test_mustInferSchema(t *testing.T) {
	type TestSchema struct {
		Name   bigquery.NullString   `bigquery:"name"`
		Active bigquery.NullBool     `bigquery:"active"`
		Score  bigquery.NullFloat64  `bigquery:"score"`
		Time   bigquery.NullDateTime `bigquery:"time"`
	}

	tests := []struct {
		name    string
		input   interface{}
		wantErr bool
		prep    func(reflect.Type) error
		verify  func(reflect.Type) error
	}{
		{
			name:    "NotRegisteredType_ShouldPanic",
			input:   TestSchema{},
			wantErr: true,
			prep:    func(t reflect.Type) error { return nil },
			verify:  func(t reflect.Type) error { return nil },
		},
		{
			name:    "AlreadyRegisteredType_ShouldNotPanic",
			input:   TestSchema{},
			wantErr: false,
			prep: func(t reflect.Type) error {
				beam.RegisterType(t)
				return nil
			},
			verify: func(t reflect.Type) error {
				mustInferSchema(t)
				return nil
			},
		},
		{
			name:    "AnonymousStruct_ShouldPanic",
			input:   struct{}{},
			wantErr: true,
			prep:    func(t reflect.Type) error { return nil },
			verify:  func(t reflect.Type) error { return nil },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if (r != nil) != tt.wantErr {
					t.Errorf("mustInferSchema() panic = %v, wantErr %v", r, tt.wantErr)
				}
			}()

			typ := reflect.TypeOf(tt.input)
			if err := tt.prep(typ); err != nil {
				t.Fatalf("failed to prep test environment, got err: %v", err)
			}

			mustInferSchema(typ)
			if tt.wantErr {
				t.Fatal("Expected panic did not occur")
			}
			if err := tt.verify(typ); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWithQuotaProject(t *testing.T) {
	qo := QueryOptions{}
	if err := WithQuotaProject("quota-project")(&qo); err != nil {
		t.Fatalf("WithQuotaProject() returned err: %v", err)
	}
	if got, want := qo.QuotaProject, "quota-project"; got != want {
		t.Errorf("qo.QuotaProject = %q, want %q", got, want)
	}
}

func TestClientOptions(t *testing.T) {
	if got := clientOptions(QueryOptions{}); len(got) != 0 {
		t.Errorf("clientOptions(no quota) = %v, want none", got)
	}
	got := clientOptions(QueryOptions{QuotaProject: "quota-project"})
	if want := []option.ClientOption{option.WithQuotaProject("quota-project")}; !reflect.DeepEqual(got, want) {
		t.Errorf("clientOptions(with quota) = %v, want %v", got, want)
	}
}

func TestQueryFn_passesQuotaProjectToClient(t *testing.T) {
	var gotProject string
	var gotOpts []option.ClientOption
	errStop := errors.New("stop")
	defer func(orig func(context.Context, string, ...option.ClientOption) (*bigquery.Client, error)) {
		newClient = orig
	}(newClient)
	newClient = func(_ context.Context, project string, opts ...option.ClientOption) (*bigquery.Client, error) {
		gotProject, gotOpts = project, opts
		return nil, errStop
	}

	fn := &queryFn{Project: "project", Options: QueryOptions{QuotaProject: "quota-project"}}
	if err := fn.ProcessElement(context.Background(), nil, nil); !errors.Is(err, errStop) {
		t.Fatalf("ProcessElement() err = %v, want %v", err, errStop)
	}
	if gotProject != "project" {
		t.Errorf("client project = %q, want %q", gotProject, "project")
	}
	if want := []option.ClientOption{option.WithQuotaProject("quota-project")}; !reflect.DeepEqual(gotOpts, want) {
		t.Errorf("client options = %v, want %v", gotOpts, want)
	}
}

// queryFnOf returns the queryFn in pipeline p.
func queryFnOf(t *testing.T, p *beam.Pipeline) *queryFn {
	t.Helper()
	edges, _, err := p.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}
	for _, e := range edges {
		if e.DoFn != nil {
			if fn, ok := e.DoFn.Recv.(*queryFn); ok {
				return fn
			}
		}
	}
	t.Fatal("no queryFn in pipeline")
	return nil
}

func TestReadAndQuery_passQuotaProjectToQueryFn(t *testing.T) {
	type quotaSchema struct {
		Name bigquery.NullString `bigquery:"name"`
	}
	beam.RegisterType(reflect.TypeOf((*quotaSchema)(nil)).Elem())
	typ := reflect.TypeOf(quotaSchema{})

	p, s := beam.NewPipelineWithRoot()
	Read(s, "project", "project:dataset.table", typ, WithQuotaProject("quota-project"))
	if got := queryFnOf(t, p).Options.QuotaProject; got != "quota-project" {
		t.Errorf("Read: queryFn.Options.QuotaProject = %q, want %q", got, "quota-project")
	}

	p, s = beam.NewPipelineWithRoot()
	Query(s, "project", "SELECT 1", typ, WithQuotaProject("quota-project"))
	if got := queryFnOf(t, p).Options.QuotaProject; got != "quota-project" {
		t.Errorf("Query: queryFn.Options.QuotaProject = %q, want %q", got, "quota-project")
	}
}

func TestWithQueryParameters(t *testing.T) {
	t.Run("WithQueryParameters sets parameters correctly", func(t *testing.T) {
		params := []bigquery.QueryParameter{
			{Name: "param1", Value: "value1"},
			{Name: "param2", Value: 42},
		}

		opt := WithQueryParameters(params...)
		queryOpts := &QueryOptions{}
		if err := opt(queryOpts); err != nil {
			t.Fatalf("WithQueryParameters() failed: %v", err)
		}

		if len(queryOpts.parameters) != 2 {
			t.Errorf("Expected 2 parameters, got %d", len(queryOpts.parameters))
		}

		if queryOpts.parameters[0].Name != "param1" {
			t.Errorf("Expected param1, got %s", queryOpts.parameters[0].Name)
		}

		if queryOpts.parameters[0].Value != "value1" {
			t.Errorf("Expected value1, got %s", queryOpts.parameters[0].Value)
		}

		if queryOpts.parameters[1].Name != "param2" {
			t.Errorf("Expected param2, got %s", queryOpts.parameters[1].Name)
		}

		if queryOpts.parameters[1].Value != 42 {
			t.Errorf("Expected 42, got %v", queryOpts.parameters[1].Value)
		}
	})
}

func TestRead_queryOnlyOptions(t *testing.T) {
	type readSchema struct {
		Name bigquery.NullString `bigquery:"name"`
	}
	beam.RegisterType(reflect.TypeOf((*readSchema)(nil)).Elem())
	typ := reflect.TypeOf(readSchema{})

	mustPanic := func(t *testing.T, opt func(*QueryOptions) error) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("Read() with query-only option does not panic")
			} else if !strings.Contains(fmt.Sprint(r), "only supported by Query") {
				t.Errorf("Read() panicked with unexpected value: %v", r)
			}
		}()
		_, s := beam.NewPipelineWithRoot()
		Read(s, "project", "project:dataset.table", typ, opt)
	}

	t.Run("UseStandardSQL panics", func(t *testing.T) {
		mustPanic(t, UseStandardSQL())
	})
	t.Run("WithQueryParameters panics", func(t *testing.T) {
		mustPanic(t, WithQueryParameters(bigquery.QueryParameter{Name: "p", Value: 1}))
	})
	t.Run("WithQuotaProject does not panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Read() with WithQuotaProject panicked: %v", r)
			}
		}()
		_, s := beam.NewPipelineWithRoot()
		Read(s, "project", "project:dataset.table", typ, WithQuotaProject("quota-project"))
	})
}
