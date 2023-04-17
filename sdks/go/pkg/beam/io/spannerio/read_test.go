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
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spansql"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestRead(t *testing.T) {
	testCases := []struct {
		name          string
		database      string
		rows          []TestDto
		expectedError bool
	}{
		{
			name:     "Successfully read 4 rows",
			database: "projects/fake-proj/instances/fake-instance/databases/fake-db-4-rows",
			rows: []TestDto{
				{
					One: "one",
					Two: 1,
				},
				{
					One: "one",
					Two: 2,
				},
				{
					One: "one",
					Two: 3,
				},
				{
					One: "one",
					Two: 4,
				},
			},
		},
		{
			name:     "Successfully read 1 rows",
			database: "projects/fake-proj/instances/fake-instance/databases/fake-db-1-rows",
			rows: []TestDto{
				{
					One: "one",
					Two: 1,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			srv, srvCleanup := newServer(t)
			defer srvCleanup()

			client, _, cleanup, err := createFakeClient(srv.Addr, testCase.database)
			if err != nil {
				t.Fatalf("Unable to create fake client: %v", err)
			}
			defer cleanup()

			ddl, err := spansql.ParseDDL("",
				`CREATE TABLE Test (
					One STRING(20),
					Two INT64,
				) PRIMARY KEY (Two)`)
			if err != nil {
				t.Fatalf("Unable to create DDL statement for spanner test: %v", err)
			}

			err = srv.UpdateDDL(ddl)
			if err != nil {
				t.Fatalf("Unable to run DDL into spanner db: %v", err)
			}

			var mutations []*spanner.Mutation
			for _, m := range testCase.rows {
				mutation, err := spanner.InsertStruct("Test", m)
				if err != nil {
					t.Fatalf("Unable to create mutation to insert struct: %v", err)
				}

				mutations = append(mutations, mutation)
			}

			_, err = client.Apply(context.Background(), mutations)
			if err != nil {
				t.Fatalf("Applying mutations: %v", err)
			}

			p := beam.NewPipeline()
			s := p.Root()
			fn := newQueryFn(testCase.database, "SELECT * from Test", reflect.TypeOf(TestDto{}))
			fn.client = client

			imp := beam.Impulse(s)
			rows := beam.ParDo(s, fn, imp, beam.TypeDefinition{Var: beam.XType, T: reflect.TypeOf(TestDto{})})

			passert.Count(s, rows, "", len(testCase.rows))
			ptest.RunAndValidate(t, p)
		})
	}
}
