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
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	spannertest "github.com/apache/beam/sdks/v2/go/test/integration/io/spannerio"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestRead(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		database      string
		table         string
		rows          []TestDto
		expectedError bool
	}{
		{
			name:     "Successfully read 4 rows",
			database: "projects/fake-proj/instances/fake-instance/databases/fake-db-4-rows",
			table:    "FourRows",
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
			table:    "OneRow",
			rows: []TestDto{
				{
					One: "one",
					Two: 1,
				},
			},
		},
	}

	srv := newServer(t)

	adminClient := spannertest.NewAdminClient(ctx, t, srv.Addr)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			spannertest.CreateTable(ctx, t, adminClient, testCase.database, []string{`CREATE TABLE ` + testCase.table + ` (
					One STRING(20),
					Two INT64,
				) PRIMARY KEY (Two)`})

			var mutations []*spanner.Mutation
			for _, m := range testCase.rows {
				mutation, err := spanner.InsertStruct(testCase.table, m)
				if err != nil {
					t.Fatalf("Unable to create mutation to insert struct: %v", err)
				}

				mutations = append(mutations, mutation)
			}

			client := spannertest.NewClient(ctx, t, srv.Addr, testCase.database)
			_, err := client.Apply(ctx, mutations)
			if err != nil {
				t.Fatalf("Applying mutations: %v", err)
			}

			p, s := beam.NewPipelineWithRoot()
			fn := newQueryFn(testCase.database, "SELECT * from "+testCase.table, reflect.TypeOf(TestDto{}), queryOptions{})
			fn.TestEndpoint = srv.Addr

			imp := beam.Impulse(s)
			rows := beam.ParDo(s, fn, imp, beam.TypeDefinition{Var: beam.XType, T: reflect.TypeOf(TestDto{})})

			passert.Count(s, rows, "", len(testCase.rows))
			ptest.RunAndValidate(t, p)
		})
	}
}
