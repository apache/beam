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
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	spannertest "github.com/apache/beam/sdks/v2/go/test/integration/io/spannerio"
	"google.golang.org/api/iterator"
)

func TestWrite(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name          string
		database      string
		table         string
		rows          []TestDto
		expectedError bool
	}{
		{
			name:     "Successfully write 4 rows",
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
	}

	srv := newServer(t)

	adminClient := spannertest.NewAdminClient(ctx, t, srv.Addr)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			client := spannertest.NewClient(ctx, t, srv.Addr, testCase.database)

			spannertest.CreateTable(ctx, t, adminClient, testCase.database, []string{`CREATE TABLE ` + testCase.table + ` (
					One STRING(20),
					Two INT64,
				) PRIMARY KEY (Two)`})

			p, s, col := ptest.CreateList(testCase.rows)

			fn := newWriteFn(testCase.database, testCase.table, col.Type().Type())
			fn.TestEndpoint = srv.Addr

			beam.ParDo0(s, fn, col)

			ptest.RunAndValidate(t, p)

			stmt := spanner.Statement{SQL: "SELECT * FROM " + testCase.table}
			it := client.Single().Query(ctx, stmt)
			defer it.Stop()

			var count int
			for {
				_, err := it.Next()
				if err != nil {
					if err == iterator.Done {
						break
					}
				}
				count++
			}

			if count != len(testCase.rows) {
				t.Fatalf("Got incorrect number of rows from spanner write, got '%v', expected '%v'", count, len(testCase.rows))
			}
		})
	}
}
