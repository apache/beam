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
	"flag"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	"os"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/spannerio"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestSpannerIO_QueryBatch(t *testing.T) {
	integration.CheckFilters(t)

	p := beam.NewPipeline()
	s := p.Root()

	db := "projects/test-project/instances/test-instance/databases/test-database"
	ctx := context.Background()

	// Setup a spanner emulator container.
	endpoint := setUpTestContainer(ctx, t)

	os.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	// Create clients that we'll need for this test. Note: these have implicit cleanup func's registered.
	client := NewClient(ctx, t, endpoint, db)
	instanceAdminClient := NewInstanceAdminClient(ctx, t, endpoint)
	adminClient := NewAdminClient(ctx, t, endpoint)

	// Create a spanner instance. Requires explicit cleanup.
	CreateInstance(ctx, t, instanceAdminClient, db)
	t.Cleanup(func() {
		DeleteInstance(ctx, t, instanceAdminClient, db)
	})

	// Create a spanner database. Requires explicit cleanup.
	CreateDatabase(ctx, t, adminClient, db)
	t.Cleanup(func() {
		DropDatabase(ctx, t, adminClient, db)
	})

	// Create a spanner table. We won't explicitly clean this up - cleaning up the database is enough.
	CreateTable(ctx, t, adminClient, db, []string{`CREATE TABLE Test (
					One STRING(20),
					Two INT64,
				) PRIMARY KEY (Two)`})

	// Load some data into spanner
	testRows := []spannerio.TestDto{
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
	}

	var mutations []*spanner.Mutation
	for _, m := range testRows {
		mutation, err := spanner.InsertStruct("Test", m)
		if err != nil {
			t.Fatalf("Unable to create spanner mutation: %v", err)
		}

		mutations = append(mutations, mutation)
	}

	_, err := client.Apply(ctx, mutations)
	if err != nil {
		t.Fatalf("Unable to apply spanner mutations: %v", err)
	}

	// Setup our pipeline
	rows := spannerio.Query(s, db, "SELECT * FROM TEST", reflect.TypeOf(spannerio.TestDto{}))

	// Setup our assertion
	passert.Count(s, rows, "Should have 4 rows", 4)

	// Run
	ptest.RunAndValidate(t, p)
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	ptest.MainRet(m)
}
