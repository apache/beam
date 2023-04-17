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
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	integrationTests = flag.Bool("int", false, "")
)

func TestExampleQueryBatch(t *testing.T) {
	if !*integrationTests {
		t.Skip("Not running in integration test mode.")
	}

	p := beam.NewPipeline()
	s := p.Root()

	db := "projects/test-project/instances/test-instance/databases/test-database"

	srv, srvCleanup := newServer(t)
	defer srvCleanup()

	client, admin, cleanup, err := createFakeClient(srv.Addr, db)
	if err != nil {
		t.Fatalf("Unable to create fake client: %v", err)
	}
	defer cleanup()

	populateSpanner(context.Background(), admin, db, client)

	rows := QueryBatch(s, db, "SELECT * FROM TEST", reflect.TypeOf(TestDto{}))

	ptest.RunAndValidate(t, p)
	passert.Count(s, rows, "Should have 4 rows", 4)
}

func populateSpanner(ctx context.Context, admin *database.DatabaseAdminClient, db string, client *spanner.Client) error {
	iter := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT 1 FROM Test"})
	defer iter.Stop()

	if _, err := iter.Next(); err == nil {
		return nil
	}

	op, err := admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: db,
		Statements: []string{`CREATE TABLE Test (
					One STRING(20),
					Two INT64,
				) PRIMARY KEY (Two)`},
	})

	if err != nil {
		return err
	}

	if err := op.Wait(context.Background()); err != nil {
		return err
	}

	testRows := []TestDto{
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
			return err
		}

		mutations = append(mutations, mutation)
	}

	_, err = client.Apply(context.Background(), mutations)
	if err != nil {
		return err
	}

	return nil
}

func TestMain(t *testing.M) {
	flag.Parse()
	t.Run()
}
