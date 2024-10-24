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

package debezium

import (
	"context"
	"flag"
	"log"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/debeziumio"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	"github.com/apache/beam/sdks/v2/go/test/integration/internal/containers"
	_ "github.com/lib/pq"
)

const (
	debeziumImage = "quay.io/ebezium/example-postgres:latest"
	debeziumPort  = "5432/tcp"
	maxRetries    = 5
)

var expansionAddr string // Populate with expansion address labelled "debeziumio".

func checkFlags(t *testing.T) {
	if expansionAddr == "" {
		t.Skip("No DebeziumIo expansion address provided.")
	}
}

func setupTestContainer(ctx context.Context, t *testing.T, dbname, username, password string) string {
	t.Helper()

	env := map[string]string{
		"POSTGRES_PASSWORD": password,
		"POSTGRES_USER":     username,
		"POSTGRES_DB":       dbname,
	}

	container := containers.NewContainer(
		ctx,
		t,
		debeziumImage,
		maxRetries,
		containers.WithEnv(env),
		containers.WithPorts([]string{debeziumPort}),
	)

	return containers.Port(ctx, t, container, debeziumPort)
}

// TestDebeziumIO_BasicRead tests basic read transform from Debezium.
func TestDebeziumIO_BasicRead(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	ctx := context.Background()
	dbname := "inventory"
	username := "debezium"
	password := "dbz"
	port := setupTestContainer(ctx, t, dbname, username, password)
	host := "localhost"
	connectionProperties := []string{
		"database.dbname=inventory",
		"database.server.name=dbserver1",
		"database.include.list=inventory",
		"include.schema.changes=false",
	}
	read := ReadPipeline(expansionAddr, username, password, dbname, host, port, debeziumio.PostgreSQL, 1, connectionProperties)
	ptest.RunAndValidate(t, read)
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	services := integration.NewExpansionServices()
	defer func() { services.Shutdown() }()
	addr, err := services.GetAddr("debeziumio")
	if err != nil {
		log.Printf("skipping missing expansion service: %v", err)
	} else {
		expansionAddr = addr
	}

	ptest.MainRet(m)
}
