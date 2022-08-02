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
	"github.com/docker/go-connections/nat"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
)

var expansionAddr string // Populate with expansion address labelled "debeziumio".

func checkFlags(t *testing.T) {
	if expansionAddr == "" {
		t.Skip("No DebeziumIo expansion address provided.")
	}
}

func setupTestContainer(t *testing.T, dbname, username, password string) string {
	t.Helper()

	var env = map[string]string{
		"POSTGRES_PASSWORD": password,
		"POSTGRES_USER":     username,
		"POSTGRES_DB":       dbname,
	}
	var port = "5432/tcp"

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "debezium/example-postgres:latest",
			ExposedPorts: []string{port},
			Env:          env,
		},
		Started: true,
	}
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatalf("failed to get container external port: %v", err)
	}
	return mappedPort.Port()
}

// TestDebeziumIO_BasicRead tests basic read transform from Debezium.
func TestDebeziumIO_BasicRead(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	dbname := "inventory"
	username := "debezium"
	password := "dbz"
	port := setupTestContainer(t, dbname, username, password)
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
