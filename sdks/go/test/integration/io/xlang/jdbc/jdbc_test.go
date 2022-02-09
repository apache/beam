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
package jdbc

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func checkFlags(t *testing.T) {
	if *integration.SchemaIoExpansionAddr == "" {
		t.Skip("No Schema IO expansion address provided.")
	}
}

func setupTestContainer(t *testing.T, dbname, username, password string) int {
	t.Helper()

	var env = map[string]string{
		"POSTGRES_PASSWORD": password,
		"POSTGRES_USER":     username,
		"POSTGRES_DB":       dbname,
	}

	var port = "5432/tcp"
	dbURL := func(port nat.Port) string {
		return fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable", username, password, port.Port(), dbname)
	}

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres",
			ExposedPorts: []string{port},
			Env:          env,
			WaitingFor:   wait.ForSQL(nat.Port(port), "postgres", dbURL).Timeout(time.Second * 5),
		},
		Started: true,
	}
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	mappedPort, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatalf("failed to get container external port: %s", err)
	}

	url := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable", username, password, mappedPort.Port(), dbname)
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatalf("failed to establish database connection: %s", err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE TABLE roles(role_id bigint PRIMARY KEY);")
	if err != nil {
		t.Fatalf("can't create table, check command and access level")
	}
	return mappedPort.Int()
}

// TestJDBCIO_BasicReadWrite tests basic read and write transform from JDBC.
func TestJDBCIO_BasicReadWrite(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	dbname := "postjdbc"
	username := "newuser"
	password := "password"
	port := setupTestContainer(t, dbname, username, password)
	tableName := "roles"
	host := "localhost"
	jdbcUrl := fmt.Sprintf("jdbc:postgresql://%s:%d/%s", host, port, dbname)

	write := WritePipeline(*integration.SchemaIoExpansionAddr, tableName, "org.postgresql.Driver", jdbcUrl, username, password)
	ptest.RunAndValidate(t, write)

	read := ReadPipeline(*integration.SchemaIoExpansionAddr, tableName, "org.postgresql.Driver", jdbcUrl, username, password)
	ptest.RunAndValidate(t, read)
}

func TestMain(m *testing.M) {
	ptest.Main(m)
}
