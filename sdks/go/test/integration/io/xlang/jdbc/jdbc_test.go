// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"flag"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	"github.com/apache/beam/sdks/v2/go/test/integration/internal/containers"
	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	postgresImage = "postgres"
	postgresPort  = "5432/tcp"
	maxRetries    = 5
)

var expansionAddr string // Populate with expansion address labelled "schemaio".

func checkFlags(t *testing.T) {
	if expansionAddr == "" {
		t.Skip("No Schema IO expansion address provided.")
	}
}

func setupTestContainer(ctx context.Context, t *testing.T, dbname, username, password string) string {
	t.Helper()

	env := map[string]string{
		"POSTGRES_PASSWORD": password,
		"POSTGRES_USER":     username,
		"POSTGRES_DB":       dbname,
	}
	hostname := "localhost"

	dbURL := func(host string, port nat.Port) string {
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", username, password, host, port.Port(), dbname)
	}
	waitStrategy := wait.ForSQL(postgresPort, "postgres", dbURL).WithStartupTimeout(time.Second * 5)

	container := containers.NewContainer(
		ctx,
		t,
		postgresImage,
		maxRetries,
		containers.WithPorts([]string{postgresPort}),
		containers.WithEnv(env),
		containers.WithHostname(hostname),
		containers.WithWaitStrategy(waitStrategy),
	)

	mappedPort := containers.Port(ctx, t, container, postgresPort)

	url := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable", username, password, mappedPort, dbname)
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatalf("failed to establish database connection: %s", err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE TABLE roles(role_id bigint PRIMARY KEY);")
	if err != nil {
		t.Fatalf("can't create table, check command and access level")
	}

	return mappedPort
}

// TestJDBCIO_BasicReadWrite tests basic read and write transform from JDBC.
func TestJDBCIO_BasicReadWrite(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	ctx := context.Background()
	dbname := "postjdbc"
	username := "newuser"
	password := "password"

	port := setupTestContainer(ctx, t, dbname, username, password)
	tableName := "roles"
	host := "localhost"
	jdbcUrl := fmt.Sprintf("jdbc:postgresql://%s:%s/%s", host, port, dbname)

	write := WritePipeline(expansionAddr, tableName, "org.postgresql.Driver", jdbcUrl, username, password)
	ptest.RunAndValidate(t, write)

	read := ReadPipeline(expansionAddr, tableName, "org.postgresql.Driver", jdbcUrl, username, password)
	ptest.RunAndValidate(t, read)
}

// TestJDBCIO_PostgresReadWrite tests basic read and write transform from JDBC with postgres.
func TestJDBCIO_PostgresReadWrite(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	dbname := "postjdbc"
	username := "newuser"
	password := "password"
	ctx := context.Background()
	port := setupTestContainer(ctx, t, dbname, username, password)
	tableName := "roles"
	host := "localhost"
	jdbcUrl := fmt.Sprintf("jdbc:postgresql://%s:%s/%s", host, port, dbname)

	write := WriteToPostgres(expansionAddr, tableName, jdbcUrl, username, password)
	ptest.RunAndValidate(t, write)

	read := ReadFromPostgres(expansionAddr, tableName, jdbcUrl, username, password)
	ptest.RunAndValidate(t, read)
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	services := integration.NewExpansionServices()
	defer func() { services.Shutdown() }()
	addr, err := services.GetAddr("schemaio")
	if err != nil {
		log.Printf("skipping missing expansion service: %v", err)
	} else {
		expansionAddr = addr
	}

	ptest.MainRet(m)
}
