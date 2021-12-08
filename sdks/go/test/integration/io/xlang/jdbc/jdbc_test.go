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
	"log"
	"net"
	"testing"
	"time"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	"github.com/docker/go-connections/nat"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func checkFlags(t *testing.T) {
	if *integration.IoExpansionAddr == "" {
		t.Skip("No IO expansion address provided.")
	}
}

// TestJDBCIO_BasicReadWrite tests basic writes and reads from JDBC.
func TestJDBCIO_BasicReadWrite(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)
	dbname := "test"
	username := "test"
	password := "test"
	ctx := context.Background()
	var env = map[string]string{
		"POSTGRES_PASSWORD": password,
		"POSTGRES_USER":     username,
		"POSTGRES_DB":       dbname,
	}
	var port = "5432/tcp"
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	dbURL := func(port nat.Port) string {
		return fmt.Sprintf("postgres://test:test@%s:%s/%s?sslmode=disable", localAddr.IP, port.Port(), dbname)
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
	container, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		t.Errorf("failed to start container: %s", err)
	}

	mappedPort, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Errorf("failed to get container external port: %s", err)
	}

	log.Println("postgres container ready and running at port: ", mappedPort)

	// url := fmt.Sprintf("postgres://test:test@localhost:%s/%s?sslmode=disable", mappedPort.Port(), dbname)
	url := fmt.Sprintf("postgresql://test:test@%s:%s/%s?sslmode=disable", localAddr.IP, mappedPort.Port(), dbname)
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Errorf("failed to establish database connection: %s", err)
	}
	defer db.Close()
	defer container.Terminate(ctx)

	table_name := "TestTable"
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s(F_id VARCHAR);", table_name))
	if err != nil {
		t.Errorf("table not created: %v", err)
	}

	jdbcUrl := fmt.Sprintf("jdbc:postgresql://%s:%s/%s", localAddr.IP, mappedPort.Port(), dbname)
	write := WritePipeline(*integration.IoExpansionAddr, table_name, "org.postgresql.Driver", jdbcUrl, username, password)
	ptest.RunAndValidate(t, write)
	// _, err = db.Exec(fmt.Sprintf("CREATE TABLE %s(f_int INTEGER)", table_name))
	// if err != nil {
	// 	t.Error("table not created")
	// }
	// _, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES(%d)", table_name, 1))
	// if err != nil {
	// 	t.Error("can't insert values")
	// }
	// read := ReadPipeline(*integration.IoExpansionAddr, table_name, "org.postgresql.Driver", url, username, password)
	// ptest.RunAndValidate(t, read)
}

func TestMain(m *testing.M) {
	ptest.Main(m)
}
