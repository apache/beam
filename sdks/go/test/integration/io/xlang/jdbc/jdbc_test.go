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
	"fmt"
	"testing"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
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
	// dbname := "postjdbc"
	// username := "newuser"
	// password := "password"

	// var env = map[string]string{
	// 	"POSTGRES_PASSWORD": password,
	// 	"POSTGRES_USER":     username,
	// 	"POSTGRES_DB":       dbname,
	// }

	// var port = "5432/tcp"
	// dbURL := func(port nat.Port) string {
	// 	return fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable", username, password, port.Port(), dbname)
	// }

	// req := testcontainers.GenericContainerRequest{
	// 	ContainerRequest: testcontainers.ContainerRequest{
	// 		Image:        "postgres",
	// 		ExposedPorts: []string{port},
	// 		Env:          env,
	// 		WaitingFor:   wait.ForSQL(nat.Port(port), "postgres", dbURL).Timeout(time.Second * 5),
	// 	},
	// 	Started: true,
	// }
	// ctx := context.Background()
	// container, err := testcontainers.GenericContainer(ctx, req)
	// if err != nil {
	// 	t.Errorf("failed to start container: %s", err)
	// }

	// mappedPort, err := container.MappedPort(ctx, nat.Port(port))
	// if err != nil {
	// 	t.Errorf("failed to get container external port: %s", err)
	// }
	// p := mappedPort.Int()
	// host, err := container.ContainerIP(ctx)
	// if err != nil {
	// 	t.Error("error in container ip")
	// }
	// t.Error(host, p)
	tableName := "roles"
	host := "localhost"
	p := 5432
	dbname := "posts"
	username := "newuser"
	password := "newuser123"
	jdbcUrl := fmt.Sprintf("jdbc:postgresql://%s:%d/%s", host, p, dbname)

	// read := ReadPipeline(*integration.IoExpansionAddr, tableName, "org.postgresql.Driver", jdbcUrl, username, password)
	// ptest.RunAndValidate(t, read)
	write := WritePipeline(*integration.IoExpansionAddr, tableName, "org.postgresql.Driver", jdbcUrl, username, password)
	ptest.RunAndValidate(t, write)
}

func TestMain(m *testing.M) {
	ptest.Main(m)
}
