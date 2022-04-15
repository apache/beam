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

// Package jdbc contains integration tests for cross-language JDBC IO transforms.
package jdbc

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/jdbcio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*JdbcTestRow)(nil)).Elem())
}

// JdbcTestRow is the sample schema for read and write transform test.
type JdbcTestRow struct {
	Role_id int32 `beam:"role_id"`
}

func writeRows(s beam.Scope, expansionAddr, tableName, driverClassName, jdbcUrl, username, password string, input beam.PCollection) {
	s = s.Scope("jdbc_test.WriteToJdbc")
	jdbcio.Write(s, tableName, driverClassName, jdbcUrl, username, password, input, jdbcio.ExpansionAddrWrite(expansionAddr),
		jdbcio.WriteClasspaths([]string{"org.postgresql:postgresql:42.3.3", "mysql:mysql-connector-java:8.0.28"}))
}

// WritePipeline creates a pipeline for JDBC IO Write transform.
func WritePipeline(expansionAddr, tableName, driverClassName, jdbcUrl, username, password string) *beam.Pipeline {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	rows := []JdbcTestRow{{1}, {2}}
	input := beam.CreateList(s, rows)
	writeRows(s, expansionAddr, tableName, driverClassName, jdbcUrl, username, password, input)
	return p
}

func readRows(s beam.Scope, expansionAddr, tableName, driverClassName, jdbcUrl, username, password string) beam.PCollection {
	s = s.Scope("jdbc_test.ReadFromJdbc")
	outT := reflect.TypeOf((*JdbcTestRow)(nil)).Elem()
	res := jdbcio.Read(s, tableName, driverClassName, jdbcUrl, username, password, outT, jdbcio.ExpansionAddrRead(expansionAddr),
		jdbcio.ReadClasspaths([]string{"org.postgresql:postgresql:42.3.3", "mysql:mysql-connector-java:8.0.28"}))
	return res
}

// ReadPipeline creates a pipeline for JDBC IO Read transform.
func ReadPipeline(expansionAddr, tableName, driverClassName, jdbcUrl, username, password string) *beam.Pipeline {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	res := readRows(s, expansionAddr, tableName, driverClassName, jdbcUrl, username, password)
	want := beam.CreateList(s, []JdbcTestRow{{1}, {2}})
	passert.Equals(s, res, want)
	return p
}

// WriteToPostgres creates a pipeline for JDBC IO Write transform.
func WriteToPostgres(expansionAddr, tableName, jdbcUrl, username, password string) *beam.Pipeline {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	rows := []JdbcTestRow{{1}, {2}}
	input := beam.CreateList(s, rows)
	jdbcio.WriteToPostgres(s.Scope("jdbc_test.ReadFromJdbc"), tableName, jdbcUrl, username, password, input, jdbcio.ExpansionAddrWrite(expansionAddr))
	return p
}

// ReadFromPostgres creates a pipeline for JDBC IO Read transform.
func ReadFromPostgres(expansionAddr, tableName, jdbcUrl, username, password string) *beam.Pipeline {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	outT := reflect.TypeOf((*JdbcTestRow)(nil)).Elem()
	res := jdbcio.ReadFromPostgres(s.Scope("jdbc_test.WriteToJdbc"), tableName, jdbcUrl, username, password, outT, jdbcio.ExpansionAddrRead(expansionAddr))
	want := beam.CreateList(s, []JdbcTestRow{{1}, {2}})
	passert.Equals(s, res, want)
	return p
}
