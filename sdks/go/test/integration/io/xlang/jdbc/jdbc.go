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
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/jdbcio"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*JdbcWriteTestRow)(nil)).Elem())
}

type JdbcWriteTestRow struct {
	F_id string `beam:"f_id"`
}

// writeList encodes a list of ints and sends encoded ints to Kafka.
func writeRows(s beam.Scope, expansionAddr, tableName, driverClassName, jdbcUrl, username, password string) {
	s = s.Scope("jdbc_test.WriteToJdbc")
	statement := fmt.Sprintf("CREATE TABLE %s(f_id int(10));", tableName)
	rows := []JdbcWriteTestRow{{"row1"}, {"row2"}}
	input := beam.CreateList(s, rows)
	jdbcio.Write(s, expansionAddr, tableName, driverClassName, jdbcUrl, username, password, input, jdbcio.WriteStatement(statement))
}

// WritePipeline creates a pipeline that writes a given slice of ints to Kafka.
func WritePipeline(expansionAddr, tableName, driverClassName, jdbcUrl, username, password string) *beam.Pipeline {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	writeRows(s, expansionAddr, tableName, driverClassName, jdbcUrl, username, password)
	return p
}
