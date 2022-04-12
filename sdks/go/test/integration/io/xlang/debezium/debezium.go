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

// Package debezium contains integration tests for cross-language Debezium IO
// transforms.
package debezium

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/debeziumio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

// ReadPipeline creates a pipeline for debeziumio.Read PTransform and validates the result.
func ReadPipeline(addr, username, password, dbname, host, port string, connectorClass debeziumio.DriverClassName, maxrecords int64, connectionProperties []string) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()
	result := debeziumio.Read(s.Scope("Read from debezium"), username, password, host, port,
		connectorClass, reflectx.String, debeziumio.MaxRecord(maxrecords),
		debeziumio.ConnectionProperties(connectionProperties), debeziumio.ExpansionAddr(addr))
	expectedJson := `{"metadata":{"connector":"postgresql","version":"1.3.1.Final","name":"dbserver1","database":"inventory","schema":"inventory","table":"customers"},"before":null,"after":{"fields":{"last_name":"Thomas","id":1001,"first_name":"Sally","email":"sally.thomas@acme.com"}}}`
	expected := beam.Create(s, expectedJson)
	passert.Equals(s, result, expected)
	return p
}
