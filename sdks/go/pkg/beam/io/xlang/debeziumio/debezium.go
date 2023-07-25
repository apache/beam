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

// Package debeziumio contains cross-language functionality for using Debezium
// (http://kafka.apache.org/). These transforms only work on runners that
// support cross-language transforms.
//
// # Setup
//
// Transforms specified here are cross-language transforms implemented in a
// different SDK (listed below). During pipeline construction, the Go SDK will
// need to connect to an expansion service containing information on these
// transforms in their native SDK.
//
// To use an expansion service, it must be run as a separate process accessible
// during pipeline construction. The address of that process must be passed to
// the transforms in this package.
//
// The version of the expansion service should match the version of the Beam SDK
// being used. For numbered releases of Beam, these expansions services are
// released to the Maven repository as modules. For development versions of
// Beam, it is recommended to build and run it from source using Gradle.
//
// # Current supported SDKs including expansion service modules
//
// Java:
//   - Vendored Module: beam-sdks-java-io-debezium-expansion-service
//   - Run via Gradle: ./gradlew :sdks:java:io:debezium:expansion-service:shadowJar
//     java -jar <path-to-debezium-jar> <port>
//   - Reference Class: org.apache.beam.io.debezium.DebeziumIO
package debeziumio

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// DriverClassName is the type for valid and supported Database connectors for Debezium IO.
type DriverClassName string

const (
	// MySQL connector for Debezium
	MySQL DriverClassName = "MySQL"
	// PostgreSQL connector for Debezium
	PostgreSQL = "PostgreSQL"
)

const serviceGradleTarget = ":sdks:java:io:debezium:expansion-service:shadowJar"

var autoStartupAddress = xlangx.UseAutomatedJavaExpansionService(serviceGradleTarget)

const readURN = "beam:transform:org.apache.beam:debezium_read:v1"

// readFromDebeziumSchema is config schema that matches exactly with the Java's Debezium IO
// for cross language payload.
type readFromDebeziumSchema struct {
	ConnectorClass       string
	Username             string
	Password             string
	Host                 string
	Port                 string
	MaxNumberOfRecords   *int64
	ConnectionProperties []string
}

type debeziumConfig struct {
	expansionAddr string
	readSchema    *readFromDebeziumSchema
}

// readOption facilitates additional parameters to debeziumio.Read() Ptransform.
type readOption func(*debeziumConfig)

// Read is an external PTransform which reads from Debezium and returns a
// JSON string. It requires the address of an expansion service for Debezium IO.
// If both the  host and port address are provided as "", an appropriate expansion
// service will be automatically started; however this is slower than having a
// persistent expansion service running.
//
// Example:
//
//	username := "debezium"
//	password := "dbz"
//	host := "localhost"
//	port := "5432"
//	connectorClass := debeziumIO.POSTGRESQL
//	maxrecords := 1
//	debeziumio.Read(s.Scope("Read from debezium"), expansionAddr, username, password, host, port, connectorClass,
//	                reflectx.String, debeziumio.MaxRecord(maxrecords), debeziumio.ExpansionAddr("localhost:9000"))
func Read(s beam.Scope, username, password, host, port string, connectorClass DriverClassName, t reflect.Type, opts ...readOption) beam.PCollection {
	rfds := readFromDebeziumSchema{
		ConnectorClass: string(connectorClass),
		Username:       username,
		Password:       password,
		Host:           host,
		Port:           port,
	}
	dc := debeziumConfig{readSchema: &rfds}
	for _, opt := range opts {
		opt(&dc)
	}

	expansionAddr := dc.expansionAddr
	if dc.expansionAddr == "" {
		expansionAddr = autoStartupAddress
	}

	pl := beam.CrossLanguagePayload(rfds)
	outT := beam.UnnamedOutput(typex.New(t))
	out := beam.CrossLanguage(s, readURN, pl, expansionAddr, nil, outT)
	return out[beam.UnnamedOutputTag()]
}

// MaxRecord specifies maximum number of records to be fetched before stop.
func MaxRecord(r int64) readOption {
	return func(cfg *debeziumConfig) {
		cfg.readSchema.MaxNumberOfRecords = &r
	}
}

// ConnectionProperties specifies properties of the debezium connection passed as
// a string with format [propertyName=property;]*
func ConnectionProperties(cp []string) readOption {
	return func(cfg *debeziumConfig) {
		cfg.readSchema.ConnectionProperties = cp
	}
}

// ExpansionAddr sets the expansion service address to use for DebeziumIO cross-langauage transform.
func ExpansionAddr(expansionAddr string) readOption {
	return func(cfg *debeziumConfig) {
		cfg.expansionAddr = expansionAddr
	}
}
