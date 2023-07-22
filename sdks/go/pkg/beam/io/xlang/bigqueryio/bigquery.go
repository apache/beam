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

// Package bigqueryio contains cross-language functionality for using Google Cloud BigQuery
// (https://cloud.google.com/bigquery). These transforms only work on runners that support
// cross-language transforms.
//
// # Setup
//
// Transforms specified here are cross-language transforms implemented in a
// different SDK (listed below). During pipeline construction, the Go SDK will
// need to connect to an expansion service containing information on these
// transforms in their native SDK. If an expansion service address is not
// provided, an appropriate expansion service will be automatically started;
// however this is slower than having a persistent expansion service running.
//
// To use a persistent expansion service, it must be run as a separate process
// accessible during pipeline construction. The address of that process must be
// passed to the transforms in this package.
//
// The version of the expansion service should match the version of the Beam SDK
// being used. For numbered releases of Beam, these expansions services are
// released to the Maven repository as modules. For development versions of
// Beam, it is recommended to build and run it from source using Gradle.
//
// Current supported SDKs, including expansion service modules and reference
// documentation:
//
// Java:
//   - Vendored Module: beam-sdks-java-extensions-schemaio-expansion-service
//   - Run via Gradle: ./gradlew :sdks:java:extensions:schemaio-expansion-service:runExpansionService
//   - Reference Class: org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaIOProvider and
//     org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
//
// # Type Conversions
//
// Elements are read from and written to BigQuery by first converting to a Beam schema Row type
// before converting to BigQuery compatible types. The following table lists all BigQuery types
// currently supported, and how they convert to Beam schema and Go types.
//
//	+----------------------------+------------------+-----------------+
//	| BigQuery Standard SQL Type | Beam Schema Type |     Go Type     |
//	+----------------------------+------------------+-----------------+
//	| BOOLEAN                    | BOOLEAN          | bool            |
//	| INT64                      | INT64            | int64           |
//	| FLOAT64                    | DOUBLE           | float64         |
//	| BYTES                      | BYTES            | []byte          |
//	| STRING                     | STRING           | string          |
//	| ARRAY                      | ARRAY            | Special: slice  |
//	| STRUCT                     | ROW              | Special: struct |
//	+----------------------------+------------------+-----------------+
//
// Array types are inferred from slice fields. For example, []int64 is equivalent to BigQuery's
// ARRAY<INT64>. Struct types are inferred from nested structs in Go.
//
// Additionally, BigQuery schema fields can have a mode assigned to specify whether the field is
// Nullable, Required, or Repeated. In Go, Nullable fields are represented as pointers, whereas
// Required fields are value types. Repeated fields are represented as slices in Go (and ARRAYS
// in SQL, as in the table above).
//
// Example of BigQuery fields with modes:
//
//	field1 *int64   // Nullable INT64
//	field2 int64    // Required INT64
//	field3 []int64  // Repeated INT64
//
// # Note On Documentation
//
// This cross-language implementation relies on the behavior of external SDKs. In order to keep
// documentation up-to-date and consistent, BigQuery functionality will not be described in detail
// in this package. Instead, references to relevant documentation in other SDKs is included where
// relevant.
package bigqueryio

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/schemaio"
)

type createDisposition string

const (
	// CreateNever specifies that new tables should not be created when writing to BigQuery.
	CreateNever createDisposition = "Never"

	// CreateIfNeeded specifies that tables should be created when writing to BigQuery, if needed.
	CreateIfNeeded createDisposition = "IfNeeded"

	readURN  = "beam:transform:org.apache.beam:schemaio_bigquery_read:v1"
	writeURN = "beam:transform:org.apache.beam:schemaio_bigquery_write:v1"

	serviceGradleTarget = ":sdks:java:io:google-cloud-platform:expansion-service:runExpansionService"
)

var autoStartupAddress = xlangx.UseAutomatedJavaExpansionService(serviceGradleTarget)

// bigQueryConfig is a struct meant to match the Schema IO config for Java's BigQuery IO. This is
// used for both reads and writes, and is meant to match the schema defined in the Java SDK method
// org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaIOProvider.configurationSchema().
type bigQueryConfig struct {
	Table             *string `beam:"table"`
	Query             *string `beam:"query"`
	QueryLocation     *string `beam:"queryLocation"`
	CreateDisposition *string `beam:"createDisposition"`
}

// Read is a cross-language PTransform which reads from a BigQuery table and returns a PCollection
// of the given type, which should correspond to the Schema type generated by reading from the
// table.
//
// Read requires a reflect.Type description of the struct to read from BigQuery. Additionally,
// either one Table or one Query must be provided via readOptions to define a destination to read
// from.
//
// Read accepts additional parameters as readOptions. All optional parameters are predefined in this
// package as functions that return readOption. To set an additional parameter, call the function
// within Read's function signature.
//
// Example:
//
//	expansionAddr := "localhost:1234"
//	table := "project_id:dataset_id.table_id"
//	outType := reflect.TypeOf((*Foo)(nil)).Elem()
//	pcol := bigqueryio.Read(s, outType,
//	    bigqueryio.FromTable(table),
//	    bigqueryio.ReadExpansionAddr(expansionAddr))
func Read(s beam.Scope, elmT reflect.Type, opts ...readOption) beam.PCollection {
	s = s.Scope("bigqueryio.Read")

	rc := readConfig{cfg: &bigQueryConfig{}}
	for _, opt := range opts {
		opt(&rc)
	}
	if rc.cfg.Table == nil && rc.cfg.Query == nil {
		panic(fmt.Sprintf("%v requires either a Table or Query specified, received none", s.String()))
	}

	addr := rc.addr
	if addr == "" {
		addr = autoStartupAddress
	}

	pl := schemaio.MustEncodePayload("", rc.cfg, nil)
	outT := typex.New(elmT)
	outs := beam.CrossLanguage(s, readURN, pl, addr, nil, beam.UnnamedOutput(outT))
	return outs[beam.UnnamedOutputTag()]
}

type readConfig struct {
	cfg  *bigQueryConfig
	addr string
}
type readOption func(*readConfig)

// ReadExpansionAddr specifies the address of a persistent expansion service to use for a Read
// transform. If this is not provided, or if an empty string is provided, the transform will
// automatically start an appropriate expansion service instead.
func ReadExpansionAddr(addr string) readOption {
	return func(rc *readConfig) {
		rc.addr = addr
	}
}

// FromTable is a Read option that specifies which table to read from.
//
// For more details see in the Java SDK:
// org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Read.from(java.lang.String)
func FromTable(table string) readOption {
	return func(rc *readConfig) {
		rc.cfg.Table = &table
	}
}

// FromQuery is a Read option that specifies a query to use for reading from BigQuery. Uses the
// BigQuery Standard SQL dialect.
//
// Important: When reading from a query, the schema of any source tables is not used and the read
// transform cannot detect which elements are Required, therefore every field in the output type
// will be a pointer (including fields within inner structs).
//
// For more details see in the Java SDK:
// org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Read.fromQuery(java.lang.String)
//
// BUG(https://github.com/apache/beam/issues/21784): Query read outputs currently cannot be named
// struct types. See link for workaround.
func FromQuery(query string) readOption {
	return func(rc *readConfig) {
		rc.cfg.Query = &query
	}
}

// WithQueryLocation is a Read option that specifies a BigQuery geographic location where the query
// job will be executed.
//
// For more details see in the Java SDK:
// org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.withQueryLocation
func WithQueryLocation(location string) readOption {
	return func(rc *readConfig) {
		rc.cfg.QueryLocation = &location
	}
}

// Write is a cross-language PTRansform which writes elements from a PCollection to a BigQuery
// table.
//
// Write requires the ID of a BigQuery table, and an input PCollection. The type of the input
// PCollection is converted to a Beam schema, so it must have a valid Beam schema definition.
//
// Write accepts additional parameters as writeOptions. All optional parameters are predefined in
// this package as functions that return writeOption. To set an additional parameter, call the
// function within Write's function signature.
//
// Example:
//
//	expansionAddr := "localhost:1234"
//	table := "project_id:dataset_id.table_id"
//	pcol := bigqueryio.Write(s, table, input,
//	   bigqueryio.CreateDisposition(bigqueryio.CreateIfNeeded),
//	   bigqueryio.WriteExpansionAddr(expansionAddr))
func Write(s beam.Scope, table string, col beam.PCollection, opts ...writeOption) {
	s = s.Scope("bigqueryio.Write")

	wc := writeConfig{cfg: &bigQueryConfig{}}
	wc.cfg.Table = &table
	for _, opt := range opts {
		opt(&wc)
	}

	addr := wc.addr
	if addr == "" {
		addr = autoStartupAddress
	}

	pl := schemaio.MustEncodePayload("", *wc.cfg, nil)
	beam.CrossLanguage(s, writeURN, pl, addr, beam.UnnamedInput(col), nil)
}

type writeConfig struct {
	cfg  *bigQueryConfig
	addr string
}
type writeOption func(*writeConfig)

// WriteExpansionAddr specifies the address of a persistent expansion service to use for a Write
// transform. If this is not provided, or if an empty string is provided, the transform will
// automatically start an appropriate expansion service instead.
func WriteExpansionAddr(addr string) writeOption {
	return func(wc *writeConfig) {
		wc.addr = addr
	}
}

// CreateDisposition specifies the write transform's behavior in regards to creating new tables.
//
// For more details see in the Java SDK:
// org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.withCreateDisposition
func CreateDisposition(disp createDisposition) writeOption {
	str := string(disp)
	return func(wc *writeConfig) {
		wc.cfg.CreateDisposition = &str
	}
}
