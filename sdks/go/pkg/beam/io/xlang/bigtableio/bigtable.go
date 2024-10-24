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

// Package bigtableio contains cross-language functionality for using Google Cloud BigQuery
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
//   - Vendored Module: beam-sdks-java-io-google-cloud-platform-expansion-service
//   - Run via Gradle: ./gradlew :sdks:java:io:google-cloud-platform:expansion-service:runExpansionService
//   - Reference Class: org.apache.beam.sdk.io.gcp.bigtable.BigtableReadSchemaTransformProvider and
//     org.apache.beam.sdk.io.gcp.bigtable.BigtableIO
//
// # Note On Documentation
//
// This cross-language implementation relies on the behavior of external SDKs. In order to keep
// documentation up-to-date and consistent, Bigtable functionality will not be described in detail
// in this package. Instead, references to relevant documentation in other SDKs is included where
// relevant.
package bigtableio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	xlschema "github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/schema"
)

type bigtableConfig struct {
	InstanceId string `beam:"instance_id"`
	ProjectId  string `beam:"project_id"`
	TableId    string `beam:"table_id"`
}

// Cell represents a single cell in a Bigtable row.
// TODO(https://github.com/apache/beam/issues/21784): Change back to a named struct once resolved.
type Cell = struct {
	Value            []uint8 `beam:"value"`
	Timestamp_micros int64   `beam:"timestamp_micros"`
}

// Row represents a row in Bigtable.
type Row struct {
	Key             []uint8                      `beam:"key"`
	Column_families map[string]map[string][]Cell `beam:"column_families"`
}

// AddCell adds cell to a Row.  Note, this method does not deduplicate cells with the same
// (family, qualifier, timestamp) nor does it maintain cells sorted in timestamp-descending order.
func (row *Row) AddCell(family string, qualifier string, value []byte, timestamp int64) {
	if row.Column_families == nil {
		row.Column_families = make(map[string]map[string][]Cell)
	}

	cf, found := row.Column_families[family]
	if !found {
		cf = make(map[string][]Cell)
		row.Column_families[family] = cf
	}
	cf[qualifier] = append(cf[qualifier], Cell{
		Value:            value,
		Timestamp_micros: timestamp,
	})
}

const (
	readURN             = "beam:schematransform:org.apache.beam:bigtable_read:v1"
	serviceGradleTarget = ":sdks:java:io:google-cloud-platform:expansion-service:runExpansionService"
)

var autoStartupAddress = xlangx.UseAutomatedJavaExpansionService(serviceGradleTarget)

type ReadOption func(*readConfig)
type readConfig struct {
	addr string
}

// ReadExpansionAddr specifies the address of a persistent expansion service to use for a Read
// transform. If this is not provided, or if an empty string is provided, the transform will
// automatically start an appropriate expansion service instead.
func ReadExpansionAddr(addr string) ReadOption {
	return func(rc *readConfig) {
		rc.addr = addr
	}
}

// Read reads rows from a Bigtable table.
func Read(s beam.Scope, projectId string, instanceId string, table string, opts ...ReadOption) beam.PCollection {
	rc := readConfig{}
	for _, opt := range opts {
		opt(&rc)
	}

	addr := rc.addr
	if addr == "" {
		addr = autoStartupAddress
	}
	btConfig := bigtableConfig{InstanceId: instanceId, ProjectId: projectId, TableId: table}

	outs := xlschema.Transform(s, btConfig, readURN, xlschema.ExpansionAddr(addr), xlschema.UnnamedOutputType(typex.New(reflect.TypeOf(Row{}))))
	return outs[beam.UnnamedOutputTag()]
}
