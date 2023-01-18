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

// Package jdbcio contains cross-language functionality for reading and writing data to JDBC.
// These transforms only work on runners that support cross-language transforms.
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
// Current supported SDKs, including expansion service modules and reference
// documentation:
//
// Java:
//   - Vendored Module: beam-sdks-java-extensions-schemaio-expansion-service
//   - Run via Gradle: ./gradlew :sdks:java:extensions:schemaio-expansion-service:build
//     java -jar <location_of_jar_file_generated_from_above> <port>
//   - Reference Class: org.apache.beam.sdk.io.jdbc.JdbcIO
package jdbcio

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*jdbcConfigSchema)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*config)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*jdbcConfig)(nil)).Elem())
}

const (
	readURN             = "beam:transform:org.apache.beam:schemaio_jdbc_read:v1"
	writeURN            = "beam:transform:org.apache.beam:schemaio_jdbc_write:v1"
	serviceGradleTarget = ":sdks:java:extensions:schemaio-expansion-service:runExpansionService"
)

var defaultClasspaths = map[string][]string{
	"org.postgresql.Driver": []string{"org.postgresql:postgresql:42.3.3"},
	"com.mysql.jdbc.Driver": []string{"mysql:mysql-connector-java:8.0.28"},
}

// jdbcConfigSchema is the config schema as per the expected corss language payload
// for JDBC IO read and write transform.
type jdbcConfigSchema struct {
	Location   string  `beam:"location"`
	Config     []byte  `beam:"config"`
	DataSchema *[]byte `beam:"dataSchema"`
}

// config is used to set the config field of jdbcConfigSchema. It contains the
// details required to make a connection to the JDBC database.
type config struct {
	DriverClassName       string    `beam:"driverClassName"`
	JDBCUrl               string    `beam:"jdbcUrl"`
	Username              string    `beam:"username"`
	Password              string    `beam:"password"`
	ConnectionProperties  *string   `beam:"connectionProperties"`
	ConnectionInitSQLs    *[]string `beam:"connectionInitSqls"`
	ReadQuery             *string   `beam:"readQuery"`
	WriteStatement        *string   `beam:"writeStatement"`
	FetchSize             *int16    `beam:"fetchSize"`
	OutputParallelization *bool     `beam:"outputParallelization"`
}

// jdbcConfig stores the expansion service and configuration for JDBC IO.
type jdbcConfig struct {
	classpaths    []string
	expansionAddr string
	config        *config
}

// TODO(riteshghorse): update the IO to use wrapper created in BigQueryIO.
func toRow(pl any) []byte {
	rt := reflect.TypeOf(pl)

	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		panic(fmt.Errorf("unable to get row encoder"))
	}
	var buf bytes.Buffer
	if err := enc(pl, &buf); err != nil {
		panic(fmt.Errorf("unable to do row encoding"))
	}
	return buf.Bytes()
}

// Write is a cross-language PTransform which writes Rows to the specified database via JDBC.
// tableName is a required parameter, and by default, the write statement is generated from it.
// The generated write statement can be overridden by passing in a WriteStatement option.
// If an expansion service address is not provided,
// an appropriate expansion service will be automatically started; however
// this is slower than having a persistent expansion service running.
//
// If no additional classpaths are provided using jdbcio.WriteClasspaths() then the default classpath
// for that driver would be used. As of now, the default classpaths are present only for PostgreSQL and MySQL.
//
// The default write statement is: "INSERT INTO tableName(column1, ...) INTO VALUES(value1, ...)"
// Example:
//
//	  tableName := "roles"
//		 driverClassName := "org.postgresql.Driver"
//		 username := "root"
//		 password := "root123"
//		 jdbcUrl := "jdbc:postgresql://localhost:5432/dbname"
//		 jdbcio.Write(s, tableName, driverClassName, jdbcurl, username, password, jdbcio.ExpansionAddrWrite("localhost:9000"))
//
// With Classpath paramater:
//
//	jdbcio.Write(s, tableName, driverClassName, jdbcurl, username, password, jdbcio.ExpansionAddrWrite("localhost:9000"), jdbcio.WriteClasspaths([]string{"org.postgresql:postgresql:42.3.3"}))
func Write(s beam.Scope, tableName, driverClassName, jdbcUrl, username, password string, col beam.PCollection, opts ...writeOption) {
	s = s.Scope("jdbcio.Write")

	wpl := config{
		DriverClassName: driverClassName,
		JDBCUrl:         jdbcUrl,
		Username:        username,
		Password:        password,
	}
	cfg := jdbcConfig{config: &wpl}
	for _, opt := range opts {
		opt(&cfg)
	}

	if len(cfg.classpaths) == 0 {
		cfg.classpaths = defaultClasspaths[driverClassName]
	}

	expansionAddr := cfg.expansionAddr
	if expansionAddr == "" {
		if len(cfg.classpaths) > 0 {
			expansionAddr = xlangx.UseAutomatedJavaExpansionService(serviceGradleTarget, xlangx.AddClasspaths(cfg.classpaths))
		} else {
			expansionAddr = xlangx.UseAutomatedJavaExpansionService(serviceGradleTarget)
		}
	}

	jcs := jdbcConfigSchema{
		Location: tableName,
		Config:   toRow(cfg.config),
	}
	pl := beam.CrossLanguagePayload(jcs)
	beam.CrossLanguage(s, writeURN, pl, expansionAddr, beam.UnnamedInput(col), nil)
}

type writeOption func(*jdbcConfig)

func WriteClasspaths(classpaths []string) writeOption {
	return func(jc *jdbcConfig) {
		jc.classpaths = classpaths
	}
}

// WriteStatement option overrides the default write statement of
// "INSERT INTO tableName(column1, ...) INTO VALUES(value1, ...)".
func WriteStatement(statement string) writeOption {
	return func(jc *jdbcConfig) {
		jc.config.WriteStatement = &statement
	}
}

// WriteConnectionProperties properties of the jdbc connection passed as string
// with format [propertyName=property;].
func WriteConnectionProperties(properties string) writeOption {
	return func(jc *jdbcConfig) {
		jc.config.ConnectionProperties = &properties
	}
}

// ConnectionInitSQLs required only for MySql and MariaDB. passed as list of strings.
func ConnectionInitSQLs(initStatements []string) writeOption {
	return func(jc *jdbcConfig) {
		jc.config.ConnectionInitSQLs = &initStatements
	}
}

// ExpansionAddrWrite sets the expansion service for JDBC IO.
func ExpansionAddrWrite(expansionAddr string) writeOption {
	return func(jc *jdbcConfig) {
		jc.expansionAddr = expansionAddr
	}
}

// WriteToPostgres is a cross-language PTransform which writes Rows to the postgres database via JDBC.
// tableName is a required parameter, and by default, a write statement is generated from it.
// The generated write statement can be overridden by passing in a WriteStatement option.
// If an expansion service address is not provided,
// an appropriate expansion service will be automatically started; however
// this is slower than having a persistent expansion service running.
// NOTE: This transform uses "org.postgresql.Driver" as the default driver. If you want to use write transform
// with custom postgres driver then use the conventional jdbcio.Write() transform.
//
// The default write statement is: "INSERT INTO tableName(column1, ...) INTO VALUES(value1, ...)"
// Example:
//
//	  tableName := "roles"
//		 username := "root"
//		 password := "root123"
//		 jdbcUrl := "jdbc:postgresql://localhost:5432/dbname"
//		 jdbcio.WriteToPostgres(s, tableName, jdbcurl, username, password, jdbcio.ExpansionAddrWrite("localhost:9000"))
func WriteToPostgres(s beam.Scope, tableName, jdbcUrl, username, password string, col beam.PCollection, opts ...writeOption) {
	driverClassName := "org.postgresql.Driver"
	Write(s, tableName, driverClassName, jdbcUrl, username, password, col, opts...)
}

// Read is a cross-language PTransform which read Rows from the specified database via JDBC.
// tableName is a required paramater, and by default, the readQuery is generated from it.
// The generated readQuery can be overridden by passing in a readQuery.If an expansion service
// address is not provided, an appropriate expansion service will be automatically started;
// however this is slower than having a persistent expansion service running.
//
// If no additional classpaths are provided using jdbcio.ReadClasspaths() then the default classpath
// for that driver would be used. As of now, the default classpaths are present only for PostgreSQL and MySQL.
//
// The default read query is "SELECT * FROM tableName;"
//
// Read also accepts optional parameters as readOptions. All optional parameters
// are predefined in this package as functions that return readOption. To set
// an optional parameter, call the function within Read's function signature.
//
// Example:
//
//	tableName := "roles"
//	driverClassName := "org.postgresql.Driver"
//	username := "root"
//	password := "root123"
//	jdbcUrl := "jdbc:postgresql://localhost:5432/dbname"
//	outT := reflect.TypeOf((*JdbcTestRow)(nil)).Elem()
//	jdbcio.Read(s, tableName, driverClassName, jdbcurl, username, password, outT, jdbcio.ExpansionAddrRead("localhost:9000"))
//
// With Classpath parameter:
//
//	jdbcio.Read(s, tableName, driverClassName, jdbcurl, username, password, outT, jdbcio.ExpansionAddrRead("localhost:9000"), jdbcio.ReadClasspaths([]string{"org.postgresql:postgresql:42.3.3"})))
func Read(s beam.Scope, tableName, driverClassName, jdbcUrl, username, password string, outT reflect.Type, opts ...readOption) beam.PCollection {
	s = s.Scope("jdbcio.Read")

	rpl := config{
		DriverClassName: driverClassName,
		JDBCUrl:         jdbcUrl,
		Username:        username,
		Password:        password,
	}
	cfg := jdbcConfig{config: &rpl}
	for _, opt := range opts {
		opt(&cfg)
	}

	if len(cfg.classpaths) == 0 {
		cfg.classpaths = defaultClasspaths[driverClassName]
	}

	expansionAddr := cfg.expansionAddr
	if expansionAddr == "" {
		if len(cfg.classpaths) > 0 {
			expansionAddr = xlangx.UseAutomatedJavaExpansionService(serviceGradleTarget, xlangx.AddClasspaths(cfg.classpaths))
		} else {
			expansionAddr = xlangx.UseAutomatedJavaExpansionService(serviceGradleTarget)
		}
	}

	jcs := jdbcConfigSchema{
		Location: tableName,
		Config:   toRow(cfg.config),
	}
	pl := beam.CrossLanguagePayload(jcs)
	result := beam.CrossLanguage(s, readURN, pl, expansionAddr, nil, beam.UnnamedOutput(typex.New(outT)))
	return result[beam.UnnamedOutputTag()]
}

type readOption func(*jdbcConfig)

func ReadClasspaths(classpaths []string) readOption {
	return func(jc *jdbcConfig) {
		jc.classpaths = classpaths
	}
}

// ReadQuery overrides the default read query "SELECT * FROM tableName;"
func ReadQuery(query string) readOption {
	return func(jc *jdbcConfig) {
		jc.config.ReadQuery = &query
	}
}

// OutputParallelization specifies if output parallelization is on.
func OutputParallelization(status bool) readOption {
	return func(jc *jdbcConfig) {
		jc.config.OutputParallelization = &status
	}
}

// FetchSize specifies how many rows to fetch.
func FetchSize(size int16) readOption {
	return func(jc *jdbcConfig) {
		jc.config.FetchSize = &size
	}
}

// ReadConnectionProperties specifies properties of the jdbc connection passed
// as string with format [propertyName=property;]*
func ReadConnectionProperties(properties string) readOption {
	return func(jc *jdbcConfig) {
		jc.config.ConnectionProperties = &properties
	}
}

// ReadConnectionInitSQLs required only for MySql and MariaDB.
// passed as list of strings.
func ReadConnectionInitSQLs(initStatements []string) readOption {
	return func(jc *jdbcConfig) {
		jc.config.ConnectionInitSQLs = &initStatements
	}
}

// ExpansionAddrRead sets the expansion service for JDBC IO.
func ExpansionAddrRead(expansionAddr string) readOption {
	return func(jc *jdbcConfig) {
		jc.expansionAddr = expansionAddr
	}
}

// ReadFromPostgres is a cross-language PTransform which read Rows from the postgres via JDBC.
// tableName is a required parameter, and by default, a read query is generated from it.
// The generated read query can be overridden by passing in a ReadQuery. If an expansion service
// address is not provided, an appropriate expansion service will be automatically started;
// however this is slower than having a persistent expansion service running.
//
// The default read query is "SELECT * FROM tableName;"
//
// Read also accepts optional parameters as readOptions. All optional parameters
// are predefined in this package as functions that return readOption. To set
// an optional parameter, call the function within Read's function signature.
// NOTE: This transform uses "org.postgresql.Driver" as the default driver. If you want to use read transform
// with custom postgres driver then use the conventional jdbcio.Read() transform.
//
// Example:
//
//	tableName := "roles"
//	username := "root"
//	password := "root123"
//	jdbcUrl := "jdbc:postgresql://localhost:5432/dbname"
//	outT := reflect.TypeOf((*JdbcTestRow)(nil)).Elem()
//	jdbcio.Read(s, tableName, jdbcurl, username, password, outT, jdbcio.ExpansionAddrRead("localhost:9000"))
func ReadFromPostgres(s beam.Scope, tableName, jdbcUrl, username, password string, outT reflect.Type, opts ...readOption) beam.PCollection {
	driverClassName := "org.postgresql.Driver"
	return Read(s, tableName, driverClassName, jdbcUrl, username, password, outT, opts...)
}
