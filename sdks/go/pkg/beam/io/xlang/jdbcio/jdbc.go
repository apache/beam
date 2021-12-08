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

package jdbcio

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

// var (
// 	JdbcCSType = reflect.TypeOf((*jdbcConfigSchema)(nil)).Elem()
// )

func init() {
	beam.RegisterType(reflect.TypeOf((*JdbcConfigSchema)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*config)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*Payload)(nil)).Elem())
}

const (
	readURN  = "beam:transform:org.apache.beam:schemaio_jdbc_read:v1"
	writeURN = "beam:transform:org.apache.beam:schemaio_jdbc_write:v1"
)

type Payload struct {
	Location   string  `beam:"location"`
	Config     []byte  `beam:"config"`
	DataSchema *[]byte `beam:"dataSchema"`
}

type JdbcConfigSchema struct {
	Location string `beam:"location"`
	Config   []byte `beam:"config"`
}

type config struct {
	DriverClassName       string
	JDBCUrl               string
	Username              string
	Password              string
	ConnectionProperties  string
	ConnectionInitSQLs    []string
	WriteStatement        string
	ReadQuery             string
	FetchSize             int16
	OutputParallelization bool
}

func toRow(pl interface{}) []byte {
	rt := reflect.TypeOf(pl)

	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		panic(fmt.Errorf("error 1"))
	}
	var buf bytes.Buffer
	if err := enc(pl, &buf); err != nil {
		panic(fmt.Errorf("error 2"))
	}
	return buf.Bytes()
}

type readOption func(*config)

func Read(s beam.Scope, addr, tableName, driverClassName, jdbcUrl, username, password string, opts ...readOption) beam.PCollection {
	s = s.Scope("jdbcio.Read")

	rpl := config{
		DriverClassName:       driverClassName,
		JDBCUrl:               jdbcUrl,
		Username:              username,
		Password:              password,
		ConnectionProperties:  "",
		ConnectionInitSQLs:    []string{},
		WriteStatement:        "",
		ReadQuery:             "",
		FetchSize:             0,
		OutputParallelization: true,
	}
	for _, opt := range opts {
		opt(&rpl)
	}
	jcs := JdbcConfigSchema{
		Location: tableName,
		Config:   toRow(rpl),
	}
	cp := toRow(jcs)
	pl := beam.CrossLanguagePayload(Payload{Config: cp, DataSchema: nil})
	outT := beam.UnnamedOutput(typex.New(reflectx.ByteSlice))
	out := beam.CrossLanguage(s, readURN, pl, addr, nil, outT)
	return out[beam.UnnamedOutputTag()]
}

func ReadQuery(query string) readOption {
	return func(pl *config) {
		pl.ReadQuery = query
	}
}

func OutputParallelization(status bool) readOption {
	return func(pl *config) {
		pl.OutputParallelization = status
	}
}

func FetchSize(size int16) readOption {
	return func(pl *config) {
		pl.FetchSize = size
	}
}

func ReadConnectionProperties(properties string) readOption {
	return func(pl *config) {
		pl.ConnectionProperties = properties
	}
}

func ReadConnectionInitSQLs(initStatements []string) readOption {
	return func(pl *config) {
		pl.ConnectionInitSQLs = initStatements
	}
}

type writeOption func(*config)

func Write(s beam.Scope, addr, tableName, driverClassName, jdbcUrl, username, password string, col beam.PCollection, opts ...writeOption) {
	s = s.Scope("jdbcio.Write")

	wpl := config{
		DriverClassName:       driverClassName,
		JDBCUrl:               jdbcUrl,
		Username:              username,
		Password:              password,
		ConnectionProperties:  "",
		ConnectionInitSQLs:    []string{},
		WriteStatement:        "",
		ReadQuery:             "",
		FetchSize:             0,
		OutputParallelization: true,
	}
	for _, opt := range opts {
		opt(&wpl)
	}
	jcs := JdbcConfigSchema{
		Location: tableName,
		Config:   toRow(wpl),
	}
	cp := toRow(jcs)
	pl := beam.CrossLanguagePayload(Payload{Config: cp, DataSchema: nil})
	outT := beam.UnnamedOutput(typex.New(reflect.TypeOf(col)))
	beam.CrossLanguage(s, writeURN, pl, addr, beam.UnnamedInput(col), outT)
}

func WriteStatement(statement string) writeOption {
	return func(pl *config) {
		pl.WriteStatement = statement
	}
}

func WriteConnectionProperties(properties string) writeOption {
	return func(pl *config) {
		pl.ConnectionProperties = properties
	}
}

func ConnectionInitSQLs(initStatements []string) writeOption {
	return func(pl *config) {
		pl.ConnectionInitSQLs = initStatements
	}
}
