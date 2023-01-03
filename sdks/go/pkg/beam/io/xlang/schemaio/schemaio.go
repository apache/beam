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

// Package schemaio contains utilities for constructing cross-language IO wrappers meant to
// interface with the Java SDK's Schema IOs. Schema IO is an interface for any IO that operates on
// Beam schema supported elements. Various IOs are implemented via Schema IO, and each
// implementation requires its own IO wrapper in the Go SDK (for example, JDBC IO or BigQuery IO),
// and those IO wrappers can make use of these utilities.
//
// For implementation details of Schema IO see https://s.apache.org/schemaio-development-guide.
package schemaio

import (
	"bytes"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/protobuf/proto"
)

// Payload is a struct matching the expected cross-language payload of a Schema IO.
//
// This documentation describes the expected usage of each field, but individual IO implementations
// are free to use this payload differently. For implementation details of those IOs, refer to
// SchemaIOProvider implementations in the Java SDK.
type Payload struct {
	// Location specifies the location to find the data (for example, a URL to a database).
	Location string `beam:"location"`

	// Config is a Beam schema encoded struct containing configuration details specific to the
	// underlying IO implementation.
	Config []byte `beam:"config"`

	// DataSchema is an optional Beam schema encoded struct representing the schema for data being
	// read or written.
	DataSchema *[]byte `beam:"dataSchema"`
}

// encodeAsRow encodes a struct as a Beam schema Row, to embed within a cross language payload.
func encodeAsRow(config any) ([]byte, error) {
	rt := reflect.TypeOf(config)
	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		err = errors.WithContextf(err, "getting Row encoder for type %s", rt.Name())
		return nil, err
	}
	var buf bytes.Buffer
	if err := enc(config, &buf); err != nil {
		err = errors.WithContextf(err, "encoding type %s as Row", rt.Name())
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeAsSchema retrieves a schema of a type, and encodes that schema into bytes.
func encodeAsSchema(rt reflect.Type) ([]byte, error) {
	scm, err := schema.FromType(rt)
	if err != nil {
		err = errors.WithContextf(err, "retrieving schema of type %s", rt.Name())
		return nil, err
	}
	encScm, err := proto.Marshal(scm)
	if err != nil {
		err = errors.WithContextf(err, "encoding schema of type %s", rt.Name())
		return nil, err
	}
	return encScm, nil
}

// EncodePayload encodes a SchemaIO payload. It takes a location for the SchemaIO's data, an
// IO-specific configuration struct, and an optional struct representing the Beam schema for the
// data.
func EncodePayload(location string, config any, dataSchema reflect.Type) ([]byte, error) {
	encCfg, err := encodeAsRow(config)
	if err != nil {
		err = errors.WithContext(err, "encoding config for SchemaIO payload")
		return nil, err
	}
	pl := Payload{
		Location: location,
		Config:   encCfg,
	}

	if dataSchema != nil {
		encScm, err := encodeAsSchema(dataSchema)
		if err != nil {
			err = errors.WithContext(err, "encoding dataSchema for SchemaIO payload")
			return nil, err
		}
		pl.DataSchema = &encScm
	}
	return beam.CrossLanguagePayload(pl), err
}

// MustEncodePayload encodes a SchemaIO payload. It takes a location for the SchemaIO's data, an
// IO-specific configuration struct, and an optional struct representing the Beam schema for the
// data. Unlike EncodePayload, this panics if an error occurs.
func MustEncodePayload(location string, config any, dataSchema reflect.Type) []byte {
	pl, err := EncodePayload(location, config, dataSchema)
	if err != nil {
		panic(err)
	}
	return pl
}
