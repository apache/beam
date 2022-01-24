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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
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

// EncodeAsRow encodes a struct as a Beam schema Row, to embed within a cross language payload.
func EncodeAsRow(config interface{}) []byte {
	rt := reflect.TypeOf(config)
	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		err = errors.WithContextf(err, "getting Row encoder for type %s", rt.Name())
		panic(err)
	}
	var buf bytes.Buffer
	if err := enc(config, &buf); err != nil {
		err = errors.WithContextf(err, "encoding type %s as Row", rt.Name())
		panic(err)
	}
	return buf.Bytes()
}

// EncodePayload encodes a Schema IO payload. It takes a location for the Schema IO's data, an
// IO-specific configuration struct, and an optional struct representing the Beam schema for the
// data.
func EncodePayload(location string, config interface{}, dataSchema *interface{}) []byte {
	pl := Payload{
		Location: location,
		Config:   EncodeAsRow(config),
	}
	if dataSchema != nil {
		row := EncodeAsRow(*dataSchema)
		pl.DataSchema = &row
	}
	return beam.CrossLanguagePayload(pl)
}
