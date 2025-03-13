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

package schemaio

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/google/go-cmp/cmp"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*testConfig)(nil)))
	beam.RegisterType(reflect.TypeOf((*testDataSchema)(nil)))
}

type testConfig struct {
	Cfg1 string
	Cfg2 *int
}

type testDataSchema struct {
	Scm1 bool
	Scm2 float64
}

const testLocation = "test_location"

// TestEncodePayload tests that EncodePayload works properly by encoding and then decoding a
// schemaio.Payload and checking that the values match.
func TestEncodePayload(t *testing.T) {
	// Create test values, encode them, and create a payload.
	i := 42
	config := testConfig{"foo", &i}
	schemaType := reflect.TypeOf((*testDataSchema)(nil)).Elem()

	encConfig, err := encodeAsRow(config)
	if err != nil {
		t.Fatalf("failed to encode Config as row: %s", err)
	}
	encSchema, err := encodeAsSchema(schemaType)
	if err != nil {
		t.Fatalf("failed to encode DataSchema as schema: %s", err)
	}

	wantPayload := Payload{
		Location:   testLocation,
		Config:     encConfig,
		DataSchema: &encSchema,
	}

	// Encode and decode Payload, comparing results.
	payloadBytes, err := EncodePayload(testLocation, config, schemaType)
	if err != nil {
		t.Fatalf("EncodePayload failed with error: %s", err)
	}
	gotPayload, err := xlangx.DecodeStructPayload(payloadBytes)
	if err != nil {
		t.Fatalf("DecodeStructPayload failed with error: %s", err)
	}

	if diff := cmp.Diff(wantPayload, gotPayload); diff != "" {
		t.Errorf("decoded test Payload does not match Payload before encoding: diff(-want,+got):\n%v", diff)
	}
}
