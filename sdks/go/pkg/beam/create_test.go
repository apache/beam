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

package beam_test

import (
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/golang/protobuf/proto"
)

type wc struct {
	K string
	V int
}

func TestCreate(t *testing.T) {
	tests := []struct {
		values []interface{}
	}{
		{[]interface{}{1, 2, 3}},
		{[]interface{}{"1", "2", "3"}},
		{[]interface{}{float32(0.1), float32(0.2), float32(0.3)}},
		{[]interface{}{float64(0.1), float64(0.2), float64(0.3)}},
		{[]interface{}{uint(1), uint(2), uint(3)}},
		{[]interface{}{false, true, true, false, true}},
		{[]interface{}{wc{"a", 23}, wc{"b", 42}, wc{"c", 5}}},
		{[]interface{}{&testProto{}, &testProto{stringValue("test")}}}, // Test for BEAM-4401
	}

	for _, test := range tests {
		p, s, c := ptest.Create(test.values)
		passert.Equals(s, c, test.values...)

		if err := ptest.Run(p); err != nil {
			t.Errorf("beam.Create(%v) failed: %v", test.values, err)
		}
	}
}

type testProto struct {
	// OneOfField is an interface-typed field and cannot be JSON-marshaled, but
	// should be specially handled by Beam as a field of a proto.Message.
	OneOfField _isOneOfField
}

type stringValue string

func (stringValue) isOneOfField() {}

type _isOneOfField interface {
	isOneOfField()
}

func (t *testProto) Reset()         { *t = testProto{} }
func (t *testProto) String() string { return fmt.Sprintf("one_of_field: %#v", t.OneOfField) }
func (t *testProto) ProtoMessage()  {}

func (t *testProto) Marshal() ([]byte, error) {
	if t.OneOfField == nil {
		return nil, nil
	}
	return []byte(t.OneOfField.(stringValue)), nil
}
func (t *testProto) Unmarshal(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	t.OneOfField = stringValue(b)
	return nil
}

// Ensure testProto is detected as a proto.Message and can be (un)marshalled by
// the proto library.
var (
	_ proto.Message     = &testProto{}
	_ proto.Marshaler   = &testProto{}
	_ proto.Unmarshaler = &testProto{}
)
