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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"google.golang.org/protobuf/protoadapt"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func init() {
	beam.RegisterType(reflect.TypeOf((*wc)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*testProto)(nil)).Elem())
}

type wc struct {
	K string
	V int
}

func TestCreate(t *testing.T) {
	tests := []struct {
		values []any
	}{
		{[]any{1, 2, 3}},
		{[]any{"1", "2", "3"}},
		{[]any{float32(0.1), float32(0.2), float32(0.3)}},
		{[]any{float64(0.1), float64(0.2), float64(0.3)}},
		{[]any{uint(1), uint(2), uint(3)}},
		{[]any{false, true, true, false, true}},
		{[]any{wc{"a", 23}, wc{"b", 42}, wc{"c", 5}}},
		{[]any{&testProto{}, &testProto{stringValue("test")}}}, // Test for BEAM-4401
	}

	for _, test := range tests {
		p, s := beam.NewPipelineWithRoot()
		c := beam.Create(s, test.values...)
		passert.Equals(s, c, test.values...)

		if err := ptest.Run(p); err != nil {
			t.Errorf("beam.Create(%v) failed: %v", test.values, err)
		}
	}
}

func TestCreateList(t *testing.T) {
	tests := []struct {
		values any
	}{
		{[]int{1, 2, 3}},
		{[]string{"1", "2", "3"}},
		{[]float32{float32(0.1), float32(0.2), float32(0.3)}},
		{[]float64{float64(0.1), float64(0.2), float64(0.3)}},
		{[]uint{uint(1), uint(2), uint(3)}},
		{[]bool{false, true, true, false, true}},
		{[]wc{{"a", 23}, {"b", 42}, {"c", 5}}},
		{[]*testProto{{}, {stringValue("test")}}}, // Test for BEAM-4401
	}

	for _, test := range tests {
		p, s := beam.NewPipelineWithRoot()
		c := beam.CreateList(s, test.values)

		var values []any
		v := reflect.ValueOf(test.values)
		for i := 0; i < v.Len(); i++ {
			values = append(values, v.Index(i).Interface())
		}
		passert.Equals(s, c, values...)

		if err := ptest.Run(p); err != nil {
			t.Errorf("beam.CreateList(%v) failed: %v", test.values, err)
		}
	}
}

func TestCreateEmptyList(t *testing.T) {
	tests := []struct {
		values any
	}{
		{[]int{}},
		{[]string{}},
		{[]float32{}},
		{[]float64{}},
		{[]uint{}},
		{[]bool{}},
		{[]wc{}},
		{[]*testProto{}}, // Test for BEAM-4401
	}

	for _, test := range tests {
		p, s := beam.NewPipelineWithRoot()
		c := beam.CreateList(s, test.values)

		passert.Empty(s, c)

		if err := ptest.Run(p); err != nil {
			t.Errorf("beam.CreateList(%v) failed: %v", test.values, err)
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
	_ protoadapt.MessageV1 = &testProto{}
)
