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

// Package testutil contains helpers to test and validate custom Beam Schema coders.
package testutil

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/google/go-cmp/cmp"
)

// SchemaCoder helps validate custom schema coders.
type SchemaCoder struct {
	encBldUT, encBldSchema coder.RowEncoderBuilder
	decBldUT, decBldSchema coder.RowDecoderBuilder

	// CmpOptions to pass into the round trip comparison
	CmpOptions cmp.Options
}

// Register adds additional custom types not under test to both the under test
// and default schema coders.
func (v *SchemaCoder) Register(rt reflect.Type, encF, decF any) {
	v.encBldUT.Register(rt, encF)
	v.encBldSchema.Register(rt, encF)
	v.decBldUT.Register(rt, decF)
	v.decBldSchema.Register(rt, decF)
}

// T is an interface to facilitate testing the tester. The methods need
// to match the one's we're using of *testing.T.
type T interface {
	Helper()
	Run(string, func(*testing.T)) bool
	Errorf(string, ...any)
	Failed() bool
	FailNow()
}

// Validate is a test utility to validate custom schema coders generate
// beam schema encoded bytes.
//
// Validate accepts the reflect.Type to register, factory functions for
// encoding and decoding, an anonymous struct type equivalent to the encoded
// format produced and consumed by the factory produced functions and test
// values. Test values must be either a struct, pointer to struct, or a slice
// where each element is a struct or pointer to struct.
//
// TODO(lostluck): Improve documentation.
// TODO(lostluck): Abstract into a configurable struct, to handle
//
// Validate will register the under test factories and generate an encoder and
// decoder function. These functions will be re-used for all test values. This
// emulates coders being re-used for all elements within a bundle.
//
// Validate mutates the SchemaCoderValidator, so the SchemaCoderValidator may not be used more than once.
func (v *SchemaCoder) Validate(t T, rt reflect.Type, encF, decF, schema any, values any) {
	t.Helper()
	testValues := reflect.ValueOf(values)
	// Check whether we have a slice type or not.
	if testValues.Type().Kind() != reflect.Slice {
		vs := reflect.MakeSlice(reflect.SliceOf(testValues.Type()), 0, 1)
		testValues = reflect.Append(vs, testValues)
	}
	if testValues.Len() == 0 {
		t.Errorf("No test values provided for ValidateSchemaCoder(%v)", rt)
	}
	// We now have non empty slice of test values!

	v.encBldUT.Register(rt, encF)
	v.decBldUT.Register(rt, decF)

	testRt := testValues.Type().Elem()
	encUT, err := v.encBldUT.Build(testRt)
	if err != nil {
		t.Errorf("Unable to build encoder function with given factory: coder.RowEncoderBuilder.Build(%v) = %v, want nil error", rt, err)
	}
	decUT, err := v.decBldUT.Build(testRt)
	if err != nil {
		t.Errorf("Unable to build decoder function with given factory: coder.RowDecoderBuilder.Build(%v) = %v, want nil error", rt, err)
	}

	schemaRt := reflect.TypeOf(schema)
	encSchema, err := v.encBldSchema.Build(schemaRt)
	if err != nil {
		t.Errorf("Unable to build encoder function for schema equivalent type: coder.RowEncoderBuilder.Build(%v) = %v, want nil error", rt, err)
	}
	decSchema, err := v.decBldSchema.Build(schemaRt)
	if err != nil {
		t.Errorf("Unable to build decoder function for schema equivalent type: coder.RowDecoderBuilder.Build(%v) = %v, want nil error", rt, err)
	}
	// We use error messages instead of fatals to allow all the cases to be
	// checked. None of the coder functions are used until the per value runs
	// so a user can get additional information per run.
	if t.Failed() {
		t.FailNow()
	}
	for i := 0; i < testValues.Len(); i++ {
		t.Run(fmt.Sprintf("%v[%d]", rt, i), func(t *testing.T) {
			var buf bytes.Buffer
			want := testValues.Index(i).Interface()
			if err := encUT(want, &buf); err != nil {
				t.Fatalf("error calling Under Test encoder[%v](%v) = %v", testRt, want, err)
			}
			initialBytes := clone(buf.Bytes())

			bufSchema := bytes.NewBuffer(clone(initialBytes))

			schemaV, err := decSchema(bufSchema)
			if err != nil {
				t.Fatalf("error calling Equivalent Schema decoder[%v]() = %v", schemaRt, err)
			}
			err = encSchema(schemaV, bufSchema)
			if err != nil {
				t.Fatalf("error calling Equivalent Schema encoder[%v](%v) = %v, want nil error", schemaRt, schemaV, err)
			}
			roundTripBytes := clone(bufSchema.Bytes())

			if d := cmp.Diff(initialBytes, roundTripBytes); d != "" {
				t.Errorf("round trip through equivalent schema type didn't produce equivalent byte slices (-initial,+roundTrip): \n%v", d)
			}
			got, err := decUT(bufSchema)
			if err != nil {
				t.Fatalf("Under Test decoder(%v) = %v, want nil error", rt, err)
			}
			if d := cmp.Diff(want, got, v.CmpOptions); d != "" {
				t.Fatalf("round trip through custom coder produced diff: (-want, +got):\n%v", d)
			}
		})
	}
}

func clone(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
