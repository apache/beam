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

package sql

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/sql/sqlx"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestOptions_Add(t *testing.T) {
	test := struct {
		opt sqlx.Option
	}{
		opt: sqlx.Option{
			Urn:     "this is a string",
			Payload: []byte{1, 2, 3, 4},
		},
	}

	o := options{}
	o.Add(test.opt)
	if o.customs == nil || !reflect.DeepEqual(o.customs[len(o.customs)-1], test.opt) {
		t.Errorf("options.Add(%v) failed. For the customs field in options, got %v, want %v", test.opt, o.customs, test.opt)
	}
}

func TestInput(t *testing.T) {
	test := struct {
		inputName string
		inputIn   beam.PCollection
	}{
		inputName: "this is a string",
		inputIn:   beam.PCollection{},
	}

	o := &options{inputs: make(map[string]beam.PCollection)}
	option := Input(test.inputName, test.inputIn)
	if option == nil {
		t.Errorf("Input(%v, %v) = %v, want not nil", test.inputName, test.inputIn, option)
	}
	option(o)
	if o.inputs == nil || !reflect.DeepEqual(o.inputs[test.inputName], test.inputIn) {
		t.Errorf("The function that Input(%v, %v) returned did not work correctly. For the inputs field in options, got %v, want %v", test.inputName, test.inputIn, o.inputs, test.inputIn)
	}
}

func TestDialect(t *testing.T) {
	test := struct {
		dialect string
	}{
		dialect: "this is a string",
	}

	o := &options{}
	option := Dialect(test.dialect)
	if option == nil {
		t.Errorf("Dialect(%v) = %v, want not nil", test.dialect, option)
	}
	option(o)
	if !reflect.DeepEqual(o.dialect, test.dialect) {
		t.Errorf("The function that Input(%v) returned did not work correctly. For the dialect field in options, got %v, want %v", test.dialect, o.dialect, test.dialect)
	}
}

func TestExpansionAddr(t *testing.T) {
	test := struct {
		addr string
	}{
		addr: "this is a string",
	}

	o := &options{}
	option := ExpansionAddr(test.addr)
	if option == nil {
		t.Errorf("ExpansionAddr(%v) = %v, want not nil", test.addr, option)
	}
	option(o)
	if !reflect.DeepEqual(o.expansionAddr, test.addr) {
		t.Errorf("The function that ExpansionAddr(%v) returned did not work correctly. For the expansionAddr field in options, got %v, want %v", test.addr, o.expansionAddr, test.addr)
	}
}

// TestOutputType tests the OutputType option for setting the output type
// in an SQL transformation in Beam. It verifies both cases: when
// components are provided and when they are not. The test checks if
// the 'outType' field in the options is set correctly based on the
// output type and components.
func TestOutputType(t *testing.T) {
	testCases := []struct {
		name       string
		typ        reflect.Type
		components []typex.FullType
		wantNil    bool
	}{
		{
			name: "output_type_without_components",
			typ:  reflect.TypeOf(int64(0)),
		},
		{
			name:       "output_type_with_components",
			typ:        reflect.TypeOf(int64(0)),
			components: []typex.FullType{typex.New(reflect.TypeOf(""))},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := &options{}

			var opt Option
			if len(tc.components) > 0 {
				opt = OutputType(tc.typ, tc.components...)
			} else {
				opt = OutputType(tc.typ)
			}

			opt(o)

			var expected typex.FullType
			if len(tc.components) > 0 {
				expected = typex.New(tc.typ, tc.components...)
			} else {
				expected = typex.New(tc.typ)
			}

			opts := cmp.Options{
				cmp.Comparer(func(x, y typex.FullType) bool {
					// Compare only the type and components
					return x.Type() == y.Type()
				}),
			}
			if d := cmp.Diff(expected, o.outType, opts); d != "" {
				t.Errorf("OutputType() failed: (-want, +got)\n%s", d)
			}
		})
	}
}

// TestTransform tests the behavior of the Transform function
// in the context of SQL transformations. It checks that the function
// panics when an output type is missing.
func TestTransform_MissingOutputType(t *testing.T) {
	p := beam.NewPipeline()
	s := p.Root()
	col := beam.Create(s, 1, 2, 3)

	defer func() {
		r := recover()
		if r == nil {
			t.Error("Transform() with missing output type should panic")
		}
		if msg, ok := r.(string); !ok || msg != "output type must be specified for sql.Transform" {
			t.Errorf("Transform() unexpected panic message: %v", r)
		}
	}()

	Transform(s, "SELECT value FROM test", Input("test", col))
}

// TestMultipleOptions tests applying multiple options at once
// and verifying that they are all correctly applied to the options object.
func TestMultipleOptions(t *testing.T) {
	testCases := []struct {
		name          string
		inputName     string
		dialect       string
		expansionAddr string
		typ           reflect.Type
		customOpt     sqlx.Option
	}{
		{
			name:          "all_options",
			inputName:     "test",
			expansionAddr: "localhost:8080",
			typ:           reflect.TypeOf(int64(0)),
			customOpt:     sqlx.Option{Urn: "test"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := beam.NewPipeline()
			s := p.Root()
			col := beam.Create(s, 1, 2, 3)

			o := &options{
				inputs: make(map[string]beam.PCollection),
			}

			opts := []Option{
				Input(tc.inputName, col),
				Dialect(tc.dialect),
				ExpansionAddr(tc.expansionAddr),
				OutputType(tc.typ),
			}

			for _, opt := range opts {
				opt(o)
			}
			o.Add(tc.customOpt)

			// Construct the expected options struct
			expected := &options{
				inputs: map[string]beam.PCollection{
					tc.inputName: col,
				},
				dialect:       tc.dialect,
				expansionAddr: tc.expansionAddr,
				outType:       typex.New(tc.typ),
				customs:       []sqlx.Option{tc.customOpt},
			}

			// Define a custom comparer for typex.FullType
			fullTypeComparer := cmp.Comparer(func(x, y typex.FullType) bool {
				return x.Type() == y.Type() // Compare only the underlying reflect.Type
			})

			if d := cmp.Diff(
				expected,
				o,
				cmp.AllowUnexported(options{}),
				cmpopts.IgnoreUnexported(beam.PCollection{}),
				fullTypeComparer, // Use the custom comparer for typex.FullType
			); d != "" {
				t.Errorf("Options mismatch: (-want, +got)\n%s", d)
			}
		})
	}
}
