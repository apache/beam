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

/*
TestOutputType tests the OutputType option for setting the output type
in an SQL transformation in Beam. It verifies both cases: when
components are provided and when they are not. The test checks if
the 'outType' field in the options is set correctly based on the
output type and components.

@Author: Mohit Paddhariya
@Description: This test checks the functionality of OutputType
option, ensuring that it properly handles setting the output type
both with and without components.

@Use: This test ensures that the OutputType option behaves as expected
when used in SQL transformations.
*/
func TestOutputType(t *testing.T) {
	o := &options{}
	typ := reflect.TypeOf(int64(0))
	components := []typex.FullType{typex.New(reflect.TypeOf(""))}

	// Test without components
	opt1 := OutputType(typ)
	opt1(o)
	expected1 := typex.New(typ)
	if !reflect.DeepEqual(o.outType, expected1) {
		t.Errorf("OutputType() without components failed: got %v, want %v", o.outType, expected1)
	}

	// Test with components
	opt2 := OutputType(typ, components...)
	opt2(o)
	expected2 := typex.New(typ, components...)
	if !reflect.DeepEqual(o.outType, expected2) {
		t.Errorf("OutputType() with components failed: got %v, want %v", o.outType, expected2)
	}
}

/*
TestTransform tests the behavior of the Transform function
in the context of SQL transformations. Specifically, it checks
that the function panics when an output type is missing.

@Author: Mohit Paddhariya
@Description: This test verifies that Transform() panics when the
output type is not specified for a SQL transformation.

@Use: Ensures that missing output type results in a panic,
helping to catch incorrect or incomplete transformations.
*/
func TestTransform(t *testing.T) {
	// Test just the panic case for missing output type
	// as we can't easily test the actual transformation without an expansion service
	t.Run("Missing output type", func(t *testing.T) {
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
	})
}

/*
TestMultipleOptions tests applying multiple options at once
and verifying that they are all correctly applied to the options object.

@Author: Mohit Paddhariya
@Description: This test ensures that multiple options, including
Input, Dialect, ExpansionAddr, and OutputType, are correctly
applied to the options object. It also verifies that a custom
option can be added and applied.

@Use: Ensures that multiple options can be applied in sequence and
all the fields in the options object are correctly set.
*/
func TestMultipleOptions(t *testing.T) {
	o := &options{
		inputs: make(map[string]beam.PCollection),
	}

	p := beam.NewPipeline()
	s := p.Root()
	col := beam.Create(s, 1, 2, 3)
	name := "test"
	dialect := "zetasql"
	addr := "localhost:8080"
	typ := reflect.TypeOf(int64(0))
	customOpt := sqlx.Option{Urn: "test"}

	// Apply multiple options
	opts := []Option{
		Input(name, col),
		Dialect(dialect),
		ExpansionAddr(addr),
		OutputType(typ),
	}

	// Apply all options
	for _, opt := range opts {
		opt(o)
	}
	o.Add(customOpt)

	// Verify all fields
	if _, ok := o.inputs[name]; !ok {
		t.Error("Input option not applied correctly")
	}
	if o.dialect != dialect {
		t.Error("Dialect option not applied correctly")
	}
	if o.expansionAddr != addr {
		t.Error("ExpansionAddr option not applied correctly")
	}
	if !reflect.DeepEqual(o.outType, typex.New(typ)) {
		t.Error("OutputType option not applied correctly")
	}
	if len(o.customs) != 1 {
		t.Error("Custom option not applied correctly")
	}
}
