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

package testutil

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
)

type UserInterface interface {
	mark()
}

type UserType1 struct {
	A string
	B int
	C string
}

func (UserType1) mark() {}

func ut1EncDropB(val any, w io.Writer) error {
	if err := coder.WriteSimpleRowHeader(2, w); err != nil {
		return err
	}
	elm := val.(UserType1)
	if err := coder.EncodeStringUTF8(elm.A, w); err != nil {
		return err
	}
	if err := coder.EncodeStringUTF8(elm.C, w); err != nil {
		return err
	}
	return nil
}

func ut1DecDropB(r io.Reader) (any, error) {
	if err := coder.ReadSimpleRowHeader(2, r); err != nil {
		return nil, err
	}
	a, err := coder.DecodeStringUTF8(r)
	if err != nil {
		return nil, fmt.Errorf("decoding string field A: %v", err)
	}
	c, err := coder.DecodeStringUTF8(r)
	if err != nil {
		return nil, fmt.Errorf("decoding string field C: %v, %v", c, err)
	}
	return UserType1{
		A: a,
		B: 42,
		C: c,
	}, nil
}

type UserType2 struct {
	A UserType1
}

func TestValidateCoder(t *testing.T) {
	// Validates a custom UserType1 encoding, which drops encoding the "B" field,
	// always setting it to a constant value.
	t.Run("SingleValue", func(t *testing.T) {
		(&SchemaCoder{}).Validate(t, reflect.TypeOf((*UserType1)(nil)).Elem(),
			func(reflect.Type) (func(any, io.Writer) error, error) { return ut1EncDropB, nil },
			func(reflect.Type) (func(io.Reader) (any, error), error) { return ut1DecDropB, nil },
			struct{ A, C string }{},
			UserType1{
				A: "cats",
				B: 42,
				C: "pjamas",
			},
		)
	})
	t.Run("SliceOfValues", func(t *testing.T) {
		(&SchemaCoder{}).Validate(t, reflect.TypeOf((*UserType1)(nil)).Elem(),
			func(reflect.Type) (func(any, io.Writer) error, error) { return ut1EncDropB, nil },
			func(reflect.Type) (func(io.Reader) (any, error), error) { return ut1DecDropB, nil },
			struct{ A, C string }{},
			[]UserType1{
				{
					A: "cats",
					B: 42,
					C: "pjamas",
				}, {
					A: "dogs",
					B: 42,
					C: "breakfast",
				}, {
					A: "fish",
					B: 42,
					C: "plenty of",
				},
			},
		)
	})
	t.Run("InterfaceCoder", func(t *testing.T) {
		(&SchemaCoder{}).Validate(t, reflect.TypeOf((*UserInterface)(nil)).Elem(),
			func(rt reflect.Type) (func(any, io.Writer) error, error) {
				return ut1EncDropB, nil
			},
			func(rt reflect.Type) (func(io.Reader) (any, error), error) {
				return ut1DecDropB, nil
			},
			struct{ A, C string }{},
			UserType1{
				A: "cats",
				B: 42,
				C: "pjamas",
			},
		)
	})
	t.Run("FailureCases", func(t *testing.T) {
		var c checker
		err := fmt.Errorf("FactoryError")
		var v SchemaCoder
		// Register the pointer type to the default encoder too.
		v.Register(reflect.TypeOf((*UserType2)(nil)),
			func(reflect.Type) (func(any, io.Writer) error, error) { return nil, err },
			func(reflect.Type) (func(io.Reader) (any, error), error) { return nil, err },
		)
		v.Validate(&c, reflect.TypeOf((*UserType1)(nil)).Elem(),
			func(reflect.Type) (func(any, io.Writer) error, error) { return ut1EncDropB, err },
			func(reflect.Type) (func(io.Reader) (any, error), error) { return ut1DecDropB, err },
			struct {
				A, C string
				B    *UserType2 // To trigger the bad factory registered earlier.
			}{},
			[]UserType1{},
		)
		if got, want := len(c.errors), 5; got != want {
			t.Fatalf("SchemaCoder.Validate did not fail as expected. Got %v errors logged, but want %v", got, want)
		}
		if !strings.Contains(c.errors[0].fmt, "No test values") {
			t.Fatalf("SchemaCoder.Validate with no values did not fail. fmt: %q", c.errors[0].fmt)
		}
		if !strings.Contains(c.errors[1].fmt, "Unable to build encoder function with given factory") {
			t.Fatalf("SchemaCoder.Validate with no values did not fail. fmt: %q", c.errors[1].fmt)
		}
		if !strings.Contains(c.errors[2].fmt, "Unable to build decoder function with given factory") {
			t.Fatalf("SchemaCoder.Validate with no values did not fail. fmt: %q", c.errors[2].fmt)
		}
		if !strings.Contains(c.errors[3].fmt, "Unable to build encoder function for schema equivalent type") {
			t.Fatalf("SchemaCoder.Validate with no values did not fail. fmt: %q", c.errors[3].fmt)
		}
		if !strings.Contains(c.errors[4].fmt, "Unable to build decoder function for schema equivalent type") {
			t.Fatalf("SchemaCoder.Validate with no values did not fail. fmt: %q", c.errors[4].fmt)
		}
	})
}

type msg struct {
	fmt    string
	params []any
}

type checker struct {
	errors []msg

	runCount      int
	failNowCalled bool
}

func (c *checker) Helper() {}

func (c *checker) Run(string, func(*testing.T)) bool {
	c.runCount++
	return true
}

func (c *checker) Errorf(fmt string, params ...any) {
	c.errors = append(c.errors, msg{
		fmt:    fmt,
		params: params,
	})
}

func (c *checker) Failed() bool {
	return len(c.errors) > 0
}

func (c *checker) FailNow() {
	c.failNowCalled = true
}
