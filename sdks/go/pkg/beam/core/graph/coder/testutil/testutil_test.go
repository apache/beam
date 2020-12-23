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
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
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

func ut1EncDropB(val interface{}, w io.Writer) error {
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

func ut1DecDropB(r io.Reader) (interface{}, error) {
	if err := coder.ReadSimpleRowHeader(2, r); err != nil {
		return nil, err
	}
	a, err := coder.DecodeStringUTF8(r)
	if err != nil {
		return nil, fmt.Errorf("decoding string field A: %w", err)
	}
	c, err := coder.DecodeStringUTF8(r)
	if err != nil {
		return nil, fmt.Errorf("decoding string field C: %v, %w", c, err)
	}
	return UserType1{
		A: a,
		B: 42,
		C: c,
	}, nil
}

// TestValidateCoder_SingleValue checks that the validate coder fun will
func TestValidateCoder(t *testing.T) {
	// Validates a custom UserType1 encoding, which drops encoding the "B" field,
	// always setting it to a constant value.
	t.Run("SingleValue", func(t *testing.T) {
		(&SchemaCoder{}).Validate(t, reflect.TypeOf((*UserType1)(nil)).Elem(),
			func(reflect.Type) (func(interface{}, io.Writer) error, error) { return ut1EncDropB, nil },
			func(reflect.Type) (func(io.Reader) (interface{}, error), error) { return ut1DecDropB, nil },
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
			func(reflect.Type) (func(interface{}, io.Writer) error, error) { return ut1EncDropB, nil },
			func(reflect.Type) (func(io.Reader) (interface{}, error), error) { return ut1DecDropB, nil },
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
			func(rt reflect.Type) (func(interface{}, io.Writer) error, error) {
				return ut1EncDropB, nil
			},
			func(rt reflect.Type) (func(io.Reader) (interface{}, error), error) {
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
}
