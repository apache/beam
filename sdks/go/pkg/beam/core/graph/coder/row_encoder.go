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

package coder

import (
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

type encoderProvider = func(reflect.Type) (func(interface{}, io.Writer) error, error)

// RowEncoderBuilder allows one to build Beam Schema row encoders for provided types.
type RowEncoderBuilder struct {
	allFuncs   map[reflect.Type]encoderProvider
	ifaceFuncs []reflect.Type
}

// Register accepts a provider for the given type to schema encode values of that type.
//
// When generating encoding functions, this builder will first check for exact type
// matches, then against interfaces with registered factories in recency order of
// registration, and then finally use the default Beam Schema encoding behavior.
//
// TODO(BEAM-9615): Add final factory types. This interface is subject to change.
// Currently f must be a function of the type func(reflect.Type) func(T, io.Writer) (error).
func (b *RowEncoderBuilder) Register(rt reflect.Type, f interface{}) {
	fe, ok := f.(encoderProvider)
	if !ok {
		panic(fmt.Sprintf("%v isn't a supported decoder function type (passed with %T)", f, rt))
	}

	if rt.Kind() == reflect.Interface && rt.NumMethod() == 0 {
		panic(fmt.Sprintf("interface type %v must have methods", rt))
	}
	if b.allFuncs == nil {
		b.allFuncs = make(map[reflect.Type]encoderProvider)
	}
	b.allFuncs[rt] = fe
	if rt.Kind() == reflect.Interface {
		b.ifaceFuncs = append(b.ifaceFuncs, rt)
	}
}

// Build constructs a Beam Schema coder for the given type, using any providers registered for
// itself or it's fields.
func (b *RowEncoderBuilder) Build(rt reflect.Type) (func(interface{}, io.Writer) error, error) {
	if err := rowTypeValidation(rt, true); err != nil {
		return nil, err
	}
	return b.encoderForType(rt), nil
}

// customFunc returns nil if no custom func exists for this.
func (b *RowEncoderBuilder) customFunc(t reflect.Type) func(interface{}, io.Writer) error {
	if fact, ok := b.allFuncs[t]; ok {
		f, err := fact(t)

		// TODO handle errors?
		if err != nil {
			return nil
		}
		return f
	}
	// Check satisfaction of interface types in reverse registration order.
	for i := len(b.ifaceFuncs) - 1; i >= 0; i-- {
		it := b.ifaceFuncs[i]
		if ok := t.AssignableTo(it); ok {
			if fact, ok := b.allFuncs[it]; ok {
				f, err := fact(t)
				// TODO handle errors?
				if err != nil {
					return nil
				}
				return f
			}
		}
	}
	return nil
}

// encoderForType returns an encoder function for the struct or pointer to struct type.
func (b *RowEncoderBuilder) encoderForType(t reflect.Type) func(interface{}, io.Writer) error {
	// Check if there are any providers registered for this type, or that this type adheres to any interfaces.
	var isPtr bool
	// Pointers become the value type for decomposition.
	if t.Kind() == reflect.Ptr {
		// If we have something for the pointer version already, we're done.
		if enc := b.customFunc(t); enc != nil {
			return enc
		}
		isPtr = true
		t = t.Elem()
	}

	if enc := b.customFunc(t); enc != nil {
		if isPtr {
			// We have the value version, but not a pointer version, so we jump through reflect to
			// get the right type to pass in.
			return func(v interface{}, w io.Writer) error {
				return enc(reflect.ValueOf(v).Elem().Interface(), w)
			}
		}
		return enc
	}

	enc := b.encoderForStructReflect(t)

	if isPtr {
		return func(v interface{}, w io.Writer) error {
			return enc(reflect.ValueOf(v).Elem(), w)
		}
	}
	return func(v interface{}, w io.Writer) error {
		return enc(reflect.ValueOf(v), w)
	}
}

// Generates coder using reflection for
func (b *RowEncoderBuilder) encoderForSingleTypeReflect(t reflect.Type) func(reflect.Value, io.Writer) error {
	// Check if there are any providers registered for this type, or that this type adheres to any interfaces.
	if enc := b.customFunc(t); enc != nil {
		return func(v reflect.Value, w io.Writer) error {
			return enc(v.Interface(), w)
		}
	}

	switch t.Kind() {
	case reflect.Struct:
		return b.encoderForStructReflect(t)
	case reflect.Bool:
		return func(rv reflect.Value, w io.Writer) error {
			return EncodeBool(rv.Bool(), w)
		}
	case reflect.Uint8:
		return func(rv reflect.Value, w io.Writer) error {
			return EncodeByte(byte(rv.Uint()), w)
		}
	case reflect.String:
		return func(rv reflect.Value, w io.Writer) error {
			return EncodeStringUTF8(rv.String(), w)
		}
	case reflect.Int, reflect.Int64, reflect.Int16, reflect.Int32, reflect.Int8:
		return func(rv reflect.Value, w io.Writer) error {
			return EncodeVarInt(int64(rv.Int()), w)
		}
	case reflect.Float32, reflect.Float64:
		return func(rv reflect.Value, w io.Writer) error {
			return EncodeDouble(float64(rv.Float()), w)
		}
	case reflect.Ptr:
		// Nils are handled at the struct field level.
		encf := b.encoderForSingleTypeReflect(t.Elem())
		return func(rv reflect.Value, w io.Writer) error {
			return encf(rv.Elem(), w)
		}
	case reflect.Slice:
		// Special case handling for byte slices.
		if t.Elem().Kind() == reflect.Uint8 {
			return func(rv reflect.Value, w io.Writer) error {
				return EncodeBytes(rv.Bytes(), w)
			}
		}
		encf := b.containerEncoderForType(t.Elem())
		return iterableEncoder(t, encf)
	case reflect.Array:
		encf := b.containerEncoderForType(t.Elem())
		return iterableEncoder(t, encf)
	case reflect.Map:
		encK := b.containerEncoderForType(t.Key())
		encV := b.containerEncoderForType(t.Elem())
		return mapEncoder(t, encK, encV)
	}
	panic(fmt.Sprintf("unimplemented type to encode: %v", t))
}

func (b *RowEncoderBuilder) containerEncoderForType(t reflect.Type) func(reflect.Value, io.Writer) error {
	if t.Kind() == reflect.Ptr {
		return containerNilEncoder(b.encoderForSingleTypeReflect(t.Elem()))
	}
	return b.encoderForSingleTypeReflect(t)
}

type typeEncoderReflect struct {
	debug  []string
	fields []func(reflect.Value, io.Writer) error
}

// encoderForStructReflect generates reflection field access closures for structs.
func (b *RowEncoderBuilder) encoderForStructReflect(t reflect.Type) func(reflect.Value, io.Writer) error {
	var coder typeEncoderReflect
	for i := 0; i < t.NumField(); i++ {
		coder.debug = append(coder.debug, t.Field(i).Type.Name())
		coder.fields = append(coder.fields, b.encoderForSingleTypeReflect(t.Field(i).Type))
	}

	return func(rv reflect.Value, w io.Writer) error {
		// Row/Structs are prefixed with the number of fields that are encoded in total.
		if err := writeRowHeader(rv, w); err != nil {
			return err
		}
		for i, f := range coder.fields {
			rvf := rv.Field(i)
			switch rvf.Kind() {
			case reflect.Ptr, reflect.Map, reflect.Slice:
				if rvf.IsNil() {
					continue
				}
			}
			if err := f(rvf, w); err != nil {
				return errors.Wrapf(err, "encoding %v, expected: %v", rvf.Type(), coder.debug[i])
			}
		}
		return nil
	}
}
