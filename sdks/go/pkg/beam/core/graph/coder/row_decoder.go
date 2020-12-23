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

// RowDecoderBuilder allows one to build Beam Schema row encoders for provided types.
type RowDecoderBuilder struct {
	allFuncs   map[reflect.Type]decoderProvider
	ifaceFuncs []reflect.Type
}

type decoderProvider = func(reflect.Type) (func(io.Reader) (interface{}, error), error)

// Register accepts a provider to decode schema encoded values
// of that type.
//
// When decoding values, decoder functions produced by this builder will
// first check for exact type matches, then interfaces implemented by
// the type in recency order of registration, and then finally the
// default Beam Schema encoding behavior.
//
// TODO(BEAM-9615): Add final factory types. This interface is subject to change.
// Currently f must be a function  func(reflect.Type) (func(io.Reader) (interface{}, error), error)
func (b *RowDecoderBuilder) Register(rt reflect.Type, f interface{}) {
	fd, ok := f.(decoderProvider)
	if !ok {
		panic(fmt.Sprintf("%v isn't a supported decoder function type (passed with %T)", f, rt))
	}

	if rt.Kind() == reflect.Interface && rt.NumMethod() == 0 {
		panic(fmt.Sprintf("interface type %v must have methods", rt))
	}

	if b.allFuncs == nil {
		b.allFuncs = make(map[reflect.Type]decoderProvider)
	}
	b.allFuncs[rt] = fd
	if rt.Kind() == reflect.Interface {
		b.ifaceFuncs = append(b.ifaceFuncs, rt)
	}
}

// Build constructs a Beam Schema coder for the given type, using any providers registered for
// itself or it's fields.
func (b *RowDecoderBuilder) Build(rt reflect.Type) (func(io.Reader) (interface{}, error), error) {
	if err := rowTypeValidation(rt, true); err != nil {
		return nil, err
	}
	return b.decoderForType(rt), nil
}

// decoderForType returns a decoder function for the struct or pointer to struct type.
func (b *RowDecoderBuilder) decoderForType(t reflect.Type) func(io.Reader) (interface{}, error) {
	// Check if there are any providers registered for this type, or that this type adheres to any interfaces.
	if f := b.customFunc(t); f != nil {
		return f
	}

	var isPtr bool
	// Pointers become the value type for decomposition.
	if t.Kind() == reflect.Ptr {
		isPtr = true
		t = t.Elem()
	}
	dec := b.decoderForStructReflect(t)

	if isPtr {
		return func(r io.Reader) (interface{}, error) {
			rv := reflect.New(t)
			err := dec(rv.Elem(), r)
			return rv.Interface(), err
		}
	}
	return func(r io.Reader) (interface{}, error) {
		rv := reflect.New(t)
		err := dec(rv.Elem(), r)
		return rv.Elem().Interface(), err
	}
}

// decoderForStructReflect returns a reflection based decoder function for the
// given struct type.
func (b *RowDecoderBuilder) decoderForStructReflect(t reflect.Type) func(reflect.Value, io.Reader) error {
	var coder typeDecoderReflect
	for i := 0; i < t.NumField(); i++ {
		i := i // avoid alias issues in the closures.
		dec := b.decoderForSingleTypeReflect(t.Field(i).Type)
		coder.fields = append(coder.fields, func(rv reflect.Value, r io.Reader) error {
			return dec(rv.Field(i), r)
		})
	}
	return func(rv reflect.Value, r io.Reader) error {
		nf, nils, err := ReadRowHeader(r)
		if err != nil {
			return err
		}
		if nf != len(coder.fields) {
			return errors.Errorf("schema[%v] changed: got %d fields, want %d fields", "TODO", nf, len(coder.fields))
		}
		for i, f := range coder.fields {
			if IsFieldNil(nils, i) {
				continue
			}
			if err := f(rv, r); err != nil {
				return err
			}
		}
		return nil
	}
}

func reflectDecodeBool(rv reflect.Value, r io.Reader) error {
	v, err := DecodeBool(r)
	if err != nil {
		return errors.Wrap(err, "error decoding bool field")
	}
	rv.SetBool(v)
	return nil
}

func reflectDecodeByte(rv reflect.Value, r io.Reader) error {
	b, err := DecodeByte(r)
	if err != nil {
		return errors.Wrap(err, "error decoding single byte field")
	}
	rv.SetUint(uint64(b))
	return nil
}

func reflectDecodeString(rv reflect.Value, r io.Reader) error {
	v, err := DecodeStringUTF8(r)
	if err != nil {
		return errors.Wrap(err, "error decoding string field")
	}
	rv.SetString(v)
	return nil
}

func reflectDecodeInt(rv reflect.Value, r io.Reader) error {
	v, err := DecodeVarInt(r)
	if err != nil {
		return errors.Wrap(err, "error decoding varint field")
	}
	rv.SetInt(v)
	return nil
}

func reflectDecodeFloat(rv reflect.Value, r io.Reader) error {
	v, err := DecodeDouble(r)
	if err != nil {
		return errors.Wrap(err, "error decoding double field")
	}
	rv.SetFloat(v)
	return nil
}

// customFunc returns nil if no custom func exists for this.
func (b *RowDecoderBuilder) customFunc(t reflect.Type) func(io.Reader) (interface{}, error) {
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

// decoderForSingleTypeReflect returns a reflection based decoder function for the
// given type.
func (b *RowDecoderBuilder) decoderForSingleTypeReflect(t reflect.Type) func(reflect.Value, io.Reader) error {
	// Check if there are any providers registered for this type, or that this type adheres to any interfaces.
	if dec := b.customFunc(t); dec != nil {
		return func(v reflect.Value, r io.Reader) error {
			elm, err := dec(r)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(elm))
			return nil
		}
	}

	switch t.Kind() {
	case reflect.Struct:
		return b.decoderForStructReflect(t)
	case reflect.Bool:
		return reflectDecodeBool
	case reflect.Uint8:
		return reflectDecodeByte
	case reflect.String:
		return reflectDecodeString
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflectDecodeInt
	case reflect.Float32, reflect.Float64:
		return reflectDecodeFloat
	case reflect.Ptr:
		decf := b.decoderForSingleTypeReflect(t.Elem())
		return func(rv reflect.Value, r io.Reader) error {
			nv := reflect.New(t.Elem())
			rv.Set(nv)
			return decf(nv.Elem(), r)
		}
	case reflect.Slice:
		// Special case handling for byte slices.
		if t.Elem().Kind() == reflect.Uint8 {
			return func(rv reflect.Value, r io.Reader) error {
				b, err := DecodeBytes(r)
				if err != nil {
					return errors.Wrap(err, "error decoding []byte field")
				}
				rv.SetBytes(b)
				return nil
			}
		}
		decf := b.containerDecoderForType(t.Elem())
		return iterableDecoderForSlice(t, decf)
	case reflect.Array:
		decf := b.containerDecoderForType(t.Elem())
		return iterableDecoderForArray(t, decf)
	case reflect.Map:
		decK := b.containerDecoderForType(t.Key())
		decV := b.containerDecoderForType(t.Elem())
		return mapDecoder(t, decK, decV)
	}
	panic(fmt.Sprintf("unimplemented type to decode: %v", t))
}

func (b *RowDecoderBuilder) containerDecoderForType(t reflect.Type) func(reflect.Value, io.Reader) error {
	if t.Kind() == reflect.Ptr {
		return containerNilDecoder(b.decoderForSingleTypeReflect(t.Elem()))
	}
	return b.decoderForSingleTypeReflect(t)
}

type typeDecoderReflect struct {
	typ    reflect.Type
	fields []func(reflect.Value, io.Reader) error
}
