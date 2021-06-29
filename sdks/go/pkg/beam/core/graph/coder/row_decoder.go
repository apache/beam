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

	// RequireAllFieldsExported when set to true will have the default decoder building fail if
	// there are any unexported fields. When set false, unexported fields in default
	// destination structs will be silently ignored when decoding.
	// This has no effect on types with registered decoder providers.
	RequireAllFieldsExported bool
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
		panic(fmt.Sprintf("%T isn't a supported decoder function type (passed with %v), currently expecting %T", f, rt, (decoderProvider)(nil)))
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
	return b.decoderForType(rt)
}

// decoderForType returns a decoder function for the struct or pointer to struct type.
func (b *RowDecoderBuilder) decoderForType(t reflect.Type) (func(io.Reader) (interface{}, error), error) {
	// Check if there are any providers registered for this type, or that this type adheres to any interfaces.
	f, err := b.customFunc(t)
	if err != nil {
		return nil, err
	}
	if f != nil {
		return f, nil
	}

	var isPtr bool
	// Pointers become the value type for decomposition.
	if t.Kind() == reflect.Ptr {
		isPtr = true
		t = t.Elem()
	}
	dec, err := b.decoderForStructReflect(t)
	if err != nil {
		return nil, err
	}

	if isPtr {
		return func(r io.Reader) (interface{}, error) {
			rv := reflect.New(t)
			err := dec(rv.Elem(), r)
			return rv.Interface(), errors.Wrapf(err, "decoding a *%v", t)
		}, nil
	}
	return func(r io.Reader) (interface{}, error) {
		rv := reflect.New(t)
		err := dec(rv.Elem(), r)
		return rv.Elem().Interface(), errors.Wrapf(err, "decoding a *%v", t)
	}, nil
}

// decoderForStructReflect returns a reflection based decoder function for the
// given struct type.
func (b *RowDecoderBuilder) decoderForStructReflect(t reflect.Type) (func(reflect.Value, io.Reader) error, error) {
	var coder typeDecoderReflect
	coder.typ = t
	for i := 0; i < t.NumField(); i++ {
		i := i // avoid alias issues in the closures.
		sf := t.Field(i)
		isUnexported := sf.PkgPath != ""
		if sf.Anonymous {
			ft := sf.Type
			if ft.Kind() == reflect.Ptr {
				// If a struct embeds a pointer to an unexported type,
				// it is not possible to set a newly allocated value
				// since the field is unexported.
				//
				// See https://golang.org/issue/21357
				//
				// Since the values are created by this package reflectively,
				// there's no work around like pre-allocating the field
				// manually.
				if isUnexported {
					return nil, errors.Errorf("cannot make schema decoder for type %v as it has an embedded field of a pointer to an unexported type %v. See https://golang.org/issue/21357", t, ft.Elem())
				}
				ft = ft.Elem()
			}
			if isUnexported && ft.Kind() != reflect.Struct {
				// Ignore embedded fields of unexported non-struct types.
				continue
			}
			// Do not ignore embedded fields of unexported struct types
			// since they may have exported fields.
		} else if isUnexported {
			if b.RequireAllFieldsExported {
				return nil, errors.Errorf("cannot make schema decoder for type %v as it has unexported fields such as %s.", t, sf.Name)
			}
			// Silently ignore, since we can't do anything about it.
			// Add a no-op coder to fill in field index
			coder.fields = append(coder.fields, func(rv reflect.Value, r io.Reader) error {
				return nil
			})
			continue
		}
		dec, err := b.decoderForSingleTypeReflect(sf.Type)
		if err != nil {
			return nil, err
		}
		coder.fields = append(coder.fields, dec)
	}
	return func(rv reflect.Value, r io.Reader) error {
		nf, nils, err := ReadRowHeader(r)
		if err != nil {
			return err
		}
		if nf != len(coder.fields) {
			return errors.Errorf("schema[%v] changed: got %d fields, want %d fields", coder.typ, nf, len(coder.fields))
		}
		for i, f := range coder.fields {
			if IsFieldNil(nils, i) {
				continue
			}
			if err := f(rv.Field(i), r); err != nil {
				return err
			}
		}
		return nil
	}, nil
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

func reflectDecodeUint(rv reflect.Value, r io.Reader) error {
	v, err := DecodeVarUint64(r)
	if err != nil {
		return errors.Wrap(err, "error decoding varint field")
	}
	rv.SetUint(v)
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

func reflectDecodeByteSlice(rv reflect.Value, r io.Reader) error {
	b, err := DecodeBytes(r)
	if err != nil {
		return errors.Wrap(err, "error decoding []byte field")
	}
	rv.SetBytes(b)
	return nil
}

// customFunc returns nil if no custom func exists for this type.
// If an error is returned, coder construction should be aborted.
func (b *RowDecoderBuilder) customFunc(t reflect.Type) (func(io.Reader) (interface{}, error), error) {
	if fact, ok := b.allFuncs[t]; ok {
		f, err := fact(t)

		if err != nil {
			return nil, err
		}
		return f, nil
	}
	// Check satisfaction of interface types in reverse registration order.
	for i := len(b.ifaceFuncs) - 1; i >= 0; i-- {
		it := b.ifaceFuncs[i]
		if ok := t.Implements(it); ok {
			if fact, ok := b.allFuncs[it]; ok {
				f, err := fact(t)
				if err != nil {
					return nil, err
				}
				return f, nil
			}
		}
	}
	return nil, nil
}

// decoderForSingleTypeReflect returns a reflection based decoder function for the
// given type.
func (b *RowDecoderBuilder) decoderForSingleTypeReflect(t reflect.Type) (func(reflect.Value, io.Reader) error, error) {
	// Check if there are any providers registered for this type, or that this type adheres to any interfaces.
	dec, err := b.customFunc(t)
	if err != nil {
		return nil, err
	}
	if dec != nil {
		return func(v reflect.Value, r io.Reader) error {
			elm, err := dec(r)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(elm))
			return nil
		}, nil
	}
	switch t.Kind() {
	case reflect.Struct:
		return b.decoderForStructReflect(t)
	case reflect.Bool:
		return reflectDecodeBool, nil
	case reflect.Uint8:
		return reflectDecodeByte, nil
	case reflect.String:
		return reflectDecodeString, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflectDecodeInt, nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16:
		return reflectDecodeUint, nil
	case reflect.Float32, reflect.Float64:
		return reflectDecodeFloat, nil
	case reflect.Ptr:
		decf, err := b.decoderForSingleTypeReflect(t.Elem())
		if err != nil {
			return nil, err
		}
		return func(rv reflect.Value, r io.Reader) error {
			rv.Set(reflect.New(t.Elem()))
			return decf(rv.Elem(), r)
		}, nil
	case reflect.Slice:
		// Special case handling for byte slices.
		if t.Elem().Kind() == reflect.Uint8 {
			return reflectDecodeByteSlice, nil
		}
		decf, err := b.containerDecoderForType(t.Elem())
		if err != nil {
			return nil, err
		}
		return iterableDecoderForSlice(t, decf), nil
	case reflect.Array:
		decf, err := b.containerDecoderForType(t.Elem())
		if err != nil {
			return nil, err
		}
		return iterableDecoderForArray(t, decf), nil
	case reflect.Map:
		decK, err := b.containerDecoderForType(t.Key())
		if err != nil {
			return nil, err
		}
		decV, err := b.containerDecoderForType(t.Elem())
		if err != nil {
			return nil, err
		}
		return mapDecoder(t, decK, decV), nil
	}
	return nil, errors.Errorf("unable to decode type: %v", t)
}

func (b *RowDecoderBuilder) containerDecoderForType(t reflect.Type) (func(reflect.Value, io.Reader) error, error) {
	dec, err := b.decoderForSingleTypeReflect(t)
	if err != nil {
		return nil, err
	}
	if t.Kind() == reflect.Ptr {
		return containerNilDecoder(dec), nil
	}
	return dec, nil
}

type typeDecoderReflect struct {
	typ    reflect.Type
	fields []func(reflect.Value, io.Reader) error
}
