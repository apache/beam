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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// RowEncoderBuilder allows one to build Beam Schema row encoders for provided types.
type RowEncoderBuilder struct {
	allFuncs   map[reflect.Type]encoderProvider
	ifaceFuncs []reflect.Type

	// RequireAllFieldsExported when set to true will have the default decoder building fail if
	// there are any unexported fields. When set false, unexported fields in default
	// destination structs will be silently ignored when decoding.
	// This has no effect on types with registered decoder providers.
	RequireAllFieldsExported bool
}

type encoderProvider = func(reflect.Type) (func(any, io.Writer) error, error)

// Register accepts a provider for the given type to schema encode values of that type.
//
// When generating encoding functions, this builder will first check for exact type
// matches, then against interfaces with registered factories in recency order of
// registration, and then finally use the default Beam Schema encoding behavior.
//
// TODO(BEAM-9615): Add final factory types. This interface is subject to change.
// Currently f must be a function of the type func(reflect.Type) func(T, io.Writer) (error).
func (b *RowEncoderBuilder) Register(rt reflect.Type, f any) {
	fe, ok := f.(encoderProvider)
	if !ok {
		panic(fmt.Sprintf("%T isn't a supported encoder function type (passed with %v)", f, rt))
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
func (b *RowEncoderBuilder) Build(rt reflect.Type) (func(any, io.Writer) error, error) {
	if err := rowTypeValidation(rt, true); err != nil {
		return nil, err
	}
	return b.encoderForType(rt)
}

// customFunc returns nil if no custom func exists for this type.
// If an error is returned, coder construction should be aborted.
func (b *RowEncoderBuilder) customFunc(t reflect.Type) (func(any, io.Writer) error, bool, error) {
	if fact, ok := b.allFuncs[t]; ok {
		f, err := fact(t)

		if err != nil {
			return nil, false, err
		}
		return f, false, err
	}
	pt := reflect.PtrTo(t)
	// Check satisfaction of interface types in reverse registration order.
	for i := len(b.ifaceFuncs) - 1; i >= 0; i-- {
		it := b.ifaceFuncs[i]
		if ok := t.Implements(it); ok {
			if fact, ok := b.allFuncs[it]; ok {
				f, err := fact(t)
				if err != nil {
					return nil, false, err
				}
				return f, false, nil
			}
		}
		// This can occur when the type uses a pointer receiver but it is included as a value in a struct field.
		if ok := pt.Implements(it); ok {
			if fact, ok := b.allFuncs[it]; ok {
				f, err := fact(pt)
				if err != nil {
					return nil, true, err
				}
				return f, true, nil
			}
		}
	}
	return nil, false, nil
}

// encoderForType returns an encoder function for the struct or pointer to struct type.
func (b *RowEncoderBuilder) encoderForType(t reflect.Type) (func(any, io.Writer) error, error) {
	// Check if there are any providers registered for this type, or that this type adheres to any interfaces.
	var isPtr bool
	// Pointers become the value type for decomposition.
	if t.Kind() == reflect.Ptr {
		// If we have something for the pointer version already, we're done.
		enc, addr, err := b.customFunc(t)
		if err != nil {
			return nil, err
		}
		if addr {
			// We cannot deal with address of here, only in embedded fields, indices, and keys/values. So clear f and continue.
			enc = nil
		}
		if enc != nil {
			return enc, nil
		}
		isPtr = true
		t = t.Elem()
	}

	{
		enc, addr, err := b.customFunc(t)
		if err != nil {
			return nil, err
		}
		if addr {
			// We cannot deal with address of here, only in embedded fields, indices, and keys/values. So clear f and continue.
			enc = nil
		}
		if enc != nil {
			if isPtr {
				// We have the value version, but not a pointer version, so we jump through reflect to
				// get the right type to pass in.
				return func(v any, w io.Writer) error {
					return enc(reflect.ValueOf(v).Elem().Interface(), w)
				}, nil
			}
			return enc, nil
		}
	}

	enc, err := b.encoderForStructReflect(t)
	if err != nil {
		return nil, err
	}

	if isPtr {
		return func(v any, w io.Writer) error {
			return enc(reflect.ValueOf(v).Elem(), w)
		}, nil
	}
	return func(v any, w io.Writer) error {
		return enc(reflect.ValueOf(v), w)
	}, nil
}

// Generates coder using reflection for
func (b *RowEncoderBuilder) encoderForSingleTypeReflect(t reflect.Type) (typeEncoderFieldReflect, error) {
	// Check if there are any providers registered for this type, or that this type adheres to any interfaces.
	enc, addr, err := b.customFunc(t)
	if err != nil {
		return typeEncoderFieldReflect{}, err
	}
	if enc != nil {
		return typeEncoderFieldReflect{
			encode: func(v reflect.Value, w io.Writer) error {
				return enc(v.Interface(), w)
			},
			addr: addr,
		}, nil
	}

	switch t.Kind() {
	case reflect.Struct:
		enc, err := b.encoderForStructReflect(t)
		return typeEncoderFieldReflect{encode: enc}, err
	case reflect.Bool:
		return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
			return EncodeBool(rv.Bool(), w)
		}}, nil
	case reflect.Uint8:
		return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
			return EncodeByte(byte(rv.Uint()), w)
		}}, nil
	case reflect.String:
		return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
			return EncodeStringUTF8(rv.String(), w)
		}}, nil
	case reflect.Int, reflect.Int64, reflect.Int16, reflect.Int32, reflect.Int8:
		return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
			return EncodeVarInt(rv.Int(), w)
		}}, nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16:
		return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
			return EncodeVarUint64(rv.Uint(), w)
		}}, nil
	case reflect.Float32:
		return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
			return EncodeSinglePrecisionFloat(float32(rv.Float()), w)
		}}, nil
	case reflect.Float64:
		return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
			return EncodeDouble(rv.Float(), w)
		}}, nil
	case reflect.Ptr:
		// Nils are handled at the struct field level.
		encf, err := b.encoderForSingleTypeReflect(t.Elem())
		if err != nil {
			return typeEncoderFieldReflect{}, err
		}
		return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
			if !encf.addr {
				rv = rv.Elem()
			}
			return encf.encode(rv, w)
		}}, nil
	case reflect.Slice:
		// Special case handling for byte slices.
		if t.Elem().Kind() == reflect.Uint8 {
			return typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
				return EncodeBytes(rv.Bytes(), w)
			}}, nil
		}
		encf, err := b.containerEncoderForType(t.Elem())
		if err != nil {
			return typeEncoderFieldReflect{}, err
		}
		return typeEncoderFieldReflect{encode: iterableEncoder(t, encf)}, nil
	case reflect.Array:
		encf, err := b.containerEncoderForType(t.Elem())
		if err != nil {
			return typeEncoderFieldReflect{}, err
		}
		return typeEncoderFieldReflect{encode: iterableEncoder(t, encf)}, nil
	case reflect.Map:
		encK, err := b.containerEncoderForType(t.Key())
		if err != nil {
			return typeEncoderFieldReflect{}, err
		}
		encV, err := b.containerEncoderForType(t.Elem())
		if err != nil {
			return typeEncoderFieldReflect{}, err
		}
		return typeEncoderFieldReflect{encode: mapEncoder(t, encK, encV)}, nil
	}
	return typeEncoderFieldReflect{}, errors.Errorf("unable to encode type: %v", t)
}

func (b *RowEncoderBuilder) containerEncoderForType(t reflect.Type) (typeEncoderFieldReflect, error) {
	encf, err := b.encoderForSingleTypeReflect(t)
	if err != nil {
		return typeEncoderFieldReflect{}, err
	}
	if t.Kind() == reflect.Ptr {
		return typeEncoderFieldReflect{encode: NullableEncoder(encf.encode), addr: encf.addr}, nil
	}
	return encf, nil
}

type typeEncoderReflect struct {
	debug  []string
	fields []typeEncoderFieldReflect
}

type typeEncoderFieldReflect struct {
	encode func(reflect.Value, io.Writer) error
	// If true the encoder is expecting us to pass it the address
	// of the field value (i.e. &foo.bar) and not the field value (i.e. foo.bar).
	addr bool
}

// encoderForStructReflect generates reflection field access closures for structs.
func (b *RowEncoderBuilder) encoderForStructReflect(t reflect.Type) (func(reflect.Value, io.Writer) error, error) {
	var coder typeEncoderReflect
	for i := 0; i < t.NumField(); i++ {
		coder.debug = append(coder.debug, t.Field(i).Name+" "+t.Field(i).Type.String())
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
					return nil, errors.Errorf("cannot make schema encoder for type %v as it has an embedded field of a pointer to an unexported type %v. See https://golang.org/issue/21357", t, ft.Elem())
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
				return nil, errors.Errorf("cannot make schema encoder for type %v as it has unexported fields such as %s.", t, sf.Name)
			}
			// Silently ignore, since we can't do anything about it.
			// Add a no-op coder to fill in field index
			coder.fields = append(coder.fields, typeEncoderFieldReflect{encode: func(rv reflect.Value, w io.Writer) error {
				return nil
			}})
			continue
		}
		enc, err := b.encoderForSingleTypeReflect(sf.Type)
		if err != nil {
			return nil, err
		}
		coder.fields = append(coder.fields, enc)
	}

	return func(rv reflect.Value, w io.Writer) error {
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
			if f.addr {
				rvf = rvf.Addr()
			}
			if err := f.encode(rvf, w); err != nil {
				return errors.Wrapf(err, "encoding %v, expected: %q", rvf.Type(), coder.debug[i])
			}
		}
		return nil
	}, nil
}
