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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// RowEncoderForStruct returns an encoding function that encodes a struct type
// or a pointer to a struct type using the beam row encoding.
//
// Returns an error if the given type is invalid or not encodable to a beam
// schema row.
func RowEncoderForStruct(rt reflect.Type) (func(interface{}, io.Writer) error, error) {
	if err := rowTypeValidation(rt, true); err != nil {
		return nil, err
	}
	return encoderForType(rt), nil
}

// RowDecoderForStruct returns a decoding function that decodes the beam row encoding
// into the given type.
//
// Returns an error if the given type is invalid or not decodable from a beam
// schema row.
func RowDecoderForStruct(rt reflect.Type) (func(io.Reader) (interface{}, error), error) {
	if err := rowTypeValidation(rt, true); err != nil {
		return nil, err
	}
	return decoderForType(rt), nil
}

func rowTypeValidation(rt reflect.Type, strictExportedFields bool) error {
	switch k := rt.Kind(); k {
	case reflect.Ptr, reflect.Struct:
	default:
		return errors.Errorf("can't generate row coder for type %v: must be a struct type or pointer to a struct type", rt)
	}
	// TODO exported field validation.
	return nil
}

// decoderForType returns a decoder function for the struct or pointer to struct type.
func decoderForType(t reflect.Type) func(io.Reader) (interface{}, error) {
	var isPtr bool
	// Pointers become the value type for decomposition.
	if t.Kind() == reflect.Ptr {
		isPtr = true
		t = t.Elem()
	}
	dec := decoderForStructReflect(t)

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

// decoderForSingleTypeReflect returns a reflection based decoder function for the
// given type.
func decoderForSingleTypeReflect(t reflect.Type) func(reflect.Value, io.Reader) error {
	switch t.Kind() {
	case reflect.Struct:
		return decoderForStructReflect(t)
	case reflect.Bool:
		return func(rv reflect.Value, r io.Reader) error {
			v, err := DecodeBool(r)
			if err != nil {
				return errors.Wrap(err, "error decoding bool field")
			}
			rv.SetBool(v)
			return nil
		}
	case reflect.Uint8:
		return func(rv reflect.Value, r io.Reader) error {
			b, err := DecodeByte(r)
			if err != nil {
				return errors.Wrap(err, "error decoding single byte field")
			}
			rv.SetUint(uint64(b))
			return nil
		}
	case reflect.String:
		return func(rv reflect.Value, r io.Reader) error {
			v, err := DecodeStringUTF8(r)
			if err != nil {
				return errors.Wrap(err, "error decoding string field")
			}
			rv.SetString(v)
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(rv reflect.Value, r io.Reader) error {
			v, err := DecodeVarInt(r)
			if err != nil {
				return errors.Wrap(err, "error decoding varint field")
			}
			rv.SetInt(v)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		return func(rv reflect.Value, r io.Reader) error {
			v, err := DecodeDouble(r)
			if err != nil {
				return errors.Wrap(err, "error decoding double field")
			}
			rv.SetFloat(v)
			return nil
		}
	case reflect.Ptr:
		decf := decoderForSingleTypeReflect(t.Elem())
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
		decf := decoderForSingleTypeReflect(t.Elem())
		sdec := iterableDecoderForSlice(t, decf)
		return func(rv reflect.Value, r io.Reader) error {
			return sdec(rv, r)
		}
	case reflect.Array:
		decf := decoderForSingleTypeReflect(t.Elem())
		sdec := iterableDecoderForArray(t, decf)
		return func(rv reflect.Value, r io.Reader) error {
			return sdec(rv, r)
		}
	}
	panic(fmt.Sprintf("unimplemented type to decode: %v", t))
}

type typeDecoderReflect struct {
	typ    reflect.Type
	fields []func(reflect.Value, io.Reader) error
}

// decoderForStructReflect returns a reflection based decoder function for the
// given struct type.
func decoderForStructReflect(t reflect.Type) func(reflect.Value, io.Reader) error {
	var coder typeDecoderReflect
	for i := 0; i < t.NumField(); i++ {
		i := i // avoid alias issues in the closures.
		dec := decoderForSingleTypeReflect(t.Field(i).Type)
		coder.fields = append(coder.fields, func(rv reflect.Value, r io.Reader) error {
			return dec(rv.Field(i), r)
		})
	}

	return func(rv reflect.Value, r io.Reader) error {
		nf, nils, err := readRowHeader(rv, r)
		if err != nil {
			return err
		}
		if nf != len(coder.fields) {
			return errors.Errorf("schema[%v] changed: got %d fields, want %d fields", "TODO", nf, len(coder.fields))
		}
		for i, f := range coder.fields {
			if isFieldNil(nils, i) {
				continue
			}
			if err := f(rv, r); err != nil {
				return err
			}
		}
		return nil
	}
}

// isFieldNil examines the passed in packed bits nils buffer
// and returns true if the field at that index wasn't encoded
// and can be skipped in decoding.
func isFieldNil(nils []byte, f int) bool {
	i, b := f/8, f%8
	return len(nils) != 0 && (nils[i]>>uint8(b))&0x1 == 1
}

// encoderForType returns an encoder function for the struct or pointer to struct type.
func encoderForType(t reflect.Type) func(interface{}, io.Writer) error {
	var isPtr bool
	// Pointers become the value type for decomposition.
	if t.Kind() == reflect.Ptr {
		isPtr = true
		t = t.Elem()
	}
	enc := encoderForStructReflect(t)

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
func encoderForSingleTypeReflect(t reflect.Type) func(reflect.Value, io.Writer) error {
	switch t.Kind() {
	case reflect.Struct:
		return encoderForStructReflect(t)
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
		encf := encoderForSingleTypeReflect(t.Elem())
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
		encf := encoderForSingleTypeReflect(t.Elem())
		return iterableEncoder(t, encf)
	case reflect.Array:
		encf := encoderForSingleTypeReflect(t.Elem())
		return iterableEncoder(t, encf)
	}
	panic(fmt.Sprintf("unimplemented type to encode: %v", t))
}

type typeEncoderReflect struct {
	fields []func(reflect.Value, io.Writer) error
}

// encoderForStructReflect generates reflection field access closures for structs.
func encoderForStructReflect(t reflect.Type) func(reflect.Value, io.Writer) error {
	var coder typeEncoderReflect
	for i := 0; i < t.NumField(); i++ {
		coder.fields = append(coder.fields, encoderForSingleTypeReflect(t.Field(i).Type))
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
				return err
			}
		}
		return nil
	}
}

// writeRowHeader handles the field header for row encodings.
func writeRowHeader(rv reflect.Value, w io.Writer) error {
	// Row/Structs are prefixed with the number of fields that are encoded in total.
	if err := EncodeVarInt(int64(rv.NumField()), w); err != nil {
		return err
	}
	// Followed by a packed bit array of the nil fields.
	var curByte byte
	var nils bool
	var bytes = make([]byte, 0, rv.NumField()/8+1)
	for i := 0; i < rv.NumField(); i++ {
		shift := i % 8
		if i != 0 && shift == 0 {
			bytes = append(bytes, curByte)
			curByte = 0
		}
		rvf := rv.Field(i)
		switch rvf.Kind() {
		// Other types can be nil, but they aren't encodable.
		case reflect.Ptr, reflect.Map, reflect.Slice:
			if rvf.IsNil() {
				curByte |= (1 << uint8(shift))
				nils = true
			}
		}
	}
	if nils {
		bytes = append(bytes, curByte)
	} else {
		// If there are no nils, we write a 0 length byte array instead.
		bytes = bytes[:0]
	}
	if err := EncodeVarInt(int64(len(bytes)), w); err != nil {
		return err
	}
	if _, err := ioutilx.WriteUnsafe(w, bytes); err != nil {
		return err
	}
	return nil
}

// readRowHeader handles the field header for row decodings.
//
// This returns the raw bitpacked byte slice because we only need to
// examine each bit once, so we may as well do so inline with field checking.
func readRowHeader(rv reflect.Value, r io.Reader) (int, []byte, error) {
	nf, err := DecodeVarInt(r) // is for checksum purposes (old vs new versions of a schemas)
	if err != nil {
		return 0, nil, err
	}
	l, err := DecodeVarInt(r) // read the length prefix for the packed bits.
	if err != nil {
		return int(nf), nil, err
	}
	if l == 0 {
		// A zero length byte array means no nils.
		return int(nf), nil, nil
	}
	var buf [32]byte // should get stack allocated?
	nils := buf[:l]
	if err := ioutilx.ReadNBufUnsafe(r, nils); err != nil {
		return int(nf), nil, err
	}
	return int(nf), nils, nil
}
