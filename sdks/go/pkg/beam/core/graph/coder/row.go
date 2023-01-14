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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

var (
	defaultEnc RowEncoderBuilder
	defaultDec RowDecoderBuilder
)

// RequireAllFieldsExported when set to true will have the default coder buildings using
// RowEncoderForStruct and RowDecoderForStruct fail if there are any unexported fields.
// When set false, unexported fields in default destination structs will be silently
// ignored when coding.
// This has no effect on types with registered coder providers.
func RequireAllFieldsExported(require bool) {
	defaultEnc.RequireAllFieldsExported = require
	defaultDec.RequireAllFieldsExported = require
}

// RegisterSchemaProviders Register Custom Schema providers.
func RegisterSchemaProviders(rt reflect.Type, enc, dec any) {
	defaultEnc.Register(rt, enc)
	defaultDec.Register(rt, dec)
}

// RowEncoderForStruct returns an encoding function that encodes a struct type
// or a pointer to a struct type using the beam row encoding.
//
// Returns an error if the given type is invalid or not encodable to a beam
// schema row.
func RowEncoderForStruct(rt reflect.Type) (func(any, io.Writer) error, error) {
	return defaultEnc.Build(rt)
}

// RowDecoderForStruct returns a decoding function that decodes the beam row encoding
// into the given type.
//
// Returns an error if the given type is invalid or not decodable from a beam
// schema row.
func RowDecoderForStruct(rt reflect.Type) (func(io.Reader) (any, error), error) {
	return defaultDec.Build(rt)
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

// WriteRowHeader handles the field header for row encodings.
func WriteRowHeader(n int, isNil func(int) bool, w io.Writer) error {
	// Row/Structs are prefixed with the number of fields that are encoded in total.
	if err := EncodeVarInt(int64(n), w); err != nil {
		return err
	}
	// Followed by a packed bit array of the nil fields.
	var curByte byte
	lastNilByte := -1
	var bytes = make([]byte, 0, n/8+1)
	for i := 0; i < n; i++ {
		shift := i % 8
		if i != 0 && shift == 0 {
			bytes = append(bytes, curByte)
			curByte = 0
		}
		if isNil(i) {
			curByte |= (1 << uint8(shift))
			// This will always be 1 less than the actual bitset length
			// since the working byte isn't appended until after the loop.
			lastNilByte = len(bytes)
		}
	}
	bytes = append(bytes, curByte)
	// Trailing 0 bytes are elided, w/0 nil fields encoding to a varint 0.
	bytes = bytes[:lastNilByte+1]

	if err := EncodeVarInt(int64(len(bytes)), w); err != nil {
		return err
	}
	if _, err := ioutilx.WriteUnsafe(w, bytes); err != nil {
		return err
	}
	return nil
}

// ReadRowHeader handles the field header for row decodings.
//
// This returns the number of encoded fileds, the raw bitpacked bytes and
// any error during decoding. Each bit only needs only needs to be
// examined once during decoding using the IsFieldNil helper function.
//
// If there are no nil fields encoded,the byte array will be nil, and no
// encoded fields will be nil.
func ReadRowHeader(r io.Reader) (int, []byte, error) {
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
	if nf < l {
		return int(nf), nil, fmt.Errorf("number of fields is less than byte array %v < %v", nf, l)
	}
	nils := make([]byte, l)
	if err := ioutilx.ReadNBufUnsafe(r, nils); err != nil {
		return int(nf), nil, err
	}
	return int(nf), nils, nil
}

// IsFieldNil examines the passed in packed bits nils buffer
// and returns true if the field at that index wasn't encoded
// and can be skipped in decoding.
func IsFieldNil(nils []byte, f int) bool {
	i, b := f/8, f%8
	// https://github.com/apache/beam/issues/21232: The row header can elide trailing 0 bytes,
	// and we shouldn't care if there are trailing 0 bytes when doing a lookup.
	return i < len(nils) && len(nils) != 0 && (nils[i]>>uint8(b))&0x1 == 1
}

// WriteSimpleRowHeader is a convenience function to write Beam Schema Row Headers
// for values that do not have any nil fields. Writes the number of fields total
// and a 0 len byte slice to indicate no fields are nil.
func WriteSimpleRowHeader(fields int, w io.Writer) error {
	if err := EncodeVarInt(int64(fields), w); err != nil {
		return err
	}
	// Never nils, so we write the 0 byte header.
	if err := EncodeVarInt(0, w); err != nil {
		return fmt.Errorf("WriteSimpleRowHeader a 0 length nils bit field: %v", err)
	}
	return nil
}

// ReadSimpleRowHeader is a convenience function to read Beam Schema Row Headers
// for values that do not have any nil fields. Reads and validates the number of
// fields total (returning an error for mismatches, and checks that there are
// no nils encoded as a bit field.
func ReadSimpleRowHeader(fields int, r io.Reader) error {
	n, err := DecodeVarInt(r)
	if err != nil {
		return fmt.Errorf("ReadSimpleRowHeader field count: %v, %v", n, err)
	}
	if int(n) != fields {
		return fmt.Errorf("ReadSimpleRowHeader field count mismatch, got %v, want %v", n, fields)
	}
	n, err = DecodeVarInt(r)
	if err != nil {
		return fmt.Errorf("ReadSimpleRowHeader reading nils count: %v, %v", n, err)
	}
	if n != 0 {
		return fmt.Errorf("ReadSimpleRowHeader expected no nils encoded count, got %v", n)
	}
	return nil
}
