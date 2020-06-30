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
	"io"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// TODO(lostluck): 2020.06.29 export these for use for others?

// iterableEncoder reflectively encodes a slice or array type using
// the beam fixed length iterable encoding.
func iterableEncoder(rt reflect.Type, encode func(reflect.Value, io.Writer) error) func(reflect.Value, io.Writer) error {
	return func(rv reflect.Value, w io.Writer) error {
		size := rv.Len()
		if err := EncodeInt32((int32)(size), w); err != nil {
			return err
		}
		for i := 0; i < size; i++ {
			if err := encode(rv.Index(i), w); err != nil {
				return err
			}
		}
		return nil
	}
}

// iterableDecoderForSlice can decode from both the fixed sized and
// multi-chunk variants of the beam iterable protocol.
// Returns an error for other protocols (such as state backed).
func iterableDecoderForSlice(rt reflect.Type, decodeToElem func(reflect.Value, io.Reader) error) func(reflect.Value, io.Reader) error {
	return func(ret reflect.Value, r io.Reader) error {
		// (1) Read count prefixed encoded data
		size, err := DecodeInt32(r)
		if err != nil {
			return err
		}
		n := int(size)
		switch {
		case n >= 0:
			rv := reflect.MakeSlice(rt, n, n)
			if err := decodeToIterable(rv, r, decodeToElem); err != nil {
				return err
			}
			ret.Set(rv)
			return nil
		case n == -1:
			chunk, err := DecodeVarInt(r)
			if err != nil {
				return err
			}
			rv := reflect.MakeSlice(rt, 0, int(chunk))
			for chunk != 0 {
				rvi := reflect.MakeSlice(rt, int(chunk), int(chunk))
				if err := decodeToIterable(rvi, r, decodeToElem); err != nil {
					return err
				}
				rv = reflect.AppendSlice(rv, rvi)
				chunk, err = DecodeVarInt(r)
				if err != nil {
					return err
				}
			}
			ret.Set(rv)
			return nil
		default:
			return errors.Errorf("unable to decode slice iterable with size: %d", n)
		}
	}
}

// iterableDecoderForArray can decode from only the fixed sized and
// multi-chunk variant of the beam iterable protocol.
// Returns an error for other protocols (such as state backed).
func iterableDecoderForArray(rt reflect.Type, decodeToElem func(reflect.Value, io.Reader) error) func(reflect.Value, io.Reader) error {
	return func(ret reflect.Value, r io.Reader) error {
		// (1) Read count prefixed encoded data
		size, err := DecodeInt32(r)
		if err != nil {
			return err
		}
		n := int(size)
		if rt.Len() != n {
			return errors.Errorf("len mismatch decoding a %v: want %d got %d", rt, rt.Len(), n)
		}
		switch {
		case n >= 0:
			if err := decodeToIterable(ret, r, decodeToElem); err != nil {
				return err
			}
			return nil
		default:
			return errors.Errorf("unable to decode array iterable with size: %d", n)
		}
	}
}

func decodeToIterable(rv reflect.Value, r io.Reader, decodeTo func(reflect.Value, io.Reader) error) error {
	for i := 0; i < rv.Len(); i++ {
		err := decodeTo(rv.Index(i), r)
		if err != nil {
			return err
		}
	}
	return nil
}
