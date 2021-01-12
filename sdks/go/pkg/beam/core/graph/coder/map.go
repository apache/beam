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
)

// TODO(lostluck): 2020.08.04 export these for use for others?

// mapDecoder produces a decoder for the beam schema map encoding.
func mapDecoder(rt reflect.Type, decodeToKey, decodeToElem func(reflect.Value, io.Reader) error) func(reflect.Value, io.Reader) error {
	return func(ret reflect.Value, r io.Reader) error {
		// (1) Read count prefixed encoded data
		size, err := DecodeInt32(r)
		if err != nil {
			return err
		}
		n := int(size)
		ret.Set(reflect.MakeMapWithSize(rt, n))
		for i := 0; i < n; i++ {
			rvk := reflect.New(rt.Key()).Elem()
			if err := decodeToKey(rvk, r); err != nil {
				return err
			}
			rvv := reflect.New(rt.Elem()).Elem()
			if err := decodeToElem(rvv, r); err != nil {
				return err
			}
			ret.SetMapIndex(rvk, rvv)
		}
		return nil
	}
}

// containerNilDecoder handles when a value is nillable for map or iterable components.
// Nillable types have an extra byte prefixing them indicating nil status.
func containerNilDecoder(decodeToElem func(reflect.Value, io.Reader) error) func(reflect.Value, io.Reader) error {
	return func(ret reflect.Value, r io.Reader) error {
		hasValue, err := DecodeBool(r)
		if err != nil {
			return err
		}
		if !hasValue {
			return nil
		}
		rv := reflect.New(ret.Type().Elem())
		if err := decodeToElem(rv.Elem(), r); err != nil {
			return err
		}
		ret.Set(rv)
		return nil
	}
}

// mapEncoder reflectively encodes a map or array type using the beam map encoding.
func mapEncoder(rt reflect.Type, encodeKey, encodeValue func(reflect.Value, io.Writer) error) func(reflect.Value, io.Writer) error {
	return func(rv reflect.Value, w io.Writer) error {
		size := rv.Len()
		if err := EncodeInt32((int32)(size), w); err != nil {
			return err
		}
		iter := rv.MapRange()
		for iter.Next() {
			if err := encodeKey(iter.Key(), w); err != nil {
				return err
			}
			if err := encodeValue(iter.Value(), w); err != nil {
				return err
			}
		}
		return nil
	}
}

// containerNilEncoder handles when a value is nillable for map or iterable components.
// Nillable types have an extra byte prefixing them indicating nil status.
func containerNilEncoder(encodeElem func(reflect.Value, io.Writer) error) func(reflect.Value, io.Writer) error {
	return func(rv reflect.Value, w io.Writer) error {
		if rv.IsNil() {
			return EncodeBool(false, w)
		}
		if err := EncodeBool(true, w); err != nil {
			return err
		}
		return encodeElem(rv.Elem(), w)
	}
}
