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
	"bytes"
	"io"
	"reflect"
	"sort"
)

// TODO(lostluck): 2020.08.04 export these for use for others?

// mapDecoder produces a decoder for the beam schema map encoding.
func mapDecoder(rt reflect.Type, decodeToKey, decodeToElem typeDecoderFieldReflect) func(reflect.Value, io.Reader) error {
	return func(ret reflect.Value, r io.Reader) error {
		// (1) Read count prefixed encoded data
		size, err := DecodeInt32(r)
		if err != nil {
			return err
		}
		n := int(size)
		ret.Set(reflect.MakeMapWithSize(rt, n))
		rtk := rt.Key()
		rtv := rt.Elem()
		for i := 0; i < n; i++ {
			rvk := reflect.New(rtk)
			var err error
			if decodeToKey.addr {
				err = decodeToKey.decode(rvk, r)
			} else {
				err = decodeToKey.decode(rvk.Elem(), r)
			}
			if err != nil {
				return err
			}
			rvv := reflect.New(rtv)
			if decodeToElem.addr {
				err = decodeToElem.decode(rvv, r)
			} else {
				err = decodeToElem.decode(rvv.Elem(), r)
			}
			if err != nil {
				return err
			}
			ret.SetMapIndex(rvk.Elem(), rvv.Elem())
		}
		return nil
	}
}

// mapEncoder reflectively encodes a map or array type using the beam map encoding.
func mapEncoder(rt reflect.Type, encodeKey, encodeValue typeEncoderFieldReflect) func(reflect.Value, io.Writer) error {
	return func(rv reflect.Value, w io.Writer) error {
		size := rv.Len()
		if err := EncodeInt32((int32)(size), w); err != nil {
			return err
		}
		keys := rv.MapKeys()
		type pair struct {
			v reflect.Value
			b []byte
		}
		rtk := rv.Type().Key()
		sorted := make([]pair, 0, size)
		var buf bytes.Buffer // Re-use the same buffer.
		for _, key := range keys {
			var rvk reflect.Value
			if encodeKey.addr {
				rvk = reflect.New(rtk)
				rvk.Elem().Set(key)
			} else {
				rvk = key
			}
			if err := encodeKey.encode(key, &buf); err != nil {
				return err
			}
			p := pair{v: key, b: make([]byte, buf.Len())}
			copy(p.b, buf.Bytes())
			sorted = append(sorted, p)
			buf.Reset() // Reset for next iteration.
		}
		sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].b, sorted[j].b) < 0 })
		rtv := rv.Type().Elem()
		for _, kp := range sorted {
			if _, err := w.Write(kp.b); err != nil {
				return err
			}
			val := rv.MapIndex(kp.v)
			var rvv reflect.Value
			if encodeValue.addr {
				rvv = reflect.New(rtv)
				rvv.Elem().Set(val)
			} else {
				rvv = val
			}
			if err := encodeValue.encode(rvv, w); err != nil {
				return err
			}
		}
		return nil
	}
}
