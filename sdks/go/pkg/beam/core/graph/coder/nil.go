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

// NullableDecoder handles when a value is nillable.
// Nillable types have an extra byte prefixing them indicating nil status.
func NullableDecoder(decodeToElem func(reflect.Value, io.Reader) error) func(reflect.Value, io.Reader) error {
	return func(ret reflect.Value, r io.Reader) error {
		hasValue, err := DecodeBool(r)
		if err != nil {
			return err
		}
		if !hasValue {
			return nil
		}
		if err := decodeToElem(ret, r); err != nil {
			return err
		}
		return nil
	}
}

// NullableEncoder handles when a value is nillable.
// Nillable types have an extra byte prefixing them indicating nil status.
func NullableEncoder(encodeElem func(reflect.Value, io.Writer) error) func(reflect.Value, io.Writer) error {
	return func(rv reflect.Value, w io.Writer) error {
		if rv.IsNil() {
			return EncodeBool(false, w)
		}
		if err := EncodeBool(true, w); err != nil {
			return err
		}
		return encodeElem(rv, w)
	}
}
