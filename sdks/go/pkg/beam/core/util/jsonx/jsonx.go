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

// Package jsonx contains utilities for working with JSON encoded data.
//
// TODO(BEAM-9616): Delete this package once the SDK defaults to schemas.
package jsonx

import (
	"bytes"
	"encoding/json"
	"io"
)

// Marshal encodes a given value as JSON, and returns the bytes.
func Marshal(elem interface{}) ([]byte, error) {
	var bb bytes.Buffer
	if err := MarshalTo(elem, &bb); err != nil {
		return nil, err
	}
	return bytes.TrimRight(bb.Bytes(), "\n"), nil
}

// MarshalTo encodes a given value as JSON, and returns the bytes.
func MarshalTo(elem interface{}, w io.Writer) error {
	enc := json.NewEncoder(w)
	return enc.Encode(elem)
}

// Unmarshal decodes a given value as JSON, and returns the bytes.
// The passed in element must be a pointer of the given type.
func Unmarshal(elem interface{}, data []byte) error {
	return UnmarshalFrom(elem, bytes.NewReader(data))
}

// UnmarshalFrom decodes a given value as JSON, and returns the bytes.
// The passed in element must be a pointer of the given type.
func UnmarshalFrom(elem interface{}, r io.Reader) error {
	dec := json.NewDecoder(r)
	return dec.Decode(r)
}
