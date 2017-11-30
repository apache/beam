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

package beam

import (
	"encoding/json"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
)

// EncodeType encodes a type as a string. Unless registered, the decoded type
// is only guaranteed to the isomorphic to the input and with no methods.
func EncodeType(t reflect.Type) (string, error) {
	return graphx.EncodeType(t)
}

// DecodeType decodes a type. Unless registered, the decoded type
// is only guaranteed to the isomorphic to the input and with no methods.
func DecodeType(data string) (reflect.Type, error) {
	return graphx.DecodeType(data)
}

// EncodedType is a serialization wrapper around a type for convenience.
type EncodedType struct {
	// T is the type to preserve across serialization.
	T reflect.Type
}

func (w EncodedType) MarshalJSON() ([]byte, error) {
	str, err := EncodeType(w.T)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

func (w *EncodedType) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	t, err := DecodeType(s)
	if err != nil {
		return err
	}
	w.T = t
	return nil
}

// EncodeFn encodes a function and parameter types as a string. The function
// symbol must be resolvable via the runtime.SymbolResolver. The types must
// be encodable.
func EncodeFn(fn reflect.Value) (string, error) {
	return graphx.EncodeFn(fn)
}

// DecodeFn encodes a function as a string. The function symbol must be
// resolvable via the runtime.SymbolResolver. The parameter types must
// be encodable.
func DecodeFn(data string) (reflect.Value, error) {
	return graphx.DecodeFn(data)
}

// EncodedFn is a serialization wrapper around a function for convenience.
type EncodedFn struct {
	// Fn is the function to preserve across serialization.
	Fn reflect.Value
}

func (w EncodedFn) MarshalJSON() ([]byte, error) {
	str, err := EncodeFn(w.Fn)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

func (w *EncodedFn) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	fn, err := DecodeFn(s)
	if err != nil {
		return err
	}
	w.Fn = fn
	return nil
}

// EncodeCoder encodes a coder as a string. Any custom coder function
// symbol must be resolvable via the runtime.SymbolResolver. The types must
// be encodable.
func EncodeCoder(c Coder) (string, error) {
	return graphx.EncodeCoder(c.coder)
}

// DecodeCoder decodes a coder. Any custom coder function symbol must be
// resolvable via the runtime.SymbolResolver. The types must be encodable.
func DecodeCoder(data string) (Coder, error) {
	c, err := graphx.DecodeCoder(data)
	if err != nil {
		return Coder{}, err
	}
	return Coder{coder: c}, nil
}

// EncodedCoder is a serialization wrapper around a coder for convenience.
type EncodedCoder struct {
	// Coder is the coder to preserve across serialization.
	Coder Coder
}

func (w EncodedCoder) MarshalJSON() ([]byte, error) {
	str, err := EncodeCoder(w.Coder)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

func (w *EncodedCoder) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	c, err := DecodeCoder(s)
	if err != nil {
		return err
	}
	w.Coder = c
	return nil
}
