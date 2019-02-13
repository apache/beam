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
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// EncodedType is a serialization wrapper around a type for convenience.
type EncodedType struct {
	// T is the type to preserve across serialization.
	T reflect.Type
}

// MarshalJSON returns the JSON encoding this value.
func (w EncodedType) MarshalJSON() ([]byte, error) {
	str, err := graphx.EncodeType(w.T)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

// UnmarshalJSON sets the state of this instance from the passed in JSON.
func (w *EncodedType) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	t, err := graphx.DecodeType(s)
	if err != nil {
		return err
	}
	w.T = t
	return nil
}

// EncodedFunc is a serialization wrapper around a function for convenience.
type EncodedFunc struct {
	// Fn is the function to preserve across serialization.
	Fn reflectx.Func
}

// MarshalJSON returns the JSON encoding this value.
func (w EncodedFunc) MarshalJSON() ([]byte, error) {
	str, err := graphx.EncodeFn(w.Fn)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

// UnmarshalJSON sets the state of this instance from the passed in JSON.
func (w *EncodedFunc) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	fn, err := graphx.DecodeFn(s)
	if err != nil {
		return err
	}
	w.Fn = fn
	return nil
}

// DecodeCoder decodes a coder. Any custom coder function symbol must be
// resolvable via the runtime.GlobalSymbolResolver. The types must be encodable.
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

// MarshalJSON returns the JSON encoding this value.
func (w EncodedCoder) MarshalJSON() ([]byte, error) {
	str, err := graphx.EncodeCoder(w.Coder.coder)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

// UnmarshalJSON sets the state of this instance from the passed in JSON.
func (w *EncodedCoder) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	c, err := graphx.DecodeCoder(s)
	if err != nil {
		return err
	}
	w.Coder = Coder{coder: c}
	return nil
}
