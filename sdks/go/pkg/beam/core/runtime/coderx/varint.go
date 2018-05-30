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

package coderx

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

func init() {
	runtime.RegisterFunction(encVarIntZ)
	runtime.RegisterFunction(decVarIntZ)
	runtime.RegisterFunction(encVarUintZ)
	runtime.RegisterFunction(decVarUintZ)
}

// NewVarIntZ returns a varint coder for the given integer type. It uses a zig-zag scheme,
// which is _different_ from the Beam standard coding scheme.
func NewVarIntZ(t reflect.Type) (*coder.CustomCoder, error) {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return coder.NewCustomCoder("varintz", t, encVarIntZ, decVarIntZ)
	default:
		return nil, fmt.Errorf("not a signed integer type: %v", t)
	}
}

// NewVarUintZ returns a uvarint coder for the given integer type. It uses a zig-zag scheme,
// which is _different_ from the Beam standard coding scheme.
func NewVarUintZ(t reflect.Type) (*coder.CustomCoder, error) {
	switch t.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return coder.NewCustomCoder("varuintz", t, encVarUintZ, decVarUintZ)
	default:
		return nil, fmt.Errorf("not a unsigned integer type: %v", t)
	}
}

func encVarIntZ(v typex.T) []byte {
	var val int64
	switch n := v.(type) {
	case int:
		val = int64(n)
	case int8:
		val = int64(n)
	case int16:
		val = int64(n)
	case int32:
		val = int64(n)
	case int64:
		val = n
	default:
		panic(fmt.Sprintf("received unknown value type: want a signed integer:, got %T", n))
	}
	ret := make([]byte, binary.MaxVarintLen64)
	size := binary.PutVarint(ret, val)
	return ret[:size]
}

func decVarIntZ(t reflect.Type, data []byte) (typex.T, error) {
	n, size := binary.Varint(data)
	if size <= 0 {
		return nil, fmt.Errorf("invalid varintz encoding for: %v", data)
	}
	switch t.Kind() {
	case reflect.Int:
		return int(n), nil
	case reflect.Int8:
		return int8(n), nil
	case reflect.Int16:
		return int16(n), nil
	case reflect.Int32:
		return int32(n), nil
	case reflect.Int64:
		return n, nil
	default:
		panic(fmt.Sprintf("unreachable statement: expected a signed integer, got %v", t))
	}
}

func encVarUintZ(v typex.T) []byte {
	var val uint64
	switch n := v.(type) {
	case uint:
		val = uint64(n)
	case uint8:
		val = uint64(n)
	case uint16:
		val = uint64(n)
	case uint32:
		val = uint64(n)
	case uint64:
		val = n
	default:
		panic(fmt.Sprintf("received unknown value type: want an unsigned integer:, got %T", n))
	}
	ret := make([]byte, binary.MaxVarintLen64)
	size := binary.PutUvarint(ret, val)
	return ret[:size]
}

func decVarUintZ(t reflect.Type, data []byte) (typex.T, error) {
	n, size := binary.Uvarint(data)
	if size <= 0 {
		return nil, fmt.Errorf("invalid varuintz encoding for: %v", data)
	}
	switch t.Kind() {
	case reflect.Uint:
		return uint(n), nil
	case reflect.Uint8:
		return uint8(n), nil
	case reflect.Uint16:
		return uint16(n), nil
	case reflect.Uint32:
		return uint32(n), nil
	case reflect.Uint64:
		return n, nil
	default:
		panic(fmt.Sprintf("unreachable statement: expected an unsigned integer, got %v", t))
	}
}
