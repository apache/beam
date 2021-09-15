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
	"fmt"
	"math"
	"math/bits"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

func encFloat(v typex.T) []byte {
	var val float64
	switch n := v.(type) {
	case float32:
		val = float64(n)
	case float64:
		val = n
	default:
		panic(fmt.Sprintf("received unknown value type: want a float, got %T", n))
	}

	return encVarUintZ(bits.ReverseBytes64(math.Float64bits(val)))
}

func decFloat(t reflect.Type, data []byte) (typex.T, error) {
	uval, err := decVarUintZ(reflectx.Uint64, data)
	if err != nil {
		return nil, errors.Errorf("invalid float encoding for: %v", data)
	}

	n := math.Float64frombits(bits.ReverseBytes64(uval.(uint64)))
	switch t.Kind() {
	case reflect.Float64:
		return n, nil
	case reflect.Float32:
		return float32(n), nil
	default:
		panic(fmt.Sprintf("unreachable statement: expected a float, got %v", t))
	}
}

// NewFloat returns a coder for the given float type. It uses the same
// encoding scheme as the gob package.
func NewFloat(t reflect.Type) (*coder.CustomCoder, error) {
	switch t.Kind() {
	case reflect.Float32, reflect.Float64:
		return coder.NewCustomCoder("float", t, encFloat, decFloat)
	default:
		return nil, errors.Errorf("not a float type: %v", t)
	}
}
