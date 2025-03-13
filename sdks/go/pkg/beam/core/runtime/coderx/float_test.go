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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func TestFloat(t *testing.T) {
	tests := []any{
		float32(0.0),
		float32(94.5),
		float64(-127.25),
		float64(8675309.4286),
	}

	for _, v := range tests {
		typ := reflect.ValueOf(v).Type()

		data := encFloat(v)
		result, err := decFloat(typ, data)
		if err != nil {
			t.Fatalf("dec(enc(%v)) failed: %v", v, err)
		}

		if v != result {
			t.Errorf("dec(enc(%v)) = %v, want id", v, result)
		}
		resultT := reflectx.UnderlyingType(reflect.ValueOf(result)).Type()
		if resultT != typ {
			t.Errorf("type(dec(enc(%v))) = %v, want id", typ, resultT)
		}
	}
}
