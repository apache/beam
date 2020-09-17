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

package exec

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func BenchmarkPrimitives(b *testing.B) {
	var value FullValue
	myHash := fnv.New64a()
	b.Run("int", func(b *testing.B) {
		test := interface{}(int(42424242))
		b.Run("native", func(b *testing.B) {
			m := make(map[int]FullValue)
			for i := 0; i < b.N; i++ {
				k := test.(int)
				value = m[k]
				m[k] = value
			}
		})
		cc, err := coderx.NewVarIntZ(reflectx.Int)
		if err != nil {
			b.Fatal(err)
		}
		encoded := &customEncodedHasher{hash: myHash, coder: makeEncoder(cc.Enc.Fn)}
		dedicated := &numberHasher{}
		hashbench(b, test, encoded, dedicated)
	})
	b.Run("float32", func(b *testing.B) {
		test := interface{}(float32(42424242.242424))
		b.Run("native", func(b *testing.B) {
			m := make(map[float32]FullValue)
			for i := 0; i < b.N; i++ {
				k := test.(float32)
				value = m[k]
				m[k] = value
			}
		})
		cc, err := coderx.NewFloat(reflectx.Float32)
		if err != nil {
			b.Fatal(err)
		}
		encoded := &customEncodedHasher{hash: myHash, coder: makeEncoder(cc.Enc.Fn)}
		dedicated := &numberHasher{}
		hashbench(b, test, encoded, dedicated)
	})

	b.Run("string", func(b *testing.B) {
		tests := []interface{}{
			"I am the very model of a modern major string.",
			"this is 10",
			strings.Repeat("100 chars!", 10),   // 100
			strings.Repeat("1k chars!!", 100),  // 1000
			strings.Repeat("10k chars!", 1000), // 10000
		}
		for _, test := range tests {
			b.Run(fmt.Sprint(len(test.(string))), func(b *testing.B) {
				b.Run("native", func(b *testing.B) {
					m := make(map[string]FullValue)
					for i := 0; i < b.N; i++ {
						k := test.(string)
						value = m[k]
						m[k] = value
					}
				})

				cc, err := coderx.NewString()
				if err != nil {
					b.Fatal(err)
				}
				encoded := &customEncodedHasher{hash: myHash, coder: makeEncoder(cc.Enc.Fn)}
				dedicated := &stringHasher{hash: myHash}
				hashbench(b, test, encoded, dedicated)
			})
		}
	})
	b.Run("struct", func(b *testing.B) {
		tests := []interface{}{
			struct {
				A      int
				B      string
				Foobar [4]int
			}{A: 56, B: "stringtastic", Foobar: [4]int{4, 2, 3, 1}},
		}
		for _, test := range tests {
			typ := reflect.TypeOf(test)
			b.Run(fmt.Sprint(typ.String()), func(b *testing.B) {
				encoded := &customEncodedHasher{hash: myHash, coder: &jsonEncoder{}}
				dedicated := &rowHasher{hash: myHash, coder: MakeElementEncoder(coder.NewR(typex.New(typ)))}
				hashbench(b, test, encoded, dedicated)
			})
		}
	})
}

type jsonEncoder struct{}

func (*jsonEncoder) Encode(t reflect.Type, element interface{}) ([]byte, error) {
	return json.Marshal(element)
}

func hashbench(b *testing.B, test interface{}, encoded, dedicated elementHasher) {
	var value FullValue
	b.Run("interface", func(b *testing.B) {
		m := make(map[interface{}]FullValue)
		for i := 0; i < b.N; i++ {
			k := test
			value = m[k]
			m[k] = value
		}
	})
	b.Run("encodedHash", func(b *testing.B) {
		m := make(map[uint64]FullValue)
		for i := 0; i < b.N; i++ {
			k, err := encoded.Hash(test)
			if err != nil {
				b.Fatal(err)
			}
			value = m[k]
			m[k] = value
		}
	})
	if dedicated == nil {
		return
	}
	b.Run("dedicatedHash", func(b *testing.B) {
		m := make(map[uint64]FullValue)
		for i := 0; i < b.N; i++ {
			k, err := dedicated.Hash(test)
			if err != nil {
				b.Fatal(err)
			}
			value = m[k]
			m[k] = value
		}
	})
}
