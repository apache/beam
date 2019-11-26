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
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/coderx"
)

func TestCoders(t *testing.T) {
	for _, test := range []struct {
		coder *coder.Coder
		val   *FullValue
	}{
		{
			coder: coder.NewBool(),
			val:   &FullValue{Elm: true},
		}, {
			coder: coder.NewBytes(),
			val:   &FullValue{Elm: []byte("myBytes")},
		}, {
			coder: coder.NewVarInt(),
			val:   &FullValue{Elm: int64(65)},
		}, {
			coder: coder.NewDouble(),
			val:   &FullValue{Elm: float64(12.9)},
		}, {
			coder: func() *coder.Coder {
				c, _ := coderx.NewString()
				return &coder.Coder{Kind: coder.Custom, Custom: c, T: typex.New(reflectx.String)}
			}(),
			val: &FullValue{Elm: "myString"},
		}, {
			coder: coder.NewKV([]*coder.Coder{coder.NewVarInt(), coder.NewBool()}),
			val:   &FullValue{Elm: int64(72), Elm2: false},
		},
	} {
		t.Run(fmt.Sprintf("%v", test.coder), func(t *testing.T) {
			var buf bytes.Buffer
			enc := MakeElementEncoder(test.coder)
			if err := enc.Encode(test.val, &buf); err != nil {
				t.Fatalf("Couldn't encode value: %v", err)
			}

			dec := MakeElementDecoder(test.coder)
			result, err := dec.Decode(&buf)
			if err != nil {
				t.Fatalf("Couldn't decode value: %v", err)
			}
			// []bytes are incomparable, convert to strings first.
			if b, ok := test.val.Elm.([]byte); ok {
				test.val.Elm = string(b)
				result.Elm = string(result.Elm.([]byte))
			}
			if got, want := result.Elm, test.val.Elm; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := result.Elm2, test.val.Elm2; got != want {
				t.Errorf("got %v, want %v", got, want)
			}

		})
	}
}
