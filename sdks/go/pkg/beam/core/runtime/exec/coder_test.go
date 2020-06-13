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
	"reflect"
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
			coder: coder.NewString(),
			val:   &FullValue{Elm: "myString"},
		}, {
			coder: func() *coder.Coder {
				c, _ := coderx.NewString()
				return &coder.Coder{Kind: coder.Custom, Custom: c, T: typex.New(reflectx.String)}
			}(),
			val: &FullValue{Elm: "myString"},
		}, {
			coder: coder.NewKV([]*coder.Coder{coder.NewVarInt(), coder.NewBool()}),
			val:   &FullValue{Elm: int64(72), Elm2: false},
		}, {
			coder: coder.NewKV([]*coder.Coder{
				coder.NewVarInt(),
				coder.NewKV([]*coder.Coder{
					coder.NewDouble(),
					coder.NewBool()})}),
			val: &FullValue{Elm: int64(42), Elm2: &FullValue{Elm: float64(3.14), Elm2: true}},
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

			compareFV(t, result, test.val)
		})
	}
}

// compareFV compares two *FullValue and fails the test with an error if an
// element is mismatched. Also performs some setup to be able to compare
// properly, and is recursive if there are nested KVs.
func compareFV(t *testing.T, got *FullValue, want *FullValue) {
	// []bytes are incomparable, convert to strings first.
	if b, ok := want.Elm.([]byte); ok {
		want.Elm = string(b)
		got.Elm = string(got.Elm.([]byte))
	}
	if b, ok := want.Elm2.([]byte); ok {
		want.Elm2 = string(b)
		got.Elm2 = string(got.Elm.([]byte))
	}

	// First check if both elements are *FV. In that case recurse to do a deep
	// comparison instead of a direct pointer comparison.
	if wantFv, ok := want.Elm.(*FullValue); ok {
		if gotFv, ok := got.Elm.(*FullValue); ok {
			compareFV(t, gotFv, wantFv)
		}
	} else if got, want := got.Elm, want.Elm; got != want {
		t.Errorf("got %v [type: %s], want %v [type %s]",
			got, reflect.TypeOf(got), wantFv, reflect.TypeOf(wantFv))
	}
	if wantFv, ok := want.Elm2.(*FullValue); ok {
		if gotFv, ok := got.Elm2.(*FullValue); ok {
			compareFV(t, gotFv, wantFv)
		}
	} else if got, want := got.Elm2, want.Elm2; got != want {
		t.Errorf("got %v [type: %s], want %v [type %s]",
			got, reflect.TypeOf(got), wantFv, reflect.TypeOf(wantFv))
	}
}
