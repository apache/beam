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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/coderx"
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
			coder: &coder.Coder{Kind: coder.LP, Components: []*coder.Coder{coder.NewString()}},
			val:   &FullValue{Elm: "myString"},
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
		}, {
			coder: &coder.Coder{Kind: coder.Window, Window: coder.NewGlobalWindow()},
			val:   &FullValue{Windows: []typex.Window{window.GlobalWindow{}}},
		}, {
			coder: &coder.Coder{Kind: coder.Window, Window: coder.NewIntervalWindow()},
			val:   &FullValue{Windows: []typex.Window{window.IntervalWindow{Start: 0, End: 100}}},
		}, {
			coder: coder.NewW(coder.NewVarInt(), coder.NewGlobalWindow()),
			val:   &FullValue{Elm: int64(13), Windows: []typex.Window{window.GlobalWindow{}}},
		}, {
			coder: coder.NewW(coder.NewVarInt(), coder.NewIntervalWindow()),
			val:   &FullValue{Elm: int64(13), Windows: []typex.Window{window.IntervalWindow{Start: 0, End: 100}, window.IntervalWindow{Start: 50, End: 150}}},
		}, {
			coder: coder.NewPW(coder.NewString(), coder.NewGlobalWindow()),
			val:   &FullValue{Elm: "myString" /*Windowing info isn't encoded for PW so we can omit it here*/},
		}, {
			coder: coder.NewN(coder.NewBytes()),
			val:   &FullValue{},
		}, {
			coder: coder.NewN(coder.NewBytes()),
			val:   &FullValue{Elm: []byte("myBytes")},
		}, {
			coder: coder.NewIntervalWindowCoder(),
			val:   &FullValue{Elm: window.IntervalWindow{Start: 0, End: 100}},
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

	// Check if the desired FV has windowing information
	if want.Windows != nil {
		if gotLen, wantLen := len(got.Windows), len(want.Windows); gotLen != wantLen {
			t.Fatalf("got %d windows in FV, want %v", gotLen, wantLen)
		}
		for i := range want.Windows {
			if gotWin, wantWin := got.Windows[i], want.Windows[i]; !wantWin.Equals(gotWin) {
				t.Errorf("got window %v at position %d, want %v", gotWin, i, wantWin)
			}
		}
	}
}

func TestIterableCoder(t *testing.T) {
	cod := coder.NewI(coder.NewVarInt())
	wantVals := []int64{8, 24, 72}
	val := &FullValue{Elm: wantVals}

	var buf bytes.Buffer
	enc := MakeElementEncoder(cod)
	if err := enc.Encode(val, &buf); err != nil {
		t.Fatalf("Couldn't encode value: %v", err)
	}

	dec := MakeElementDecoder(cod)
	result, err := dec.Decode(&buf)
	if err != nil {
		t.Fatalf("Couldn't decode value: %v", err)
	}

	gotVals, ok := result.Elm.([]int64)
	if !ok {
		t.Fatalf("got output element %v, want []int64", result.Elm)
	}

	if got, want := len(gotVals), len(wantVals); got != want {
		t.Errorf("got %d elements in iterable, want %d", got, want)
	}

	for i := range gotVals {
		if got, want := gotVals[i], wantVals[i]; got != want {
			t.Errorf("got %d at position %d, want %d", got, i, want)
		}
	}
}

type namedTypeForTest struct {
	A, B int64
	C    string
}

func TestRowCoder(t *testing.T) {
	var buf bytes.Buffer
	rCoder := coder.NewR(typex.New(reflect.TypeOf((*namedTypeForTest)(nil))))
	wantStruct := &namedTypeForTest{A: int64(8), B: int64(24), C: "myString"}
	wantVal := &FullValue{Elm: wantStruct}

	enc := MakeElementEncoder(rCoder)
	if err := enc.Encode(wantVal, &buf); err != nil {
		t.Fatalf("Couldn't encode value: %v", err)
	}

	dec := MakeElementDecoder(rCoder)
	result, err := dec.Decode(&buf)
	if err != nil {
		t.Fatalf("Couldn't decode value: %v", err)
	}
	gotPtr, ok := result.Elm.(*namedTypeForTest)
	gotStruct := *gotPtr
	if !ok {
		t.Fatalf("got %v, want namedTypeForTest struct", result.Elm)
	}
	if got, want := gotStruct.A, wantStruct.A; got != want {
		t.Errorf("got A field value %d, want %d", got, want)
	}
	if got, want := gotStruct.B, wantStruct.B; got != want {
		t.Errorf("got B field value %d, want %d", got, want)
	}
	if got, want := gotStruct.C, wantStruct.C; got != want {
		t.Errorf("got C field value %v, want %v", got, want)
	}
}

func TestPaneCoder(t *testing.T) {
	pn := coder.NewPane(0x04)
	val := &FullValue{Pane: pn}
	cod := &coder.Coder{Kind: coder.PaneInfo}

	var buf bytes.Buffer
	enc := MakeElementEncoder(cod)
	if err := enc.Encode(val, &buf); err != nil {
		t.Fatalf("Couldn't encode value: %v", err)
	}

	dec := MakeElementDecoder(cod)
	result, err := dec.Decode(&buf)
	if err != nil {
		t.Fatalf("Couldn't decode value: %v", err)
	}

	if got, want := result.Pane.Timing, pn.Timing; got != want {
		t.Errorf("got pane timing %v, want %v", got, want)
	}
	if got, want := result.Pane.IsFirst, pn.IsFirst; got != want {
		t.Errorf("got IsFirst %v, want %v", got, want)
	}
	if got, want := result.Pane.IsLast, pn.IsLast; got != want {
		t.Errorf("got IsLast %v, want %v", got, want)
	}
	if got, want := result.Pane.Index, pn.Index; got != want {
		t.Errorf("got pane index %v, want %v", got, want)
	}
	if got, want := result.Pane.NonSpeculativeIndex, pn.NonSpeculativeIndex; got != want {
		t.Errorf("got pane non-speculative index %v, want %v", got, want)
	}
}
