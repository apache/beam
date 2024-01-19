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
	"io"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func makeInput(vs ...any) []MainInput {
	var ret []MainInput
	for _, v := range makeValues(vs...) {
		ret = append(ret, MainInput{Key: v})
	}
	return ret
}

func makeValues(vs ...any) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		ret = append(ret, FullValue{
			Windows:   window.SingleGlobalWindow,
			Timestamp: mtime.ZeroTimestamp,
			Elm:       v,
		})
	}
	return ret
}

func makeWindowedInput(ws []typex.Window, vs ...any) []MainInput {
	var ret []MainInput
	for _, v := range makeWindowedValues(ws, vs...) {
		ret = append(ret, MainInput{Key: v})
	}
	return ret
}

func makeWindowedValues(ws []typex.Window, vs ...any) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		ret = append(ret, FullValue{
			Windows:   ws,
			Timestamp: mtime.ZeroTimestamp,
			Elm:       v,
		})
	}
	return ret
}

func makeValuesNoWindowOrTime(vs ...any) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		ret = append(ret, FullValue{
			Elm: v,
		})
	}
	return ret
}

// makeKVValues returns a list of KV<K,V> inputs as a list of main inputs.
func makeKVInput(key any, vs ...any) []MainInput {
	var ret []MainInput
	for _, v := range makeKVValues(key, vs...) {
		ret = append(ret, MainInput{Key: v})
	}
	return ret
}

// makeKVValues returns a list of KV<K,V> inputs.
func makeKVValues(key any, vs ...any) []FullValue {
	var ret []FullValue
	for _, v := range vs {
		k := FullValue{
			Windows:   window.SingleGlobalWindow,
			Timestamp: mtime.ZeroTimestamp,
			Elm:       key,
			Elm2:      v,
		}
		ret = append(ret, k)
	}
	return ret
}

// makeKeyedInput returns a CoGBK<K, V> where the list of values are a stream
// in a single main input.
func makeKeyedInput(key any, vs ...any) []MainInput {
	k := FullValue{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       key,
	}
	return []MainInput{{
		Key:    k,
		Values: []ReStream{&FixedReStream{Buf: makeValues(vs...)}},
	}}
}

func makeKV(k, v any) []FullValue {
	return []FullValue{{
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.ZeroTimestamp,
		Elm:       k,
		Elm2:      v,
	}}
}

func extractValues(vs ...FullValue) []any {
	var ret []any
	for _, v := range vs {
		ret = append(ret, v.Elm)
	}
	return ret
}

func extractKeyedValues(vs ...FullValue) []any {
	var ret []any
	for _, v := range vs {
		ret = append(ret, v.Elm2)
	}
	return ret
}

func equalList(a, b []FullValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if !equal(v, b[i]) {
			return false
		}
	}
	return true
}

func equal(a, b FullValue) bool {
	if a.Timestamp != b.Timestamp {
		return false
	}
	if (a.Elm == nil) != (b.Elm == nil) {
		return false
	}
	if (a.Elm2 == nil) != (b.Elm2 == nil) {
		return false
	}

	if a.Elm != nil {
		if !reflect.DeepEqual(a.Elm, b.Elm) {
			return false
		}
	}
	if a.Elm2 != nil {
		if !reflect.DeepEqual(a.Elm2, b.Elm2) {
			return false
		}
	}
	if len(a.Windows) != len(b.Windows) {
		return false
	}
	for i, w := range a.Windows {
		if !w.Equals(b.Windows[i]) {
			return false
		}
	}
	return true
}

// Conversion tests.
func TestConvert(t *testing.T) {
	tests := []struct {
		name    string
		to      reflect.Type
		v, want any
	}{
		{
			name: "int_to_int",
			to:   reflectx.Int,
			v:    typex.T(42),
			want: int(42),
		},
		{
			name: "typexT_to_int",
			to:   reflectx.Int,
			v:    typex.T(42),
			want: int(42),
		},
		{
			name: "[]typexT_to_[]int",
			to:   reflect.TypeOf([]int{}),
			v:    []typex.T{1, 2, 3},
			want: []int{1, 2, 3},
		},
		{
			name: "[]typexT_to_typexX",
			to:   typex.XType,
			v:    []typex.T{1, 2, 3},
			want: []int{1, 2, 3},
		},
		{
			name: "empty_[]typexT_to_typexX",
			to:   typex.XType,
			v:    []typex.T{},
			want: []typex.T{},
		},
		{
			name: "nil_[]typexT_to_typexX",
			to:   typex.XType,
			v:    []typex.T(nil),
			want: []typex.T(nil),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if got := Convert(test.v, test.to); !reflect.DeepEqual(got, test.want) {
				t.Errorf("Convert(%v,%v) = %v,  want %v", test.v, test.to, got, test.want)
			}
		})
		t.Run("Fn_"+test.name, func(t *testing.T) {
			fn := ConvertFn(reflect.TypeOf(test.v), test.to)
			if got := fn(test.v); !reflect.DeepEqual(got, test.want) {
				t.Errorf("ConvertFn(%T, %v)(%v) = %v,  want %v", test.v, test.to, test.v, got, test.want)
			}
		})
	}
}

func TestDecodeStream(t *testing.T) {
	c := coder.NewVarInt()
	e := MakeElementEncoder(c)
	d := MakeElementDecoder(c)

	const size = 10
	setup := func() *singleUseReStream {
		r, w := io.Pipe()
		// Since the io.Pipe is blocking, run in a goroutine.
		go func() {
			for i := int64(0); i < size; i++ {
				e.Encode(&FullValue{Elm: i}, w)
			}
		}()
		return &singleUseReStream{d: d, r: r, size: int(size)}
	}

	t.Run("ReadAll", func(t *testing.T) {
		drs := setup()
		vals, err := ReadAll(drs)
		if err != nil {
			t.Fatalf("unable to ReadAll from decodeStream: %v", err)
		}
		if got, want := len(vals), int(size); got != want {
			t.Errorf("unable to ReadAll from decodeStream, got %v elements, want %v elements", got, want)
		}
		var wants []any
		for i := int64(0); i < size; i++ {
			wants = append(wants, i)
		}
		if got, want := vals, makeValuesNoWindowOrTime(wants...); !equalList(got, want) {
			t.Errorf("unable to ReadAll from decodeStream got %v, want %v", got, want)
		}
	})
	t.Run("ShortReadAndClose", func(t *testing.T) {
		const shortSize = 5
		drs := setup()
		ds, err := drs.Open()
		if err != nil {
			t.Fatalf("unable to create decodeStream: %v", err)
		}
		var vals []FullValue
		for i := 0; i < 5; i++ {
			fv, err := ds.Read()
			if err != nil {
				t.Fatalf("unexpected error on decodeStream.Read: %v", err)
			}
			vals = append(vals, *fv)
		}
		if got, want := len(vals), int(shortSize); got != want {
			t.Errorf("unable to ReadAll from decodeStream, got %v elements, want %v elements", got, want)
		}
		var wants []any
		for i := int64(0); i < shortSize; i++ {
			wants = append(wants, i)
		}
		if got, want := vals, makeValuesNoWindowOrTime(wants...); !equalList(got, want) {
			t.Errorf("unable to short read from decodeStream got %v, want %v", got, want)
		}

		// Check close Behavior.
		if err := ds.Close(); err != nil {
			t.Fatalf("unexpected error on decodeStream.Close: %v", err)
		}

		if fv, err := ds.Read(); err != io.EOF {
			t.Errorf("unexpected error on decodeStream.Read after close: %v, %v", fv, err)
		}
		// Check that next was iterated to equal size
		dds := ds.(*decodeStream)
		if got, want := dds.next, size; got != want {
			t.Errorf("unexpected configuration after decodeStream.Close: got %v, want %v", got, want)
		}

		// Check that a 2nd stream will fail:
		if s, err := drs.Open(); err == nil || s != nil {
			t.Fatalf("unexpected values for second decodeReStream.Open: %T, %v", s, err)
		}
	})
}

func TestDecodeMultiChunkStream(t *testing.T) {
	c := coder.NewVarInt()
	e := MakeElementEncoder(c)
	d := MakeElementDecoder(c)

	const size = 10
	setup := func() *singleUseMultiChunkReStream {
		r, w := io.Pipe()
		// Since the io.Pipe is blocking, run in a goroutine.
		go func() {
			coder.EncodeVarInt(size, w)
			for i := int64(0); i < size; i++ {
				e.Encode(&FullValue{Elm: i}, w)
			}
			coder.EncodeVarInt(0, w)
		}()
		var byteCount int
		bcr := &byteCountReader{reader: r, count: &byteCount}
		return &singleUseMultiChunkReStream{d: d, r: bcr}
	}

	t.Run("ReadAll", func(t *testing.T) {
		drs := setup()
		vals, err := ReadAll(drs)
		if err != nil {
			t.Fatalf("unable to ReadAll from decodeStream: %v", err)
		}
		if got, want := len(vals), int(size); got != want {
			t.Errorf("unable to ReadAll from decodeStream, got %v elements, want %v elements", got, want)
		}
		var wants []any
		for i := int64(0); i < size; i++ {
			wants = append(wants, i)
		}
		if got, want := vals, makeValuesNoWindowOrTime(wants...); !equalList(got, want) {
			t.Errorf("unable to ReadAll from decodeStream got %v, want %v", got, want)
		}
	})
	t.Run("ShortReadAndClose", func(t *testing.T) {
		const shortSize = 5
		drs := setup()
		ds, err := drs.Open()
		if err != nil {
			t.Fatalf("unable to create decodeStream: %v", err)
		}
		var vals []FullValue
		for i := 0; i < 5; i++ {
			fv, err := ds.Read()
			if err != nil {
				t.Fatalf("unexpected error on decodeStream.Read: %v", err)
			}
			vals = append(vals, *fv)
		}
		if got, want := len(vals), int(shortSize); got != want {
			t.Errorf("unable to ReadAll from decodeStream, got %v elements, want %v elements", got, want)
		}
		var wants []any
		for i := int64(0); i < shortSize; i++ {
			wants = append(wants, i)
		}
		if got, want := vals, makeValuesNoWindowOrTime(wants...); !equalList(got, want) {
			t.Errorf("unable to short read from decodeStream got %v, want %v", got, want)
		}

		// Check close Behavior.
		if err := ds.Close(); err != nil {
			t.Fatalf("unexpected error on decodeStream.Close: %v", err)
		}

		if fv, err := ds.Read(); err != io.EOF {
			t.Errorf("unexpected error on decodeStream.Read after close: %v, %v", fv, err)
		}
		// Check that next was iterated to an empty stream.
		dds := ds.(*decodeMultiChunkStream)
		if got, want := dds.next, int64(0); got != want {
			t.Errorf("unexpected configuration after decodeStream.Close: got %v, want %v", got, want)
		}
		if dds.stream != nil {
			t.Errorf("got non-nil stream after close: %#v", dds.stream)
		}

		// Check that a 2nd stream will fail:
		if s, err := drs.Open(); err == nil || s != nil {
			t.Fatalf("unexpected values for second decodeReStream.Open: %T, %v", s, err)
		}
	})
}
