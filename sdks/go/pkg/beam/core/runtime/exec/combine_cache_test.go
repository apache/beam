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
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type cacheRowKey struct {
	Value   int64
	Payload string
}

func encodedCacheKeys(t testing.TB) []struct {
	name  string
	coder *coder.Coder
	key   func(uint64) any
} {
	t.Helper()
	cc, err := coder.NewCustomCoder("codable", myCodableType, codableEncoder, codableDecoder)
	if err != nil {
		t.Fatal(err)
	}
	row := coder.NewR(typex.New(reflect.TypeOf(cacheRowKey{})))
	custom := &coder.Coder{Kind: coder.Custom, T: typex.New(myCodableType), Custom: cc}
	return []struct {
		name  string
		coder *coder.Coder
		key   func(uint64) any
	}{
		{"row", row, func(n uint64) any { return cacheRowKey{Value: int64(n)} }},
		{"row_lp", &coder.Coder{Kind: coder.LP, Components: []*coder.Coder{row}}, func(n uint64) any { return cacheRowKey{Value: int64(n)} }},
		{"custom", custom, func(n uint64) any { return &myCodable{val: n} }},
		{"custom_lp", &coder.Coder{Kind: coder.LP, Components: []*coder.Coder{custom}}, func(n uint64) any { return &myCodable{val: n} }},
	}
}

type cacheCountingElementEncoder struct {
	ElementEncoder
	calls *int
}

func (e *cacheCountingElementEncoder) Encode(fv *FullValue, w io.Writer) error {
	*e.calls++
	return e.ElementEncoder.Encode(fv, w)
}

type cacheCountingCustomEncoder struct {
	Encoder
	calls *int
}

func (e *cacheCountingCustomEncoder) Encode(t reflect.Type, value any) ([]byte, error) {
	*e.calls++
	return e.Encoder.Encode(t, value)
}

func TestLiftingCacheReusesKeyEncoding(t *testing.T) {
	for _, tc := range encodedCacheKeys(t) {
		t.Run(tc.name, func(t *testing.T) {
			c := newLiftingCache(10, tc.coder, coder.NewGlobalWindow())
			c.start()
			calls := 0
			c.keyCoder = &cacheCountingElementEncoder{ElementEncoder: c.keyCoder, calls: &calls}
			switch h := c.keyHash.(type) {
			case *rowHasher:
				h.coder = &cacheCountingElementEncoder{ElementEncoder: h.coder, calls: &calls}
			case *customEncodedHasher:
				h.coder = &cacheCountingCustomEncoder{Encoder: h.coder, calls: &calls}
			}
			w := window.SingleGlobalWindow[0]
			_, fv, _, err := c.lookup(&FullValue{Elm: tc.key(42)}, w)
			if err != nil {
				t.Fatal(err)
			}
			*fv = FullValue{Elm: tc.key(42), Windows: []typex.Window{w}}
			calls = 0
			_, _, found, err := c.lookup(&FullValue{Elm: tc.key(42)}, w)
			if err != nil || !found {
				t.Fatalf("lookup existing key: found=%v, err=%v", found, err)
			}
			// Encode the input once for both hashing and equality, and the
			// cached key once for equality. Do not retain another key copy.
			if calls != 2 {
				t.Errorf("key encodings on cache hit = %d, want 2", calls)
			}
		})
	}
}

type cacheCollisionHash struct{}

func (cacheCollisionHash) Write(p []byte) (int, error) { return len(p), nil }
func (cacheCollisionHash) Sum(b []byte) []byte         { return append(b, make([]byte, 8)...) }
func (cacheCollisionHash) Reset()                      {}
func (cacheCollisionHash) Size() int                   { return 8 }
func (cacheCollisionHash) BlockSize() int              { return 1 }
func (cacheCollisionHash) Sum64() uint64               { return 0 }

func TestLiftingCacheEncodedKeyCollisions(t *testing.T) {
	for _, tc := range encodedCacheKeys(t) {
		t.Run(tc.name, func(t *testing.T) {
			c := newLiftingCache(10, tc.coder, coder.NewIntervalWindow())
			switch h := c.keyHash.(type) {
			case *rowHasher:
				h.hash = cacheCollisionHash{}
			case *customEncodedHasher:
				h.hash = cacheCollisionHash{}
			}
			c.start()
			windows := []typex.Window{
				window.IntervalWindow{Start: 0, End: 10},
				window.IntervalWindow{Start: 10, End: 20},
			}
			for i := 0; i < 6; i++ {
				w := windows[i%2]
				key := tc.key(uint64(i / 2))
				_, fv, found, err := c.lookup(&FullValue{Elm: key}, w)
				if err != nil || found {
					t.Fatalf("insert %d: found=%v, err=%v", i, found, err)
				}
				*fv = FullValue{Elm: key, Elm2: i, Windows: []typex.Window{w}}
			}
			for i := 5; i >= 0; i-- {
				_, fv, found, err := c.lookup(&FullValue{Elm: tc.key(uint64(i / 2))}, windows[i%2])
				if err != nil || !found || fv.Elm2 != i {
					t.Fatalf("lookup %d: value=%v, found=%v, err=%v", i, fv, found, err)
				}
			}
		})
	}
}

type cacheFailingEncoder struct {
	ElementEncoder
	remaining int
	err       error
}

func (e *cacheFailingEncoder) Encode(fv *FullValue, w io.Writer) error {
	if e.remaining == 0 {
		return e.err
	}
	e.remaining--
	return e.ElementEncoder.Encode(fv, w)
}

func TestLiftingCacheEncodingFailure(t *testing.T) {
	for _, tc := range encodedCacheKeys(t) {
		for _, successfulEncodings := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/after_%d", tc.name, successfulEncodings), func(t *testing.T) {
				c := newLiftingCache(10, tc.coder, coder.NewGlobalWindow())
				c.start()
				w := window.SingleGlobalWindow[0]
				_, fv, _, err := c.lookup(&FullValue{Elm: tc.key(42)}, w)
				if err != nil {
					t.Fatal(err)
				}
				*fv = FullValue{Elm: tc.key(42), Elm2: "retained", Windows: []typex.Window{w}}
				encoder := c.keyCoder
				want := fmt.Errorf("injected encoding failure")
				c.keyCoder = &cacheFailingEncoder{ElementEncoder: encoder, remaining: successfulEncodings, err: want}
				if _, _, _, err := c.lookup(&FullValue{Elm: tc.key(42)}, w); err != want {
					t.Fatalf("lookup error = %v, want %v", err, want)
				}
				c.keyCoder = encoder
				_, fv, found, err := c.lookup(&FullValue{Elm: tc.key(42)}, w)
				if err != nil || !found || fv.Elm2 != "retained" {
					t.Fatalf("lookup after failure: value=%v, found=%v, err=%v", fv, found, err)
				}
			})
		}
	}
}

func BenchmarkLiftingCacheLookup(b *testing.B) {
	for _, tc := range encodedCacheKeys(b) {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkCacheHit(b, tc.coder, tc.key(42))
		})
	}
	row := coder.NewR(typex.New(reflect.TypeOf(cacheRowKey{})))
	for _, size := range []int{128, 1024} {
		b.Run(fmt.Sprintf("row_%d", size), func(b *testing.B) {
			benchmarkCacheHit(b, row, cacheRowKey{Value: 42, Payload: strings.Repeat("x", size)})
		})
	}
	for _, tc := range []struct {
		name  string
		coder *coder.Coder
		key   any
	}{
		{"int", coder.NewVarInt(), int64(42)},
		{"string", coder.NewString(), strings.Repeat("x", 128)},
		{"bytes", coder.NewBytes(), []byte(strings.Repeat("x", 128))},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkCacheHit(b, tc.coder, tc.key)
		})
	}
}

func benchmarkCacheHit(b *testing.B, kc *coder.Coder, key any) {
	c := newLiftingCache(10, kc, coder.NewGlobalWindow())
	c.start()
	w := window.SingleGlobalWindow[0]
	input := &FullValue{Elm: key}
	_, fv, _, err := c.lookup(input, w)
	if err != nil {
		b.Fatal(err)
	}
	*fv = FullValue{Elm: key, Windows: []typex.Window{w}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, found, err := c.lookup(input, w); err != nil || !found {
			b.Fatalf("lookup: found=%v, err=%v", found, err)
		}
	}
}

func BenchmarkLiftingCacheMiss(b *testing.B) {
	for _, tc := range encodedCacheKeys(b) {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkCacheMiss(b, tc.coder, tc.key)
		})
	}
	b.Run("int", func(b *testing.B) {
		benchmarkCacheMiss(b, coder.NewVarInt(), func(n uint64) any { return int64(n) })
	})
}

func benchmarkCacheMiss(b *testing.B, kc *coder.Coder, key func(uint64) any) {
	c := newLiftingCache(2000, kc, coder.NewGlobalWindow())
	c.start()
	w := window.SingleGlobalWindow[0]
	var input FullValue
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2000 == 0 {
			clear(c.cache)
		}
		input.Elm = key(uint64(i))
		_, fv, found, err := c.lookup(&input, w)
		if err != nil || found {
			b.Fatalf("lookup: found=%v, err=%v", found, err)
		}
		*fv = FullValue{Elm: input.Elm, Windows: []typex.Window{w}}
	}
}

func TestLiftingCacheLPPartialEncodingFailure(t *testing.T) {
	row := coder.NewR(typex.New(reflect.TypeOf(cacheRowKey{})))
	c := newLiftingCache(10, &coder.Coder{Kind: coder.LP, Components: []*coder.Coder{row}}, coder.NewGlobalWindow())
	c.start()
	w := window.SingleGlobalWindow[0]
	input := &FullValue{Elm: cacheRowKey{Value: 42}}
	_, fv, _, err := c.lookup(input, w)
	if err != nil {
		t.Fatal(err)
	}
	*fv = FullValue{Elm: input.Elm, Elm2: "retained", Windows: []typex.Window{w}}

	lp := c.keyCoder.(*lpEncoder)
	inner := lp.enc.(*rowEncoder)
	want := fmt.Errorf("injected partial row encoding failure")
	failNext := true
	lp.enc = &rowEncoder{enc: func(value any, out io.Writer) error {
		if failNext {
			failNext = false
			if _, err := out.Write([]byte{0xff}); err != nil {
				return err
			}
			return want
		}
		return inner.enc(value, out)
	}}
	if _, _, _, err := c.lookup(input, w); err != want {
		t.Fatalf("lookup error = %v, want %v", err, want)
	}
	_, fv, found, err := c.lookup(input, w)
	if err != nil || !found || fv.Elm2 != "retained" {
		t.Fatalf("lookup after partial encoding failure: value=%v, found=%v, err=%v", fv, found, err)
	}
}
