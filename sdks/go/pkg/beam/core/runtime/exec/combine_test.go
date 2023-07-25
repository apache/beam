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
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

var intInput = []any{int(1), int(2), int(3), int(4), int(5), int(6)}
var int64Input = []any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)}
var strInput = []any{"1", "2", "3", "4", "5", "6"}

var tests = []struct {
	Fn         any
	AccumCoder *coder.Coder
	Input      []any
	Expected   any
}{
	{Fn: mergeFn, AccumCoder: intCoder(reflectx.Int), Input: intInput, Expected: int(21)},
	{Fn: nonBinaryMergeFn, AccumCoder: intCoder(reflectx.Int), Input: intInput, Expected: int(21)},
	{Fn: &MyCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int(21)},
	{Fn: &MyOtherCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: "21"},
	{Fn: &MyThirdCombine{}, AccumCoder: intCoder(reflectx.Int), Input: strInput, Expected: int(21)},
	{Fn: &MyContextCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int(21)},
	{Fn: &MyErrorCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int(21)},
}

func fnName(x any) string {
	v := reflect.ValueOf(x)
	if v.Kind() != reflect.Func {
		return v.Type().String()
	}
	return runtime.FuncForPC(uintptr(v.Pointer())).Name()
}

// TestCombine verifies that the Combine node works correctly.
func TestCombine(t *testing.T) {
	for _, test := range tests {
		t.Run(fnName(test.Fn), func(t *testing.T) {
			edge := getCombineEdge(t, test.Fn, reflectx.Int, test.AccumCoder)

			out := &CaptureNode{UID: 1}
			combine := &Combine{UID: 2, Fn: edge.CombineFn, Out: out}
			n := &FixedRoot{UID: 3, Elements: makeKeyedInput(42, test.Input...), Out: combine}

			constructAndExecutePlan(t, []Unit{n, combine, out})

			expected := makeKV(42, test.Expected)
			if !equalList(out.Elements, expected) {
				t.Errorf("combine(%s) = %v, want %v", edge.CombineFn.Name(), extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
			}
		})
	}
}

// TestLiftedCombine verifies that the LiftedCombine, MergeAccumulators, and
// ExtractOutput nodes work correctly after the lift has been performed.
func TestLiftedCombine(t *testing.T) {
	withCoder := func(t *testing.T, suffix string, key any, keyCoder *coder.Coder) {
		// The test values are all single global window.
		wc := coder.NewGlobalWindow()
		for _, test := range tests {
			t.Run(fnName(test.Fn)+"_"+suffix, func(t *testing.T) {
				edge := getCombineEdge(t, test.Fn, reflectx.Int, test.AccumCoder)

				out := &CaptureNode{UID: 1}
				extract := &ExtractOutput{Combine: &Combine{UID: 2, Fn: edge.CombineFn, Out: out}}
				merge := &MergeAccumulators{Combine: &Combine{UID: 3, Fn: edge.CombineFn, Out: extract}}
				gbk := &simpleGBK{UID: 4, KeyCoder: keyCoder, WindowCoder: wc, Out: merge}
				precombine := &LiftedCombine{Combine: &Combine{UID: 5, Fn: edge.CombineFn, Out: gbk}, KeyCoder: keyCoder, WindowCoder: wc}
				n := &FixedRoot{UID: 6, Elements: makeKVInput(key, test.Input...), Out: precombine}

				constructAndExecutePlan(t, []Unit{n, precombine, gbk, merge, extract, out})
				expected := makeKV(key, test.Expected)
				if !equalList(out.Elements, expected) {
					t.Errorf("liftedCombineChain(%s) = %v, want %v", edge.CombineFn.Name(), extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
				}
			})
		}
	}
	withCoder(t, "intKeys", 42, intCoder(reflectx.Int))
	withCoder(t, "int64Keys", int64(42), intCoder(reflectx.Int64))

	cc, err := coder.NewCustomCoder("codable", myCodableType, codableEncoder, codableDecoder)
	if err != nil {
		t.Fatalf("%v", err)
	}
	withCoder(t, "pointerKeys", &myCodable{42}, &coder.Coder{Kind: coder.Custom, T: typex.New(myCodableType), Custom: cc})

}

// pigeonHasher only returns 0 for even hashes, and 1 for odd hashes
// nearly guaranteeing that overflow behavior must be tested for small sets.
type pigeonHasher struct {
	hasher elementHasher
}

func (p *pigeonHasher) Hash(element any, w typex.Window) (uint64, error) {
	k, err := p.hasher.Hash(element, w)
	if err != nil {
		return 0, err
	}
	return k & 0x1, nil
}

func TestLiftingCache(t *testing.T) {
	zeroVal, zeroWindow := &FullValue{Elm: 0}, window.SingleGlobalWindow[0]
	t.Run("lookup", func(t *testing.T) {
		c := newLiftingCache(1, intCoder(reflectx.Int), coder.NewGlobalWindow())
		// Replace with pigeon hasher to ensure pigeonholing.
		c.keyHash = &pigeonHasher{hasher: c.keyHash}
		c.start()

		zerokey, fv, notfirst, err := c.lookup(zeroVal, zeroWindow)
		if err != nil {
			t.Fatalf("error on liftingCache.lookup(0, GW) = %v", err)
		}

		// Check properties of the cache
		if got, want := len(c.cache), 1; got != want {
			t.Fatalf("len(liftingCache.cache) = %v, want %v after first lookup", got, want)
		}
		if notfirst {
			t.Fatalf("liftingCache.lookup(0, GW) = key(%v), %v, notfirst(%v) but want first value for key", zerokey, fv, notfirst)
		}
		// Set value to ensure we get the same one we expect later.
		// the cache expects the value to have the key (Elm) and
		// a window set.
		fv.Elm = 0
		fv.Elm2 = "want"
		fv.Windows = window.SingleGlobalWindow

		key, fv2, notfirst, err := c.lookup(zeroVal, zeroWindow)
		if err != nil {
			t.Fatalf("error on liftingCache.lookup(0, GW) = %v", err)
		}
		if got, want := len(c.cache), 1; got != want {
			t.Fatalf("len(liftingCache.cache) = %v, want %v after lookup of existing key", got, want)
		}
		if got, want := key, zerokey; got != want {
			t.Fatalf("liftingCache.lookup(0, GW) = key(%v), want %v", got, want)
		}
		// This shouldn't be the first value.
		if !notfirst {
			t.Fatalf("liftingCache.lookup(0, GW) = key(%v), %v, notfirst(%v) but want existing value for key", key, fv2, notfirst)
		}
		if got, want := fv2.Elm2, fv.Elm2; got != want {
			t.Fatalf("liftingCache.lookup(0, GW) = %v, want %v", got, want)
		}

		// Now the hard part: We lookup new values until we pigeon hole & get a collision.
		// This will validates overflow lookups.
		var collision *FullValue
		for i := 1; i < 100; i++ {
			collision = &FullValue{Elm: i}
			key, fv2, notfirst, err = c.lookup(collision, zeroWindow)
			if err != nil {
				t.Fatalf("error on liftingCache.lookup(%v, GW) = %v", i, err)
			}
			if key == zerokey {
				break
			}
			// Set values so subsequent hits won't fail.
			fv2.Elm = collision.Elm
			fv2.Elm2 = "ignoreme"
			fv2.Windows = window.SingleGlobalWindow
		}
		if notfirst {
			t.Errorf("liftingCache.lookup(%v, GW) = key(%v), %v, notfirst(%v) but want first value for key", collision.Elm, key, fv2, notfirst)
		}
		// Set values so we can validate later.
		fv2.Elm = collision.Elm
		fv2.Elm2 = "newthing"
		fv2.Windows = window.SingleGlobalWindow

		key, fv2, notfirst, err = c.lookup(collision, zeroWindow)
		if err != nil {
			t.Fatalf("error on liftingCache.lookup(%v, GW) = %v", collision.Elm, err)
		}
		if !notfirst {
			t.Errorf("liftingCache.lookup(%v, GW) = key(%v), %v, notfirst(%v) but want first value for key", collision.Elm, key, fv2, notfirst)
		}
		if got, want := fv2.Elm2, "newthing"; got != want {
			t.Fatalf("liftingCache.lookup(%v, GW) = %v, want %v", collision.Elm, got, want)
		}

		// Check the original key again.
		key, fv2, notfirst, err = c.lookup(zeroVal, zeroWindow)
		if err != nil {
			t.Fatalf("error on liftingCache.lookup(0, GW) = %v", err)
		}
		if got, want := key, zerokey; got != want {
			t.Fatalf("liftingCache.lookup(0, GW) = key(%v), want %v", got, want)
		}
		if !notfirst {
			t.Fatalf("liftingCache.lookup(0, GW) = key(%v), %v, notfirst(%v) but want existing value for key", key, fv2, notfirst)
		}
		if got, want := fv2.Elm2, "want"; got != want {
			t.Fatalf("liftingCache.lookup(0, GW) = %v, want %v", got, want)
		}
	})
	load := func(c *liftingCache, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			_, fv, _, err := c.lookup(&FullValue{Elm: i}, zeroWindow)
			if err != nil {
				t.Fatalf("error on liftingCache.lookup(%v, GW) = %v", i, err)
			}
			// Set values so subsequent hits won't fail.
			fv.Elm = i
			fv.Elm2 = "ignoreme"
			fv.Windows = window.SingleGlobalWindow
		}
	}
	t.Run("compact_sparse", func(t *testing.T) {
		max := 10
		c := newLiftingCache(max, intCoder(reflectx.Int), coder.NewGlobalWindow())
		c.start()
		load(c, 100)
		zerokey, _, _, err := c.lookup(zeroVal, zeroWindow)
		if err != nil {
			t.Fatalf("error on liftingCache.lookup(%v, GW) = %v", 0, err)
		}
		c.compact(context.Background(), zerokey, func(ctx context.Context, elm *FullValue, values ...ReStream) error { return nil })
		if got := len(c.cache); got >= max {
			t.Errorf("len(c.cache) = %v, want < %v", got, max)
		}
		// Validate that "current" key wasn't removed.
		key, fv, notfirst, err := c.lookup(zeroVal, zeroWindow)
		if err != nil {
			t.Fatalf("error on liftingCache.lookup(%v, GW) = %v", 0, err)
		}
		if !notfirst {
			t.Fatalf("liftingCache.lookup(0, GW) = key(%v), %v, notfirst(%v) but want existing value for key", key, fv, notfirst)
		}
		if got, want := fv.Elm2, "ignoreme"; got != want {
			t.Fatalf("liftingCache.lookup(0, GW) = %v, want %v", got, want)
		}
	})
	t.Run("compact_dense", func(t *testing.T) {
		max := 1
		c := newLiftingCache(max, intCoder(reflectx.Int), coder.NewGlobalWindow())
		// Replace with pigeon hasher to ensure pigeonholing.
		c.keyHash = &pigeonHasher{hasher: c.keyHash}
		c.start()
		load(c, 100)
		zerokey, _, _, err := c.lookup(zeroVal, zeroWindow)
		if err != nil {
			t.Fatalf("error on liftingCache.lookup(%v, GW) = %v", 0, err)
		}
		c.compact(context.Background(), zerokey, func(ctx context.Context, elm *FullValue, values ...ReStream) error { return nil })
		if got, want := len(c.cache), 1; got != want {
			t.Errorf("len(c.cache) = %v, want %v", got, want)
		}
		key, fv, notfirst, err := c.lookup(zeroVal, zeroWindow)
		if err != nil {
			t.Fatalf("error on liftingCache.lookup(%v, GW) = %v", 0, err)
		}
		if !notfirst {
			t.Fatalf("liftingCache.lookup(0, GW) = key(%v), %v, notfirst(%v) but want existing value for key", key, fv, notfirst)
		}
		if got, want := fv.Elm2, "ignoreme"; got != want {
			t.Fatalf("liftingCache.lookup(0, GW) = %v, want %v", got, want)
		}
	})
	t.Run("emitAll", func(t *testing.T) {
		c := newLiftingCache(1, intCoder(reflectx.Int), coder.NewGlobalWindow())
		// Replace with pigeon hasher to ensure pigeonholing.
		c.keyHash = &pigeonHasher{hasher: c.keyHash}
		c.start()
		want := 100
		load(c, want)
		var got int
		c.emitAll(context.Background(), func(ctx context.Context, elm *FullValue, values ...ReStream) error {
			got++
			return nil
		})
		if got != want {
			t.Errorf("c.emitAll emitted %v values, want %v", got, want)
		}
		if got, want := len(c.cache), 0; got != want {
			t.Errorf("len(c.cache) = %v, want %v", got, want)
		}

	})
}

// TestConvertToAccumulators verifies that the ConvertToAccumulators phase
// correctly doesn't accumulate values at all.
func TestConvertToAccumulators(t *testing.T) {
	tests := []struct {
		Fn         any
		AccumCoder *coder.Coder
		Input      []any
		Expected   []any
	}{
		{Fn: mergeFn, AccumCoder: intCoder(reflectx.Int), Input: intInput, Expected: intInput},
		{Fn: nonBinaryMergeFn, AccumCoder: intCoder(reflectx.Int), Input: intInput, Expected: intInput},
		{Fn: &MyCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int64Input},
		{Fn: &MyOtherCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int64Input},
		{Fn: &MyThirdCombine{}, AccumCoder: intCoder(reflectx.Int), Input: strInput, Expected: intInput},
		{Fn: &MyContextCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int64Input},
		{Fn: &MyErrorCombine{}, AccumCoder: intCoder(reflectx.Int64), Input: intInput, Expected: int64Input},
	}
	for _, test := range tests {
		t.Run(fnName(test.Fn), func(t *testing.T) {
			edge := getCombineEdge(t, test.Fn, reflectx.Int, test.AccumCoder)

			testKey := 42
			out := &CaptureNode{UID: 1}
			convertToAccumulators := &ConvertToAccumulators{Combine: &Combine{UID: 2, Fn: edge.CombineFn, Out: out}}
			n := &FixedRoot{UID: 3, Elements: makeKVInput(testKey, test.Input...), Out: convertToAccumulators}

			constructAndExecutePlan(t, []Unit{n, convertToAccumulators, out})

			expected := makeKVValues(testKey, test.Expected...)
			if !equalList(out.Elements, expected) {
				t.Errorf("convertToAccumulators(%s) = %#v, want %#v", edge.CombineFn.Name(), extractKeyedValues(out.Elements...), extractKeyedValues(expected...))
			}
		})
	}
}

type codable interface {
	EncodeMe() []byte
	DecodeMe([]byte)
}

func codableEncoder(v codable) []byte {
	return v.EncodeMe()
}

var myCodableType = reflect.TypeOf((*myCodable)(nil))

func codableDecoder(t reflect.Type, b []byte) codable {
	var v codable
	switch t {
	case myCodableType:
		v = &myCodable{}
	default:
		panic("don't know this type" + t.String())
	}
	v.DecodeMe(b)
	return v
}

type myCodable struct {
	val uint64
}

func (c *myCodable) EncodeMe() []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, c.val)
	return data
}

func (c *myCodable) DecodeMe(b []byte) {
	c.val = binary.LittleEndian.Uint64(b)
}

func getCombineEdge(t *testing.T, cfn any, kt reflect.Type, ac *coder.Coder) *graph.MultiEdge {
	t.Helper()
	fn, err := graph.NewCombineFn(cfn)
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	g := graph.New()
	var vtype reflect.Type
	if fn.AddInputFn() != nil {
		// This makes the assumption that the AddInput function is unkeyed.
		vtype = fn.AddInputFn().Param[1].T
	} else {
		vtype = fn.MergeAccumulatorsFn().Param[1].T
	}
	inT := typex.NewCoGBK(typex.New(kt), typex.New(vtype))
	in := g.NewNode(inT, window.DefaultWindowingStrategy(), true)

	edge, err := graph.NewCombine(g, g.Root(), fn, in, ac, nil)
	if err != nil {
		t.Fatalf("invalid combinefn: %v", err)
	}
	return edge
}

func constructAndExecutePlan(t *testing.T, us []Unit) {
	p, err := NewPlan("a", us)
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}
}

// The following functions and struct represent the combine contract implemented
// in various ways.
//
// Instead of a ProcessElement method, a liftable combine can be written by
// breaking down an operation into four component methods:
//
//   CreateAccumulator() AccumT
//   AddInput(a AccumT, v InputT) AccumT
//   MergeAccumulators(a, b AccumT) AccumT
//   ExtractOutput(v AccumT) OutputT
//
// In addition, there can be three distinct types, depending on where
// they are used in the combine, the input type, InputT, the output type, OutputT,
// and the accumulator type AccumT. Depending on the equality of the types, one
// or more of the methods can be unspecified.
//
// The only required method is MergeAccumulators.
//
// When InputT == OutputT == AccumT, the only required method is MergeAccumulators.
//
// When InputT == AccumT , then AddInput can be omitted, and MergeAccumulators used instead.
// When AccumT == Output , then ExtractOutput can be omitted, and the identity used instead.
//
// These two combined mean that when InputT == OutputT == AccumT, only MergeAccumulators
// needs to be specified.
//
// CreateAccumulator() is only required if the AccumT cannot be used in it's zero state,
// and needs initialization.

// mergeFn represents a combine that is just a binary merge, where
//
//	InputT == OutputT == AccumT == int
func mergeFn(a, b int) int {
	return a + b
}

// nonBinaryMergeFn represents a combine with a context parameter and an error return, where
//
//	InputT == OutputT == AccumT == int
func nonBinaryMergeFn(ctx context.Context, a, b int) (int, error) {
	return a + b, nil
}

// MyCombine represents a combine with the same Input and Output type (int), but a
// distinct accumulator type (int64).
//
//	InputT == OutputT == int
//	AccumT == int64
type MyCombine struct{}

func (*MyCombine) AddInput(a int64, v int) int64 {
	return a + int64(v)
}

func (*MyCombine) MergeAccumulators(a, b int64) int64 {
	return a + b
}

func (*MyCombine) ExtractOutput(a int64) int {
	return int(a)
}

// MyOtherCombine is the same as MyCombine, but has strings extracted as output.
//
//	InputT == int
//	AccumT == int64
//	OutputT == string
type MyOtherCombine struct {
	MyCombine // Embedding to re-use the exisitng AddInput and MergeAccumulators implementations
}

func (*MyOtherCombine) ExtractOutput(a int64) string {
	return fmt.Sprintf("%d", a)
}

// MyThirdCombine parses strings as Input, and doesn't specify an ExtractOutput
//
//	InputT == string
//	AccumT == int
//	OutputT == int
type MyThirdCombine struct{}

func (c *MyThirdCombine) AddInput(a int, s string) (int, error) {
	v, err := strconv.ParseInt(s, 0, 0)
	if err != nil {
		return 0, err
	}
	return c.MergeAccumulators(a, int(v)), nil
}

func (*MyThirdCombine) MergeAccumulators(a, b int) int {
	return a + b
}

// MyContextCombine is the same as MyCombine, but requires a context parameter.
//
//	InputT == int
//	AccumT == int64
//	OutputT == string
type MyContextCombine struct {
	MyCombine // Embedding to re-use the exisitng AddInput implementations
}

func (*MyContextCombine) MergeAccumulators(_ context.Context, a, b int64) int64 {
	return a + b
}

// MyErrorCombine is the same as MyCombine, but may return an error.
//
//	InputT == int
//	AccumT == int64
//	OutputT == string
type MyErrorCombine struct {
	MyCombine // Embedding to re-use the exisitng AddInput implementations
}

func (*MyErrorCombine) CreateAccumulator() (int64, error) {
	return 0, nil
}

func (*MyErrorCombine) MergeAccumulators(a, b int64) (int64, error) {
	return a + b, nil
}

func intCoder(t reflect.Type) *coder.Coder {
	c, err := coderx.NewVarIntZ(t)
	if err != nil {
		panic(errors.Wrapf(err, "Couldn't get VarInt coder for %v", t))
	}
	return coder.CoderFrom(c)
}

// simpleGBK buffers all input and continues on FinishBundle. Use with small single-bundle data only.
type simpleGBK struct {
	UID         UnitID
	Out         Node
	KeyCoder    *coder.Coder
	WindowCoder *coder.WindowCoder

	hasher elementHasher
	m      map[uint64]*group
}

type group struct {
	key    FullValue
	values []FullValue
}

func (n *simpleGBK) ID() UnitID {
	return n.UID
}

func (n *simpleGBK) Up(ctx context.Context) error {
	n.m = make(map[uint64]*group)
	n.hasher = makeElementHasher(n.KeyCoder, n.WindowCoder)
	return nil
}

func (n *simpleGBK) StartBundle(ctx context.Context, id string, data DataContext) error {
	return n.Out.StartBundle(ctx, id, data)
}

func (n *simpleGBK) ProcessElement(ctx context.Context, elm *FullValue, _ ...ReStream) error {
	key := elm.Elm
	value := elm.Elm2

	// Consider generalizing this to multiple windows.
	// The test values are all single global window.
	keyHash, err := n.hasher.Hash(key, elm.Windows[0])
	if err != nil {
		return err
	}
	g, ok := n.m[keyHash]
	if !ok {
		g = &group{
			key:    FullValue{Elm: key, Timestamp: elm.Timestamp, Windows: elm.Windows},
			values: make([]FullValue, 0),
		}
		n.m[keyHash] = g
	}
	g.values = append(g.values, FullValue{Elm: value, Timestamp: elm.Timestamp})

	return nil
}

func (n *simpleGBK) FinishBundle(ctx context.Context) error {
	for _, g := range n.m {
		values := &FixedReStream{Buf: g.values}
		if err := n.Out.ProcessElement(ctx, &g.key, values); err != nil {
			return err
		}
	}
	return n.Out.FinishBundle(ctx)
}

func (n *simpleGBK) Down(ctx context.Context) error {
	return nil
}

func (n *simpleGBK) String() string {
	return fmt.Sprintf("simpleGBK: %v Out:%v", n.ID(), n.Out.ID())
}
