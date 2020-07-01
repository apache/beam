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
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
)

func TestDataSource_PerElement(t *testing.T) {
	tests := []struct {
		name     string
		expected []interface{}
		Coder    *coder.Coder
		driver   func(*coder.Coder, io.WriteCloser, []interface{})
	}{
		{
			name:     "perElement",
			expected: []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
			Coder:    coder.NewW(coder.NewVarInt(), coder.NewGlobalWindow()),
			driver: func(c *coder.Coder, pw io.WriteCloser, expected []interface{}) {
				wc := MakeWindowEncoder(c.Window)
				ec := MakeElementEncoder(coder.SkipW(c))
				for _, v := range expected {
					EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, pw)
					ec.Encode(&FullValue{Elm: v}, pw)
				}
				pw.Close()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := &CaptureNode{UID: 1}
			source := &DataSource{
				UID:   2,
				SID:   StreamID{PtransformID: "myPTransform"},
				Name:  test.name,
				Coder: test.Coder,
				Out:   out,
			}
			pr, pw := io.Pipe()
			go test.driver(source.Coder, pw, test.expected)

			constructAndExecutePlanWithContext(t, []Unit{out, source}, DataContext{
				Data: &TestDataManager{R: pr},
			})

			validateSource(t, out, source, makeValues(test.expected...))
		})
	}
}

const tokenString = "token"

// TestDataSource_Iterators per wire protocols for ITERABLEs beam_runner_api.proto
func TestDataSource_Iterators(t *testing.T) {
	extractCoders := func(c *coder.Coder) (WindowEncoder, ElementEncoder, ElementEncoder) {
		wc := MakeWindowEncoder(c.Window)
		cc := coder.SkipW(c)
		kc := MakeElementEncoder(cc.Components[0])
		vc := MakeElementEncoder(cc.Components[1])
		return wc, kc, vc
	}

	tests := []struct {
		name       string
		keys, vals []interface{}
		Coder      *coder.Coder
		driver     func(c *coder.Coder, dmw io.WriteCloser, siwFn func() io.WriteCloser, ks, vs []interface{})
	}{
		{
			name:  "beam:coder:iterable:v1-singleChunk",
			keys:  []interface{}{int64(42), int64(53)},
			vals:  []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
			Coder: coder.NewW(coder.NewCoGBK([]*coder.Coder{coder.NewVarInt(), coder.NewVarInt()}), coder.NewGlobalWindow()),
			driver: func(c *coder.Coder, dmw io.WriteCloser, _ func() io.WriteCloser, ks, vs []interface{}) {
				wc, kc, vc := extractCoders(c)
				for _, k := range ks {
					EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, dmw)
					kc.Encode(&FullValue{Elm: k}, dmw)
					coder.EncodeInt32(int32(len(vs)), dmw) // Number of elements.
					for _, v := range vs {
						vc.Encode(&FullValue{Elm: v}, dmw)
					}
				}
				dmw.Close()
			},
		},
		{
			name:  "beam:coder:iterable:v1-multiChunk",
			keys:  []interface{}{int64(42), int64(53)},
			vals:  []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
			Coder: coder.NewW(coder.NewCoGBK([]*coder.Coder{coder.NewVarInt(), coder.NewVarInt()}), coder.NewGlobalWindow()),
			driver: func(c *coder.Coder, dmw io.WriteCloser, _ func() io.WriteCloser, ks, vs []interface{}) {
				wc, kc, vc := extractCoders(c)
				for _, k := range ks {
					EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, dmw)
					kc.Encode(&FullValue{Elm: k}, dmw)

					coder.EncodeInt32(-1, dmw) // Mark this as a multi-Chunk (though beam runner proto says to use 0)
					for _, v := range vs {
						coder.EncodeVarInt(1, dmw) // Number of elements in this chunk.
						vc.Encode(&FullValue{Elm: v}, dmw)
					}
					coder.EncodeVarInt(0, dmw) // Terminate the multi-chunk for this key.
				}
				dmw.Close()
			},
		},
		{
			name:  "beam:coder:state_backed_iterable:v1",
			keys:  []interface{}{int64(42), int64(53)},
			vals:  []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
			Coder: coder.NewW(coder.NewCoGBK([]*coder.Coder{coder.NewVarInt(), coder.NewVarInt()}), coder.NewGlobalWindow()),
			driver: func(c *coder.Coder, dmw io.WriteCloser, swFn func() io.WriteCloser, ks, vs []interface{}) {
				wc, kc, vc := extractCoders(c)
				for _, k := range ks {
					EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, dmw)
					kc.Encode(&FullValue{Elm: k}, dmw)
					coder.EncodeInt32(-1, dmw)  // Mark as multi-chunk (though beam, runner says to use 0)
					coder.EncodeVarInt(-1, dmw) // Mark subsequent chunks as "state backed"

					token := []byte(tokenString)
					coder.EncodeVarInt(int64(len(token)), dmw) // token.
					dmw.Write(token)
					// Each state stream needs to be a different writer, so get a new writer.
					sw := swFn()
					for _, v := range vs {
						vc.Encode(&FullValue{Elm: v}, sw)
					}
					sw.Close()
				}
				dmw.Close()
			},
		},
		// TODO: Test progress.
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := &IteratorCaptureNode{CaptureNode: CaptureNode{UID: 1}}
			source := &DataSource{
				UID:   2,
				SID:   StreamID{PtransformID: "myPTransform"},
				Name:  test.name,
				Coder: test.Coder,
				Out:   out,
			}
			dmr, dmw := io.Pipe()

			// Simulate individual state channels with pipes and a channel.
			sRc := make(chan io.ReadCloser)
			swFn := func() io.WriteCloser {
				sr, sw := io.Pipe()
				sRc <- sr
				return sw
			}
			go test.driver(source.Coder, dmw, swFn, test.keys, test.vals)

			constructAndExecutePlanWithContext(t, []Unit{out, source}, DataContext{
				Data:  &TestDataManager{R: dmr},
				State: &TestStateReader{Rc: sRc},
			})
			if len(out.CapturedInputs) == 0 {
				t.Fatal("did not capture source output")
			}

			expectedKeys := makeValues(test.keys...)
			expectedValues := makeValuesNoWindowOrTime(test.vals...)
			if got, want := len(out.CapturedInputs), len(expectedKeys); got != want {
				t.Fatalf("lengths don't match: got %v, want %v", got, want)
			}
			var iVals []FullValue
			for _, i := range out.CapturedInputs {
				iVals = append(iVals, i.Key)

				if got, want := i.Values, expectedValues; !equalList(got, want) {
					t.Errorf("DataSource => key(%v) = %#v, want %#v", i.Key, extractValues(got...), extractValues(want...))
				}
			}

			if got, want := iVals, expectedKeys; !equalList(got, want) {
				t.Errorf("DataSource => %#v, want %#v", extractValues(got...), extractValues(want...))
			}
		})
	}
}

func TestDataSource_Split(t *testing.T) {
	elements := []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}
	initSourceTest := func(name string) (*DataSource, *CaptureNode, io.ReadCloser) {
		out := &CaptureNode{UID: 1}
		c := coder.NewW(coder.NewVarInt(), coder.NewGlobalWindow())
		source := &DataSource{
			UID:   2,
			SID:   StreamID{PtransformID: "myPTransform"},
			Name:  name,
			Coder: c,
			Out:   out,
		}
		pr, pw := io.Pipe()

		go func(c *coder.Coder, pw io.WriteCloser, elements []interface{}) {
			wc := MakeWindowEncoder(c.Window)
			ec := MakeElementEncoder(coder.SkipW(c))
			for _, v := range elements {
				EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, pw)
				ec.Encode(&FullValue{Elm: v}, pw)
			}
			pw.Close()
		}(c, pw, elements)
		return source, out, pr
	}

	tests := []struct {
		name     string
		expected []interface{}
		splitIdx int64
	}{
		{splitIdx: 1},
		{splitIdx: 2},
		{splitIdx: 3},
		{splitIdx: 4},
		{splitIdx: 5},
		{
			name:     "wellBeyondRange",
			expected: elements,
			splitIdx: 1000,
		},
	}
	for _, test := range tests {
		test := test
		if len(test.name) == 0 {
			test.name = fmt.Sprintf("atIndex%d", test.splitIdx)
		}
		if test.expected == nil {
			test.expected = elements[:test.splitIdx]
		}
		t.Run(test.name, func(t *testing.T) {
			source, out, pr := initSourceTest(test.name)
			p, err := NewPlan("a", []Unit{out, source})
			if err != nil {
				t.Fatalf("failed to construct plan: %v", err)
			}
			dc := DataContext{Data: &TestDataManager{R: pr}}
			ctx := context.Background()

			// StartBundle resets the source, so no splits can be actuated before then,
			// which means we need to actuate the plan manually, and insert the split request
			// after StartBundle.
			for i, root := range p.units {
				if err := root.Up(ctx); err != nil {
					t.Fatalf("error in root[%d].Up: %v", i, err)
				}
			}
			p.status = Active

			runOnRoots(ctx, t, p, "StartBundle", func(root Root, ctx context.Context) error { return root.StartBundle(ctx, "1", dc) })

			// SDK never splits on 0, so check that every test.
			if splitIdx, err := p.Split(SplitPoints{Splits: []int64{0, test.splitIdx}}); err != nil {
				t.Fatalf("error in Split: %v", err)
			} else if got, want := splitIdx, test.splitIdx; got != want {
				t.Fatalf("error in Split: got splitIdx = %v, want %v ", got, want)
			}
			runOnRoots(ctx, t, p, "Process", Root.Process)
			runOnRoots(ctx, t, p, "FinishBundle", Root.FinishBundle)

			validateSource(t, out, source, makeValues(test.expected...))
		})
	}

	t.Run("whileProcessing", func(t *testing.T) {
		// Check splitting *while* elements are in process.
		tests := []struct {
			name     string
			expected []interface{}
			splitIdx int64
		}{
			{splitIdx: 1},
			{splitIdx: 2},
			{splitIdx: 3},
			{splitIdx: 4},
			{splitIdx: 5},
			{
				name:     "wellBeyondRange",
				expected: elements,
				splitIdx: 1000,
			},
		}
		for _, test := range tests {
			test := test
			if len(test.name) == 0 {
				test.name = fmt.Sprintf("atIndex%d", test.splitIdx)
			}
			if test.expected == nil {
				test.expected = elements[:test.splitIdx]
			}
			t.Run(test.name, func(t *testing.T) {
				source, out, pr := initSourceTest(test.name)
				unblockCh, blockedCh := make(chan struct{}), make(chan struct{}, 1)
				// Block on the one less than the desired split,
				// so the desired split is the first valid split.
				blockOn := test.splitIdx - 1
				blocker := &BlockingNode{
					UID: 3,
					Block: func(elm *FullValue) bool {
						if source.index == blockOn {
							// Signal to call Split
							blockedCh <- struct{}{}
							return true
						}
						return false
					},
					Unblock: unblockCh,
					Out:     out,
				}
				source.Out = blocker

				go func() {
					// Wait to call Split until the DoFn is blocked at the desired element.
					<-blockedCh
					// Validate that we do not split on the element we're blocking on index.
					// The first valid split is at test.splitIdx.
					if splitIdx, err := source.Split([]int64{0, 1, 2, 3, 4, 5}, -1, 0); err != nil {
						t.Errorf("error in Split: %v", err)
					} else if got, want := splitIdx, test.splitIdx; got != want {
						t.Errorf("error in Split: got splitIdx = %v, want %v ", got, want)
					}
					// Validate that our progress is where we expect it to be. (test.splitIdx - 1)
					if got, want := source.Progress().Count, test.splitIdx-1; got != want {
						t.Errorf("error in Progress: got finished processing Count = %v, want %v ", got, want)
					}
					unblockCh <- struct{}{}
				}()

				constructAndExecutePlanWithContext(t, []Unit{out, blocker, source}, DataContext{
					Data: &TestDataManager{R: pr},
				})

				validateSource(t, out, source, makeValues(test.expected...))

				// Adjust expectations to maximum number of elements.
				adjustedExpectation := test.splitIdx
				if adjustedExpectation > int64(len(elements)) {
					adjustedExpectation = int64(len(elements))
				}
				if got, want := source.Progress().Count, adjustedExpectation; got != want {
					t.Fatalf("progress didn't match split: got %v, want %v", got, want)
				}
			})
		}
	})

	// Test that the bufSize param can be used to affect the split range.
	t.Run("bufSize", func(t *testing.T) {
		test := struct {
			splitPts []int64
			frac     float64
			bufSize  int64
			splitIdx int64
			expected []interface{}
		}{
			// splitIdx defaults to the max int64, so if bufSize is respected
			// the closest splitPt is 3, otherwise it'll be 5000.
			splitPts: []int64{3, 5000},
			frac:     0.5,
			bufSize:  10,
			splitIdx: 3,
			expected: elements[:3],
		}

		source, out, pr := initSourceTest("bufSize")
		p, err := NewPlan("a", []Unit{out, source})
		if err != nil {
			t.Fatalf("failed to construct plan: %v", err)
		}
		dc := DataContext{Data: &TestDataManager{R: pr}}
		ctx := context.Background()

		// StartBundle resets the source, so no splits can be actuated before then,
		// which means we need to actuate the plan manually, and insert the split request
		// after StartBundle.
		for i, root := range p.units {
			if err := root.Up(ctx); err != nil {
				t.Fatalf("error in root[%d].Up: %v", i, err)
			}
		}
		p.status = Active

		runOnRoots(ctx, t, p, "StartBundle", func(root Root, ctx context.Context) error { return root.StartBundle(ctx, "1", dc) })

		// SDK never splits on 0, so check that every test.
		sp := SplitPoints{Splits: test.splitPts, Frac: test.frac, BufSize: test.bufSize}
		if splitIdx, err := p.Split(sp); err != nil {
			t.Fatalf("error in Split: %v", err)
		} else if got, want := splitIdx, test.splitIdx; got != want {
			t.Fatalf("error in Split: got splitIdx = %v, want %v ", got, want)
		}
		runOnRoots(ctx, t, p, "Process", Root.Process)
		runOnRoots(ctx, t, p, "FinishBundle", Root.FinishBundle)

		validateSource(t, out, source, makeValues(test.expected...))
	})

	// Test expects splitting errors, but for processing to be successful.
	t.Run("errors", func(t *testing.T) {
		source, out, pr := initSourceTest("noSplitsUntilStarted")
		p, err := NewPlan("a", []Unit{out, source})
		if err != nil {
			t.Fatalf("failed to construct plan: %v", err)
		}
		dc := DataContext{Data: &TestDataManager{R: pr}}
		ctx := context.Background()

		if _, err := p.Split(SplitPoints{Splits: []int64{0, 3}, Frac: -1}); err == nil {
			t.Fatal("plan uninitialized, expected error when splitting, got nil")
		}
		for i, root := range p.units {
			if err := root.Up(ctx); err != nil {
				t.Fatalf("error in root[%d].Up: %v", i, err)
			}
		}
		p.status = Active
		if _, err := p.Split(SplitPoints{Splits: []int64{0, 3}, Frac: -1}); err == nil {
			t.Fatal("plan not started, expected error when splitting, got nil")
		}
		runOnRoots(ctx, t, p, "StartBundle", func(root Root, ctx context.Context) error { return root.StartBundle(ctx, "1", dc) })
		if _, err := p.Split(SplitPoints{Splits: []int64{0}, Frac: -1}); err == nil {
			t.Fatal("plan started, expected error when splitting, got nil")
		}
		runOnRoots(ctx, t, p, "Process", Root.Process)
		if _, err := p.Split(SplitPoints{Splits: []int64{0}, Frac: -1}); err == nil {
			t.Fatal("plan in progress, expected error when unable to get a desired split, got nil")
		}
		runOnRoots(ctx, t, p, "FinishBundle", Root.FinishBundle)
		if _, err := p.Split(SplitPoints{Splits: []int64{0}, Frac: -1}); err == nil {
			t.Fatal("plan finished, expected error when splitting, got nil")
		}
		validateSource(t, out, source, makeValues(elements...))
	})

	t.Run("sanity_errors", func(t *testing.T) {
		var source *DataSource
		if _, err := source.Split([]int64{0}, -1, 0); err == nil {
			t.Fatal("expected error splitting nil *DataSource")
		}
		if _, err := source.Split(nil, -1, 0); err == nil {
			t.Fatal("expected error splitting nil desired splits")
		}
	})
}

func floatEquals(a, b, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}

// TestSplitHelper tests the underlying split logic to confirm that various
// cases produce expected split points.
func TestSplitHelper(t *testing.T) {
	eps := 0.00001 // Float comparison precision.

	// Test splits at various fractions.
	t.Run("SimpleSplits", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			frac       float64
			want       int64
		}{
			// Split as close to the beginning as possible.
			{curr: 0, size: 16, frac: 0, want: 1},
			// The closest split is at 4, even when just above or below it.
			{curr: 0, size: 16, frac: 0.24, want: 4},
			{curr: 0, size: 16, frac: 0.25, want: 4},
			{curr: 0, size: 16, frac: 0.26, want: 4},
			// Split the *remainder* in half.
			{curr: 0, size: 16, frac: 0.5, want: 8},
			{curr: 2, size: 16, frac: 0.5, want: 9},
			{curr: 6, size: 16, frac: 0.5, want: 11},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v])", test.frac, test.curr, test.size), func(t *testing.T) {
				wantFrac := 0.0
				got, gotFrac, err := splitHelper(test.curr, test.size, 0.0, nil, test.frac, false)
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, wantFrac)
				}
			})
		}
	})

	t.Run("WithElementProgress", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			currProg   float64
			frac       float64
			want       int64
		}{
			// Progress into the active element influences where the split of
			// the remainder falls.
			{curr: 0, currProg: 0.5, size: 4, frac: 0.25, want: 1},
			{curr: 0, currProg: 0.9, size: 4, frac: 0.25, want: 2},
			{curr: 1, currProg: 0.0, size: 4, frac: 0.25, want: 2},
			{curr: 1, currProg: 0.1, size: 4, frac: 0.25, want: 2},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v])", test.frac, float64(test.curr)+test.currProg, test.size), func(t *testing.T) {
				wantFrac := 0.0
				got, gotFrac, err := splitHelper(test.curr, test.size, test.currProg, nil, test.frac, false)
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, wantFrac)
				}
			})
		}
	})

	// Test splits with allowed split points.
	t.Run("WithAllowedSplits", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			splits     []int64
			frac       float64
			want       int64
			err        bool // True if test should cause a failure.
		}{
			// The desired split point is at 4.
			{curr: 0, size: 16, splits: []int64{2, 3, 4, 5}, frac: 0.25, want: 4},
			// If we can't split at 4, choose the closest possible split point.
			{curr: 0, size: 16, splits: []int64{2, 3, 5}, frac: 0.25, want: 5},
			{curr: 0, size: 16, splits: []int64{2, 3, 6}, frac: 0.25, want: 3},
			// Also test the case where all possible split points lie above or
			// below the desired split point.
			{curr: 0, size: 16, splits: []int64{5, 6, 7}, frac: 0.25, want: 5},
			{curr: 0, size: 16, splits: []int64{1, 2, 3}, frac: 0.25, want: 3},
			// We have progressed beyond all possible split points, so can't split.
			{curr: 5, size: 16, splits: []int64{1, 2, 3}, frac: 0.25, err: true},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v], splits = %v)", test.frac, test.curr, test.size, test.splits), func(t *testing.T) {
				wantFrac := 0.0
				got, gotFrac, err := splitHelper(test.curr, test.size, 0.0, test.splits, test.frac, false)
				if test.err {
					if err == nil {
						t.Fatalf("splitHelper should have errored, instead got: %v", got)
					}
					return
				}
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, wantFrac)
				}
			})
		}
	})

	t.Run("SdfSplits", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			currProg   float64
			frac       float64
			want       int64
			wantFrac   float64
		}{
			// Split between future elements at element boundaries.
			{curr: 0, currProg: 0, size: 4, frac: 0.51, want: 2},
			{curr: 0, currProg: 0, size: 4, frac: 0.49, want: 2},
			{curr: 0, currProg: 0, size: 4, frac: 0.26, want: 1},
			{curr: 0, currProg: 0, size: 4, frac: 0.25, want: 1},

			// If the split falls inside the first, splittable element, split there.
			{curr: 0, currProg: 0, size: 4, frac: 0.20, want: 0, wantFrac: 0.8},
			// The choice of split depends on the progress into the first element.
			{curr: 0, currProg: 0, size: 4, frac: 0.125, want: 0, wantFrac: 0.5},
			// Here we are far enough into the first element that splitting at 0.2 of the
			// remainder falls outside the first element.
			{curr: 0, currProg: 0.5, size: 4, frac: 0.2, want: 1},

			// Verify the above logic when we are partially through the stream.
			{curr: 2, currProg: 0, size: 4, frac: 0.6, want: 3},
			{curr: 2, currProg: 0.9, size: 4, frac: 0.6, want: 4},
			{curr: 2, currProg: 0.5, size: 4, frac: 0.2, want: 2, wantFrac: 0.8},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v])", test.frac, float64(test.curr)+test.currProg, test.size), func(t *testing.T) {
				got, gotFrac, err := splitHelper(test.curr, test.size, test.currProg, nil, test.frac, true)
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, test.wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, test.wantFrac)
				}
			})
		}
	})

	t.Run("SdfWithAllowedSplits", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			currProg   float64
			frac       float64
			splits     []int64
			want       int64
			wantFrac   float64
		}{
			// This is where we would like to split, when all split points are available.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 2, 3, 4, 5}, want: 2, wantFrac: 0.6},
			// We can't split element at index 2, because 3 is not a split point.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 2, 4, 5}, want: 4},
			// We can't even split element at index 4 as above, because 4 is also not a
			// split point.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 2, 5}, want: 5},
			// We can't split element at index 2, because 2 is not a split point.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 3, 4, 5}, want: 3},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v])", test.frac, float64(test.curr)+test.currProg, test.size), func(t *testing.T) {
				got, gotFrac, err := splitHelper(test.curr, test.size, test.currProg, test.splits, test.frac, true)
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, test.wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, test.wantFrac)
				}
			})
		}
	})
}

func runOnRoots(ctx context.Context, t *testing.T, p *Plan, name string, mthd func(Root, context.Context) error) {
	t.Helper()
	for i, root := range p.roots {
		if err := mthd(root, ctx); err != nil {
			t.Fatalf("error in root[%d].%s: %v", i, name, err)
		}
	}
}

type TestDataManager struct {
	R io.ReadCloser
}

func (dm *TestDataManager) OpenRead(ctx context.Context, id StreamID) (io.ReadCloser, error) {
	return dm.R, nil
}

func (dm *TestDataManager) OpenWrite(ctx context.Context, id StreamID) (io.WriteCloser, error) {
	return nil, nil
}

// TestSideInputReader simulates state reads using channels.
type TestStateReader struct {
	StateReader
	Rc <-chan io.ReadCloser
}

func (si *TestStateReader) OpenIterable(ctx context.Context, id StreamID, key []byte) (io.ReadCloser, error) {
	return <-si.Rc, nil
}

func constructAndExecutePlanWithContext(t *testing.T, us []Unit, dc DataContext) {
	p, err := NewPlan("a", us)
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", dc); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}
}

func validateSource(t *testing.T, out *CaptureNode, source *DataSource, expected []FullValue) {
	t.Helper()
	if got, want := len(out.Elements), len(expected); got != want {
		t.Fatalf("lengths don't match: got %v, want %v", got, want)
	}
	if got, want := source.Progress().Count, int64(len(expected)); got != want {
		t.Fatalf("progress count didn't match: got %v, want %v", got, want)
	}
	if !equalList(out.Elements, expected) {
		t.Errorf("DataSource => %#v, want %#v", extractValues(out.Elements...), extractValues(expected...))
	}
}
