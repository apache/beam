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
	"context"
	"fmt"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func compareProgressReportSnapshots(expected ProgressReportSnapshot, got ProgressReportSnapshot) bool {
	return expected.Name == got.Name && expected.ID == got.ID && expected.Count == got.Count && expected.pcol == got.pcol
}

func TestDataSource_PerElement(t *testing.T) {
	tests := []struct {
		name     string
		expected []any
		Coder    *coder.Coder
		driver   func(*coder.Coder, *chanWriter, []any)
	}{
		{
			name:     "perElement",
			expected: []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
			Coder:    coder.NewW(coder.NewVarInt(), coder.NewGlobalWindow()),
			driver: func(c *coder.Coder, cw *chanWriter, expected []any) {
				wc := MakeWindowEncoder(c.Window)
				ec := MakeElementEncoder(coder.SkipW(c))
				for _, v := range expected {
					EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, typex.NoFiringPane(), cw)
					ec.Encode(&FullValue{Elm: v}, cw)
				}
				cw.Close()
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
			cw := makeChanWriter()
			go test.driver(source.Coder, cw, test.expected)

			constructAndExecutePlanWithContext(t, []Unit{out, source}, DataContext{
				Data: &TestDataManager{Ch: cw.Ch},
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
		keys, vals []any
		Coder      *coder.Coder
		driver     func(c *coder.Coder, dmw *chanWriter, siwFn func() io.WriteCloser, ks, vs []any)
	}{
		{
			name:  "beam:coder:iterable:v1-singleChunk",
			keys:  []any{int64(42), int64(53)},
			vals:  []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
			Coder: coder.NewW(coder.NewCoGBK([]*coder.Coder{coder.NewVarInt(), coder.NewVarInt()}), coder.NewGlobalWindow()),
			driver: func(c *coder.Coder, dmw *chanWriter, _ func() io.WriteCloser, ks, vs []any) {
				wc, kc, vc := extractCoders(c)
				for _, k := range ks {
					EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, typex.NoFiringPane(), dmw)
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
			keys:  []any{int64(42), int64(53)},
			vals:  []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
			Coder: coder.NewW(coder.NewCoGBK([]*coder.Coder{coder.NewVarInt(), coder.NewVarInt()}), coder.NewGlobalWindow()),
			driver: func(c *coder.Coder, dmw *chanWriter, _ func() io.WriteCloser, ks, vs []any) {
				wc, kc, vc := extractCoders(c)
				for _, k := range ks {
					EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, typex.NoFiringPane(), dmw)
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
			keys:  []any{int64(42), int64(53)},
			vals:  []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
			Coder: coder.NewW(coder.NewCoGBK([]*coder.Coder{coder.NewVarInt(), coder.NewVarInt()}), coder.NewGlobalWindow()),
			driver: func(c *coder.Coder, dmw *chanWriter, swFn func() io.WriteCloser, ks, vs []any) {
				wc, kc, vc := extractCoders(c)
				for _, k := range ks {
					EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, typex.NoFiringPane(), dmw)
					kc.Encode(&FullValue{Elm: k}, dmw)
					coder.EncodeInt32(-1, dmw)  // Mark as multi-chunk (though beam, runner says to use 0)
					coder.EncodeVarInt(-1, dmw) // Mark subsequent chunks as "state backed"

					token := []byte(tokenString)
					coder.EncodeVarInt(int64(len(token)), dmw) // token.
					dmw.Write(token)
					dmw.Flush() // Flush here to allow state IO from this goroutine.

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
	for _, singleIterate := range []bool{true, false} {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fmt.Println(test.name)
				capture := &IteratorCaptureNode{CaptureNode: CaptureNode{UID: 1}}
				out := Node(capture)
				units := []Unit{out}
				uid := 2
				if singleIterate {
					out = &Multiplex{UID: UnitID(uid), Out: []Node{capture}}
					units = append(units, out)
					uid++
				}
				source := &DataSource{
					UID:   UnitID(uid),
					SID:   StreamID{PtransformID: "myPTransform"},
					Name:  test.name,
					Coder: test.Coder,
					Out:   out,
				}
				units = append(units, source)
				cw := makeChanWriter()
				// Simulate individual state channels with pipes and a channel.
				sRc := make(chan io.ReadCloser)
				swFn := func() io.WriteCloser {
					sr, sw := io.Pipe()
					sRc <- sr
					return sw
				}
				go test.driver(source.Coder, cw, swFn, test.keys, test.vals)

				constructAndExecutePlanWithContext(t, units, DataContext{
					Data:  &TestDataManager{Ch: cw.Ch},
					State: &TestStateReader{Rc: sRc},
				})
				if len(capture.CapturedInputs) == 0 {
					t.Fatal("did not capture source output")
				}

				expectedKeys := makeValues(test.keys...)
				expectedValues := makeValuesNoWindowOrTime(test.vals...)
				if got, want := len(capture.CapturedInputs), len(expectedKeys); got != want {
					t.Fatalf("lengths don't match: got %v, want %v", got, want)
				}
				var iVals []FullValue
				for _, i := range capture.CapturedInputs {
					iVals = append(iVals, i.Key)

					if got, want := i.Values, expectedValues; !equalList(got, want) {
						t.Errorf("DataSource => key(%v) = %#v, want %#v", i.Key, extractValues(got...), extractValues(want...))
					}
				}

				if got, want := iVals, expectedKeys; !equalList(got, want) {
					t.Errorf("DataSource => %#v, want %#v", extractValues(got...), extractValues(want...))
				}

				// We're using integers that encode to 1 byte, so do some quick math to validate.
				sizeOfSmallInt := 1
				snap := quickTestSnapshot(source, int64(len(test.keys)))
				snap.pcol.SizeSum = int64(len(test.keys) * (1 + len(test.vals)) * sizeOfSmallInt)
				snap.pcol.SizeMin = int64((1 + len(test.vals)) * sizeOfSmallInt)
				snap.pcol.SizeMax = int64((1 + len(test.vals)) * sizeOfSmallInt)
				if got, want := source.Progress(), snap; !compareProgressReportSnapshots(want, got) {
					t.Errorf("progress didn't match: got %v, want %v", got, want)
				}
			})
		}
	}
}

func TestDataSource_Split(t *testing.T) {
	elements := []any{int64(1), int64(2), int64(3), int64(4), int64(5)}
	initSourceTest := func(name string) (*DataSource, *CaptureNode, chan Elements) {
		out := &CaptureNode{UID: 1}
		c := coder.NewW(coder.NewVarInt(), coder.NewGlobalWindow())
		source := &DataSource{
			UID:   2,
			SID:   StreamID{PtransformID: "myPTransform"},
			Name:  name,
			Coder: c,
			Out:   out,
		}
		cw := makeChanWriter()

		go func(c *coder.Coder, pw io.WriteCloser, elements []any) {
			wc := MakeWindowEncoder(c.Window)
			ec := MakeElementEncoder(coder.SkipW(c))
			for _, v := range elements {
				EncodeWindowedValueHeader(wc, window.SingleGlobalWindow, mtime.ZeroTimestamp, typex.NoFiringPane(), pw)
				ec.Encode(&FullValue{Elm: v}, pw)
			}
			pw.Close()
		}(c, cw, elements)
		return source, out, cw.Ch
	}

	tests := []struct {
		name     string
		expected []any
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
			source, out, ch := initSourceTest(test.name)
			p, err := NewPlan("a", []Unit{out, source})
			if err != nil {
				t.Fatalf("failed to construct plan: %v", err)
			}
			dc := DataContext{Data: &TestDataManager{Ch: ch}}
			ctx := context.Background()

			// StartBundle resets the source, so no splits can be actuated before then,
			// which means we need to actuate the plan manually, and insert the split request
			// after StartBundle.
			for i, root := range p.units {
				if err := root.Up(ctx); err != nil {
					t.Fatalf("error in root[%d].Up: %v", i, err)
				}
			}
			p.setStatus(Active)

			runOnRoots(ctx, t, p, "StartBundle", func(root Root, ctx context.Context) error { return root.StartBundle(ctx, "1", dc) })

			// SDK never splits on 0, so check that every test.
			splitRes, err := p.Split(ctx, SplitPoints{Splits: []int64{0, test.splitIdx}})
			if err != nil {
				t.Fatalf("error in Split: %v", err)
			}
			if got, want := splitRes.RI, test.splitIdx; got != want {
				t.Fatalf("error in Split: got splitIdx = %v, want %v ", got, want)
			}
			if got, want := splitRes.PI, test.splitIdx-1; got != want {
				t.Fatalf("error in Split: got primary index = %v, want %v ", got, want)
			}

			runOnRoots(ctx, t, p, "Process", func(root Root, ctx context.Context) error {
				_, err := root.Process(ctx)
				return err
			})
			runOnRoots(ctx, t, p, "FinishBundle", Root.FinishBundle)

			validateSource(t, out, source, makeValues(test.expected...))
		})
	}

	t.Run("whileProcessing", func(t *testing.T) {
		// Check splitting *while* elements are in process.
		tests := []struct {
			name     string
			expected []any
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
				source, out, ch := initSourceTest(test.name)
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
					if splitRes, err := source.Split(context.Background(), []int64{0, 1, 2, 3, 4, 5}, -1, 0); err != nil {
						t.Errorf("error in Split: %v", err)
					} else {
						if got, want := splitRes.RI, test.splitIdx; got != want {
							t.Errorf("error in Split: got splitIdx = %v, want %v ", got, want)
						}
						if got, want := splitRes.PI, test.splitIdx-1; got != want {
							t.Errorf("error in Split: got primary index = %v, want %v ", got, want)
						}
					}
					// Validate that our progress is where we expect it to be. (test.splitIdx - 1)
					if got, want := source.Progress().Count, test.splitIdx-1; got != want {
						t.Errorf("error in Progress: got finished processing Count = %v, want %v ", got, want)
					}
					unblockCh <- struct{}{}
				}()

				constructAndExecutePlanWithContext(t, []Unit{out, blocker, source}, DataContext{
					Data: &TestDataManager{Ch: ch},
				})

				validateSource(t, out, source, makeValues(test.expected...))
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
			expected []any
		}{
			// splitIdx defaults to the max int64, so if bufSize is respected
			// the closest splitPt is 3, otherwise it'll be 5000.
			splitPts: []int64{3, 5000},
			frac:     0.5,
			bufSize:  10,
			splitIdx: 3,
			expected: elements[:3],
		}

		source, out, ch := initSourceTest("bufSize")
		p, err := NewPlan("a", []Unit{out, source})
		if err != nil {
			t.Fatalf("failed to construct plan: %v", err)
		}
		dc := DataContext{Data: &TestDataManager{Ch: ch}}
		ctx := context.Background()

		// StartBundle resets the source, so no splits can be actuated before then,
		// which means we need to actuate the plan manually, and insert the split request
		// after StartBundle.
		for i, root := range p.units {
			if err := root.Up(ctx); err != nil {
				t.Fatalf("error in root[%d].Up: %v", i, err)
			}
		}
		p.setStatus(Active)

		runOnRoots(ctx, t, p, "StartBundle", func(root Root, ctx context.Context) error { return root.StartBundle(ctx, "1", dc) })

		// SDK never splits on 0, so check that every test.
		sp := SplitPoints{Splits: test.splitPts, Frac: test.frac, BufSize: test.bufSize}
		splitRes, err := p.Split(ctx, sp)
		if err != nil {
			t.Fatalf("error in Split: %v", err)
		}
		if got, want := splitRes.RI, test.splitIdx; got != want {
			t.Fatalf("error in Split: got splitIdx = %v, want %v ", got, want)
		}
		if got, want := splitRes.PI, test.splitIdx-1; got != want {
			t.Fatalf("error in Split: got primary index = %v, want %v ", got, want)
		}
		runOnRoots(ctx, t, p, "Process", func(root Root, ctx context.Context) error {
			_, err := root.Process(ctx)
			return err
		})
		runOnRoots(ctx, t, p, "FinishBundle", Root.FinishBundle)

		validateSource(t, out, source, makeValues(test.expected...))
	})

	// Test splitting on sub-elements works when available.
	t.Run("subElement", func(t *testing.T) {
		// Each test will process up to an element, then split at different
		// fractions and check that a sub-element split either was, or was not
		// performed.
		const blockOn int64 = 3 // Should leave 2 elements unprocessed, including blocked element.
		numElms := int64(len(elements))
		tests := []struct {
			fraction float64
			splitIdx int64
			isSubElm bool
		}{
			{fraction: 0.0, splitIdx: blockOn + 1, isSubElm: true},
			{fraction: 0.01, splitIdx: blockOn + 1, isSubElm: true},
			{fraction: 0.49, splitIdx: blockOn + 1, isSubElm: true},  // Should be just within current element.
			{fraction: 0.51, splitIdx: blockOn + 1, isSubElm: false}, // Should be just past current element.
			{fraction: 0.99, splitIdx: numElms, isSubElm: false},
		}
		for _, test := range tests {
			test := test
			name := fmt.Sprintf("withFraction_%v", test.fraction)
			t.Run(name, func(t *testing.T) {
				source, out, ch := initSourceTest(name)
				unblockCh, blockedCh := make(chan struct{}), make(chan struct{}, 1)
				// Block on the one less than the desired split,
				// so the desired split is the first valid split.
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

				splittableCh := make(chan SplittableUnit, 1)
				source.su = splittableCh
				splittableCh <- &TestSplittableUnit{elm: elements[blockOn]}

				go func() {
					// Wait to call Split until the DoFn is blocked at the desired element.
					<-blockedCh
					// Validate that we either do or do not perform a sub-element split with the
					// given fraction.
					if splitRes, err := source.Split(context.Background(), []int64{0, 1, 2, 3, 4, 5}, test.fraction, int64(len(elements))); err != nil {
						t.Errorf("error in Split: %v", err)
					} else {
						// For sub-element splits, check sub-element split only results.
						isSubElm := splitRes.RS != nil && splitRes.PS != nil
						if isSubElm != test.isSubElm {
							t.Errorf("error in Split: got sub-element split = %t, want %t", isSubElm, test.isSubElm)
						}
						if isSubElm {
							if got, want := splitRes.TId, testTransformID; got != want {
								t.Errorf("error in Split: got incorrect Transform Id = %v, want %v", got, want)
							}
							if got, want := splitRes.InId, testInputID; got != want {
								t.Errorf("error in Split: got incorrect Input Id = %v, want %v", got, want)
							}
							if _, ok := splitRes.OW["output1"]; !ok {
								t.Errorf("error in Split: no output watermark for output1")
							}
						}

						// Check that split indices are correct, for both sub-element and channel splits.
						var wantPI, wantRI = test.splitIdx - 1, test.splitIdx
						if isSubElm {
							// In sub-element splits, primary index is expected to be one element
							// before the current (split) element.
							wantPI--
						}
						if splitRes.PI != wantPI || splitRes.RI != wantRI {
							t.Errorf("error in Split: got split indices of (primary, residual) = (%d, %d), want (%d, %d)",
								splitRes.PI, splitRes.RI, wantPI, wantRI)
						}
					}
					// Validate that our progress is where we expect it to be. (blockOn)
					if got, want := source.Progress().Count, blockOn; got != want {
						t.Errorf("error in Progress: got finished processing Count = %v, want %v ", got, want)
					}
					unblockCh <- struct{}{}
				}()

				constructAndExecutePlanWithContext(t, []Unit{out, blocker, source}, DataContext{
					Data: &TestDataManager{Ch: ch},
				})

				validateSource(t, out, source, makeValues(elements[:test.splitIdx]...))
				if got, want := source.Progress().Count, test.splitIdx; got != want {
					t.Fatalf("progress didn't match split: got %v, want %v", got, want)
				}
			})
		}
	})

	// Test expects splitting errors, but for processing to be successful.
	t.Run("errors", func(t *testing.T) {
		source, out, ch := initSourceTest("noSplitsUntilStarted")
		p, err := NewPlan("a", []Unit{out, source})
		if err != nil {
			t.Fatalf("failed to construct plan: %v", err)
		}
		dc := DataContext{Data: &TestDataManager{Ch: ch}}
		ctx := context.Background()

		if sr, err := p.Split(ctx, SplitPoints{Splits: []int64{0, 3}, Frac: -1}); err != nil || !sr.Unsuccessful {
			t.Fatalf("p.Split(before active) = %v,%v want unsuccessful split & nil err", sr, err)
		}
		for i, root := range p.units {
			if err := root.Up(ctx); err != nil {
				t.Fatalf("error in root[%d].Up: %v", i, err)
			}
		}
		p.setStatus(Active)
		if sr, err := p.Split(ctx, SplitPoints{Splits: []int64{0, 3}, Frac: -1}); err != nil || !sr.Unsuccessful {
			t.Fatalf("p.Split(active, not started) = %v,%v want unsuccessful split & nil err", sr, err)
		}
		runOnRoots(ctx, t, p, "StartBundle", func(root Root, ctx context.Context) error { return root.StartBundle(ctx, "1", dc) })
		if sr, err := p.Split(ctx, SplitPoints{Splits: []int64{0}, Frac: -1}); err != nil || !sr.Unsuccessful {
			t.Fatalf("p.Split(active) = %v,%v want unsuccessful split & nil err", sr, err)
		}
		runOnRoots(ctx, t, p, "Process", func(root Root, ctx context.Context) error {
			_, err := root.Process(ctx)
			return err
		})
		if sr, err := p.Split(ctx, SplitPoints{Splits: []int64{0}, Frac: -1}); err != nil || !sr.Unsuccessful {
			t.Fatalf("p.Split(active, unable to get desired split) = %v,%v want unsuccessful split & nil err", sr, err)
		}
		runOnRoots(ctx, t, p, "FinishBundle", Root.FinishBundle)
		if sr, err := p.Split(ctx, SplitPoints{Splits: []int64{0}, Frac: -1}); err != nil || !sr.Unsuccessful {
			t.Fatalf("p.Split(finished) = %v,%v want unsuccessful split & nil err", sr, err)
		}
		validateSource(t, out, source, makeValues(elements...))
	})

	t.Run("sanity_errors", func(t *testing.T) {
		var source *DataSource
		if _, err := source.Split(context.Background(), []int64{0}, -1, 0); err == nil {
			t.Fatal("expected error splitting nil *DataSource")
		}
		if _, err := source.Split(context.Background(), nil, -1, 0); err == nil {
			t.Fatal("expected error splitting nil desired splits")
		}
	})
}

const testTransformID = "transform_id"
const testInputID = "input_id"

// TestSplittableUnit is an implementation of the SplittableUnit interface
// for DataSource tests.
type TestSplittableUnit struct {
	elm any // The element to split.
}

// Split checks the input fraction for correctness, but otherwise always returns
// a successful split. The split elements are just copies of the original.
func (n *TestSplittableUnit) Split(_ context.Context, f float64) ([]*FullValue, []*FullValue, error) {
	if f > 1.0 || f < 0.0 {
		return nil, nil, errors.Errorf("Error")
	}
	return []*FullValue{{Elm: n.elm}}, []*FullValue{{Elm: n.elm}}, nil
}

// Checkpoint routes through the Split() function to satisfy the interface.
func (n *TestSplittableUnit) Checkpoint(ctx context.Context) ([]*FullValue, error) {
	_, r, err := n.Split(ctx, 0.0)
	return r, err
}

// GetProgress always returns 0, to keep tests consistent.
func (n *TestSplittableUnit) GetProgress() float64 {
	return 0
}

// GetTransformId returns a constant transform ID that can be tested for.
func (n *TestSplittableUnit) GetTransformId() string {
	return testTransformID
}

// GetInputId returns a constant input ID that can be tested for.
func (n *TestSplittableUnit) GetInputId() string {
	return testInputID
}

// GetOutputWatermark gets the current output watermark of the splittable unit
// if one is defined, or returns nil otherwise
func (n *TestSplittableUnit) GetOutputWatermark() map[string]*timestamppb.Timestamp {
	ow := make(map[string]*timestamppb.Timestamp)
	ow["output1"] = timestamppb.New(time.Date(2022, time.January, 1, 1, 0, 0, 0, time.UTC))
	return ow
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
				wantFrac := -1.0
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
				wantFrac := -1.0
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
				wantFrac := -1.0
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
			{curr: 0, currProg: 0, size: 4, frac: 0.51, want: 2, wantFrac: -1.0},
			{curr: 0, currProg: 0, size: 4, frac: 0.49, want: 2, wantFrac: -1.0},
			{curr: 0, currProg: 0, size: 4, frac: 0.26, want: 1, wantFrac: -1.0},
			{curr: 0, currProg: 0, size: 4, frac: 0.25, want: 1, wantFrac: -1.0},

			// If the split falls inside the first, splittable element, split there.
			{curr: 0, currProg: 0, size: 4, frac: 0.20, want: 0, wantFrac: 0.8},
			// The choice of split depends on the progress into the first element.
			{curr: 0, currProg: 0, size: 4, frac: 0.125, want: 0, wantFrac: 0.5},
			// Here we are far enough into the first element that splitting at 0.2 of the
			// remainder falls outside the first element.
			{curr: 0, currProg: 0.5, size: 4, frac: 0.2, want: 1, wantFrac: -1.0},

			// Verify the above logic when we are partially through the stream.
			{curr: 2, currProg: 0, size: 4, frac: 0.6, want: 3, wantFrac: -1.0},
			{curr: 2, currProg: 0.9, size: 4, frac: 0.6, want: 4, wantFrac: -1.0},
			{curr: 2, currProg: 0.5, size: 4, frac: 0.2, want: 2, wantFrac: 0.6},

			// Verify that the fraction returned is in terms of remaining work.
			{curr: 0, currProg: 0.8, size: 1, frac: 0.5, want: 0, wantFrac: 0.5},
			{curr: 0, currProg: 0.4, size: 2, frac: 0.1875, want: 0, wantFrac: 0.5},
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
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 2, 4, 5}, want: 4, wantFrac: -1.0},
			// We can't even split element at index 4 as above, because 4 is also not a
			// split point.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 2, 5}, want: 5, wantFrac: -1.0},
			// We can't split element at index 2, because 2 is not a split point.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 3, 4, 5}, want: 3, wantFrac: -1.0},
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

func TestCheckpointing(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		cps, err := (&DataSource{}).checkpointThis(context.Background(), nil)
		if err != nil {
			t.Fatalf("checkpointThis() = %v, %v", cps, err)
		}
	})
	t.Run("Stop", func(t *testing.T) {
		cps, err := (&DataSource{}).checkpointThis(context.Background(), sdf.StopProcessing())
		if err != nil {
			t.Fatalf("checkpointThis() = %v, %v", cps, err)
		}
	})
	t.Run("Delay_no_residuals", func(t *testing.T) {
		wesInv, _ := newWatermarkEstimatorStateInvoker(nil)
		root := &DataSource{
			Out: &ProcessSizedElementsAndRestrictions{
				PDo:    &ParDo{},
				wesInv: wesInv,
				rt:     offsetrange.NewTracker(offsetrange.Restriction{}),
				elm: &FullValue{
					Windows: window.SingleGlobalWindow,
				},
			},
		}
		cp, err := root.checkpointThis(context.Background(), sdf.ResumeProcessingIn(time.Second*13))
		if err != nil {
			t.Fatalf("checkpointThis() = %v, %v, want nil", cp, err)
		}
		if cp != nil {
			t.Fatalf("checkpointThis() = %v, want nil", cp)
		}
	})
	dfn, err := graph.NewDoFn(&CheckpointingSdf{delay: time.Minute}, graph.NumMainInputs(graph.MainSingle))
	if err != nil {
		t.Fatalf("invalid function: %v", err)
	}

	intCoder, _ := coderx.NewVarIntZ(reflectx.Int)
	ERSCoder := coder.NewKV([]*coder.Coder{
		coder.NewKV([]*coder.Coder{
			coder.CoderFrom(intCoder), // Element
			coder.NewKV([]*coder.Coder{
				coder.NewR(typex.New(reflect.TypeOf((*offsetrange.Restriction)(nil)).Elem())), // Restriction
				coder.NewBool(), // Watermark State
			}),
		}),
		coder.NewDouble(), // Size
	})
	wvERSCoder := coder.NewW(
		ERSCoder,
		coder.NewGlobalWindow(),
	)

	rest := offsetrange.Restriction{Start: 1, End: 10}
	value := &FullValue{
		Elm: &FullValue{
			Elm: 42,
			Elm2: &FullValue{
				Elm:  rest,  // Restriction
				Elm2: false, // Watermark State falsie
			},
		},
		Elm2:      rest.Size(),
		Windows:   window.SingleGlobalWindow,
		Timestamp: mtime.MaxTimestamp,
		Pane:      typex.NoFiringPane(),
	}
	t.Run("Delay_residuals_Process", func(t *testing.T) {
		ctx := context.Background()
		wesInv, _ := newWatermarkEstimatorStateInvoker(nil)
		rest := offsetrange.Restriction{Start: 1, End: 10}
		root := &DataSource{
			Coder: wvERSCoder,
			Out: &ProcessSizedElementsAndRestrictions{
				PDo: &ParDo{
					Fn:  dfn,
					Out: []Node{&Discard{}},
				},
				TfId:   "testTransformID",
				wesInv: wesInv,
				rt:     offsetrange.NewTracker(rest),
			},
		}
		if err := root.Up(ctx); err != nil {
			t.Fatalf("invalid function: %v", err)
		}
		if err := root.Out.Up(ctx); err != nil {
			t.Fatalf("invalid function: %v", err)
		}

		enc := MakeElementEncoder(wvERSCoder)
		cw := makeChanWriter()

		// We encode the element several times to ensure we don't
		// drop any residuals, the root of issue #24931.
		wantCount := 3
		for i := 0; i < wantCount; i++ {
			if err := enc.Encode(value, cw); err != nil {
				t.Fatalf("couldn't encode value: %v", err)
			}
		}
		cw.Close()

		if err := root.StartBundle(ctx, "testBund", DataContext{
			Data: &TestDataManager{
				Ch: cw.Ch,
			},
		},
		); err != nil {
			t.Fatalf("invalid function: %v", err)
		}
		cps, err := root.Process(ctx)
		if err != nil {
			t.Fatalf("Process() = %v, %v, want nil", cps, err)
		}
		if got, want := len(cps), wantCount; got != want {
			t.Fatalf("Process() = len %v checkpoints, want %v", got, want)
		}
		// Check each checkpoint has the expected values.
		for _, cp := range cps {
			if got, want := cp.Reapply, time.Minute; got != want {
				t.Errorf("Process(delay(%v)) delay = %v, want %v", want, got, want)
			}
			if got, want := cp.SR.TId, root.Out.(*ProcessSizedElementsAndRestrictions).TfId; got != want {
				t.Errorf("Process() transformID = %v, want %v", got, want)
			}
			if got, want := cp.SR.InId, "i0"; got != want {
				t.Errorf("Process() transformID = %v, want %v", got, want)
			}
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
	Ch chan Elements

	TimerWrites map[string]*bytes.Buffer
}

func (dm *TestDataManager) OpenElementChan(ctx context.Context, id StreamID, expectedTimerTransforms []string) (<-chan Elements, error) {
	return dm.Ch, nil
}

func (dm *TestDataManager) OpenWrite(ctx context.Context, id StreamID) (io.WriteCloser, error) {
	return nil, nil
}

func (dm *TestDataManager) OpenTimerWrite(ctx context.Context, id StreamID, family string) (io.WriteCloser, error) {
	if dm.TimerWrites == nil {
		dm.TimerWrites = map[string]*bytes.Buffer{}
	}
	buf, ok := dm.TimerWrites[family]
	if !ok {
		buf = &bytes.Buffer{}
		dm.TimerWrites[family] = buf
	}
	return struct {
		*bytes.Buffer
		io.Closer
	}{
		Buffer: buf,
		Closer: noopCloser{},
	}, nil
}

type noopCloser struct{}

func (noopCloser) Close() error { return nil }

type chanWriter struct {
	Ch  chan Elements
	Buf []byte
}

func (cw *chanWriter) Write(p []byte) (int, error) {
	cw.Buf = append(cw.Buf, p...)
	return len(p), nil
}

func (cw *chanWriter) Close() error {
	cw.Flush()
	close(cw.Ch)
	return nil
}

func (cw *chanWriter) Flush() {
	cw.Ch <- Elements{Data: cw.Buf, PtransformID: "myPTransform"}
	cw.Buf = nil
}

func makeChanWriter() *chanWriter { return &chanWriter{Ch: make(chan Elements, 20)} }

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

func quickTestSnapshot(source *DataSource, count int64) ProgressReportSnapshot {
	return ProgressReportSnapshot{
		Name:  source.Name,
		ID:    source.SID.PtransformID,
		Count: count,
		pcol: PCollectionSnapshot{
			ElementCount: count,
			SizeCount:    count,
			SizeSum:      count,
			// We're only encoding small ints here, so size will only be 1.
			SizeMin: 1,
			SizeMax: 1,
		},
	}
}

func validateSource(t *testing.T, out *CaptureNode, source *DataSource, expected []FullValue) {
	t.Helper()
	if got, want := len(out.Elements), len(expected); got != want {
		t.Fatalf("lengths don't match: got %v, want %v", got, want)
	}
	if got, want := source.Progress(), quickTestSnapshot(source, int64(len(expected))); !compareProgressReportSnapshots(want, got) {
		t.Fatalf("progress snapshot didn't match: got %v, want %v", got, want)
	}
	if !equalList(out.Elements, expected) {
		t.Errorf("DataSource => %#v, want %#v", extractValues(out.Elements...), extractValues(expected...))
	}
}
