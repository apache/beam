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
	"io"
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

			expected := makeValues(test.expected...)
			if got, want := len(out.Elements), len(expected); got != want {
				t.Fatalf("lengths don't match: got %v, want %v", got, want)
			}
			if !equalList(out.Elements, expected) {
				t.Errorf("DataSource => %#v, want %#v", extractValues(out.Elements...), extractValues(expected...))
			}
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
		// TODO: Test splitting.
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
