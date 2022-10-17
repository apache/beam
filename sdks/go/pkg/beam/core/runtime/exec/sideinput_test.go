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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func makeWindowedCoder() *coder.Coder {
	vCoder := coder.NewDouble()
	return coder.NewW(vCoder, coder.NewGlobalWindow())
}

func makeWindowedKVCoder() *coder.Coder {
	kCoder := coder.NewString()
	vCoder := coder.NewDouble()
	kvCoder := coder.NewKV([]*coder.Coder{kCoder, vCoder})
	return coder.NewW(kvCoder, coder.NewGlobalWindow())
}

func TestNewSideInputAdapter(t *testing.T) {
	tests := []struct {
		name        string
		sid         StreamID
		sideInputID string
		c           *coder.Coder
		kc          ElementEncoder
		ec          ElementDecoder
	}{
		{
			name:        "KV coder",
			sid:         StreamID{Port: Port{URL: "localhost:8099"}, PtransformID: "n0"},
			sideInputID: "i0",
			c:           makeWindowedKVCoder(),
			kc:          &stringEncoder{},
			ec:          &doubleDecoder{},
		},
		{
			name:        "V coder",
			sid:         StreamID{Port: Port{URL: "localhost:8099"}, PtransformID: "n0"},
			sideInputID: "i0",
			c:           makeWindowedCoder(),
			kc:          nil,
			ec:          &doubleDecoder{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := NewSideInputAdapter(test.sid, test.sideInputID, test.c, nil)
			adapterStruct, ok := adapter.(*sideInputAdapter)
			if !ok {
				t.Errorf("failed to convert interface to sideInputAdapter struct in test %v", test)
			}
			if got, want := adapterStruct.sid, test.sid; got != want {
				t.Errorf("got SID %v, want %v", got, want)
			}
			if got, want := adapterStruct.sideInputID, test.sideInputID; got != want {
				t.Errorf("got sideInputID %v, want %v", got, want)
			}
			if got, want := adapterStruct.c, test.c; got != want {
				t.Errorf("got coder %v, want %v", got, want)
			}
			if got, want := reflect.TypeOf(adapterStruct.kc), reflect.TypeOf(test.kc); got != want {
				t.Errorf("got ElementEncoder type %v, want %v", got, want)
			}
			if got, want := reflect.TypeOf(adapterStruct.ec), reflect.TypeOf(test.ec); got != want {
				t.Errorf("got ElementDecoder type %v, want %v", got, want)
			}
		})
	}
}

func TestNewKeyedIterable_Unkeyed(t *testing.T) {
	adapter := NewSideInputAdapter(StreamID{}, "", makeWindowedCoder(), nil)
	rs, err := adapter.NewKeyedIterable(context.Background(), nil, nil, nil)
	if err == nil {
		t.Error("NewKeyedIterable() succeeded when it should have failed")
	}
	if rs != nil {
		t.Errorf("NewKeyedIterable() returned a ReStream when it should not have, got %v", rs)
	}
}

type testSideCache struct{}

// QueryCache for the testSideCache is a no-op.
func (t *testSideCache) QueryCache(ctx context.Context, transformID, sideInputID string, win, key []byte) ReStream {
	return nil
}

// SetCache for the testSideCache is a no-op that returns the input ReStream.
func (t *testSideCache) SetCache(ctx context.Context, transformID, sideInputID string, win, key []byte, input ReStream) ReStream {
	return input
}

type testStateReader struct{}

// OpenIterableSideInput for the testStateReader is a no-op.
func (t *testStateReader) OpenIterableSideInput(ctx context.Context, id StreamID, sideInputID string, w []byte) (io.ReadCloser, error) {
	return nil, nil
}

// OpenMultimapSideInput for the testStateReader is a no-op.
func (t *testStateReader) OpenMultiMapSideInput(ctx context.Context, id StreamID, sideInputID string, key, w []byte) (io.ReadCloser, error) {
	return nil, nil
}

// OpenIterable for the testStateReader is a no-op
func (t *testStateReader) OpenIterable(ctx context.Context, id StreamID, key []byte) (io.ReadCloser, error) {
	return nil, nil
}

func (t *testStateReader) OpenBagUserStateReader(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.ReadCloser, error) {
	tbr := testBagReader{
		userStateID: userStateID,
		key:         key,
		w:           w,
	}
	return &tbr, nil
}

func (t *testStateReader) OpenBagUserStateAppender(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.Writer, error) {
	return nil, nil
}

func (t *testStateReader) OpenBagUserStateClearer(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.Writer, error) {
	return nil, nil
}

// OpenMultimapUserStateReader opens a byte stream for reading user multimap state.
func (t *testStateReader) OpenMultimapUserStateReader(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte, mk []byte) (io.ReadCloser, error) {
	return nil, nil
}

// OpenMultimapUserStateAppender opens a byte stream for appending user multimap state.
func (t *testStateReader) OpenMultimapUserStateAppender(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte, mk []byte) (io.Writer, error) {
	return nil, nil
}

// OpenMultimapUserStateClearer opens a byte stream for clearing user multimap state by key.
func (t *testStateReader) OpenMultimapUserStateClearer(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte, mk []byte) (io.Writer, error) {
	return nil, nil
}

// OpenMultimapKeysUserStateReader opens a byte stream for reading the keys of user multimap state.
func (t *testStateReader) OpenMultimapKeysUserStateReader(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.ReadCloser, error) {
	return nil, nil
}

// OpenMultimapKeysUserStateClearer opens a byte stream for clearing all keys of user multimap state.
func (t *testStateReader) OpenMultimapKeysUserStateClearer(ctx context.Context, id StreamID, userStateID string, key []byte, w []byte) (io.Writer, error) {
	return nil, nil
}

func (t *testStateReader) GetSideInputCache() SideCache {
	return &testSideCache{}
}

type testWindowMapper struct{}

func (t *testWindowMapper) MapWindow(w typex.Window) (typex.Window, error) {
	return w, nil
}

type testFaultyWindowMapper struct{}

func (t *testFaultyWindowMapper) MapWindow(w typex.Window) (typex.Window, error) {
	return nil, fmt.Errorf("some error for window %v", w)
}

func TestNewIterable(t *testing.T) {
	sid := StreamID{Port: Port{URL: "localhost:8099"}, PtransformID: "n0"}
	sideID := "i0"
	c := makeWindowedCoder()
	adapter := NewSideInputAdapter(sid, sideID, c, &testWindowMapper{})

	_, err := adapter.NewIterable(context.Background(), &testStateReader{}, window.GlobalWindow{})
	if err != nil {
		t.Errorf("NewIterable() failed when it should have succeeded, got %v", err)
	}
}

func TestNewIterable_BadMapper(t *testing.T) {
	sid := StreamID{Port: Port{URL: "localhost:8099"}, PtransformID: "n0"}
	sideID := "i0"
	c := makeWindowedCoder()
	adapter := NewSideInputAdapter(sid, sideID, c, &testFaultyWindowMapper{})

	_, err := adapter.NewIterable(context.Background(), &testStateReader{}, window.GlobalWindow{})
	if err == nil {
		t.Error("NewIterable() succeeded when it should have failed.")
	}
}

func TestNewKeyedIterable_BadMapper(t *testing.T) {
	sid := StreamID{Port: Port{URL: "localhost:8099"}, PtransformID: "n0"}
	sideID := "i0"
	c := makeWindowedKVCoder()
	adapter := NewSideInputAdapter(sid, sideID, c, &testFaultyWindowMapper{})

	_, err := adapter.NewKeyedIterable(context.Background(), &testStateReader{}, window.GlobalWindow{}, "")
	if err == nil {
		t.Error("NewKeyedIterable succeeded when it should have failed.")
	}
}

func TestNewIterable_KVType(t *testing.T) {
	sid := StreamID{Port: Port{URL: "localhost:8099"}, PtransformID: "n0"}
	sideID := "i0"
	c := makeWindowedKVCoder()
	adapter := NewSideInputAdapter(sid, sideID, c, &testWindowMapper{})
	_, err := adapter.NewIterable(context.Background(), &testStateReader{}, window.GlobalWindow{})
	if err != nil {
		t.Fatalf("NewIterable() failed when it should have succeeded, got %v", err)
	}
	adapterStruct, ok := adapter.(*sideInputAdapter)
	if !ok {
		t.Fatal("failed to convert SideInputAdapter interface to sideInputAdapter type.")
	}
	// Check that the element decoder type was updated to be a KV type
	if got, want := reflect.TypeOf(adapterStruct.ec), reflect.TypeOf(&kvDecoder{}); got != want {
		t.Errorf("got element decoder type %v, want %v", got, want)
	}
}
