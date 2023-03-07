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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func TestReadValueState(t *testing.T) {
	intCoder, err := makeIntCoder()
	if err != nil {
		t.Fatalf("Failed to construct int coder with error: %v", err)
	}

	tests := []struct {
		name    string
		stateID string
		coder   *coder.Coder
		ret     any
		err     bool
	}{
		{
			name:    "IntRead",
			stateID: "IntRead",
			coder:   intCoder,
			ret:     5,
			err:     false,
		},
		{
			name:    "IntRead_NoVal",
			stateID: "IntRead_NoVal",
			coder:   intCoder,
			ret:     nil,
			err:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sp := buildStateProvider()
			sp.codersByKey[test.stateID] = test.coder
			ret, _, err := sp.ReadValueState(test.stateID)
			if err == nil && test.err {
				t.Errorf("sp.ReadValueState(%v) didn't return an error when one was expected", test.stateID)
			} else if err != nil && !test.err {
				t.Errorf("sp.ReadValueState(%v) returned error: %v", test.stateID, err)
			}
			if ret != test.ret {
				t.Errorf("sp.ReadValueState(%v)=%v, want %v", test.stateID, ret, test.ret)
			}
		})
	}
}

func buildStateProvider() stateProvider {
	return stateProvider{
		ctx:               context.Background(),
		sr:                &testStateReader{},
		elementKey:        []byte{1},
		window:            []byte{1},
		transactionsByKey: make(map[string][]state.Transaction),
		initialValueByKey: make(map[string]any),
		initialBagByKey:   make(map[string][]any),
		readersByKey:      make(map[string]io.ReadCloser),
		appendersByKey:    make(map[string]io.Writer),
		clearersByKey:     make(map[string]io.Writer),
		combineFnsByKey:   make(map[string]*graph.CombineFn), // Each test can specify coders as needed
		codersByKey:       make(map[string]*coder.Coder),     // Each test can specify coders as needed
	}
}

type testBagReader struct {
	userStateID string
	key         []byte
	w           []byte
}

func (tbr *testBagReader) Read(buf []byte) (int, error) {
	intCoder, err := makeIntCoder()
	if err != nil {
		panic(fmt.Sprintf("Failed to construct int coder with error: %v", err))
	}

	w := testIoWriter{}
	w.b = []byte{}

	if tbr.userStateID == "IntRead" {
		enc := MakeElementEncoder(coder.SkipW(intCoder))
		enc.Encode(&FullValue{Elm: 5}, &w)
	}

	copy(buf, w.b)

	return len(buf), io.EOF
}

func (tbr *testBagReader) Close() error {
	return nil
}

func makeIntCoder() (*coder.Coder, error) {
	var t int
	c, err := coderx.NewVarIntZ(typex.New(reflect.TypeOf(t)).Type())
	if err != nil {
		return nil, err
	}
	return coder.CoderFrom(c), nil
}

type testIoWriter struct {
	b []byte
}

func (t *testIoWriter) Write(b []byte) (int, error) {
	t.b = b
	return len(b), nil
}

func TestNewUserStateAdapter(t *testing.T) {
	testCoder := &coder.Coder{
		Kind: coder.WindowedValue,
		T:    nil,
		Components: []*coder.Coder{
			{
				Kind: coder.KV,
				Components: []*coder.Coder{
					{
						Kind: coder.Double,
					},
					{
						Kind: coder.Bool,
					},
				},
			},
		},
		Custom: nil,
		Window: &coder.WindowCoder{
			Kind:    coder.GlobalWindow,
			Payload: "",
		},
		ID: "",
	}
	tests := []struct {
		name               string
		sid                StreamID
		c                  *coder.Coder
		stateIDToCoder     map[string]*coder.Coder
		stateIDToKeyCoder  map[string]*coder.Coder
		stateIDToCombineFn map[string]*graph.CombineFn
		adapter            UserStateAdapter
	}{
		{
			name: "",
			sid: StreamID{
				Port:         Port{},
				PtransformID: "",
			},
			c:                  testCoder,
			stateIDToCoder:     nil,
			stateIDToKeyCoder:  nil,
			stateIDToCombineFn: nil,
			adapter: &userStateAdapter{
				sid:                StreamID{},
				wc:                 &globalWindowEncoder{},
				kc:                 MakeElementEncoder(coder.SkipW(testCoder).Components[0]),
				stateIDToCoder:     nil,
				stateIDToKeyCoder:  nil,
				stateIDToCombineFn: nil,
				c:                  testCoder,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := NewUserStateAdapter(test.sid, test.c, test.stateIDToCoder, test.stateIDToKeyCoder, test.stateIDToCombineFn)
			if !reflect.DeepEqual(adapter, test.adapter) {
				t.Errorf("NewUserStateAdapter(%v, %v, %v, %v, %v)=%v, want %v", test.sid, test.c, test.stateIDToCoder, test.stateIDToKeyCoder, test.stateIDToCombineFn, adapter, test.adapter)
			}
		})
	}
}
