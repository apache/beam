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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
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
