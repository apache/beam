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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

func TestUnmarshalKeyedValues(t *testing.T) {
	tests := []struct {
		in  map[string]string
		exp []string
	}{
		{ // ordered
			map[string]string{"i0": "a", "i2": "c", "i1": "b"},
			[]string{"a", "b", "c"},
		},
		{ // ordered
			map[string]string{"i2": "c", "i1": "b", "i0": "a"},
			[]string{"a", "b", "c"},
		},
		{ // unordered fill in
			map[string]string{"i0": "a", "i2": "c", "foo": "b"},
			[]string{"a", "b", "c"},
		},
		{ // bogus
			map[string]string{"bogus": "b"},
			nil,
		},
		{ // out-of-bound also fill in (somewhat counter-intuitively in some cases)
			map[string]string{"i3": "a", "i2": "c", "i1": "b"},
			[]string{"a", "b", "c"},
		},
	}

	for _, test := range tests {
		actual := unmarshalKeyedValues(test.in)
		if !reflect.DeepEqual(actual, test.exp) {
			t.Errorf("unmarshalKeyedValues(%v) = %v, want %v", test.in, actual, test.exp)
		}
	}
}

func TestUnmarshalReshuffleCoders(t *testing.T) {
	payloads := map[string][]byte{}
	encode := func(id, urn string, comps ...string) {
		payloads[id] = protox.MustEncode(&pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn: urn,
			},
			ComponentCoderIds: comps,
		})
	}
	encode("a", "beam:coder:bytes:v1")
	encode("b", "beam:coder:string_utf8:v1")
	encode("c", "beam:coder:kv:v1", "b", "a")

	got, err := unmarshalReshuffleCoders("c", payloads)
	if err != nil {
		t.Fatalf("unmarshalReshuffleCoders() err: %v", err)
	}

	want := coder.NewKV([]*coder.Coder{coder.NewString(), coder.NewBytes()})
	if !want.Equals(got) {
		t.Errorf("got %v, want != %v", got, want)
	}
}
