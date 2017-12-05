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

package provision

import (
	"reflect"
	"testing"
)

type s struct {
	A int    `json:"a,omitempty"`
	B string `json:"b,omitempty"`
	C bool   `json:"c,omitempty"`
	D *s     `json:"d,omitempty"`
}

// TestConversions verifies that we can process proto structs via JSON.
func TestConversions(t *testing.T) {
	tests := []s{
		s{},
		s{A: 2},
		s{B: "foo"},
		s{C: true},
		s{D: &s{A: 3}},
		s{A: 1, B: "bar", C: true, D: &s{A: 3, B: "baz"}},
	}

	for _, test := range tests {
		enc, err := OptionsToProto(test)
		if err != nil {
			t.Errorf("Failed to marshal %v: %v", test, err)
		}
		var ret s
		if err := ProtoToOptions(enc, &ret); err != nil {
			t.Errorf("Failed to unmarshal %v from %v: %v", test, enc, err)
		}
		if !reflect.DeepEqual(test, ret) {
			t.Errorf("Unmarshal(Marshal(%v)) = %v, want %v", test, ret, test)
		}
	}
}
