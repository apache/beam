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

package internal

import (
	"testing"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
)

func Test_isLeafCoder(t *testing.T) {
	tests := []struct {
		urn    string
		isLeaf bool
	}{
		{urns.CoderBytes, true},
		{urns.CoderStringUTF8, true},
		{urns.CoderLengthPrefix, true},
		{urns.CoderVarInt, true},
		{urns.CoderDouble, true},
		{urns.CoderBool, true},
		{urns.CoderGlobalWindow, true},
		{urns.CoderIntervalWindow, true},
		{urns.CoderIterable, false},
		{urns.CoderRow, false},
		{urns.CoderKV, false},
	}
	for _, test := range tests {
		undertest := &pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn: test.urn,
			},
		}
		if got, want := isLeafCoder(undertest), test.isLeaf; got != want {
			t.Errorf("isLeafCoder(%v) = %v, want %v", test.urn, got, want)
		}
	}
}

func Test_makeWindowedValueCoder(t *testing.T) {

}

func Test_pullDecoder(t *testing.T) {

}
