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

package beam_test

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func TestJSONCoder(t *testing.T) {
	tests := []int{43, 12431235, -2, 0, 1}

	for _, test := range tests {
		data, err := beam.JSONEnc(test)
		if err != nil {
			t.Fatalf("Failed to encode %v: %v", tests, err)
		}
		decoded, err := beam.JSONDec(reflectx.Int, data)
		if err != nil {
			t.Fatalf("Failed to decode: %v", err)
		}
		actual := decoded.(int)

		if test != actual {
			t.Errorf("Corrupt coding: %v, want %v", actual, test)
		}
	}
}
