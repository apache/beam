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

package postgresio

import "testing"

func TestFormatVectorLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   []float32
		want string
	}{
		{"mixed", []float32{1.5, -2.0, 3.25}, "[1.5,-2,3.25]"},
		{"empty", []float32{}, "[]"},
		{"nil", nil, "[]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := formatVectorLiteral(tc.in), tc.want; got != want {
				t.Errorf("formatVectorLiteral(%v) = %q, want %q", tc.in, got, want)
			}
		})
	}
}
