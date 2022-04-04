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

// specialize is a low-level tool to generate type-specialized code. It is a
// convenience wrapper over text/template suitable for go generate. Unlike
// many other template tools, it does not parse Go code and allows use of
// text/template control within the template itself.
package main

import "testing"

func TestMakeName(t *testing.T) {
	var tests = []struct {
		name        string
		input       string
		expectedOut string
	}{
		{
			"single word",
			"testing",
			"Testing",
		},
		{
			"multi-word",
			"romeo and juliet",
			"Romeo And Juliet",
		},
		{
			"slice",
			"[]integer",
			"IntegerSlice",
		},
		{
			"brackets",
			"[strings]",
			"_Strings_",
		},
		{
			"brackets and slices",
			"[][strings]",
			"_Strings_Slice",
		},
		{
			"period",
			"Strings.",
			"Strings_",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := makeName(test.input), test.expectedOut; got != want {
				t.Errorf("got name %v, want %v", got, want)
			}
		})
	}
}
