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

import "testing"

func TestParse(t *testing.T) {
	for _, s := range []struct {
		str      string
		expected Sdk
	}{
		{"Go", SDK_GO},
		{"Python", SDK_PYTHON},
		{"Java", SDK_JAVA},
		{"SCIO", SDK_SCIO},
		{"Bad", SDK_UNDEFINED},
		{"", SDK_UNDEFINED},
	} {
		if parsed := ParseSdk(s.str); parsed != s.expected {
			t.Errorf("Failed to parse %v: got %v (expected %v)", s.str, parsed, s.expected)
		}
	}
}

func TestSerialize(t *testing.T) {
	for _, s := range []struct {
		expected string
		sdk      Sdk
	}{
		{"Go", SDK_GO},
		{"Python", SDK_PYTHON},
		{"Java", SDK_JAVA},
		{"SCIO", SDK_SCIO},
		{"", SDK_UNDEFINED},
	} {
		if txt := s.sdk.String(); txt != s.expected {
			t.Errorf("Failed to serialize %v to string: got %v (expected %v)", s.sdk, txt, s.expected)
		}
	}
}

func TestSdkList(t *testing.T) {
	if SdksList() != [4]string{"Java", "Python", "Go", "SCIO"} {
		t.Error("Sdk list mismatch: ", SdksList())
	}
}
