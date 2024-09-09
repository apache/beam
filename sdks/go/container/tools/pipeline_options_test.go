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

package tools

import (
	"os"
	"testing"
)

func TestMakePipelineOptionsFileAndEnvVar(t *testing.T) {
	tests := []struct {
		name          string
		inputOptions  string
		expectedError string
	}{
		{
			"empty options",
			"{}",
			"",
		},
		{
			"valid options",
			"{\"abc\": 123}",
			"",
		},
		{
			"invalid options",
			"{4}",
			"options string is not JSON formatted {4}",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(os.Clearenv)
			err := MakePipelineOptionsFileAndEnvVar(test.inputOptions)
			if err != nil {
				if got, want := err.Error(), test.expectedError; got != want {
					t.Errorf("got error: %v, want error: %v", got, want)
				}
			}
		})
	}
	os.Remove("pipeline_options.json")
}
