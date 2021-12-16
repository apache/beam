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

package validators

import (
	"testing"
)

const (
	javaUnitTestFilePath = "unitTestCode.java"
	javaCodePath         = "code.java"
	javaUnitTestCode     = "@RunWith(JUnit4.class)\npublic class DeduplicateTest {\n\n  @Rule public TestPipeline p = TestPipeline.create();\n\n  @Test\n  @Category({NeedsRunner.class, UsesTestStream.class})\n  public void testInDifferentWindows() {}}"
	javaCode             = "package org.apache.beam.sdk.transforms; \n public class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
)

func TestCheckIsUnitTests(t *testing.T) {
	testValidatorArgs := make([]interface{}, 1)
	testValidatorArgs[0] = javaUnitTestFilePath

	validatorArgs := make([]interface{}, 1)
	validatorArgs[0] = javaCodePath

	type args struct {
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			// Test if code is unit test code
			name: "if unit test",
			args: args{
				testValidatorArgs,
			},
			want:    true,
			wantErr: false,
		},
		{
			// Test if code is not unit test code
			name: "if not unit test",
			args: args{
				validatorArgs,
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckIsUnitTestJava(tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckIsUnitTestJava() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CheckIsUnitTestJava() got = %v, want %v", got, tt.want)
			}
		})
	}
}
