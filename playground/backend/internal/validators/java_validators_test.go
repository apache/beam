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
	"fmt"
	"os"
	"testing"
)

const unitTestFilePath = "unitTestCode.java"
const filePath = "code.java"
const unitTestCode = "package org.apache.beam.sdk.transforms; \n @RunWith(JUnit4.class)\npublic class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
const code = "package org.apache.beam.sdk.transforms; \n public class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"

func TestMain(m *testing.M) {
	setup()
	defer teardown()
	m.Run()
}

func setup() {
	writeFile(unitTestFilePath, unitTestCode)
	writeFile(filePath, code)
}

func teardown() {
	removeFile(unitTestFilePath)
	removeFile(filePath)
}

func removeFile(path string) {
	err := os.Remove(path)
	if err != nil {
		panic(fmt.Errorf("error during test teardown: %s", err.Error()))
	}
}

func writeFile(path string, code string) {
	err := os.WriteFile(path, []byte(code), 0600)
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
}

func TestCheckIsUnitTests(t *testing.T) {
	testValidatorArgs := make([]interface{}, 1)
	testValidatorArgs[0] = unitTestFilePath

	validatorArgs := make([]interface{}, 1)
	validatorArgs[0] = filePath

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
			got, err := CheckIsUnitTests(tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckIsUnitTests() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CheckIsUnitTests() got = %v, want %v", got, tt.want)
			}
		})
	}
}
