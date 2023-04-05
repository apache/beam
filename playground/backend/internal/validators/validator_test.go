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
	playground "beam.apache.org/playground/backend/internal/api/v1"
	"fmt"
	"os"
	"reflect"
	"testing"
)

// TestMain setups and teardown all necessary functionality for tests
// in 'validators' package (i.e. for java_validators_test, go_validators_test,
// python_validators_test)
func TestMain(m *testing.M) {
	setup()
	defer teardown()
	m.Run()
}

func setup() {
	writeFile(javaUnitTestFilePath, javaUnitTestCode)
	writeFile(javaKataFilePath, javaKataCode)
	writeFile(javaCodePath, javaCode)
	writeFile(goUnitTestFilePath, goUnitTestCode)
	writeFile(goCodePath, goCode)
	writeFile(pyUnitTestFilePath, pyUnitTestCode)
	writeFile(pyCodeFilePath, pyTestCode)
}

func teardown() {
	removeFile(javaUnitTestFilePath)
	removeFile(javaCodePath)
	removeFile(goUnitTestFilePath)
	removeFile(goCodePath)
	removeFile(javaKataFilePath)
	removeFile(pyUnitTestFilePath)
	removeFile(pyCodeFilePath)
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

func TestGetValidators(t *testing.T) {
	type args struct {
		sdk      playground.Sdk
		filepath string
	}
	tests := []struct {
		name    string
		args    args
		want    *[]Validator
		wantErr bool
	}{
		{
			// Test case with calling GetValidators method with incorrect SDK.
			// As a result, want to receive an error.
			name: "Incorrect sdk",
			args: args{
				sdk:      playground.Sdk_SDK_UNSPECIFIED,
				filepath: "",
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetValidators method with correct SDK.
			// As a result, want to receive an expected slice of validators.
			name: "Correct java sdk",
			args: args{
				sdk:      playground.Sdk_SDK_JAVA,
				filepath: "",
			},
			want:    GetJavaValidators(""),
			wantErr: false,
		},
		{
			// Test case with calling GetValidators method with correct SDK.
			// As a result, want to receive an expected slice of validators.
			name: "Correct python sdk",
			args: args{
				sdk:      playground.Sdk_SDK_PYTHON,
				filepath: "",
			},
			want:    GetPyValidators(""),
			wantErr: false,
		},
		{
			// Test case with calling GetValidators method with correct SDK.
			// As a result, want to receive an expected slice of validators.
			name: "Correct go sdk",
			args: args{
				sdk:      playground.Sdk_SDK_GO,
				filepath: "",
			},
			want:    GetGoValidators(""),
			wantErr: false,
		},
		{
			// Test case with calling GetValidators method with correct SDK.
			// As a result, want to receive an expected slice of validators.
			name: "Correct scio sdk",
			args: args{
				sdk:      playground.Sdk_SDK_SCIO,
				filepath: "",
			},
			want:    GetScioValidators(""),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetValidators(tt.args.sdk, tt.args.filepath)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetValidators() err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != nil {
				if !reflect.DeepEqual(len(*got), len(*tt.want)) {
					t.Errorf("GetValidators() len = %v, want %v", len(*got), len(*tt.want))
				}
				for i := range *got {
					gotVal := (*got)[i]
					wantVal := (*tt.want)[i]
					if !reflect.DeepEqual(gotVal.Args, wantVal.Args) {
						t.Errorf("GetValidators() %d = %v, want %v", i, gotVal.Args, wantVal.Args)
					}
				}
			}
		})
	}
}
