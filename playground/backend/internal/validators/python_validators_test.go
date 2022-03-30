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

import "testing"

const (
	pyUnitTestFilePath = "test.py"
	pyUnitTestCode     = "import unittest py code"
	pyCodeFilePath     = "notTest.py"
	pyTestCode         = "py code"
)

func TestCheckIsUnitTestPy(t *testing.T) {
	unitTestValidatorArgs := make([]interface{}, 1)
	unitTestValidatorArgs[0] = pyUnitTestFilePath
	validatorArgs := make([]interface{}, 1)
	validatorArgs[0] = pyCodeFilePath
	argsWithoutRealFile := make([]interface{}, 1)
	argsWithoutRealFile[0] = "fileNotExists.py"
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
			name:    "Check that file is a python unit test",
			args:    args{args: unitTestValidatorArgs},
			want:    true,
			wantErr: false,
		},
		{
			name:    "Check that file is not a python unit test",
			args:    args{args: validatorArgs},
			want:    false,
			wantErr: false,
		},
		{
			name:    "Error if file not exists",
			args:    args{args: argsWithoutRealFile},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckIsUnitTestPy(tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckIsUnitTestPy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CheckIsUnitTestPy() got = %v, want %v", got, tt.want)
			}
		})
	}
}
