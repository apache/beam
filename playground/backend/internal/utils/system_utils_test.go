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

package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestGetFuncName(t *testing.T) {
	type args struct {
		i interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Get function name",
			args: args{i: TestGetFuncName},
			want: "TestGetFuncName",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFuncName(tt.args.i); got != tt.want {
				t.Errorf("GetFuncName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkNumOfTheParallelJobs(t *testing.T) {
	type args struct {
		workingDir        string
		numOfParallelJobs int
	}
	tests := []struct {
		name        string
		args        args
		prepareFunc func()
		want        bool
	}{
		{
			// Test case with calling CheckNumOfTheParallelJobs when there is no code processing folders.
			// As a result, want to receive true
			name: "There is no code processing folder",
			args: args{
				workingDir:        "",
				numOfParallelJobs: 0,
			},
			prepareFunc: func() {},
			want:        true,
		},
		{
			// Test case with calling CheckNumOfTheParallelJobs when there is one code processing folder.
			// As a result, want to receive true
			name: "Less than needed",
			args: args{
				workingDir:        "",
				numOfParallelJobs: 2,
			},
			prepareFunc: func() {
				err := os.MkdirAll(filepath.Join(executableFiles, "1"), fs.ModePerm)
				if err != nil {
					panic(err)
				}
			},
			want: true,
		},
		{
			// Test case with calling CheckNumOfTheParallelJobs when the number of the code processing folders is equals numOfParallelJobs.
			// As a result, want to receive false
			name: "There are enough code processing folders",
			args: args{
				workingDir:        "",
				numOfParallelJobs: 1,
			},
			prepareFunc: func() {
				err := os.MkdirAll(filepath.Join(executableFiles, "1"), fs.ModePerm)
				if err != nil {
					panic(err)
				}
			},
			want: false,
		},
		{
			// Test case with calling CheckNumOfTheParallelJobs when the number of the code processing folders is more than numOfParallelJobs.
			// As a result, want to receive false
			name: "More than needed",
			args: args{
				workingDir:        "",
				numOfParallelJobs: 0,
			},
			prepareFunc: func() {
				err := os.MkdirAll(filepath.Join(executableFiles, "1"), fs.ModePerm)
				if err != nil {
					panic(err)
				}
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareFunc()
			if got := CheckNumOfTheParallelJobs(tt.args.workingDir, tt.args.numOfParallelJobs); got != tt.want {
				t.Errorf("CheckNumOfTheParallelJobs() = %v, want %v", got, tt.want)
			}
			os.RemoveAll(executableFiles)
		})
	}
}
