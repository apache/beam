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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestSpacesToEqualsOption(t *testing.T) {
	type args struct {
		pipelineOptions string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "args is empty string",
			args: args{pipelineOptions: ""},
			want: "",
		},
		{
			name: "args with one option",
			args: args{pipelineOptions: "--opt1 valOpt"},
			want: "--opt1=valOpt",
		},
		{
			name: "args with some options",
			args: args{pipelineOptions: "--opt1 valOpt --opt2 valOpt --opt3 valOpt"},
			want: "--opt1=valOpt --opt2=valOpt --opt3=valOpt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReplaceSpacesWithEquals(tt.args.pipelineOptions); got != tt.want {
				t.Errorf("ReplaceSpacesWithEquals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ChangeTestFileName(t *testing.T) {
	codeWithPublicClass := "package org.apache.beam.sdk.transforms; \n public class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	path = filepath.Join(path, "temp.java")
	err = ioutil.WriteFile(path, []byte(codeWithPublicClass), 0644)
	if err != nil {
		panic(err)
	}
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		wantName string
	}{
		{
			// Test that file changes its name to the name of its public class
			name:     "file with java unit test code to be renamed",
			args:     args{[]interface{}{path, "public class (.*?) [{|implements(.*)]"}},
			wantErr:  false,
			wantName: "Class.java",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ChangeTestFileName(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("changeTestFileName() error = %v, wantErr %v", err, tt.wantErr)
			}
			path, _ = os.Getwd()
			files, err := filepath.Glob(fmt.Sprintf("%s/*java", path))
			if err != nil {
				t.Errorf("changeTestFileName() error = %v, wantErr %v", err, tt.wantErr)
			}
			if filepath.Base(files[0]) != "Class.java" {
				t.Errorf("changeTestFileName() expected name = %v, got %v", tt.wantName, filepath.Base(files[0]))
			}
		})
	}
}
