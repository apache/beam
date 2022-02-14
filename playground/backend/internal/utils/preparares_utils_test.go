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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/validators"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"sync"
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

func Test_ChangeJavaTestFileName(t *testing.T) {
	codeWithPublicClass := "package org.apache.beam.sdk.transforms; \n public class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, uuid.New(), filepath.Join(path, "temp"))
	_ = lc.CreateFolders()
	defer os.RemoveAll(filepath.Join(path, "temp"))
	_ = lc.CreateSourceCodeFile(codeWithPublicClass)
	validationResults := sync.Map{}
	validationResults.Store(validators.UnitTestValidatorName, true)

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
			args:     args{[]interface{}{lc.Paths.AbsoluteSourceFilePath, &validationResults}},
			wantErr:  false,
			wantName: "Class.java",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ChangeTestFileName(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("changeTestFileName() error = %v, wantErr %v", err, tt.wantErr)
			}
			files, err := filepath.Glob(fmt.Sprintf("%s/*java", lc.Paths.AbsoluteSourceFileFolderPath))
			if err != nil {
				t.Errorf("changeTestFileName() error = %v, wantErr %v", err, tt.wantErr)
			}
			if filepath.Base(files[0]) != "Class.java" {
				t.Errorf("changeTestFileName() expected name = %v, got %v", tt.wantName, filepath.Base(files[0]))
			}
		})
	}
}
