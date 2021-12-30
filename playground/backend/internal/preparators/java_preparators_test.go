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

package preparators

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/validators"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const pipelinesFolder = "executable_files"

func Test_replace(t *testing.T) {
	codeWithPublicClass := "package org.apache.beam.sdk.transforms; \n public class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
	codeWithoutPublicClass := "package org.apache.beam.sdk.transforms; \n class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
	codeWithImportedPackage := "import org.apache.beam.sdk.transforms.*; \n class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, uuid.New(), filepath.Join(path, "temp", pipelinesFolder))
	_ = lc.CreateFolders()
	defer os.RemoveAll(filepath.Join(path, "temp"))
	_, _ = lc.CreateSourceCodeFile(codeWithPublicClass)

	type args struct {
		args []interface{}
	}
	tests := []struct {
		name     string
		args     args
		wantCode string
		wantErr  bool
	}{
		{
			name:    "original file doesn't exist",
			args:    args{[]interface{}{"someFile.java", classWithPublicModifierPattern, classWithoutPublicModifierPattern}},
			wantErr: true,
		},
		{
			name:     "original file exists",
			args:     args{[]interface{}{lc.GetAbsoluteSourceFilePath(), classWithPublicModifierPattern, classWithoutPublicModifierPattern}},
			wantCode: codeWithoutPublicClass,
			wantErr:  false,
		},
		{
			// Test that file where package is used changes to import all dependencies from this package
			name:     "original file with package",
			args:     args{[]interface{}{lc.GetAbsoluteSourceFilePath(), packagePattern, importStringPattern}},
			wantCode: codeWithImportedPackage,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := replace(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("removePublicClassModifier() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				data, err := os.ReadFile(tt.args.args[0].(string))
				if err != nil {
					t.Errorf("removePublicClassModifier() unexpected error = %v", err)
				}
				if !strings.EqualFold(string(data), tt.wantCode) {
					t.Errorf("removePublicClassModifier() code = {%v}, wantCode {%v}", string(data), tt.wantCode)
				}
			}
		})
	}
}

func TestGetJavaPreparators(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "all success",
			args: args{"MOCK_FILEPATH"},
			want: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetJavaPreparators(tt.args.filePath); len(*got) != tt.want {
				t.Errorf("GetJavaPreparation() returns %v Preparators, want %v", len(*got), tt.want)
			}
		})
	}
}

func Test_changeJavaTestFileName(t *testing.T) {
	codeWithPublicClass := "package org.apache.beam.sdk.transforms; \n public class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, uuid.New(), filepath.Join(path, "temp", pipelinesFolder))
	_ = lc.CreateFolders()
	defer os.RemoveAll(filepath.Join(path, "temp"))
	_, _ = lc.CreateSourceCodeFile(codeWithPublicClass)
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
			args:     args{[]interface{}{lc.GetAbsoluteSourceFilePath(), &validationResults}},
			wantErr:  false,
			wantName: "Class.java",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := changeJavaTestFileName(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("changeJavaTestFileName() error = %v, wantErr %v", err, tt.wantErr)
			}
			files, err := filepath.Glob(fmt.Sprintf("%s/*java", lc.GetAbsoluteSourceFolderPath()))
			if err != nil {
				t.Errorf("changeJavaTestFileName() error = %v, wantErr %v", err, tt.wantErr)
			}
			if filepath.Base(files[0]) != "Class.java" {
				t.Errorf("changeJavaTestFileName() expected name = %v, got %v", tt.wantName, filepath.Base(files[0]))
			}
		})
	}
}
