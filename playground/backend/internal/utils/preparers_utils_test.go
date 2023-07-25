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
	"errors"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

const (
	sourceDir         = "sourceDir"
	fileName          = "file.txt"
	fileContent       = "content"
	testDataDir       = "test_data"
	javaFileName      = "JavaFileName.java"
	emptyFileName     = "emptyFile.java"
	pythonExampleName = "wordcount.py"
	filePermission    = 0600
	fullPermission    = 0755
)

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
	defer teardown()
	m.Run()
}

func setup() error {
	err := os.Mkdir(sourceDir, fullPermission)
	if err != nil {
		return err
	}
	filePath := filepath.Join(sourceDir, fileName)
	err = os.WriteFile(filePath, []byte(fileContent), filePermission)
	if err != nil {
		return err
	}
	sourceJavaFilePath := filepath.Join(testDataDir, javaFileName)
	javaFilePath := filepath.Join(sourceDir, javaFileName)
	err = CopyFile(sourceJavaFilePath, javaFilePath)
	if err != nil {
		return err
	}
	emptyFilePath := filepath.Join(sourceDir, emptyFileName)
	err = os.WriteFile(emptyFilePath, []byte(""), filePermission)
	if err != nil {
		return err
	}
	sourceWordCountPythonPath := filepath.Join(testDataDir, pythonExampleName)
	wordCountPythonPath := filepath.Join(sourceDir, pythonExampleName)
	err = CopyFile(sourceWordCountPythonPath, wordCountPythonPath)
	return err
}

func teardown() error {
	return os.RemoveAll(sourceDir)
}

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
			name: "Args is empty string",
			args: args{pipelineOptions: ""},
			want: "",
		},
		{
			name: "Args with one option",
			args: args{pipelineOptions: "--opt1 valOpt"},
			want: "--opt1=valOpt",
		},
		{
			name: "Args with some options",
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

func TestInitVars(t *testing.T) {
	tests := []struct {
		name string
		want []interface{}
	}{
		{
			name: "Create empty variables",
			want: []interface{}{EmptyLine, EmptyLine, errors.New(EmptyLine), false, RegularDefinition},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2, got3, got4 := InitVars()
			variables := append([]interface{}{}, got, got1, got2, got3, got4)
			if !reflect.DeepEqual(variables, tt.want) {
				t.Errorf("InitVars() variables = %v, want %v", variables, tt.want)
			}
		})
	}
}

func TestAddGraphToEndOfFile(t *testing.T) {
	txtFilePath := filepath.Join(sourceDir, fileName)
	txtFile, err := os.OpenFile(txtFilePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer txtFile.Close()
	incorrectFile, err := os.Open(txtFilePath)
	if err != nil {
		panic(err)
	}
	defer incorrectFile.Close()
	type args struct {
		spaces       string
		err          error
		tempFile     *os.File
		pipelineName string
	}
	type fields struct {
		fileContent string
		filePath    string
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		wantErr bool
	}{
		{
			name: "Add graph to the end of an existing file",
			args: args{
				spaces:       "",
				err:          nil,
				tempFile:     txtFile,
				pipelineName: uuid.New().String(),
			},
			fields: fields{
				fileContent: fileContent,
				filePath:    txtFilePath,
			},
			wantErr: false,
		},
		{
			name: "Error during write data to file",
			args: args{
				spaces:       "",
				err:          nil,
				tempFile:     incorrectFile,
				pipelineName: uuid.New().String(),
			},
			fields: fields{
				fileContent: fileContent,
				filePath:    txtFilePath,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			AddGraphToEndOfFile(tt.args.spaces, tt.args.err, tt.args.tempFile, tt.args.pipelineName)
			data, err := os.ReadFile(tt.fields.filePath)
			if err != nil {
				t.Errorf("AddGraphToEndOfFile() error during reading from a file = %v", err)
			}
			graphCode := fmt.Sprintf(pythonGraphCodePattern, tt.args.pipelineName, GraphFileName)
			graphCodeWithIndentation := strings.ReplaceAll(graphCode, indentationReplacement, tt.args.spaces)
			fileContentWithGraph := fileContent + "\n" + graphCodeWithIndentation
			if (string(data) != fileContentWithGraph) != tt.wantErr {
				t.Error("AddGraphToEndOfFile() wrong graph addition")
			}
		})
	}
}

func TestGetPublicClassName(t *testing.T) {
	javaPublicClassNamePattern := "public class (.*?) [{|implements(.*)]"
	type args struct {
		filePath string
		pattern  string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Get public class name from existing java file",
			args: args{
				filePath: filepath.Join(sourceDir, javaFileName),
				pattern:  javaPublicClassNamePattern,
			},
			want:    "MinimalWordCount",
			wantErr: false,
		},
		{
			name: "Get public class name from empty file",
			args: args{
				filePath: filepath.Join(sourceDir, emptyFileName),
				pattern:  javaPublicClassNamePattern,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Get public class name from non-existent file",
			args: args{
				filePath: filepath.Join(sourceDir, "file1.java"),
				pattern:  javaPublicClassNamePattern,
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetPublicClassName(tt.args.filePath, tt.args.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPublicClassName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetPublicClassName() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAddNewLine(t *testing.T) {
	javaFile, err := os.OpenFile(filepath.Join(sourceDir, javaFileName), os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer javaFile.Close()
	txtFile, err := os.Open(filepath.Join(sourceDir, fileName))
	if err != nil {
		panic(err)
	}
	defer txtFile.Close()
	type args struct {
		newLine bool
		file    *os.File
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "No line added to file",
			args: args{
				newLine: false,
				file:    nil,
			},
			wantErr: false,
		},
		{
			name: "Add a new line to an existing javaFile",
			args: args{
				newLine: true,
				file:    javaFile,
			},
			wantErr: false,
		},
		{
			name: "Error during write data to file",
			args: args{
				newLine: true,
				file:    txtFile,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := AddNewLine(tt.args.newLine, tt.args.file); (err != nil) != tt.wantErr {
				t.Errorf("AddNewLine() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProcessLine(t *testing.T) {
	pipelineName := uuid.New().String()
	pythonExample, err := os.OpenFile(filepath.Join(sourceDir, pythonExampleName), os.O_RDWR, fullPermission)
	if err != nil {
		panic(err)
	}
	defer pythonExample.Close()
	findPipelinePattern := `^(\s*)(.+) = beam.Pipeline`
	findWithPipelinePattern := `(\s*)with.+Pipeline.+as (.+):`
	emptyLine := EmptyLine

	type args struct {
		curLine      string
		pipelineName *string
		spaces       *string
		regs         *[]*regexp.Regexp
		tempFile     *os.File
		err          error
	}
	tests := []struct {
		name        string
		args        args
		want        bool
		wantDefType PipelineDefinitionType
		wantErr     bool
	}{
		{
			name: "Empty current line",
			args: args{
				curLine:      "",
				pipelineName: &pipelineName,
				spaces:       &emptyLine,
				regs: &[]*regexp.Regexp{
					regexp.MustCompile(findPipelinePattern),
					regexp.MustCompile(findWithPipelinePattern),
				},
				tempFile: pythonExample,
				err:      errors.New(EmptyLine),
			},
			want:        false,
			wantDefType: RegularDefinition,
			wantErr:     false,
		},
		{
			name: "With correct line",
			args: args{
				curLine:      "with beam.Pipeline(options=pipeline_options) as p:",
				pipelineName: &pipelineName,
				spaces:       &emptyLine,
				regs: &[]*regexp.Regexp{
					regexp.MustCompile(findPipelinePattern),
					regexp.MustCompile(findWithPipelinePattern),
				},
				tempFile: pythonExample,
				err:      errors.New(EmptyLine),
			},
			want:        true,
			wantDefType: RegularDefinition,
			wantErr:     false,
		},
		{
			name: "With empty pipelineId",
			args: args{
				curLine:      "with beam.Pipeline(options=pipeline_options) as p:",
				pipelineName: &emptyLine,
				spaces:       &emptyLine,
				regs: &[]*regexp.Regexp{
					regexp.MustCompile(findPipelinePattern),
					regexp.MustCompile(findWithPipelinePattern),
				},
				tempFile: pythonExample,
				err:      errors.New(EmptyLine),
			},
			want:        false,
			wantDefType: WithDefinition,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotDefType, err := ProcessLine(tt.args.curLine, tt.args.pipelineName, tt.args.spaces, tt.args.regs, tt.args.tempFile, tt.args.err)
			if (err != nil) != tt.wantErr {
				t.Errorf("ProcessLine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ProcessLine() got = %v, want %v", got, tt.want)
			}
			if gotDefType != tt.wantDefType {
				t.Errorf("ProcessLine() gotDefType = %v, want %v", gotDefType, tt.wantDefType)
			}
		})
	}
}

func TestRenameSourceCodeFile(t *testing.T) {
	type args struct {
		filePath  string
		className string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Rename an existing file",
			args: args{
				filePath:  filepath.Join(sourceDir, fileName),
				className: "MOCK_CLASS_NAME",
			},
			wantErr: false,
		},
		{
			name: "Rename non-existent file",
			args: args{
				filePath:  filepath.Join(sourceDir, "file1.txt"),
				className: "MOCK_CLASS_NAME",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := RenameSourceCodeFile(tt.args.filePath, tt.args.className); (err != nil) != tt.wantErr {
				t.Errorf("RenameSourceCodeFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
