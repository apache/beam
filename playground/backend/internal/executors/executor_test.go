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

package executors

import (
	"context"
	"os/exec"
	"reflect"
	"sync"
	"testing"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/preparers"
	"beam.apache.org/playground/backend/internal/validators"
)

const pipelineOptions = "--output t.txt"

func TestExecutor_Compile(t *testing.T) {
	type fields struct {
		compileArgs CmdConfiguration
		runArgs     CmdConfiguration
		validators  []validators.Validator
	}
	tests := []struct {
		name   string
		fields fields
		want   *exec.Cmd
	}{
		{
			name: "TestCompile",
			fields: fields{
				compileArgs: CmdConfiguration{
					fileNames:       []string{"filePath"},
					workingDir:      "./",
					commandName:     "testCommand",
					commandArgs:     []string{"-d", "bin", "-parameters", "-classpath", "/opt/apache/beam/jars/beam-sdks-java-harness.jar"},
					pipelineOptions: []string{""},
				},
			},
			want: &exec.Cmd{
				Path:         "testCommand",
				Args:         []string{"javac", "-d", "bin", "-parameters", "-classpath", "/opt/apache/beam/jars/beam-sdks-java-harness.jar", "filePath"},
				Env:          nil,
				Dir:          "",
				Stdin:        nil,
				Stdout:       nil,
				Stderr:       nil,
				ExtraFiles:   nil,
				SysProcAttr:  nil,
				Process:      nil,
				ProcessState: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex := &Executor{
				compileArgs: tt.fields.compileArgs,
				runArgs:     tt.fields.runArgs,
				validators:  tt.fields.validators,
			}
			if got := ex.Compile(context.Background()); !reflect.DeepEqual(got.String(), tt.want.String()) {
				t.Errorf("WithCompiler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutor_Run(t *testing.T) {
	type fields struct {
		compileArgs CmdConfiguration
		runArgs     CmdConfiguration
		testArgs    CmdConfiguration
		validators  []validators.Validator
		preparers   []preparers.Preparer
	}
	tests := []struct {
		name   string
		fields fields
		want   *exec.Cmd
	}{
		{
			name: "TestRun",
			fields: fields{
				runArgs: CmdConfiguration{
					fileNames:   []string{"HelloWorld"},
					workingDir:  "./",
					commandName: "runCommand",
					commandArgs: []string{"-cp", "bin:/opt/apache/beam/jars/beam-sdks-java-harness.jar:" +
						"/opt/apache/beam/jars/beam-runners-direct.jar:/opt/apache/beam/jars/slf4j-jdk14.jar"},
					pipelineOptions: []string{""},
				},
			},
			want: &exec.Cmd{
				Path: "runCommand",
				Args: []string{"java", "-cp", "bin:/opt/apache/beam/jars/beam-sdks-java-harness.jar:" +
					"/opt/apache/beam/jars/beam-runners-direct.jar:/opt/apache/beam/jars/slf4j-jdk14.jar", "HelloWorld"},
				Env:          nil,
				Dir:          "",
				Stdin:        nil,
				Stdout:       nil,
				Stderr:       nil,
				ExtraFiles:   nil,
				SysProcAttr:  nil,
				Process:      nil,
				ProcessState: nil,
			},
		},
		{
			name: "TestRun with pipelineOptions",
			fields: fields{
				runArgs: CmdConfiguration{
					fileNames:   []string{"HelloWorld"},
					workingDir:  "./",
					commandName: "runCommand",
					commandArgs: []string{"-cp", "bin:/opt/apache/beam/jars/beam-sdks-java-harness.jar:" +
						"/opt/apache/beam/jars/beam-runners-direct.jar:/opt/apache/beam/jars/slf4j-jdk14.jar"},
					pipelineOptions: []string{pipelineOptions},
				},
			},
			want: &exec.Cmd{
				Path: "runCommand",
				Args: []string{"java", "-cp", "bin:/opt/apache/beam/jars/beam-sdks-java-harness.jar:" +
					"/opt/apache/beam/jars/beam-runners-direct.jar:/opt/apache/beam/jars/slf4j-jdk14.jar", "HelloWorld", pipelineOptions},
				Env:          nil,
				Dir:          "",
				Stdin:        nil,
				Stdout:       nil,
				Stderr:       nil,
				ExtraFiles:   nil,
				SysProcAttr:  nil,
				Process:      nil,
				ProcessState: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex := &Executor{
				compileArgs: tt.fields.compileArgs,
				runArgs:     tt.fields.runArgs,
				testArgs:    tt.fields.testArgs,
				validators:  tt.fields.validators,
				preparers:   tt.fields.preparers,
			}
			if got := ex.Run(context.Background()); !reflect.DeepEqual(got.String(), tt.want.String()) {
				t.Errorf("WithRunner() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutor_RunTest(t *testing.T) {
	type fields struct {
		compileArgs CmdConfiguration
		runArgs     CmdConfiguration
		testArgs    CmdConfiguration
		validators  []validators.Validator
		preparers   []preparers.Preparer
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *exec.Cmd
	}{
		{
			name: "TestRunTest",
			fields: fields{
				testArgs: CmdConfiguration{
					fileNames:       []string{"HelloWorld"},
					workingDir:      "./",
					commandName:     "testCommand",
					commandArgs:     []string{"-cp", "option1:option2"},
					pipelineOptions: []string{""},
				},
			},
			args: args{context.Background()},
			want: &exec.Cmd{
				Path:         "testCommand",
				Args:         []string{"java", "-cp", "option1:option2", "HelloWorld"},
				Env:          nil,
				Dir:          "./",
				Stdin:        nil,
				Stdout:       nil,
				Stderr:       nil,
				ExtraFiles:   nil,
				SysProcAttr:  nil,
				Process:      nil,
				ProcessState: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex := &Executor{
				compileArgs: tt.fields.compileArgs,
				runArgs:     tt.fields.runArgs,
				testArgs:    tt.fields.testArgs,
				validators:  tt.fields.validators,
				preparers:   tt.fields.preparers,
			}
			if got := ex.RunTest(tt.args.ctx); !reflect.DeepEqual(got.String(), tt.want.String()) {
				t.Errorf("RunTest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutor_Prepare(t *testing.T) {
	valResult := &sync.Map{}
	prepareParams := make(map[string]string)
	valResult.Store(validators.UnitTestValidatorName, false)
	valResult.Store(validators.KatasValidatorName, false)
	preparersArray, err := preparers.GetPreparers(pb.Sdk_SDK_JAVA, "./", valResult, prepareParams)
	if err != nil {
		panic(err)
	}
	type fields struct {
		compileArgs CmdConfiguration
		runArgs     CmdConfiguration
		testArgs    CmdConfiguration
		validators  []validators.Validator
		preparers   []preparers.Preparer
		boolChan    chan bool
		errorChan   chan error
		valResult   *sync.Map
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
		{
			name: "Test Prepare method with prepared preparers",
			fields: fields{
				preparers: *preparersArray,
				boolChan:  make(chan bool),
				errorChan: make(chan error),
				valResult: valResult,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Test Prepare method without preparers",
			fields: fields{
				preparers: nil,
				boolChan:  make(chan bool),
				errorChan: make(chan error),
				valResult: valResult,
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex := &Executor{
				compileArgs: tt.fields.compileArgs,
				runArgs:     tt.fields.runArgs,
				testArgs:    tt.fields.testArgs,
				validators:  tt.fields.validators,
				preparers:   tt.fields.preparers,
			}
			prepareFunc := ex.Prepare()
			go prepareFunc(tt.fields.boolChan, tt.fields.errorChan, tt.fields.valResult)
			if tt.wantErr {
				err := <-tt.fields.errorChan
				if (err != nil) != tt.wantErr {
					t.Errorf("Prepare() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
			got := <-tt.fields.boolChan
			if got != tt.want {
				t.Errorf("Prepare() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutor_Validate(t *testing.T) {
	valResult := &sync.Map{}
	validatorsArray, err := validators.GetValidators(pb.Sdk_SDK_JAVA, "./")
	if err != nil {
		panic(err)
	}
	type fields struct {
		compileArgs CmdConfiguration
		runArgs     CmdConfiguration
		testArgs    CmdConfiguration
		validators  []validators.Validator
		preparers   []preparers.Preparer
		boolChan    chan bool
		errorChan   chan error
		valResult   *sync.Map
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
		{
			name: "Test Validate method with prepared validators",
			fields: fields{
				validators: *validatorsArray,
				boolChan:   make(chan bool),
				errorChan:  make(chan error),
				valResult:  valResult,
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "Test Validate method without validators",
			fields: fields{
				validators: nil,
				boolChan:   make(chan bool),
				errorChan:  make(chan error),
				valResult:  valResult,
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex := &Executor{
				compileArgs: tt.fields.compileArgs,
				runArgs:     tt.fields.runArgs,
				testArgs:    tt.fields.testArgs,
				validators:  tt.fields.validators,
				preparers:   tt.fields.preparers,
			}
			valFunc := ex.Validate()
			go valFunc(tt.fields.boolChan, tt.fields.errorChan, tt.fields.valResult)
			if tt.wantErr {
				err := <-tt.fields.errorChan
				if (err != nil) != tt.wantErr {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
			got := <-tt.fields.boolChan
			if got != tt.want {
				t.Errorf("Validate() = %v, want %v", got, tt.want)
			}
		})
	}
}
