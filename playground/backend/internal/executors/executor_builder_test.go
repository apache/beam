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
	"reflect"
	"testing"
)

var handlers []handler

func TestMain(m *testing.M) {
	handlers = []handler{
		func(e *Executor) {
			e.testArgs.fileNames = append(e.testArgs.fileNames, "file name")
		},
		func(e *Executor) {
			e.runArgs.pipelineOptions = []string{"--opt val"}
		},
	}
	m.Run()
}

func TestNewExecutorBuilder(t *testing.T) {
	tests := []struct {
		name string
		want *ExecutorBuilder
	}{
		{
			name: "Get executor builder",
			want: &ExecutorBuilder{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewExecutorBuilder(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewExecutorBuilder() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutorBuilder_WithCompiler(t *testing.T) {
	type fields struct {
		actions []handler
	}
	tests := []struct {
		name   string
		fields fields
		want   *CompileBuilder
	}{
		{
			name:   "Get CompileBuilder with prepared actions",
			fields: fields{actions: handlers},
			want:   &CompileBuilder{ExecutorBuilder{actions: handlers}},
		},
		{
			name:   "Get CompileBuilder with empty actions",
			fields: fields{actions: []handler{}},
			want:   &CompileBuilder{ExecutorBuilder{actions: []handler{}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ExecutorBuilder{
				actions: tt.fields.actions,
			}
			if got := b.WithCompiler(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithCompiler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutorBuilder_WithRunner(t *testing.T) {
	type fields struct {
		actions []handler
	}
	tests := []struct {
		name   string
		fields fields
		want   *RunBuilder
	}{
		{
			name:   "Get RunBuilder with prepared actions",
			fields: fields{actions: handlers},
			want:   &RunBuilder{ExecutorBuilder{actions: handlers}},
		},
		{
			name:   "Get RunBuilder with empty actions",
			fields: fields{actions: []handler{}},
			want:   &RunBuilder{ExecutorBuilder{actions: []handler{}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ExecutorBuilder{
				actions: tt.fields.actions,
			}
			if got := b.WithRunner(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithRunner() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutorBuilder_WithValidator(t *testing.T) {
	type fields struct {
		actions []handler
	}
	tests := []struct {
		name   string
		fields fields
		want   *ValidatorBuilder
	}{
		{
			name:   "Get ValidatorBuilder with prepared actions",
			fields: fields{actions: handlers},
			want:   &ValidatorBuilder{ExecutorBuilder{actions: handlers}},
		},
		{
			name:   "Get ValidatorBuilder with empty actions",
			fields: fields{actions: []handler{}},
			want:   &ValidatorBuilder{ExecutorBuilder{actions: []handler{}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ExecutorBuilder{
				actions: tt.fields.actions,
			}
			if got := b.WithValidator(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithValidator() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutorBuilder_WithPreparer(t *testing.T) {
	type fields struct {
		actions []handler
	}
	tests := []struct {
		name   string
		fields fields
		want   *PreparerBuilder
	}{
		{
			name:   "Get PreparerBuilder with prepared actions",
			fields: fields{actions: handlers},
			want:   &PreparerBuilder{ExecutorBuilder{actions: handlers}},
		},
		{
			name:   "Get PreparerBuilder with empty actions",
			fields: fields{actions: []handler{}},
			want:   &PreparerBuilder{ExecutorBuilder{actions: []handler{}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ExecutorBuilder{
				actions: tt.fields.actions,
			}
			if got := b.WithPreparer(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithPreparer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutorBuilder_WithTestRunner(t *testing.T) {
	type fields struct {
		actions []handler
	}
	tests := []struct {
		name   string
		fields fields
		want   *UnitTestExecutorBuilder
	}{
		{
			name:   "Get UnitTestExecutorBuilder with prepared actions",
			fields: fields{actions: handlers},
			want:   &UnitTestExecutorBuilder{ExecutorBuilder{actions: handlers}},
		},
		{
			name:   "Get UnitTestExecutorBuilder with empty actions",
			fields: fields{actions: []handler{}},
			want:   &UnitTestExecutorBuilder{ExecutorBuilder{actions: []handler{}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ExecutorBuilder{
				actions: tt.fields.actions,
			}
			if got := b.WithTestRunner(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithTestRunner() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutorBuilder_Build(t *testing.T) {
	type fields struct {
		actions []handler
	}
	tests := []struct {
		name   string
		fields fields
		want   Executor
	}{
		{
			name: "Builds the executor object",
			fields: fields{actions: []handler{func(e *Executor) {
				e.runArgs.pipelineOptions = []string{pipelineOptions}
			}}},
			want: Executor{
				compileArgs: CmdConfiguration{},
				runArgs:     CmdConfiguration{pipelineOptions: []string{pipelineOptions}},
				testArgs:    CmdConfiguration{},
				validators:  nil,
				preparers:   nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ExecutorBuilder{
				actions: tt.fields.actions,
			}
			if got := b.Build(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Build() = %v, want %v", got, tt.want)
			}
		})
	}
}
