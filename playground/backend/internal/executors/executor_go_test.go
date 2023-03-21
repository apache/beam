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
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"errors"
	"github.com/google/go-cmp/cmp"
	"os/exec"
	"testing"
)

var comparer = cmp.Comparer(func(a, b *exec.Cmd) bool {
	return a.Dir == b.Dir && a.Path == b.Path && cmp.Equal(a.Args, b.Args)
})

func TestGetGoCompileCmd(t *testing.T) {
	ctx := context.Background()

	folderPath := "testpath/"
	goSources := []string{
		"abc",
		"def",
	}

	tests := []struct {
		name    string
		args    *fs_tool.LifeCyclePaths
		want    *exec.Cmd
		wantErr bool
	}{
		{
			name: "TestCompileCmd",
			args: &fs_tool.LifeCyclePaths{
				AbsoluteBaseFolderPath: folderPath,
				GetSourceFiles: func() ([]string, error) {
					return goSources, nil
				},
			},
			want: func() *exec.Cmd {
				cmd := exec.CommandContext(ctx, goCompileCmd, append(goCompileArgs, goSources...)...)
				cmd.Dir = folderPath
				return cmd
			}(),
			wantErr: false,
		},
		{
			name: "TestCompileCmd error",
			args: &fs_tool.LifeCyclePaths{
				AbsoluteBaseFolderPath: folderPath,
				GetSourceFiles: func() ([]string, error) {
					return nil, errors.New("test error")
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getGoCompileCmd(ctx, tt.args)
			if tt.wantErr && (err == nil || err.Error() != "test error") {
				t.Errorf("getGoCompileCmd() expected error, got %v", got)
			}
			if !tt.wantErr && !cmp.Equal(got, tt.want, comparer) {
				t.Errorf("getGoCompileCmd() got/want diff = %v", cmp.Diff(got, tt.want, comparer))
			}
		})
	}
}

func TestGetGoRunCmd(t *testing.T) {
	ctx := context.Background()

	pipelineOptions := "--output t.txt"
	pipelineOptionsSplit := []string{"--output", "t.txt"}
	folderPath := "testpath/"
	executableName := "testexecutable"

	type args struct {
		paths           *fs_tool.LifeCyclePaths
		pipelineOptions string
	}

	tests := []struct {
		name    string
		args    args
		want    *exec.Cmd
		wantErr bool
	}{
		{
			name: "TestGetGoRunCmd",
			args: args{
				paths: &fs_tool.LifeCyclePaths{
					AbsoluteExecutableFilePath: executableName,
					AbsoluteBaseFolderPath:     folderPath,
				},
				pipelineOptions: pipelineOptions,
			},
			want: func() *exec.Cmd {
				cmd := exec.CommandContext(ctx, executableName, pipelineOptionsSplit...)
				cmd.Dir = folderPath
				return cmd
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getGoRunCmd(ctx, tt.args.paths, tt.args.pipelineOptions)
			if tt.wantErr && (err == nil || err.Error() != "test error") {
				t.Errorf("getGoRunCmd() expected error, got %v", got)
			}
			if !tt.wantErr && !cmp.Equal(got, tt.want, comparer) {
				t.Errorf("getGoRunCmd() got/want diff = %v", cmp.Diff(got, tt.want, comparer))
			}
		})
	}
}

func TestGetGoRunTestCmd(t *testing.T) {
	ctx := context.Background()

	sourcePath := "sourcepath/"

	type args struct {
		paths *fs_tool.LifeCyclePaths
	}

	tests := []struct {
		name    string
		args    args
		want    *exec.Cmd
		wantErr bool
	}{
		{
			name: "TestGetGoRunCmd",
			args: args{
				paths: &fs_tool.LifeCyclePaths{
					AbsoluteSourceFileFolderPath: sourcePath,
				},
			},
			want: func() *exec.Cmd {
				cmd := exec.CommandContext(ctx, goTestCmd, append(goTestArgs, sourcePath)...)
				cmd.Dir = sourcePath
				return cmd
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getGoRunTestCmd(ctx, tt.args.paths)
			if tt.wantErr && (err == nil || err.Error() != "test error") {
				t.Errorf("getGoRunTestCmd() expected error, got %v", got)
			}
			if !tt.wantErr && !cmp.Equal(got, tt.want, comparer) {
				t.Errorf("getGoRunTestCmd() got/want diff = %v", cmp.Diff(got, tt.want, comparer))
			}
		})
	}
}
