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
	"fmt"
	"github.com/google/go-cmp/cmp"
	"os/exec"
	"testing"
)

func TestGetScioRunCmd(t *testing.T) {
	ctx := context.Background()

	pipelineOptions := "--output t.txt"
	pipelineOptionsExpected := "--output=t.txt"
	folderPath := "testpath/"
	executableName := "TestExecutable"

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
			name: "TestGetScioRunCmd",
			args: args{
				paths: &fs_tool.LifeCyclePaths{
					ProjectDir: folderPath,
					FindExecutableName: func(_ context.Context) (string, error) {
						return executableName, nil
					},
				},
				pipelineOptions: pipelineOptions,
			},
			want: func() *exec.Cmd {
				cmd := exec.CommandContext(ctx, scioRunCmd,
					fmt.Sprintf("%s %s %s", scioRunArg, executableName, pipelineOptionsExpected))
				cmd.Dir = folderPath
				return cmd
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getScioRunCmd(ctx, tt.args.paths, tt.args.pipelineOptions)
			if tt.wantErr && (err == nil || err.Error() != "test error") {
				t.Errorf("getScioRunCmd() expected error, got %v", got)
			}
			if !tt.wantErr && !cmp.Equal(got, tt.want, comparer) {
				t.Errorf("getScioRunCmd() got/want diff = %v", cmp.Diff(got, tt.want, comparer))
			}
		})
	}
}

func TestGetScioRunTestCmd(t *testing.T) {
	ctx := context.Background()

	workingDir := "sourcepath/"
	sourcePath := "sourcepath/file.py"

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
			name: "TestGetScioRunTestCmd",
			args: args{
				paths: &fs_tool.LifeCyclePaths{
					AbsoluteSourceFileFolderPath: workingDir,
					AbsoluteSourceFilePath:       sourcePath,
				},
			},
			want: func() *exec.Cmd {
				cmd := exec.CommandContext(ctx, scioTestCmd, sourcePath)
				cmd.Dir = workingDir
				return cmd
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getScioRunTestCmd(ctx, tt.args.paths)
			if tt.wantErr && (err == nil || err.Error() != "test error") {
				t.Errorf("getScioRunTestCmd() expected error, got %v", got)
			}
			if !tt.wantErr && !cmp.Equal(got, tt.want, comparer) {
				t.Errorf("getScioRunTestCmd() got/want diff = %v", cmp.Diff(got, tt.want, comparer))
			}
		})
	}
}
