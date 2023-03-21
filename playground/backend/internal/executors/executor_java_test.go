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
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const (
	testJarsPaths = "jar1:jar2:jar3"
)

func TestMain(m *testing.M) {
	getBeamJars = func() (string, error) {
		return testJarsPaths, nil
	}
	exitValue := m.Run()
	os.Exit(exitValue)
}

func TestGetJavaCompileCmd(t *testing.T) {
	ctx := context.Background()

	type args struct {
		paths *fs_tool.LifeCyclePaths
	}

	workingDir := "testdir/"

	expectedCompileArgs := []string{"-d", "bin", "-parameters", "-classpath", testJarsPaths}
	expectedSourceFiles := []string{"abc.java", "def.java"}

	tests := []struct {
		name    string
		args    args
		want    *exec.Cmd
		wantErr bool
	}{
		{
			name: "TestGetJavaCompileCmd",
			args: args{
				paths: &fs_tool.LifeCyclePaths{
					AbsoluteBaseFolderPath: workingDir,
					GetSourceFiles: func() ([]string, error) {
						return expectedSourceFiles, nil
					},
				},
			},
			want: func() *exec.Cmd {
				cmd := exec.CommandContext(ctx, javaCompileCmd, append(expectedCompileArgs, expectedSourceFiles...)...)
				cmd.Dir = workingDir
				return cmd
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getJavaCompileCmd(ctx, tt.args.paths)
			if tt.wantErr && (err == nil || err.Error() != "test error") {
				t.Errorf("getJavaCompileCmd() expected error, got %v", got)
			}
			if !tt.wantErr && !cmp.Equal(got, tt.want, comparer) {
				t.Errorf("getJavaCompileCmd() got/want diff = %v", cmp.Diff(got, tt.want, comparer))
			}
		})
	}
}

func TestGetJavaRunCmd(t *testing.T) {
	ctx := context.Background()

	type args struct {
		paths           *fs_tool.LifeCyclePaths
		pipelineOptions string
	}

	workingdir := "testdir/"

	logConfigFilePath := filepath.Join(workingdir, javaLogConfigFileName)

	pipelineOptions := "--output t.txt"
	expectedPipelineOptions := []string{"--output=t.txt"}

	expectedRunArgs := []string{"-cp", fmt.Sprintf("bin:%s", testJarsPaths)}
	expectedExecutableName := "TestExecutable"

	tests := []struct {
		name    string
		args    args
		want    *exec.Cmd
		wantErr bool
	}{
		{
			name: "TestGetJavaRunCmd",
			args: args{
				paths: &fs_tool.LifeCyclePaths{
					AbsoluteBaseFolderPath: workingdir,
					FindExecutableName: func(_ context.Context) (string, error) {
						return expectedExecutableName, nil
					},
				},
				pipelineOptions: pipelineOptions,
			},
			want: func() *exec.Cmd {
				cmd := exec.CommandContext(ctx, javaRunCmd,
					append(
						append(
							expectedRunArgs,
							fmt.Sprintf("%s=%s", javaLoggConfigOption, logConfigFilePath),
							expectedExecutableName,
						),
						expectedPipelineOptions...,
					)...,
				)
				cmd.Dir = workingdir
				return cmd
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getJavaRunCmd(ctx, tt.args.paths, tt.args.pipelineOptions)
			if tt.wantErr && (err == nil || err.Error() != "test error") {
				t.Errorf("getJavaRunCmd() expected error, got %v", got)
			}
			if !tt.wantErr && !cmp.Equal(got, tt.want, comparer) {
				t.Errorf("getJavaRunCmd() got/want diff = %v", cmp.Diff(got, tt.want, comparer))
			}
		})
	}
}

func TestGetJavaRunTestCmd(t *testing.T) {
	ctx := context.Background()

	type args struct {
		paths *fs_tool.LifeCyclePaths
	}

	workingdir := "testdir/"

	expectedTestArgs := []string{"-cp", fmt.Sprintf("bin:%s", testJarsPaths), "org.junit.runner.JUnitCore"}
	expectedExecutableName := "TestExecutable"

	tests := []struct {
		name    string
		args    args
		want    *exec.Cmd
		wantErr bool
	}{
		{
			name: "TestGetJavaRunTestCmd",
			args: args{
				paths: &fs_tool.LifeCyclePaths{
					AbsoluteBaseFolderPath: workingdir,
					FindTestExecutableName: func(_ context.Context) (string, error) {
						return expectedExecutableName, nil
					},
				},
			},
			want: func() *exec.Cmd {
				cmd := exec.CommandContext(ctx, javaTestCmd,
					append(
						expectedTestArgs,
						expectedExecutableName,
					)...,
				)
				cmd.Dir = workingdir
				return cmd
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getJavaRunTestCmd(ctx, tt.args.paths)
			if tt.wantErr && (err == nil || err.Error() != "test error") {
				t.Errorf("getJavaRunTestCmd() expected error, got %v", got)
			}
			if !tt.wantErr && !cmp.Equal(got, tt.want, comparer) {
				t.Errorf("getJavaRunTestCmd() got/want diff = %v", cmp.Diff(got, tt.want, comparer))
			}
		})
	}
}
