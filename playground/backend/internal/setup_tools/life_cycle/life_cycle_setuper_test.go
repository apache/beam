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

package life_cycle

import (
	playground "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"fmt"
	"github.com/google/uuid"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

const (
	workingDir                = "workingDir"
	sourceFolder              = "src"
	executableFolder          = "bin"
	javaSourceFileExtension   = ".java"
	javaCompiledFileExtension = ".class"
	pipelinesFolder           = "executable_files"
	logFileName               = "logs.log"
)

func TestSetup(t *testing.T) {
	errorPipelineId := uuid.New()
	successPipelineId := uuid.New()
	baseFileFolder, _ := filepath.Abs(filepath.Join(workingDir, pipelinesFolder, successPipelineId.String()))
	srcFileFolder := filepath.Join(baseFileFolder, sourceFolder)
	execFileFolder := filepath.Join(baseFileFolder, executableFolder)

	err := os.MkdirAll(workingDir, fs.ModePerm)
	if err != nil {
		panic(err)
	}
	_, err = os.Create(filepath.Join(workingDir, javaLogConfigFileName))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(workingDir)
	type args struct {
		sdk             playground.Sdk
		code            string
		pipelineId      uuid.UUID
		workingDir      string
		preparedModDir  string
		pipelinesFolder string
	}
	tests := []struct {
		name    string
		args    args
		check   func() bool
		want    *fs_tool.LifeCycle
		wantErr bool
	}{
		{
			// Test case with calling Setup method with incorrect SDK.
			// As a result, want to receive an error.
			name: "incorrect sdk",
			args: args{
				sdk:             playground.Sdk_SDK_UNSPECIFIED,
				code:            "",
				pipelineId:      errorPipelineId,
				workingDir:      workingDir,
				pipelinesFolder: pipelinesFolder,
			},
			check: func() bool {
				if _, err := os.Stat(filepath.Join(pipelinesFolder, errorPipelineId.String())); os.IsNotExist(err) {
					return true
				}
				return false
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling Setup method with correct SDK.
			// As a result, want to receive an expected life cycle.
			name: "correct sdk",
			args: args{
				sdk:             playground.Sdk_SDK_JAVA,
				code:            "",
				pipelineId:      successPipelineId,
				workingDir:      workingDir,
				preparedModDir:  "",
				pipelinesFolder: pipelinesFolder,
			},
			check: func() bool {
				if _, err := os.Stat(filepath.Join(workingDir, pipelinesFolder, successPipelineId.String())); os.IsNotExist(err) {
					return false
				}
				if _, err := os.Stat(filepath.Join(workingDir, pipelinesFolder, successPipelineId.String(), sourceFolder)); os.IsNotExist(err) {
					return false
				}
				if _, err := os.Stat(filepath.Join(workingDir, pipelinesFolder, successPipelineId.String(), executableFolder)); os.IsNotExist(err) {
					return false
				}
				if _, err := os.Stat(filepath.Join(workingDir, pipelinesFolder, successPipelineId.String(), sourceFolder, successPipelineId.String()+javaSourceFileExtension)); os.IsNotExist(err) {
					return false
				}
				return true
			},
			want: &fs_tool.LifeCycle{
				Paths: fs_tool.LifeCyclePaths{
					SourceFileName:                   fmt.Sprintf("%s%s", successPipelineId.String(), javaSourceFileExtension),
					AbsoluteSourceFileFolderPath:     srcFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(srcFileFolder, fmt.Sprintf("%s%s", successPipelineId.String(), javaSourceFileExtension)),
					ExecutableFileName:               fmt.Sprintf("%s%s", successPipelineId.String(), javaCompiledFileExtension),
					AbsoluteExecutableFileFolderPath: execFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(execFileFolder, fmt.Sprintf("%s%s", successPipelineId.String(), javaCompiledFileExtension)),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Setup(tt.args.sdk, tt.args.code, tt.args.pipelineId, tt.args.workingDir, tt.args.pipelinesFolder, tt.args.preparedModDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("Setup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				if !checkPathsEqual(got.Paths, tt.want.Paths) {
					t.Errorf("Setup() got.Paths = %v, want %v", got.Paths, tt.want.Paths)
				}
			}
			os.RemoveAll(pipelinesFolder)
		})
	}
}

func checkPathsEqual(paths1, paths2 fs_tool.LifeCyclePaths) bool {
	return paths1.SourceFileName == paths2.SourceFileName &&
		paths1.AbsoluteSourceFileFolderPath == paths2.AbsoluteSourceFileFolderPath &&
		paths1.AbsoluteSourceFilePath == paths2.AbsoluteSourceFilePath &&
		paths1.ExecutableFileName == paths2.ExecutableFileName &&
		paths1.AbsoluteExecutableFileFolderPath == paths2.AbsoluteExecutableFileFolderPath &&
		paths1.AbsoluteExecutableFilePath == paths2.AbsoluteExecutableFilePath &&
		paths1.AbsoluteBaseFolderPath == paths2.AbsoluteBaseFolderPath &&
		paths1.AbsoluteLogFilePath == paths2.AbsoluteLogFilePath
}
