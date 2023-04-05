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
	"beam.apache.org/playground/backend/internal/emulators"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	playground "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/utils"
)

const (
	workingDir                  = "workingDir"
	sourceFolder                = "src"
	executableFolder            = "bin"
	javaSourceFileExtension     = ".java"
	javaCompiledFileExtension   = ".class"
	goSourceFileExtension       = ".go"
	goCompiledFileExtension     = ""
	scioExecutableFileExtension = ".scala"
	pipelinesFolder             = "executable_files"
	incorrectWorkingDir         = "incorrectWorkingDir"
	preparedModDir              = "preparedModDir"
	scioShContent               = "yes | sbt new spotify/scio-template.g8"
)

func TestMain(m *testing.M) {
	setup()
	defer teardown()
	m.Run()
}

func setup() {
	err := os.MkdirAll(filepath.Join(workingDir, preparedModDir), fs.ModePerm)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(incorrectWorkingDir, fs.ModePerm)
	if err != nil {
		panic(err)
	}
}

func teardown() {
	os.RemoveAll(workingDir)
	os.RemoveAll(incorrectWorkingDir)
}

func TestSetup(t *testing.T) {
	errorPipelineId := uuid.New()
	successPipelineId := uuid.New()
	absWorkingDir, _ := filepath.Abs(workingDir)
	baseFileFolder, _ := filepath.Abs(filepath.Join(workingDir, pipelinesFolder, successPipelineId.String()))
	srcFileFolder := filepath.Join(baseFileFolder, sourceFolder)
	execFileFolder := filepath.Join(baseFileFolder, executableFolder)

	scalaFileName := fmt.Sprintf("%s%s", successPipelineId.String(), scioExecutableFileExtension)
	sourceScalaFileFolder := filepath.Join(baseFileFolder, scioProjectPath)
	absScalaFileFolderPath, _ := filepath.Abs(sourceScalaFileFolder)
	absScalaFilePath, _ := filepath.Abs(filepath.Join(absScalaFileFolderPath, scalaFileName))
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
		prep    func() error
		want    *fs_tool.LifeCycle
		wantErr bool
	}{
		{
			// Test case with calling Setup method with incorrect SDK.
			// As a result, want to receive an error.
			name: "Incorrect sdk",
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
			prep: func() error {
				return nil
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling Setup method with correct SDK.
			// As a result, want to receive an expected life cycle.
			name: "Successfully setup life cycle with java sdk",
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
			prep: func() error {
				_, err := os.Create(filepath.Join(workingDir, javaLogConfigFileName))
				if err != nil {
					return err
				}
				return nil
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
					AbsoluteGraphFilePath:            filepath.Join(baseFileFolder, utils.GraphFileName),
				},
			},
			wantErr: false,
		},
		{
			name: "Error during create necessary files for the Java sdk",
			args: args{
				sdk:             playground.Sdk_SDK_JAVA,
				code:            "",
				pipelineId:      errorPipelineId,
				workingDir:      incorrectWorkingDir,
				pipelinesFolder: pipelinesFolder,
			},
			prep: func() error {
				return nil
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Error during copy go.mod for the Go sdk",
			args: args{
				sdk:             playground.Sdk_SDK_GO,
				code:            "",
				pipelineId:      errorPipelineId,
				workingDir:      workingDir,
				pipelinesFolder: pipelinesFolder,
				preparedModDir:  "",
			},
			prep: func() error {
				return nil
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Error during copy go.sum for the Go sdk",
			args: args{
				sdk:             playground.Sdk_SDK_GO,
				code:            "",
				pipelineId:      errorPipelineId,
				workingDir:      workingDir,
				pipelinesFolder: pipelinesFolder,
				preparedModDir:  filepath.Join(workingDir, preparedModDir),
			},
			prep: func() error {
				_, err := os.Create(filepath.Join(workingDir, preparedModDir, goModFileName))
				if err != nil {
					return err
				}
				return nil
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Successfully setup life cycle with Go sdk",
			args: args{
				sdk:             playground.Sdk_SDK_GO,
				code:            "",
				pipelineId:      successPipelineId,
				workingDir:      workingDir,
				preparedModDir:  filepath.Join(workingDir, preparedModDir),
				pipelinesFolder: pipelinesFolder,
			},
			prep: func() error {
				_, err := os.Create(filepath.Join(workingDir, preparedModDir, goSumFileName))
				if err != nil {
					return err
				}
				return nil
			},
			want: &fs_tool.LifeCycle{
				Paths: fs_tool.LifeCyclePaths{
					SourceFileName:                   fmt.Sprintf("%s%s", successPipelineId.String(), goSourceFileExtension),
					AbsoluteSourceFileFolderPath:     srcFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(srcFileFolder, fmt.Sprintf("%s%s", successPipelineId.String(), goSourceFileExtension)),
					ExecutableFileName:               fmt.Sprintf("%s%s", successPipelineId.String(), goCompiledFileExtension),
					AbsoluteExecutableFileFolderPath: execFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(execFileFolder, fmt.Sprintf("%s%s", successPipelineId.String(), goCompiledFileExtension)),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
					AbsoluteGraphFilePath:            filepath.Join(baseFileFolder, utils.GraphFileName),
				},
			},
			wantErr: false,
		},
		{
			name: "Error during copy common constants for Scio sdk",
			args: args{
				sdk:             playground.Sdk_SDK_SCIO,
				code:            "",
				pipelineId:      errorPipelineId,
				workingDir:      absWorkingDir,
				pipelinesFolder: pipelinesFolder,
				preparedModDir:  "",
			},
			prep: func() error {
				scioSh, err := os.Create(filepath.Join(workingDir, scioProject))
				if err != nil {
					return err
				}
				_, err = scioSh.Write([]byte(scioShContent))
				if err != nil {
					return err
				}
				return nil
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Successfully setup life cycle with Scio sdk",
			args: args{
				sdk:             playground.Sdk_SDK_SCIO,
				code:            "",
				pipelineId:      successPipelineId,
				workingDir:      absWorkingDir,
				pipelinesFolder: pipelinesFolder,
			},
			prep: func() error {
				sourceScioShFile := "../../../new_scio_project.sh"
				scioShFile := filepath.Join(workingDir, scioProject)
				err := utils.CopyFile(sourceScioShFile, scioShFile)
				if err != nil {
					return err
				}
				_, err = os.Create(filepath.Join(workingDir, scioCommonConstants))
				if err != nil {
					return err
				}
				return nil
			},
			want: &fs_tool.LifeCycle{
				Paths: fs_tool.LifeCyclePaths{
					SourceFileName:                   scalaFileName,
					AbsoluteSourceFileFolderPath:     absScalaFileFolderPath,
					AbsoluteSourceFilePath:           absScalaFilePath,
					ExecutableFileName:               scalaFileName,
					AbsoluteExecutableFileFolderPath: absScalaFileFolderPath,
					AbsoluteExecutableFilePath:       absScalaFilePath,
					AbsoluteBaseFolderPath:           absScalaFileFolderPath,
					AbsoluteLogFilePath:              filepath.Join(absScalaFileFolderPath, logFileName),
					AbsoluteGraphFilePath:            filepath.Join(absScalaFileFolderPath, utils.GraphFileName),
					ProjectDir:                       filepath.Join(pipelinesFolder, scioProjectName),
				},
			},
			wantErr: false,
		},
		{
			name: "Error during sh file missing for Scio sdk",
			args: args{
				sdk:             playground.Sdk_SDK_SCIO,
				code:            "",
				pipelineId:      errorPipelineId,
				workingDir:      workingDir,
				pipelinesFolder: pipelinesFolder,
				preparedModDir:  "",
			},
			prep: func() error {
				return nil
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.prep()
			if err != nil {
				t.Errorf("Setup() error during test preparetion: %v", err)
			}

			sources := []entity.FileEntity{{Name: "main.java", Content: tt.args.code, IsMain: true}}
			got, err := Setup(tt.args.sdk, sources, tt.args.pipelineId, tt.args.workingDir, tt.args.pipelinesFolder, tt.args.preparedModDir, emulators.EmulatorConfiguration{})
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
