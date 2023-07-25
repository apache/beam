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

package fs_tool

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/uuid"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/utils"
)

const (
	sourceDir       = "sourceDir"
	destinationDir  = "destinationDir"
	testFileMode    = 0755
	pipelinesFolder = "executable_files"
	fileName        = "file.txt"
	emptyFolder     = "emptyFolder"
)

func TestMain(m *testing.M) {
	err := prepareFiles()
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
	defer teardownFiles()
	m.Run()
}

func prepareFiles() error {
	err := os.MkdirAll(filepath.Join(sourceDir, emptyFolder), testFileMode)
	if err != nil {
		return err
	}
	err = os.Mkdir(destinationDir, testFileMode)
	if err != nil {
		return err
	}
	filePath := filepath.Join(sourceDir, fileName)
	_, err = os.Create(filePath)
	return err
}

func teardownFiles() error {
	err := os.RemoveAll(sourceDir)
	if err != nil {
		return err
	}
	return os.RemoveAll(destinationDir)
}

func prepareFolders(baseFileFolder string) error {
	srcFileFolder := filepath.Join(baseFileFolder, "src")

	return os.MkdirAll(srcFileFolder, testFileMode)
}

func teardownFolders(baseFileFolder string) error {
	err := os.RemoveAll(baseFileFolder)
	return err
}

func TestLifeCycle_CopyFile(t *testing.T) {
	type fields struct {
		folderGlobs []string
		Paths       LifeCyclePaths
	}
	type args struct {
		fileName       string
		sourceDir      string
		destinationDir string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "File doesn't exist",
			fields: fields{
				folderGlobs: nil,
			},
			args: args{
				fileName:       "file1.txt",
				sourceDir:      sourceDir,
				destinationDir: destinationDir,
			},
			wantErr: true,
		},
		{
			name: "File exists",
			fields: fields{
				folderGlobs: nil,
			},
			args: args{
				fileName:       fileName,
				sourceDir:      sourceDir,
				destinationDir: destinationDir,
			},
			wantErr: false,
		},
		{
			name: "Copy directory instead of file",
			fields: fields{
				folderGlobs: nil,
			},
			args: args{
				fileName:       emptyFolder,
				sourceDir:      sourceDir,
				destinationDir: destinationDir,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := utils.CopyFilePreservingName(tt.args.fileName, tt.args.sourceDir, tt.args.destinationDir); (err != nil) != tt.wantErr {
				t.Errorf("CopyFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLifeCycle_CreateFolders(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := pipelineId.String()

	type fields struct {
		folderGlobs []string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "CreateFolders",
			fields:  fields{folderGlobs: []string{baseFileFolder}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				folderGlobs: tt.fields.folderGlobs,
			}
			if err := l.CreateFolders(); (err != nil) != tt.wantErr {
				t.Errorf("CreateFolders() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		os.RemoveAll(baseFileFolder)
	}
}

func TestLifeCycle_CreateSourceCodeFile(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder, _ := filepath.Abs(pipelineId.String())
	if err := prepareFolders(baseFileFolder); err != nil {
		t.Fatalf("Error during preparing folders for test: %s", err)
	}
	defer teardownFolders(baseFileFolder)

	type fields struct {
		Paths LifeCyclePaths
	}
	type args struct {
		code string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Source file folder path doesn't exist",
			fields: fields{
				Paths: LifeCyclePaths{
					AbsoluteSourceFileFolderPath: "src",
				},
			}, wantErr: true,
		},
		{
			name: "Source file folder path exists",
			fields: fields{
				Paths: LifeCyclePaths{
					AbsoluteSourceFileFolderPath: filepath.Join(baseFileFolder, "src"),
					AbsoluteSourceFilePath:       filepath.Join(baseFileFolder, "src", fmt.Sprintf("%s.%s", pipelineId.String(), "txt")),
				},
			},
			args:    args{code: "TEST_CODE"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				Paths: tt.fields.Paths,
			}
			sources := []entity.FileEntity{{Name: "main.java", Content: tt.args.code, IsMain: true}}
			if err := l.CreateSourceCodeFiles(sources); (err != nil) != tt.wantErr {
				t.Errorf("CreateSourceCodeFile() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if _, err := os.Stat(l.Paths.AbsoluteSourceFilePath); os.IsNotExist(err) {
					t.Error("CreateSourceCodeFile() should create a new file, but it doesn't")
				} else {
					data, err := os.ReadFile(l.Paths.AbsoluteSourceFilePath)
					if err != nil {
						t.Errorf("CreateSourceCodeFile() error during open created file: %s", err)
					}
					if string(data) != tt.args.code {
						t.Errorf("CreateSourceCodeFile() code = %s, want code %s", string(data), tt.args.code)
					}
				}
			}
		})
	}
}

func TestLifeCycle_DeleteFolders(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := pipelineId.String()
	if err := prepareFolders(baseFileFolder); err != nil {
		t.Fatalf("Error during preparing folders for test: %s", err)
	}

	type fields struct {
		folderGlobs []string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "DeleteFolders",
			fields:  fields{folderGlobs: []string{baseFileFolder}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				folderGlobs: tt.fields.folderGlobs,
			}
			if err := l.DeleteFolders(); (err != nil) != tt.wantErr {
				t.Errorf("DeleteFolders() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if _, err := os.Stat(baseFileFolder); err == nil || !os.IsNotExist(err) {
					t.Error("DeleteFolders() should remove folders, but it doesn't")
				}
			}
		})
	}
}

func TestNewLifeCycle(t *testing.T) {
	pipelineId := uuid.New()
	pipelinesFolder, _ := filepath.Abs(pipelinesFolder)
	baseFileFolder := filepath.Join(pipelinesFolder, pipelineId.String())
	srcFileFolder := filepath.Join(baseFileFolder, "src")
	execFileFolder := filepath.Join(baseFileFolder, "bin")

	type args struct {
		sdk             pb.Sdk
		pipelineId      uuid.UUID
		pipelinesFolder string
	}
	tests := []struct {
		name    string
		args    args
		want    *LifeCycle
		wantErr bool
	}{
		{
			name: "Java LifeCycle",
			args: args{
				sdk:             pb.Sdk_SDK_JAVA,
				pipelineId:      pipelineId,
				pipelinesFolder: pipelinesFolder,
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder, srcFileFolder, execFileFolder},
				Paths: LifeCyclePaths{
					SourceFileName:                   fmt.Sprintf("%s%s", pipelineId.String(), JavaSourceFileExtension),
					AbsoluteSourceFileFolderPath:     srcFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(srcFileFolder, fmt.Sprintf("%s%s", pipelineId.String(), JavaSourceFileExtension)),
					ExecutableFileName:               fmt.Sprintf("%s%s", pipelineId.String(), javaCompiledFileExtension),
					AbsoluteExecutableFileFolderPath: execFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(execFileFolder, fmt.Sprintf("%s%s", pipelineId.String(), javaCompiledFileExtension)),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
					AbsoluteGraphFilePath:            filepath.Join(srcFileFolder, utils.GraphFileName),
				},
			},
		},
		{
			name: "Go LifeCycle",
			args: args{
				sdk:             pb.Sdk_SDK_GO,
				pipelineId:      pipelineId,
				pipelinesFolder: pipelinesFolder,
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder, srcFileFolder, execFileFolder},
				Paths: LifeCyclePaths{
					SourceFileName:                   fmt.Sprintf("%s%s", pipelineId.String(), GoSourceFileExtension),
					AbsoluteSourceFileFolderPath:     srcFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(srcFileFolder, fmt.Sprintf("%s%s", pipelineId.String(), GoSourceFileExtension)),
					ExecutableFileName:               fmt.Sprintf("%s%s", pipelineId.String(), goExecutableFileExtension),
					AbsoluteExecutableFileFolderPath: execFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(execFileFolder, fmt.Sprintf("%s%s", pipelineId.String(), goExecutableFileExtension)),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
					AbsoluteGraphFilePath:            filepath.Join(srcFileFolder, utils.GraphFileName),
				},
			},
		},
		{
			name: "Python LifeCycle",
			args: args{
				sdk:             pb.Sdk_SDK_PYTHON,
				pipelineId:      pipelineId,
				pipelinesFolder: pipelinesFolder,
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder},
				Paths: LifeCyclePaths{
					SourceFileName:                   fmt.Sprintf("%s%s", pipelineId.String(), pythonExecutableFileExtension),
					AbsoluteSourceFileFolderPath:     baseFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(baseFileFolder, fmt.Sprintf("%s%s", pipelineId.String(), pythonExecutableFileExtension)),
					ExecutableFileName:               fmt.Sprintf("%s%s", pipelineId.String(), pythonExecutableFileExtension),
					AbsoluteExecutableFileFolderPath: baseFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(baseFileFolder, fmt.Sprintf("%s%s", pipelineId.String(), pythonExecutableFileExtension)),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
					AbsoluteGraphFilePath:            filepath.Join(baseFileFolder, utils.GraphFileName),
				},
			},
		},
		{
			name: "SCIO LifeCycle",
			args: args{
				sdk:             pb.Sdk_SDK_SCIO,
				pipelineId:      pipelineId,
				pipelinesFolder: pipelinesFolder,
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder},
				Paths: LifeCyclePaths{
					SourceFileName:                   fmt.Sprintf("%s%s", pipelineId.String(), scioExecutableFileExtension),
					AbsoluteSourceFileFolderPath:     baseFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(baseFileFolder, fmt.Sprintf("%s%s", pipelineId.String(), scioExecutableFileExtension)),
					ExecutableFileName:               fmt.Sprintf("%s%s", pipelineId.String(), scioExecutableFileExtension),
					AbsoluteExecutableFileFolderPath: baseFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(baseFileFolder, fmt.Sprintf("%s%s", pipelineId.String(), scioExecutableFileExtension)),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
				},
			},
		},
		{
			name: "Unavailable SDK",
			args: args{
				sdk:             pb.Sdk_SDK_UNSPECIFIED,
				pipelineId:      pipelineId,
				pipelinesFolder: pipelinesFolder,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLifeCycle(tt.args.sdk, tt.args.pipelineId, tt.args.pipelinesFolder)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLifeCycle() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got.folderGlobs, tt.want.folderGlobs) {
				t.Errorf("NewLifeCycle() got folderGlobs = %v, want folderGlobs %v", got.folderGlobs, tt.want.folderGlobs)
			}
			if !tt.wantErr && !checkPathsEqual(got.Paths, tt.want.Paths) {
				t.Errorf("NewLifeCycle() got Paths = %v, want Paths %v", got.Paths, tt.want.Paths)
			}
		})
	}
}

func checkPathsEqual(paths1, paths2 LifeCyclePaths) bool {
	return paths1.SourceFileName == paths2.SourceFileName &&
		paths1.AbsoluteSourceFileFolderPath == paths2.AbsoluteSourceFileFolderPath &&
		paths1.AbsoluteSourceFilePath == paths2.AbsoluteSourceFilePath &&
		paths1.ExecutableFileName == paths2.ExecutableFileName &&
		paths1.AbsoluteExecutableFileFolderPath == paths2.AbsoluteExecutableFileFolderPath &&
		paths1.AbsoluteExecutableFilePath == paths2.AbsoluteExecutableFilePath &&
		paths1.AbsoluteBaseFolderPath == paths2.AbsoluteBaseFolderPath &&
		paths1.AbsoluteLogFilePath == paths2.AbsoluteLogFilePath
}
