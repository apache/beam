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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/logger"
	"fmt"
	"github.com/google/uuid"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const (
	sourceDir      = "sourceDir"
	destinationDir = "destinationDir"
)

func TestMain(m *testing.M) {
	err := setupPreparedFiles()
	if err != nil {
		logger.Fatal(err)
	}
	defer teardown()
	m.Run()
}

func setupPreparedFiles() error {
	err := os.Mkdir(sourceDir, 0755)
	if err != nil {
		return err
	}
	err = os.Mkdir(destinationDir, 0755)
	if err != nil {
		return err
	}
	filePath := filepath.Join(sourceDir, "file.txt")
	_, err = os.Create(filePath)
	if err != nil {
		return err
	}
	return nil
}

func teardown() {
	err := os.RemoveAll(sourceDir)
	if err != nil {
		logger.Fatal(err)
	}
	err = os.RemoveAll(destinationDir)
	if err != nil {
		logger.Fatal(err)
	}
}

func TestLifeCycle_CreateSourceCodeFile(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", baseFileFolder, pipelineId)
	srcFileFolder := baseFileFolder + "/src"
	binFileFolder := baseFileFolder + "/bin"

	type fields struct {
		folderGlobs []string
		folder      Folder
		extension   Extension
		pipelineId  uuid.UUID
	}
	type args struct {
		code string
	}
	tests := []struct {
		name          string
		createFolders []string
		fields        fields
		args          args
		want          string
		wantErr       bool
	}{
		{
			name: "executable folder doesn't exist",
			fields: fields{
				folder: Folder{
					SourceFileFolder:     srcFileFolder,
					ExecutableFileFolder: binFileFolder,
				},
				pipelineId: pipelineId,
			},
			args:    args{},
			want:    "",
			wantErr: true,
		},
		{
			name:          "executable folder exists",
			createFolders: []string{srcFileFolder},
			fields: fields{
				folder:     Folder{SourceFileFolder: srcFileFolder},
				extension:  Extension{SourceFileExtension: JavaSourceFileExtension},
				pipelineId: pipelineId,
			},
			args:    args{code: "TEST_CODE"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		for _, folder := range tt.createFolders {
			os.MkdirAll(folder, fs.ModePerm)
		}
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				folderGlobs: tt.fields.folderGlobs,
				Dto: LifeCycleDTO{
					Folder:     tt.fields.folder,
					Extension:  tt.fields.extension,
					PipelineId: tt.fields.pipelineId,
				},
			}
			err := l.CreateSourceCodeFile(tt.args.code)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateSourceCodeFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
		os.RemoveAll(baseFileFolder)
	}
}

func TestLifeCycle_CreateFolders(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", baseFileFolder, pipelineId)

	type fields struct {
		folderGlobs []string
		folder      Folder
		extension   Extension
		pipelineId  uuid.UUID
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
				Dto: LifeCycleDTO{
					Folder:     tt.fields.folder,
					Extension:  tt.fields.extension,
					PipelineId: tt.fields.pipelineId,
				},
			}
			if err := l.CreateFolders(); (err != nil) != tt.wantErr {
				t.Errorf("CreateFolders() error = %v, wantErr %v", err, tt.wantErr)
			}
			for _, folder := range tt.fields.folderGlobs {
				if _, err := os.Stat(folder); os.IsNotExist(err) {
					t.Errorf("CreateFolders() should create folder %s, but it dosn't", folder)
				}
			}
		})
		os.RemoveAll(baseFileFolder)
	}
}

func TestLifeCycle_DeleteFolders(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", baseFileFolder, pipelineId)

	type fields struct {
		folderGlobs []string
		folder      Folder
		extension   Extension
		pipelineId  uuid.UUID
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "DeleteFolders",
			fields: fields{
				folderGlobs: []string{baseFileFolder},
				pipelineId:  pipelineId,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				folderGlobs: tt.fields.folderGlobs,
				Dto: LifeCycleDTO{
					Folder:     tt.fields.folder,
					Extension:  tt.fields.extension,
					PipelineId: tt.fields.pipelineId,
				},
			}
			if err := l.DeleteFolders(); (err != nil) != tt.wantErr {
				t.Errorf("DeleteFolders() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewLifeCycle(t *testing.T) {
	pipelineId := uuid.New()
	workingDir := "workingDir"
	baseFileFolder := fmt.Sprintf("%s/%s/%s", workingDir, baseFileFolder, pipelineId)
	srcFileFolder := baseFileFolder + "/src"
	binFileFolder := baseFileFolder + "/bin"

	type args struct {
		sdk        pb.Sdk
		pipelineId uuid.UUID
		workingDir string
	}
	tests := []struct {
		name    string
		args    args
		want    *LifeCycle
		wantErr bool
	}{
		{
			name: "Available SDK",
			args: args{
				sdk:        pb.Sdk_SDK_JAVA,
				pipelineId: pipelineId,
				workingDir: workingDir,
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder, srcFileFolder, binFileFolder},
				Dto: LifeCycleDTO{
					Folder: Folder{
						BaseFolder:           baseFileFolder,
						SourceFileFolder:     srcFileFolder,
						ExecutableFileFolder: binFileFolder,
					},
					Extension: Extension{
						SourceFileExtension:     JavaSourceFileExtension,
						ExecutableFileExtension: javaCompiledFileExtension,
					},
					ExecutableName: executableName,
					PipelineId:     pipelineId,
				},
			},
			wantErr: false,
		},
		{
			name: "Unavailable SDK",
			args: args{
				sdk:        pb.Sdk_SDK_UNSPECIFIED,
				pipelineId: pipelineId,
				workingDir: workingDir,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLifeCycle(tt.args.sdk, tt.args.pipelineId, tt.args.workingDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLifeCycle() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got.folderGlobs, tt.want.folderGlobs) {
				t.Errorf("NewLifeCycle() folderGlobs = %v, want %v", got.folderGlobs, tt.want.folderGlobs)
			}
			if !tt.wantErr && !reflect.DeepEqual(got.Dto.Folder, tt.want.Dto.Folder) {
				t.Errorf("NewLifeCycle() Folder = %v, want %v", got.Dto.Folder, tt.want.Dto.Folder)
			}
			if !tt.wantErr && !reflect.DeepEqual(got.Dto.Extension, tt.want.Dto.Extension) {
				t.Errorf("NewLifeCycle() Extension = %v, want %v", got.Dto.Extension, tt.want.Dto.Extension)
			}
			if !tt.wantErr && !reflect.DeepEqual(got.Dto.PipelineId, tt.want.Dto.PipelineId) {
				t.Errorf("NewLifeCycle() pipelineId = %v, want %v", got.Dto.PipelineId, tt.want.Dto.PipelineId)
			}
		})
	}
}

func TestLifeCycle_CopyFile(t *testing.T) {
	type fields struct {
		folderGlobs    []string
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
		pipelineId     uuid.UUID
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
			name: "file doesn't exist",
			fields: fields{
				folderGlobs:    nil,
				Folder:         Folder{},
				Extension:      Extension{},
				ExecutableName: nil,
				pipelineId:     uuid.UUID{},
			},
			args: args{
				fileName:       "file1.txt",
				sourceDir:      sourceDir,
				destinationDir: destinationDir,
			},
			wantErr: true,
		},
		{
			name: "file exists",
			fields: fields{
				folderGlobs:    nil,
				Folder:         Folder{},
				Extension:      Extension{},
				ExecutableName: nil,
				pipelineId:     uuid.UUID{},
			},
			args: args{
				fileName:       "file.txt",
				sourceDir:      sourceDir,
				destinationDir: destinationDir,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				folderGlobs: tt.fields.folderGlobs,
				Dto: LifeCycleDTO{
					Folder:         tt.fields.Folder,
					Extension:      tt.fields.Extension,
					ExecutableName: tt.fields.ExecutableName,
					PipelineId:     tt.fields.pipelineId,
				},
			}
			err := l.CopyFile(tt.args.fileName, tt.args.sourceDir, tt.args.destinationDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("CopyFile() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && !tt.wantErr {
				newFilePath := filepath.Join(destinationDir, tt.args.fileName)
				_, err = os.Stat(newFilePath)
				if os.IsNotExist(err) {
					t.Errorf("CopyFile() should create a new file: %s", newFilePath)
				}
			}
		})
	}
}

func TestLifeCycleDTO_GetAbsoluteBaseFolderPath(t *testing.T) {
	baseFolder := "baseFolder"
	absoluteBaseFileFolder, _ := filepath.Abs(baseFolder)

	type fields struct {
		PipelineId     uuid.UUID
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "GetAbsoluteBaseFolderPath",
			fields: fields{Folder: Folder{BaseFolder: baseFolder}},
			want:   absoluteBaseFileFolder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycleDTO{
				PipelineId:     tt.fields.PipelineId,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
			}
			if got := l.GetAbsoluteBaseFolderPath(); got != tt.want {
				t.Errorf("GetAbsoluteBaseFolderPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycleDTO_GetAbsoluteExecutableFileFolderPath(t *testing.T) {
	executableFileFolder := "executableFileFolder"
	absoluteExecutableFileFolder, _ := filepath.Abs(executableFileFolder)

	type fields struct {
		PipelineId     uuid.UUID
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "GetAbsoluteExecutableFileFolderPath",
			fields: fields{Folder: Folder{ExecutableFileFolder: executableFileFolder}},
			want:   absoluteExecutableFileFolder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycleDTO{
				PipelineId:     tt.fields.PipelineId,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
			}
			if got := l.GetAbsoluteExecutableFileFolderPath(); got != tt.want {
				t.Errorf("GetAbsoluteExecutableFileFolderPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycleDTO_GetAbsoluteExecutableFilePath(t *testing.T) {
	pipelineId := uuid.New()
	executableFileFolder := "executableFileFolder"
	executableFileExtension := ".executableFileExtension"
	absoluteExecutableFilePath, _ := filepath.Abs(filepath.Join(executableFileFolder, pipelineId.String()+executableFileExtension))

	type fields struct {
		PipelineId     uuid.UUID
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetAbsoluteExecutableFilePath",
			fields: fields{
				PipelineId: pipelineId,
				Folder:     Folder{ExecutableFileFolder: executableFileFolder},
				Extension:  Extension{ExecutableFileExtension: executableFileExtension},
			},
			want: absoluteExecutableFilePath,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycleDTO{
				PipelineId:     tt.fields.PipelineId,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
			}
			if got := l.GetAbsoluteExecutableFilePath(); got != tt.want {
				t.Errorf("GetAbsoluteExecutableFilePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycleDTO_GetAbsoluteLogFilePath(t *testing.T) {
	baseFileFolder := "baseFileFolder"
	absoluteLogFilePath, _ := filepath.Abs(filepath.Join(baseFileFolder, logFileName))

	type fields struct {
		PipelineId     uuid.UUID
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetAbsoluteLogFilePath",
			fields: fields{
				Folder: Folder{BaseFolder: baseFileFolder},
			},
			want: absoluteLogFilePath,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycleDTO{
				PipelineId:     tt.fields.PipelineId,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
			}
			if got := l.GetAbsoluteLogFilePath(); got != tt.want {
				t.Errorf("GetAbsoluteLogFilePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycleDTO_GetAbsoluteSourceFileFolderPath(t *testing.T) {
	sourceFileFolder := "sourceFileFolder"
	absoluteSourceFileFolder, _ := filepath.Abs(sourceFileFolder)

	type fields struct {
		PipelineId     uuid.UUID
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetAbsoluteSourceFileFolderPath",
			fields: fields{
				Folder: Folder{SourceFileFolder: sourceFileFolder},
			},
			want: absoluteSourceFileFolder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycleDTO{
				PipelineId:     tt.fields.PipelineId,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
			}
			if got := l.GetAbsoluteSourceFileFolderPath(); got != tt.want {
				t.Errorf("GetAbsoluteSourceFileFolderPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycleDTO_GetAbsoluteSourceFilePath(t *testing.T) {
	pipelineId := uuid.New()
	sourceFileFolder := "sourceFileFolder"
	sourceFileExtension := ".sourceFileExtension"
	absoluteSourceFilePath, _ := filepath.Abs(filepath.Join(sourceFileFolder, pipelineId.String()+sourceFileExtension))

	type fields struct {
		PipelineId     uuid.UUID
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetAbsoluteSourceFilePath",
			fields: fields{
				PipelineId: pipelineId,
				Folder:     Folder{SourceFileFolder: sourceFileFolder},
				Extension:  Extension{SourceFileExtension: sourceFileExtension},
			},
			want: absoluteSourceFilePath,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycleDTO{
				PipelineId:     tt.fields.PipelineId,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
			}
			if got := l.GetAbsoluteSourceFilePath(); got != tt.want {
				t.Errorf("GetAbsoluteSourceFilePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycleDTO_GetExecutableFileName(t *testing.T) {
	pipelineId := uuid.New()
	executableFileExtension := ".executableFileExtension"

	type fields struct {
		PipelineId     uuid.UUID
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetExecutableFileName",
			fields: fields{
				PipelineId: pipelineId,
				Extension:  Extension{ExecutableFileExtension: executableFileExtension},
			},
			want: pipelineId.String() + executableFileExtension,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycleDTO{
				PipelineId:     tt.fields.PipelineId,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
			}
			if got := l.GetExecutableFileName(); got != tt.want {
				t.Errorf("GetExecutableFileName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycleDTO_GetSourceFileName(t *testing.T) {
	pipelineId := uuid.New()
	sourceFileExtension := ".sourceFileExtension"

	type fields struct {
		PipelineId     uuid.UUID
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetSourceFileName",
			fields: fields{
				PipelineId: pipelineId,
				Extension:  Extension{SourceFileExtension: sourceFileExtension},
			},
			want: pipelineId.String() + sourceFileExtension,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycleDTO{
				PipelineId:     tt.fields.PipelineId,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
			}
			if got := l.GetSourceFileName(); got != tt.want {
				t.Errorf("GetSourceFileName() = %v, want %v", got, tt.want)
			}
		})
	}
}
