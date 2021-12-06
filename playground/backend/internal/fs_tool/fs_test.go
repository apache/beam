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

func TestLifeCycle_CreateExecutableFile(t *testing.T) {
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
				extension:  Extension{SourceFileExtension: javaSourceFileExtension},
				pipelineId: pipelineId,
			},
			args:    args{code: "TEST_CODE"},
			want:    pipelineId.String() + javaSourceFileExtension,
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
				Folder:      tt.fields.folder,
				Extension:   tt.fields.extension,
				pipelineId:  tt.fields.pipelineId,
			}
			got, err := l.CreateSourceCodeFile(tt.args.code)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateSourceCodeFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CreateSourceCodeFile() got = %v, want %v", got, tt.want)
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
				Folder:      tt.fields.folder,
				Extension:   tt.fields.extension,
				pipelineId:  tt.fields.pipelineId,
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
				Folder:      tt.fields.folder,
				Extension:   tt.fields.extension,
				pipelineId:  tt.fields.pipelineId,
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
				Folder: Folder{
					BaseFolder:           baseFileFolder,
					SourceFileFolder:     srcFileFolder,
					ExecutableFileFolder: binFileFolder,
				},
				Extension: Extension{
					SourceFileExtension:     javaSourceFileExtension,
					ExecutableFileExtension: javaCompiledFileExtension,
				},
				ExecutableName: executableName,
				pipelineId:     pipelineId,
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
			if !tt.wantErr && !reflect.DeepEqual(got.Folder, tt.want.Folder) {
				t.Errorf("NewLifeCycle() Folder = %v, want %v", got.Folder, tt.want.Folder)
			}
			if !tt.wantErr && !reflect.DeepEqual(got.Extension, tt.want.Extension) {
				t.Errorf("NewLifeCycle() Extension = %v, want %v", got.Extension, tt.want.Extension)
			}
			if !tt.wantErr && !reflect.DeepEqual(got.pipelineId, tt.want.pipelineId) {
				t.Errorf("NewLifeCycle() pipelineId = %v, want %v", got.pipelineId, tt.want.pipelineId)
			}
		})
	}
}

func TestLifeCycle_GetAbsoluteExecutableFilePath(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", baseFileFolder, pipelineId)
	srcFileFolder := baseFileFolder + "/src"

	filePath := fmt.Sprintf("%s/%s", srcFileFolder, pipelineId.String()+javaSourceFileExtension)
	absolutePath, _ := filepath.Abs(filePath)
	type fields struct {
		folderGlobs []string
		Folder      Folder
		Extension   Extension
		pipelineId  uuid.UUID
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "GetAbsoluteSourceFilePath",
			fields: fields{
				Folder: Folder{
					BaseFolder:       baseFileFolder,
					SourceFileFolder: srcFileFolder,
				},
				Extension:  Extension{SourceFileExtension: javaSourceFileExtension},
				pipelineId: pipelineId,
			},
			want: absolutePath,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				folderGlobs: tt.fields.folderGlobs,
				Folder:      tt.fields.Folder,
				Extension:   tt.fields.Extension,
				pipelineId:  tt.fields.pipelineId,
			}
			got := l.GetAbsoluteSourceFilePath()
			if got != tt.want {
				t.Errorf("GetAbsoluteSourceFilePath() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycle_GetAbsoluteExecutableFilesFolderPath(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", baseFileFolder, pipelineId)

	absolutePath, _ := filepath.Abs(baseFileFolder)
	type fields struct {
		folderGlobs []string
		Folder      Folder
		Extension   Extension
		pipelineId  uuid.UUID
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "GetAbsoluteExecutableFolderPath",
			fields: fields{
				Folder:     Folder{BaseFolder: baseFileFolder},
				Extension:  Extension{SourceFileExtension: javaSourceFileExtension},
				pipelineId: pipelineId,
			},
			want: absolutePath,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				folderGlobs: tt.fields.folderGlobs,
				Folder:      tt.fields.Folder,
				Extension:   tt.fields.Extension,
				pipelineId:  tt.fields.pipelineId,
			}
			got := l.GetAbsoluteBaseFolderPath()
			if got != tt.want {
				t.Errorf("GetAbsoluteBaseFolderPath() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycle_ExecutableName(t *testing.T) {
	pipelineId := uuid.New()
	workingDir := "workingDir"
	baseFileFolder := fmt.Sprintf("%s/%s/%s", workingDir, baseFileFolder, pipelineId)
	binFileFolder := baseFileFolder + "/bin"

	type fields struct {
		folderGlobs    []string
		Folder         Folder
		Extension      Extension
		ExecutableName func(uuid.UUID, string) (string, error)
		pipelineId     uuid.UUID
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "ExecutableName",
			fields: fields{
				Folder: Folder{
					BaseFolder:           baseFileFolder,
					ExecutableFileFolder: binFileFolder,
				},
				ExecutableName: func(u uuid.UUID, s string) (string, error) {
					return "MOCK_EXECUTABLE_NAME", nil
				},
				pipelineId:  pipelineId,
				folderGlobs: []string{baseFileFolder, binFileFolder},
			},
			want:    "MOCK_EXECUTABLE_NAME",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LifeCycle{
				folderGlobs:    tt.fields.folderGlobs,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
				pipelineId:     tt.fields.pipelineId,
			}
			got, err := l.ExecutableName(pipelineId, workingDir)
			if got != tt.want {
				t.Errorf("GetExecutableName() got = %v, want %v", got, tt.want)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("GetExecutableName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCopyFile(t *testing.T) {
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
				folderGlobs:    tt.fields.folderGlobs,
				Folder:         tt.fields.Folder,
				Extension:      tt.fields.Extension,
				ExecutableName: tt.fields.ExecutableName,
				pipelineId:     tt.fields.pipelineId,
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
