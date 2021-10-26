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
	"fmt"
	"github.com/google/uuid"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLifeCycle_CreateExecutableFile(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", javaBaseFileFolder, pipelineId)
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
					ExecutableFolder: srcFileFolder,
					CompiledFolder:   binFileFolder,
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
				folder:     Folder{ExecutableFolder: srcFileFolder},
				extension:  Extension{ExecutableExtension: javaExecutableFileExtension},
				pipelineId: pipelineId,
			},
			args:    args{code: "TEST_CODE"},
			want:    fmt.Sprintf("%s.%s", pipelineId, javaExecutableFileExtension),
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
			got, err := l.CreateExecutableFile(tt.args.code)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateExecutableFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CreateExecutableFile() got = %v, want %v", got, tt.want)
			}
		})
		os.RemoveAll(baseFileFolder)
	}
}

func TestLifeCycle_CreateFolders(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", javaBaseFileFolder, pipelineId)

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
	baseFileFolder := fmt.Sprintf("%s_%s", javaBaseFileFolder, pipelineId)

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
	baseFileFolder := fmt.Sprintf("%s/%s/%s", workingDir, javaBaseFileFolder, pipelineId)
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
					BaseFolder:       baseFileFolder,
					ExecutableFolder: srcFileFolder,
					CompiledFolder:   binFileFolder,
				},
				Extension: Extension{
					ExecutableExtension: javaExecutableFileExtension,
					CompiledExtension:   javaCompiledFileExtension,
				},
				pipelineId: pipelineId,
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLifeCycle() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getFileName(t *testing.T) {
	pipelineId := uuid.New()
	type args struct {
		pipelineId uuid.UUID
		fileType   string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "getFileName",
			args: args{
				pipelineId: pipelineId,
				fileType:   javaExecutableFileExtension,
			},
			want: fmt.Sprintf("%s.%s", pipelineId, javaExecutableFileExtension),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFileName(tt.args.pipelineId, tt.args.fileType); got != tt.want {
				t.Errorf("getFileName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycle_GetAbsoluteExecutableFilePath(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", javaBaseFileFolder, pipelineId)
	srcFileFolder := baseFileFolder + "/src"

	filePath := fmt.Sprintf("%s/%s.%s", srcFileFolder, pipelineId, javaExecutableFileExtension)
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
			name: "GetAbsoluteExecutableFilePath",
			fields: fields{
				Folder: Folder{
					BaseFolder:       baseFileFolder,
					ExecutableFolder: srcFileFolder,
				},
				Extension:  Extension{ExecutableExtension: javaExecutableFileExtension},
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
			got := l.GetAbsoluteExecutableFilePath()
			if got != tt.want {
				t.Errorf("GetAbsoluteExecutableFilePath() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycle_GetAbsoluteExecutableFilesFolderPath(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", javaBaseFileFolder, pipelineId)

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
				Extension:  Extension{ExecutableExtension: javaExecutableFileExtension},
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
			got := l.GetAbsoluteExecutableFilesFolderPath()
			if got != tt.want {
				t.Errorf("GetAbsoluteExecutableFilesFolderPath() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLifeCycle_GetExecutableName(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", javaBaseFileFolder, pipelineId)
	binFileFolder := baseFileFolder + "/bin"

	type fields struct {
		folderGlobs []string
		Folder      Folder
		Extension   Extension
		pipelineId  uuid.UUID
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "GetExecutableName",
			fields: fields{
				Folder: Folder{
					BaseFolder:     baseFileFolder,
					CompiledFolder: binFileFolder,
				},
				pipelineId:  pipelineId,
				folderGlobs: []string{baseFileFolder, binFileFolder},
			},
			want: pipelineId.String(),
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
			if err := l.CreateFolders(); err != nil {
				t.Errorf("CreateFolders() error = %v", err)
			}
			_, err := os.Create(binFileFolder + "/" + pipelineId.String() + ".class")
			if err != nil {
				t.Errorf("Unable to write file: %v", err)
			}
			got, err := l.GetExecutableName()
			if got != tt.want {
				t.Errorf("GetExecutableName() got = %v, want %v", got, tt.want)
			}
			os.RemoveAll(baseFileFolder)
		})
	}
}
