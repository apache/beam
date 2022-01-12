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
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func Test_newJavaLifeCycle(t *testing.T) {
	pipelineId := uuid.New()
	workingDir, _ := filepath.Abs("workingDir")
	baseFileFolder := filepath.Join(workingDir, pipelinesFolder, pipelineId.String())
	srcFileFolder := filepath.Join(baseFileFolder, "src")
	binFileFolder := filepath.Join(baseFileFolder, "bin")

	type args struct {
		pipelineId      uuid.UUID
		pipelinesFolder string
	}
	tests := []struct {
		name string
		args args
		want *LifeCycle
	}{
		{
			// Test case with calling newJavaLifeCycle method with correct pipelineId and workingDir.
			// As a result, want to receive an expected java life cycle.
			name: "newJavaLifeCycle",
			args: args{
				pipelineId:      pipelineId,
				pipelinesFolder: filepath.Join(workingDir, pipelinesFolder),
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder, srcFileFolder, binFileFolder},
				Paths: LifeCyclePaths{
					SourceFileName:                   pipelineId.String() + javaSourceFileExtension,
					AbsoluteSourceFileFolderPath:     srcFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(srcFileFolder, pipelineId.String()+javaSourceFileExtension),
					ExecutableFileName:               pipelineId.String() + javaCompiledFileExtension,
					AbsoluteExecutableFileFolderPath: binFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(binFileFolder, pipelineId.String()+javaCompiledFileExtension),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
					ExecutableName:                   executableName,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newJavaLifeCycle(tt.args.pipelineId, tt.args.pipelinesFolder)
			if !reflect.DeepEqual(got.folderGlobs, tt.want.folderGlobs) {
				t.Errorf("newJavaLifeCycle() folderGlobs = %v, want %v", got.folderGlobs, tt.want.folderGlobs)
			}
			if !checkPathsEqual(got.Paths, tt.want.Paths) {
				t.Errorf("newJavaLifeCycle() Paths = %v, want %v", got.Paths, tt.want.Paths)
			}
		})
	}
}

func Test_executableName(t *testing.T) {
	pipelineId := uuid.New()
	workDir := "workingDir"
	preparedPipelinesFolder := filepath.Join(workDir, pipelinesFolder)
	lc := newJavaLifeCycle(pipelineId, preparedPipelinesFolder)
	lc.CreateFolders()
	defer os.RemoveAll(workDir)

	type args struct {
		pipelineId      uuid.UUID
		pipelinesFolder string
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		want    string
		wantErr bool
	}{
		{
			// Test case with calling sourceFileName method with correct pipelineId and workingDir.
			// As a result, want to receive a name that should be executed
			name: "get executable name",
			prepare: func() {
				compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
				filePath := filepath.Join(compiled, "temp.class")
				err := os.WriteFile(filePath, []byte("TEMP_DATA"), 0600)
				if err != nil {
					panic(err)
				}
			},
			args: args{
				pipelineId:      pipelineId,
				pipelinesFolder: preparedPipelinesFolder,
			},
			want:    "temp",
			wantErr: false,
		},
		{
			// Test case with calling sourceFileName method with correct pipelineId and workingDir.
			// As a result, want to receive an error.
			name:    "directory doesn't exist",
			prepare: func() {},
			args: args{
				pipelineId: uuid.New(),
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			got, err := executableName(tt.args.pipelineId, tt.args.pipelinesFolder)
			if (err != nil) != tt.wantErr {
				t.Errorf("sourceFileName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("sourceFileName() got = %v, want %v", got, tt.want)
			}
		})
	}
}
