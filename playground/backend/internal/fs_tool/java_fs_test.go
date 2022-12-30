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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"beam.apache.org/playground/backend/internal/utils"
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
			name: "NewJavaLifeCycle",
			args: args{
				pipelineId:      pipelineId,
				pipelinesFolder: filepath.Join(workingDir, pipelinesFolder),
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder, srcFileFolder, binFileFolder},
				Paths: LifeCyclePaths{
					SourceFileName:                   pipelineId.String() + JavaSourceFileExtension,
					AbsoluteSourceFileFolderPath:     srcFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(srcFileFolder, pipelineId.String()+JavaSourceFileExtension),
					ExecutableFileName:               pipelineId.String() + javaCompiledFileExtension,
					AbsoluteExecutableFileFolderPath: binFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(binFileFolder, pipelineId.String()+javaCompiledFileExtension),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
					AbsoluteGraphFilePath:            filepath.Join(baseFileFolder, utils.GraphFileName),
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
	err := lc.CreateFolders()
	if err != nil {
		panic(err)
	}
	defer func() {
		err := os.RemoveAll(workDir)
		if err != nil {
			panic(err)
		}
	}()

	cleanupFunc := func() {
		compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
		dirEntries, err := os.ReadDir(compiled)
		if err != nil {
			panic(err)
		}

		for _, entry := range dirEntries {
			err := os.Remove(filepath.Join(compiled, entry.Name()))
			if err != nil {
				panic(err)
			}
		}
	}

	type args struct {
		executableFolder string
	}
	tests := []struct {
		name    string
		prepare func()
		cleanup func()
		args    args
		want    string
		wantErr bool
	}{
		{
			// Test case with calling sourceFileName method with empty directory.
			// As a result, want to receive an error.
			name:    "Directory is empty",
			prepare: func() {},
			cleanup: func() {},
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "",
			wantErr: true,
		},
		{
			// Test case with calling sourceFileName method with correct pipelineId and workingDir.
			// As a result, want to receive a name that should be executed
			name: "Get executable name",
			prepare: func() {
				compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
				filePath := filepath.Join(compiled, "temp.class")
				err := os.WriteFile(filePath, []byte("TEMP_DATA"), 0600)
				if err != nil {
					panic(err)
				}
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "temp",
			wantErr: false,
		},
		{
			// Test case with calling sourceFileName method with wrong directory.
			// As a result, want to receive an error.
			name:    "Directory doesn't exist",
			prepare: func() {},
			cleanup: func() {},
			args: args{
				executableFolder: filepath.Join(workDir, pipelineId.String()),
			},
			want:    "",
			wantErr: true,
		},
		{
			// Test case with calling sourceFileName method with multiple files where one of them is main
			// As a result, want to receive a name that should be executed
			name: "Multiple files where one of them is main",
			prepare: func() {
				compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
				primaryFilePath := filepath.Join(compiled, "main.scala")
				err := os.WriteFile(primaryFilePath, []byte("object MinimalWordCount {def main(cmdlineArgs: Array[String]): Unit = {}}"), 0600)
				if err != nil {
					panic(err)
				}
				secondaryFilePath := filepath.Join(compiled, "temp.scala")
				err = os.WriteFile(secondaryFilePath, []byte("TEMP_DATA"), 0600)
				if err != nil {
					panic(err)
				}
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "main",
			wantErr: false,
		},
		{
			// Test case with calling sourceFileName method with multiple files where one of them is a .class file
			// with main() method
			// As a result, want to receive a name that should be executed
			name: "Multiple Java class files where one of them contains main",
			prepare: func() {
				testdataPath := "java_testdata"
				dirEntries, err := os.ReadDir(testdataPath)
				if err != nil {
					panic(err)
				}

				compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)

				for _, entry := range dirEntries {
					src := filepath.Join(testdataPath, entry.Name())
					dst := filepath.Join(compiled, entry.Name())
					err = os.Link(src, dst)
					if err != nil {
						panic(err)
					}
				}
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "Foo",
			wantErr: false,
		},
		{
			// Test case with calling sourceFileName method with file which has multiple dots in its name
			// As a result, want to receive a name that should be executed
			name: "File with multiple dots in the name",
			prepare: func() {
				compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
				primaryFilePath := filepath.Join(compiled, "main.function.scala")
				err := os.WriteFile(primaryFilePath, []byte("object MinimalWordCount {def main(cmdlineArgs: Array[String]): Unit = {}}"), 0600)
				if err != nil {
					panic(err)
				}
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "main.function",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			defer tt.cleanup()
			got, err := executableName(tt.args.executableFolder)
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
