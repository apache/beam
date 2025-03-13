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
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/uuid"
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
					FindExecutableName:               findExecutableName,
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
		t.Errorf("Failed to create folders %s, error = %v", workDir, err)
	}
	defer func() {
		err := os.RemoveAll(workDir)
		if err != nil {
			t.Errorf("Failed to cleanup %s, error = %v", workDir, err)
		}
	}()

	compileJavaFiles := func(sourceFiles ...string) error {
		compiledDir := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)

		args := append([]string{"-d", compiledDir}, sourceFiles...)
		err := exec.Command("javac", args...).Run()
		if err != nil {
			return err
		}
		return nil
	}

	cleanupFunc := func() error {
		compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
		dirEntries, err := os.ReadDir(compiled)
		if err != nil {
			return err
		}

		for _, entry := range dirEntries {
			err := os.Remove(filepath.Join(compiled, entry.Name()))
			if err != nil {
				return err
			}
		}

		return nil
	}

	type args struct {
		executableFolder string
	}
	tests := []struct {
		name    string
		prepare func() error
		cleanup func() error
		args    args
		want    string
		wantErr bool
	}{
		{
			// Test case with calling findExecutableName() function with empty directory.
			// As a result, want to receive an error.
			name:    "Directory is empty",
			prepare: func() error { return nil },
			cleanup: func() error { return nil },
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "",
			wantErr: true,
		},
		{
			// Test case with calling findExecutableName() function with correct pipelineId and workingDir.
			// As a result, want to receive a name that should be executed
			name: "Get executable name",
			prepare: func() error {
				compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
				filePath := filepath.Join(compiled, "temp.class")
				err := os.WriteFile(filePath, []byte("TEMP_DATA"), 0600)
				if err != nil {
					return err
				}
				return nil
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "temp",
			wantErr: false,
		},
		{
			// Test case with calling findExecutableName() function with wrong directory.
			// As a result, want to receive an error.
			name:    "Directory doesn't exist",
			prepare: func() error { return nil },
			cleanup: func() error { return nil },
			args: args{
				executableFolder: filepath.Join(workDir, pipelineId.String()),
			},
			want:    "",
			wantErr: true,
		},
		{
			// Test case with calling findExecutableName() function with multiple files where one of them is main
			// As a result, want to receive a name that should be executed
			name: "Multiple files where one of them is main",
			prepare: func() error {
				compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
				primaryFilePath := filepath.Join(compiled, "main.scala")
				err := os.WriteFile(primaryFilePath, []byte("object MinimalWordCount {def main(cmdlineArgs: Array[String]): Unit = {}}"), 0600)
				if err != nil {
					return err
				}
				secondaryFilePath := filepath.Join(compiled, "temp.scala")
				err = os.WriteFile(secondaryFilePath, []byte("TEMP_DATA"), 0600)
				if err != nil {
					return err
				}
				return nil
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "main",
			wantErr: false,
		},
		{
			// Test case with calling findExecutableName() function with multiple files where one of them is a .class file
			// with main() method. The executable class name is the same as the source file name.
			// As a result, want to receive a name that should be executed
			name: "Multiple Java class files where one of them contains main",
			prepare: func() error {
				testdataPath := "java_testdata"
				sourceFile := filepath.Join(testdataPath, "HasMainTest1.java")

				err := compileJavaFiles(sourceFile)
				if err != nil {
					return err
				}

				return nil
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "HasMainTest1",
			wantErr: false,
		},
		{
			// Test case with calling findExecutableName() function with multiple files where one of them is a .class file
			// with main() method. The executable class name is different from the source file name.
			// As a result, want to receive a name that should be executed
			name: "Multiple Java class files where one of them contains main",
			prepare: func() error {
				testdataPath := "java_testdata"
				// The source file contains two classes, one of them has main() method (class name is different from the source file name)
				sourceFile := filepath.Join(testdataPath, "HasMainTest2.java")

				err := compileJavaFiles(sourceFile)
				if err != nil {
					return err
				}

				return nil
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "Bar",
			wantErr: false,
		},
		{
			// Test case with calling findExecutableName() function with multiple files where one of them is a .class file
			// with main() method with incorrect signature.
			// As a result, want to receive a name that should be executed
			name: "Multiple Java class files where one of them contains main() with incorrect signature",
			prepare: func() error {
				testdataPath := "java_testdata"
				sourceFile := filepath.Join(testdataPath, "HasIncorrectMain.java")

				err := compileJavaFiles(sourceFile)
				if err != nil {
					return err
				}

				return nil
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "",
			wantErr: true,
		},
		{
			// Test case with calling findExecutableName() function with multiple files where none of them has main().
			// As a result, want to receive a name that should be executed
			name: "Multiple Java class files where none of them contain main()",
			prepare: func() error {
				testdataPath := "java_testdata"
				sourceFile := filepath.Join(testdataPath, "HasNoMain.java")

				err := compileJavaFiles(sourceFile)
				if err != nil {
					return err
				}

				return nil
			},
			cleanup: cleanupFunc,
			args: args{
				executableFolder: filepath.Join(workDir, pipelinesFolder, pipelineId.String(), "bin"),
			},
			want:    "",
			wantErr: true,
		},
		{
			// Test case with calling findExecutableName() function with file which has multiple dots in its name
			// As a result, want to receive a name that should be executed
			name: "File with multiple dots in the name",
			prepare: func() error {
				compiled := filepath.Join(workDir, pipelinesFolder, pipelineId.String(), compiledFolderName)
				primaryFilePath := filepath.Join(compiled, "main.function.scala")
				err := os.WriteFile(primaryFilePath, []byte("object MinimalWordCount {def main(cmdlineArgs: Array[String]): Unit = {}}"), 0600)
				if err != nil {
					return err
				}
				return nil
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
			err := tt.prepare()
			if err != nil {
				t.Errorf("java_fs_test cleanup error = %v", err)
			}
			defer func() {
				err = tt.cleanup()
				if err != nil {
					t.Errorf("java_fs_test cleanup error = %v", err)
				}
			}()
			got, err := findExecutableName(context.Background(), tt.args.executableFolder)
			if (err != nil) != tt.wantErr {
				t.Errorf("java_fs_test error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("java_fs_test got = %v, want %v", got, tt.want)
			}
		})
	}
}
