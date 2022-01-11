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
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	fileMode    = 0600
	logFileName = "logs.log"
)

// LifeCyclePaths contains all files/folders paths
type LifeCyclePaths struct {
	SourceFileName                   string // {pipelineId}.{sourceFileExtension}
	AbsoluteSourceFileFolderPath     string // /path/to/workingDir/executable_files/{pipelineId}/src
	AbsoluteSourceFilePath           string // /path/to/workingDir/executable_files/{pipelineId}/src/{pipelineId}.{sourceFileExtension}
	ExecutableFileName               string // {pipelineId}.{executableFileExtension}
	AbsoluteExecutableFileFolderPath string // /path/to/workingDir/executable_files/{pipelineId}/bin
	AbsoluteExecutableFilePath       string // /path/to/workingDir/executable_files/{pipelineId}/bin/{pipelineId}.{executableFileExtension}
	AbsoluteBaseFolderPath           string // /path/to/workingDir/executable_files/{pipelineId}
	AbsoluteLogFilePath              string // /path/to/workingDir/executable_files/{pipelineId}/logs.log
	ExecutableName                   func(uuid.UUID, string) (string, error)
}

// LifeCycle is used for preparing folders and files to process code for one code processing request.
type LifeCycle struct {
	folderGlobs []string // folders that should be created to process code
	Paths       LifeCyclePaths
}

// NewLifeCycle returns a corresponding LifeCycle depending on the given SDK.
// workingDir should be existed and be prepared to create/delete/modify folders into him.
func NewLifeCycle(sdk pb.Sdk, pipelineId uuid.UUID, workingDir string) (*LifeCycle, error) {
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		return newJavaLifeCycle(pipelineId, workingDir), nil
	case pb.Sdk_SDK_GO:
		return newGoLifeCycle(pipelineId, workingDir), nil
	case pb.Sdk_SDK_PYTHON:
		return newPythonLifeCycle(pipelineId, workingDir), nil
	default:
		return nil, fmt.Errorf("%s isn't supported now", sdk)
	}
}

// CreateFolders creates all folders which will be used for code execution.
func (l *LifeCycle) CreateFolders() error {
	for _, folder := range l.folderGlobs {
		err := os.MkdirAll(folder, fs.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteFolders deletes all previously provisioned folders.
func (l *LifeCycle) DeleteFolders() error {
	for _, folder := range l.folderGlobs {
		err := os.RemoveAll(folder)
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateSourceCodeFile creates an executable file (i.e. file.{sourceFileExtension}).
func (l *LifeCycle) CreateSourceCodeFile(code string) error {
	if _, err := os.Stat(l.Paths.AbsoluteSourceFileFolderPath); os.IsNotExist(err) {
		return err
	}

	filePath := l.Paths.AbsoluteSourceFilePath
	err := os.WriteFile(filePath, []byte(code), fileMode)
	if err != nil {
		return err
	}
	return nil
}

// CopyFile copies a file with fileName from sourceDir to destinationDir.
func (l *LifeCycle) CopyFile(fileName, sourceDir, destinationDir string) error {
	absSourcePath := filepath.Join(sourceDir, fileName)
	absDestinationPath := filepath.Join(destinationDir, fileName)
	sourceFileStat, err := os.Stat(absSourcePath)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", fileName)
	}

	sourceFile, err := os.Open(absSourcePath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(absDestinationPath)
	if err != nil {
		return err
	}
	defer destinationFile.Close()
	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}
	return nil
}
