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

// Folder contains names of folders with executable and compiled files.
// For each SDK these values should be set depending on folders that need for the SDK.
type Folder struct {
	BaseFolder           string
	SourceFileFolder     string
	ExecutableFileFolder string
}

// Extension contains executable and compiled files' extensions.
// For each SDK these values should be set depending on SDK's extensions.
type Extension struct {
	SourceFileExtension     string
	ExecutableFileExtension string
}

// LifeCycle is used for preparing folders and files to process code for one request.
// For each SDK folders (Folder) and extensions (Extension) should be set correctly.
type LifeCycle struct {
	folderGlobs    []string //folders that should be created to process code
	Folder         Folder
	Extension      Extension
	ExecutableName func(uuid.UUID, string) (string, error)
	pipelineId     uuid.UUID
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
func (l *LifeCycle) CreateSourceCodeFile(code string) (string, error) {
	if _, err := os.Stat(l.Folder.SourceFileFolder); os.IsNotExist(err) {
		return "", err
	}

	fileName := l.pipelineId.String() + l.Extension.SourceFileExtension
	filePath := filepath.Join(l.Folder.SourceFileFolder, fileName)
	err := os.WriteFile(filePath, []byte(code), fileMode)
	if err != nil {
		return "", err
	}
	return fileName, nil
}

// GetAbsoluteSourceFilePath returns absolute filepath to executable file (/path/to/workingDir/executable_files/{pipelineId}/src/{pipelineId}.{sourceFileExtension}).
func (l *LifeCycle) GetAbsoluteSourceFilePath() string {
	fileName := l.pipelineId.String() + l.Extension.SourceFileExtension
	filePath := filepath.Join(l.Folder.SourceFileFolder, fileName)
	absoluteFilePath, _ := filepath.Abs(filePath)
	return absoluteFilePath
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

// GetAbsoluteExecutableFilePath returns absolute filepath to compiled file (/path/to/workingDir/executable_files/{pipelineId}/bin/{pipelineId}.{executableExtension}).
func (l *LifeCycle) GetAbsoluteExecutableFilePath() string {
	fileName := l.pipelineId.String() + l.Extension.ExecutableFileExtension
	filePath := filepath.Join(l.Folder.ExecutableFileFolder, fileName)
	absoluteFilePath, _ := filepath.Abs(filePath)
	return absoluteFilePath
}

// GetAbsoluteBaseFolderPath returns absolute path to executable folder (/path/to/workingDir/executable_files/{pipelineId}).
func (l *LifeCycle) GetAbsoluteBaseFolderPath() string {
	absoluteFilePath, _ := filepath.Abs(l.Folder.BaseFolder)
	return absoluteFilePath
}

// GetAbsoluteLogFilePath returns absolute path to the logs file (/path/to/workingDir/executable_files/{pipelineId}/logs.log)
func (l *LifeCycle) GetAbsoluteLogFilePath() string {
	filePath := filepath.Join(l.Folder.BaseFolder, logFileName)
	absoluteFilePath, _ := filepath.Abs(filePath)
	return absoluteFilePath
}

// GetAbsoluteSourceFolderPath returns absolute path to executable folder (/path/to/workingDir/executable_files/{pipelineId}/src).
func (l *LifeCycle) GetAbsoluteSourceFolderPath() string {
	absoluteFilePath, _ := filepath.Abs(l.Folder.SourceFileFolder)
	return absoluteFilePath
}
