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
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	fileMode = 0600
)

// Folder contains names of folders with executable and compiled files.
// For each SDK these values should be set depending on folders that need for the SDK (/src and /bin for java SDK).
type Folder struct {
	BaseFolder       string
	ExecutableFolder string
	CompiledFolder   string
}

// Extension contains executable and compiled files' extensions.
// For each SDK these values should be set depending on SDK's extensions (.java and .class for java SDK).
type Extension struct {
	ExecutableExtension string
	CompiledExtension   string
}

// LifeCycle is used for preparing folders and files to process code for one request.
// For each SDK folders (Folder) and extensions (Extension) should be set correctly.
type LifeCycle struct {
	folderGlobs []string //folders that should be created to process code
	Folder      Folder
	Extension   Extension
	pipelineId  uuid.UUID
}

// NewLifeCycle returns a corresponding LifeCycle depending on the given SDK.
// workingDir should be existed and be prepared to create/delete/modify folders into him.
func NewLifeCycle(sdk pb.Sdk, pipelineId uuid.UUID, workingDir string) (*LifeCycle, error) {
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		return newJavaLifeCycle(pipelineId, workingDir), nil
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

// CreateExecutableFile creates an executable file (i.e. file.java for the Java SDK).
func (l *LifeCycle) CreateExecutableFile(code string) (string, error) {
	if _, err := os.Stat(l.Folder.ExecutableFolder); os.IsNotExist(err) {
		return "", err
	}

	fileName := getFileName(l.pipelineId, l.Extension.ExecutableExtension)
	filePath := filepath.Join(l.Folder.ExecutableFolder, fileName)
	err := os.WriteFile(filePath, []byte(code), fileMode)
	if err != nil {
		return "", err
	}
	return fileName, nil
}

// GetAbsoluteExecutableFilePath returns absolute filepath to executable file (/path/to/workingDir/executable_files/{pipelineId}/src/{pipelineId}.java for java SDK).
func (l *LifeCycle) GetAbsoluteExecutableFilePath() string {
	fileName := getFileName(l.pipelineId, l.Extension.ExecutableExtension)
	filePath := filepath.Join(l.Folder.ExecutableFolder, fileName)
	absoluteFilePath, _ := filepath.Abs(filePath)
	return absoluteFilePath
}

// GetAbsoluteExecutableFilesFolderPath returns absolute path to executable folder (/path/to/workingDir/executable_files/{pipelineId}).
func (l *LifeCycle) GetAbsoluteExecutableFilesFolderPath() string {
	absoluteFilePath, _ := filepath.Abs(l.Folder.BaseFolder)
	return absoluteFilePath
}

// GetExecutableName returns name that should be executed (HelloWorld for HelloWorld.class for java SDK)
func (l *LifeCycle) GetExecutableName() (string, error) {
	dirEntries, err := os.ReadDir(l.Folder.CompiledFolder)
	if err != nil {
		return "", err
	}
	if len(dirEntries) < 1 {
		return "", errors.New("number of executable files must be more than 0")
	}
	return strings.Split(dirEntries[len(dirEntries)-1].Name(), ".")[0], nil
}

// getFileName returns fileName by pipelineId and fileType ({pipelineId}.java for java SDK).
func getFileName(pipelineId uuid.UUID, fileType string) string {
	return fmt.Sprintf("%s.%s", pipelineId, fileType)
}
