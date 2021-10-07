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
	pb "beam.apache.org/playground/backend/internal/api"
	"fmt"
	"github.com/google/uuid"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	parentBaseFileFolder = "internal"
	fileMode             = 0600
)

// Folder contains names of folders with executable and compiled files (/src and /bin for java SDK)
type Folder struct {
	BaseFolder       string
	ExecutableFolder string
	CompiledFolder   string
}

// Extension contains executable and compiled files' extensions (.java and .class for java SDK)
type Extension struct {
	ExecutableExtension string
	CompiledExtension   string
}

type LifeCycle struct {
	folderGlobs []string
	Folder      Folder
	Extension   Extension
	pipelineId  uuid.UUID
}

// NewLifeCycle returns a corresponding LifeCycle depending on the given SDK.
func NewLifeCycle(sdk pb.Sdk, pipelineId uuid.UUID) (*LifeCycle, error) {
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		return newJavaLifeCycle(pipelineId), nil
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

// DeleteExecutableFile deletes executable file (file.java for java SDK).
func (l *LifeCycle) DeleteExecutableFile() error {
	fileName := getFileName(l.pipelineId, l.Extension.ExecutableExtension)
	filePath := filepath.Join(l.Folder.ExecutableFolder, fileName)
	return os.RemoveAll(filePath)
}

// DeleteCompiledFile deletes compiled file (file.class for java SDK).
func (l *LifeCycle) DeleteCompiledFile() error {
	fileName := getFileName(l.pipelineId, l.Extension.CompiledExtension)
	filePath := filepath.Join(l.Folder.CompiledFolder, fileName)
	return os.RemoveAll(filePath)
}

// GetAbsoluteExecutableFilePath returns absolute filepath to executable file (.../internal/executable_files/src/file.java for java SDK).
func (l *LifeCycle) GetAbsoluteExecutableFilePath() (string, error) {
	fileName := getFileName(l.pipelineId, l.Extension.ExecutableExtension)
	filePath := filepath.Join(l.Folder.ExecutableFolder, fileName)
	return filepath.Abs(filePath)
}

// GetRelativeExecutableFilePath returns relative filepath to executable file (src/file.java for java SDK).
func (l *LifeCycle) GetRelativeExecutableFilePath() string {
	fileName := getFileName(l.pipelineId, l.Extension.ExecutableExtension)
	filePath := filepath.Join(l.Folder.ExecutableFolder, fileName)
	return filePath[len(l.Folder.BaseFolder)+1:]
}

// getFileName returns fileName by pipelineId and fileType (pipelineId.java for java SDK).
func getFileName(pipelineId uuid.UUID, fileType string) string {
	return fmt.Sprintf("%s.%s", pipelineId, fileType)
}
