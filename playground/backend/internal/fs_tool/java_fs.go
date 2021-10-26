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
	"errors"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
)

const (
	javaBaseFileFolder          = "executable_files"
	javaExecutableFileExtension = "java"
	javaCompiledFileExtension   = "class"
	javaSourceFolderName        = "src"
	javaCompiledFolderName      = "bin"
)

// newJavaLifeCycle creates LifeCycle with java SDK environment.
func newJavaLifeCycle(pipelineId uuid.UUID, workingDir string) *LifeCycle {
	baseFileFolder := filepath.Join(workingDir, javaBaseFileFolder, pipelineId.String())
	srcFileFolder := filepath.Join(baseFileFolder, javaSourceFolderName)
	binFileFolder := filepath.Join(baseFileFolder, javaCompiledFolderName)

	return &LifeCycle{
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
		ExecutableName: executableName,
		pipelineId:     pipelineId,
	}
}

// executableName returns name that should be executed (HelloWorld for HelloWorld.class for java SDK)
func executableName(pipelineId uuid.UUID, workingDir string) (string, error) {
	baseFileFolder := filepath.Join(workingDir, javaBaseFileFolder, pipelineId.String())
	binFileFolder := filepath.Join(baseFileFolder, javaCompiledFolderName)
	dirEntries, err := os.ReadDir(binFileFolder)
	if err != nil {
		return "", err
	}
	if len(dirEntries) < 1 {
		return "", errors.New("number of executable files should be more than one")
	}
	return strings.Split(dirEntries[len(dirEntries)-1].Name(), ".")[0], nil
}
