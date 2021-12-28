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
	"path/filepath"
)

const (
	baseFileFolder     = "executable_files"
	sourceFolderName   = "src"
	compiledFolderName = "bin"
)

// newCompilingLifeCycle creates LifeCycle for compiled SDK environment.
func newCompilingLifeCycle(pipelineId uuid.UUID, workingDir string, sourceFileExtension string, compiledFileExtension string) *LifeCycle {
	baseFileFolder := filepath.Join(workingDir, baseFileFolder, pipelineId.String())
	srcFileFolder := filepath.Join(baseFileFolder, sourceFolderName)
	binFileFolder := filepath.Join(baseFileFolder, compiledFolderName)
	return &LifeCycle{
		folderGlobs: []string{baseFileFolder, srcFileFolder, binFileFolder},
		Dto: LifeCycleDTO{
			PipelineId: pipelineId,
			Folder: Folder{
				BaseFolder:           baseFileFolder,
				SourceFileFolder:     srcFileFolder,
				ExecutableFileFolder: binFileFolder,
			},
			Extension: Extension{
				SourceFileExtension:     sourceFileExtension,
				ExecutableFileExtension: compiledFileExtension,
			},
		},
	}
}

// newInterpretedLifeCycle creates LifeCycle for interpreted SDK environment.
func newInterpretedLifeCycle(pipelineId uuid.UUID, workingDir string, sourceFileExtension string) *LifeCycle {
	sourceFileFolder := filepath.Join(workingDir, baseFileFolder, pipelineId.String())
	return &LifeCycle{
		folderGlobs: []string{sourceFileFolder},
		Dto: LifeCycleDTO{
			PipelineId: pipelineId,
			Folder: Folder{
				BaseFolder:           sourceFileFolder,
				SourceFileFolder:     sourceFileFolder,
				ExecutableFileFolder: sourceFileFolder,
			},
			Extension: Extension{
				ExecutableFileExtension: sourceFileExtension,
				SourceFileExtension:     sourceFileExtension,
			},
		},
	}
}
