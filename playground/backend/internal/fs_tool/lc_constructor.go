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
	"github.com/google/uuid"
	"path/filepath"
)

const (
	sourceFolderName   = "src"
	compiledFolderName = "bin"
)

// newCompilingLifeCycle creates LifeCycle for compiled SDK environment.
func newCompilingLifeCycle(pipelineId uuid.UUID, pipelinesFolder, sourceFileExtension, compiledFileExtension string) *LifeCycle {
	baseFileFolder := filepath.Join(pipelinesFolder, pipelineId.String())
	srcFileFolder := filepath.Join(baseFileFolder, sourceFolderName)
	binFileFolder := filepath.Join(baseFileFolder, compiledFolderName)

	srcFileName := pipelineId.String() + sourceFileExtension
	absSrcFileFolderPath, _ := filepath.Abs(srcFileFolder)
	absSrcFilePath, _ := filepath.Abs(filepath.Join(absSrcFileFolderPath, srcFileName))
	execFileName := pipelineId.String() + compiledFileExtension
	absExecFileFolderPath, _ := filepath.Abs(binFileFolder)
	absExecFilePath, _ := filepath.Abs(filepath.Join(absExecFileFolderPath, execFileName))
	absBaseFolderPath, _ := filepath.Abs(baseFileFolder)
	absLogFilePath, _ := filepath.Abs(filepath.Join(absBaseFolderPath, logFileName))
	absGraphFilePath, _ := filepath.Abs(filepath.Join(absSrcFileFolderPath, utils.GraphFileName))

	return &LifeCycle{
		folderGlobs: []string{baseFileFolder, srcFileFolder, binFileFolder},
		Paths: LifeCyclePaths{
			SourceFileName:                   srcFileName,
			AbsoluteSourceFileFolderPath:     absSrcFileFolderPath,
			AbsoluteSourceFilePath:           absSrcFilePath,
			ExecutableFileName:               execFileName,
			AbsoluteExecutableFileFolderPath: absExecFileFolderPath,
			AbsoluteExecutableFilePath:       absExecFilePath,
			AbsoluteBaseFolderPath:           absBaseFolderPath,
			AbsoluteLogFilePath:              absLogFilePath,
			AbsoluteGraphFilePath:            absGraphFilePath,
		},
	}
}

// newInterpretedLifeCycle creates LifeCycle for interpreted SDK environment.
func newInterpretedLifeCycle(pipelineId uuid.UUID, pipelinesFolder, sourceFileExtension string) *LifeCycle {
	sourceFileFolder := filepath.Join(pipelinesFolder, pipelineId.String())

	fileName := pipelineId.String() + sourceFileExtension
	absFileFolderPath, _ := filepath.Abs(sourceFileFolder)
	absFilePath, _ := filepath.Abs(filepath.Join(absFileFolderPath, fileName))
	absLogFilePath, _ := filepath.Abs(filepath.Join(absFileFolderPath, logFileName))
	absGraphFilePath, _ := filepath.Abs(filepath.Join(absFileFolderPath, utils.GraphFileName))

	return &LifeCycle{
		folderGlobs: []string{sourceFileFolder},
		Paths: LifeCyclePaths{
			SourceFileName:                   fileName,
			AbsoluteSourceFileFolderPath:     absFileFolderPath,
			AbsoluteSourceFilePath:           absFilePath,
			ExecutableFileName:               fileName,
			AbsoluteExecutableFileFolderPath: absFileFolderPath,
			AbsoluteExecutableFilePath:       absFilePath,
			AbsoluteBaseFolderPath:           absFileFolderPath,
			AbsoluteLogFilePath:              absLogFilePath,
			AbsoluteGraphFilePath:            absGraphFilePath,
		},
	}
}
