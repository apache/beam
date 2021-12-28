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

package life_cycle

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/logger"
	"bufio"
	"errors"
	"github.com/google/uuid"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	javaLogConfigFileName  = "logging.properties"
	javaTmpLogConfigFile   = "logging_updated.properties"
	javaLogFilePlaceholder = "{logFilePath}"
	goModFileName          = "go.mod"
	goSumFileName          = "go.sum"
	baseFileFolder         = "executable_files"
)

// Setup returns fs_tool.LifeCycle.
// Also, prepares files and folders needed to code processing according to sdk
func Setup(sdk pb.Sdk, code string, pipelineId uuid.UUID, workingDir string, preparedModDir string) (*fs_tool.LifeCycle, error) {
	// create file system service
	lc, err := fs_tool.NewLifeCycle(sdk, pipelineId, workingDir)
	if err != nil {
		logger.Errorf("%s: error during create new life cycle: %s\n", pipelineId, err.Error())
		return nil, errors.New("error during create a new file system")
	}

	// create folders
	err = lc.CreateFolders()
	if err != nil {
		logger.Errorf("%s: error during create folders: %s\n", pipelineId, err.Error())
		return nil, errors.New("error during prepare necessary folders")
	}

	// copy necessary files
	switch sdk {
	case pb.Sdk_SDK_GO:
		if err = prepareGoFiles(lc, preparedModDir, pipelineId); err != nil {
			lc.DeleteFolders()
			return nil, errors.New("error during create necessary files for the Go sdk")
		}
	case pb.Sdk_SDK_JAVA:
		if err = prepareJavaFiles(lc, workingDir, pipelineId); err != nil {
			lc.DeleteFolders()
			return nil, errors.New("error during create necessary files for the Java sdk")
		}
	}

	// create file with code
	err = lc.CreateSourceCodeFile(code)
	if err != nil {
		logger.Errorf("%s: RunCode(): CreateSourceCodeFile(): %s\n", pipelineId, err.Error())
		lc.DeleteFolders()
		return nil, errors.New("error during create file with code")
	}
	return lc, nil
}

// prepareGoFiles prepares file for Go environment.
// Copy go.mod and go.sum file from /path/to/preparedModDir to /path/to/workingDir/executable_files/{pipelineId}
func prepareGoFiles(lc *fs_tool.LifeCycle, preparedModDir string, pipelineId uuid.UUID) error {
	if err := lc.CopyFile(goModFileName, preparedModDir, lc.Dto.GetAbsoluteBaseFolderPath()); err != nil {
		logger.Errorf("%s: error during copying %s file: %s\n", pipelineId, goModFileName, err.Error())
		return err
	}
	if err := lc.CopyFile(goSumFileName, preparedModDir, lc.Dto.GetAbsoluteBaseFolderPath()); err != nil {
		logger.Errorf("%s: error during copying %s file: %s\n", pipelineId, goSumFileName, err.Error())
		return err
	}
	return nil
}

// prepareJavaFiles prepares file for Java environment.
// Copy log config file from /path/to/workingDir to /path/to/workingDir/executable_files/{pipelineId}
//	and update this file according to pipeline.
func prepareJavaFiles(lc *fs_tool.LifeCycle, workingDir string, pipelineId uuid.UUID) error {
	err := lc.CopyFile(javaLogConfigFileName, workingDir, lc.Dto.GetAbsoluteBaseFolderPath())
	if err != nil {
		logger.Errorf("%s: error during copying logging.properties file: %s\n", pipelineId, err.Error())
		return err
	}
	err = updateJavaLogConfigFile(lc.Dto)
	if err != nil {
		logger.Errorf("%s: error during updating logging.properties file: %s\n", pipelineId, err.Error())
		return err
	}
	return nil
}

// updateJavaLogConfigFile updates java log config file according to pipeline
func updateJavaLogConfigFile(lcDto fs_tool.LifeCycleDTO) error {
	logConfigFilePath := filepath.Join(lcDto.GetAbsoluteBaseFolderPath(), javaLogConfigFileName)
	logConfigUpdatedFilePath := filepath.Join(lcDto.GetAbsoluteBaseFolderPath(), javaTmpLogConfigFile)
	if _, err := os.Stat(logConfigFilePath); os.IsNotExist(err) {
		return err
	}

	logConfigFile, err := os.Open(logConfigFilePath)
	if err != nil {
		return err
	}

	updatedFile, err := os.Create(logConfigUpdatedFilePath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(logConfigFile)

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.ReplaceAll(line, javaLogFilePlaceholder, lcDto.GetAbsoluteLogFilePath())
		if _, err = io.WriteString(updatedFile, line+"\n"); err != nil {
			return err
		}
	}
	if err = scanner.Err(); err != nil {
		return err
	}

	if err = os.Rename(updatedFile.Name(), logConfigFilePath); err != nil {
		return err
	}
	return nil
}
