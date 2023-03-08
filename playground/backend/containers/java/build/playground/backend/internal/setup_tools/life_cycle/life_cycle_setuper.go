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
	"beam.apache.org/playground/backend/internal/utils"
	"bufio"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	javaLogConfigFileName  = "logging.properties"
	javaTmpLogConfigFile   = "logging_updated.properties"
	javaLogFilePlaceholder = "{logFilePath}"
	goModFileName          = "go.mod"
	goSumFileName          = "go.sum"
	scioProjectName        = "y"
	scioProjectPath        = scioProjectName + "/src/main/scala/" + scioProjectName
	logFileName            = "logs.log"
	defaultExampleInSbt    = "WordCount.scala"
	shCmd                  = "sh"
	rmCmd                  = "rm"
	cpCmd                  = "cp"
	scioProject            = "new_scio_project.sh"
	scioCommonConstants    = "ExampleData.scala"
)

// Setup returns fs_tool.LifeCycle.
// Also, prepares files and folders needed to code processing according to sdk
func Setup(sdk pb.Sdk, code string, pipelineId uuid.UUID, workingDir, pipelinesFolder, preparedModDir string) (*fs_tool.LifeCycle, error) {
	// create file system service
	lc, err := fs_tool.NewLifeCycle(sdk, pipelineId, filepath.Join(workingDir, pipelinesFolder))
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
			return nil, fmt.Errorf("error during create necessary files for the Go sdk: %s", err.Error())
		}
	case pb.Sdk_SDK_JAVA:
		if err = prepareJavaFiles(lc, workingDir, pipelineId); err != nil {
			lc.DeleteFolders()
			return nil, fmt.Errorf("error during create necessary files for the Java sdk: %s", err.Error())
		}
	case pb.Sdk_SDK_SCIO:
		if lc, err = prepareSbtFiles(lc, lc.Paths.AbsoluteBaseFolderPath, workingDir); err != nil {
			lc.DeleteFolders()
			return nil, fmt.Errorf("error during create necessary files for the Scio sdk: %s", err.Error())
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
// Copy go.mod and go.sum file from /path/to/preparedModDir to /path/to/workingDir/pipelinesFolder/{pipelineId}
func prepareGoFiles(lc *fs_tool.LifeCycle, preparedModDir string, pipelineId uuid.UUID) error {
	if err := lc.CopyFile(goModFileName, preparedModDir, lc.Paths.AbsoluteBaseFolderPath); err != nil {
		logger.Errorf("%s: error during copying %s file: %s\n", pipelineId, goModFileName, err.Error())
		return err
	}
	if err := lc.CopyFile(goSumFileName, preparedModDir, lc.Paths.AbsoluteBaseFolderPath); err != nil {
		logger.Errorf("%s: error during copying %s file: %s\n", pipelineId, goSumFileName, err.Error())
		return err
	}
	return nil
}

// prepareJavaFiles prepares file for Java environment.
// Copy log config file from /path/to/workingDir to /path/to/workingDir/pipelinesFolder/{pipelineId}
//	and update this file according to pipeline.
func prepareJavaFiles(lc *fs_tool.LifeCycle, workingDir string, pipelineId uuid.UUID) error {
	err := lc.CopyFile(javaLogConfigFileName, workingDir, lc.Paths.AbsoluteBaseFolderPath)
	if err != nil {
		logger.Errorf("%s: error during copying logging.properties file: %s\n", pipelineId, err.Error())
		return err
	}
	err = updateJavaLogConfigFile(lc.Paths)
	if err != nil {
		logger.Errorf("%s: error during updating logging.properties file: %s\n", pipelineId, err.Error())
		return err
	}
	return nil
}

// updateJavaLogConfigFile updates java log config file according to pipeline
func updateJavaLogConfigFile(paths fs_tool.LifeCyclePaths) error {
	logConfigFilePath := filepath.Join(paths.AbsoluteBaseFolderPath, javaLogConfigFileName)
	logConfigUpdatedFilePath := filepath.Join(paths.AbsoluteBaseFolderPath, javaTmpLogConfigFile)
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
		line = strings.ReplaceAll(line, javaLogFilePlaceholder, paths.AbsoluteLogFilePath)
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

func prepareSbtFiles(lc *fs_tool.LifeCycle, pipelineFolder string, workingDir string) (*fs_tool.LifeCycle, error) {
	cmd := exec.Command(shCmd, filepath.Join(workingDir, scioProject))
	cmd.Dir = pipelineFolder
	_, err := cmd.Output()
	if err != nil {
		return lc, err
	}

	sourceFileFolder := filepath.Join(pipelineFolder, scioProjectPath)
	fileName := lc.Paths.SourceFileName
	absFileFolderPath, _ := filepath.Abs(sourceFileFolder)
	absFilePath, _ := filepath.Abs(filepath.Join(absFileFolderPath, fileName))
	absLogFilePath, _ := filepath.Abs(filepath.Join(absFileFolderPath, logFileName))
	absGraphFilePath, _ := filepath.Abs(filepath.Join(absFileFolderPath, utils.GraphFileName))
	projectFolder, _ := filepath.Abs(filepath.Join(pipelineFolder, scioProjectName))
	executableName := lc.Paths.ExecutableName

	_, err = exec.Command(rmCmd, filepath.Join(absFileFolderPath, defaultExampleInSbt)).Output()
	if err != nil {
		return lc, err
	}

	_, err = exec.Command(cpCmd, filepath.Join(workingDir, scioCommonConstants), absFileFolderPath).Output()
	if err != nil {
		return lc, err
	}

	lc = &fs_tool.LifeCycle{
		Paths: fs_tool.LifeCyclePaths{
			SourceFileName:                   fileName,
			AbsoluteSourceFileFolderPath:     absFileFolderPath,
			AbsoluteSourceFilePath:           absFilePath,
			ExecutableFileName:               fileName,
			AbsoluteExecutableFileFolderPath: absFileFolderPath,
			AbsoluteExecutableFilePath:       absFilePath,
			AbsoluteBaseFolderPath:           absFileFolderPath,
			AbsoluteLogFilePath:              absLogFilePath,
			AbsoluteGraphFilePath:            absGraphFilePath,
			ProjectDir:                       projectFolder,
		},
	}
	lc.Paths.ExecutableName = executableName
	return lc, nil
}
