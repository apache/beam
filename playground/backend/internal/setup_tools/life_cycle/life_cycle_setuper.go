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
	"beam.apache.org/playground/backend/internal/emulators"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/logger"
	utils "beam.apache.org/playground/backend/internal/utils"
)

const (
	javaLogConfigFileName  = "logging.properties"
	javaTmpLogConfigFile   = "logging_updated.properties"
	javaLogFilePlaceholder = "{logFilePath}"
	goModFileName          = "go.mod"
	goSumFileName          = "go.sum"
	bashCmd                = "bash"
	scioProjectName        = "scio"
	scioProjectPath        = scioProjectName + "/src/main/scala/" + scioProjectName
	logFileName            = "logs.log"
	defaultExampleInSbt    = "WordCount.scala"
	scioProject            = "new_scio_project.sh"
	scioCommonConstants    = "ExampleData.scala"
)

// Setup returns fs_tool.LifeCycle.
// Also, prepares files and folders needed to code processing according to sdk
func Setup(sdk pb.Sdk, sources []entity.FileEntity, pipelineId uuid.UUID, workingDir, pipelinesFolder, preparedModDir string, emulatorConfiguration emulators.EmulatorConfiguration) (*fs_tool.LifeCycle, error) {
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
			errDelete := lc.DeleteFolders()
			if errDelete != nil {
				return nil, fmt.Errorf("error during cleanup handling error when creating necessary files for Go sdk: %s, cleanup error: %s", err.Error(), errDelete.Error())
			}
			return nil, fmt.Errorf("error during create necessary files for the Go sdk: %s", err.Error())
		}
	case pb.Sdk_SDK_JAVA:
		if err = prepareJavaFiles(lc, workingDir, pipelineId); err != nil {
			errDelete := lc.DeleteFolders()
			if errDelete != nil {
				return nil, fmt.Errorf("error during cleanup handling error when creating necessary files for Java sdk: %s, cleanup error: %s", err.Error(), errDelete.Error())
			}
			return nil, fmt.Errorf("error during create necessary files for the Java sdk: %s", err.Error())
		}
	case pb.Sdk_SDK_SCIO:
		if lc, err = prepareSbtFiles(lc, lc.Paths.AbsoluteBaseFolderPath, workingDir); err != nil {
			errDelete := lc.DeleteFolders()
			if errDelete != nil {
				return nil, fmt.Errorf("error during cleanup handling error when creating necessary files for Scio sdk: %s, cleanup error: %s", err.Error(), errDelete.Error())
			}
			return nil, fmt.Errorf("error during create necessary files for the Scio sdk: %s", err.Error())
		}
	}

	// create file with code
	err = lc.CreateSourceCodeFiles(sources)
	if err != nil {
		logger.Errorf("%s: RunCode(): CreateSourceCodeFile(): %s\n", pipelineId, err.Error())
		errDelete := lc.DeleteFolders()
		if errDelete != nil {
			return nil, fmt.Errorf("error during cleaning up when handling error %s, cleanup error: %s", err.Error(), errDelete.Error())
		}
		return nil, errors.New("error during create file with code")
	}

	// start emulators
	if emulatorConfiguration.KafkaEmulatorExecutablePath != "" {
		err = lc.StartEmulators(emulatorConfiguration)
		if err != nil {
			logger.Errorf("error during starting emulators: %s", err.Error())
			errDelete := lc.DeleteFolders()
			if errDelete != nil {
				return nil, fmt.Errorf("error during cleaning up when handling error %s, cleanup error: %s", err.Error(), errDelete.Error())
			}
			lc.StopEmulators()
			return nil, err
		}
	} else {
		logger.Warnf("kafka emulator executable path is empty, emulators will not be started")
	}

	return lc, nil
}

// prepareGoFiles prepares file for Go environment.
// Copy go.mod and go.sum file from /path/to/preparedModDir to /path/to/workingDir/pipelinesFolder/{pipelineId}
func prepareGoFiles(lc *fs_tool.LifeCycle, preparedModDir string, pipelineId uuid.UUID) error {
	if err := utils.CopyFilePreservingName(goModFileName, preparedModDir, lc.Paths.AbsoluteBaseFolderPath); err != nil {
		logger.Errorf("%s: error during copying %s file: %s\n", pipelineId, goModFileName, err.Error())
		return err
	}
	if err := utils.CopyFilePreservingName(goSumFileName, preparedModDir, lc.Paths.AbsoluteBaseFolderPath); err != nil {
		logger.Errorf("%s: error during copying %s file: %s\n", pipelineId, goSumFileName, err.Error())
		return err
	}
	return nil
}

// prepareJavaFiles prepares file for Java environment.
// Copy log config file from /path/to/workingDir to /path/to/workingDir/pipelinesFolder/{pipelineId}
//
//	and update this file according to pipeline.
func prepareJavaFiles(lc *fs_tool.LifeCycle, workingDir string, pipelineId uuid.UUID) error {
	err := utils.CopyFilePreservingName(javaLogConfigFileName, workingDir, lc.Paths.AbsoluteBaseFolderPath)
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
	cmd := exec.Command(bashCmd, filepath.Join(workingDir, scioProject))
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
	executableName := lc.Paths.FindExecutableName

	err = os.Remove(filepath.Join(absFileFolderPath, defaultExampleInSbt))
	if err != nil {
		return lc, err
	}

	err = utils.CopyFilePreservingName(scioCommonConstants, workingDir, absFileFolderPath)
	if err != nil {
		return lc, err
	}

	lc.Paths = fs_tool.LifeCyclePaths{
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
		FindExecutableName:               executableName,
	}

	return lc, nil
}
