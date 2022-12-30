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
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/utils"
)

const (
	JavaSourceFileExtension   = ".java"
	javaCompiledFileExtension = ".class"
	javaEntryPointFullName    = "public static void main(java.lang.String[])"
	javaDecompilerCommand     = "javap"
)

// newJavaLifeCycle creates LifeCycle with java SDK environment.
func newJavaLifeCycle(pipelineId uuid.UUID, pipelinesFolder string) *LifeCycle {
	javaLifeCycle := newCompilingLifeCycle(pipelineId, pipelinesFolder, JavaSourceFileExtension, javaCompiledFileExtension)
	javaLifeCycle.Paths.ExecutableName = executableName
	return javaLifeCycle
}

// executableName returns name that should be executed (HelloWorld for HelloWorld.class for java SDK)
func executableName(executableFileFolderPath string) (string, error) {
	dirEntries, err := os.ReadDir(executableFileFolderPath)
	if err != nil {
		return "", err
	}
	if len(dirEntries) < 1 {
		return "", errors.New("number of executable files should be at least one")
	}

	if len(dirEntries) == 1 {
		return utils.TrimExtension(dirEntries[0].Name()), nil
	}

	for _, entry := range dirEntries {
		filePath := fmt.Sprintf("%s/%s", executableFileFolderPath, entry.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			logger.Error(fmt.Sprintf("error during file reading: %s", err.Error()))
			break
		}
		ext := filepath.Ext(entry.Name())
		filename := strings.TrimSuffix(entry.Name(), ext)
		sdk := utils.ToSDKFromExt(ext)

		if sdk == pb.Sdk_SDK_UNSPECIFIED {
			logger.Error("invalid file extension")
			break
		}

		switch ext {
		case javaCompiledFileExtension:
			isMain, err := isMainClass(executableFileFolderPath, filename)
			if err != nil {
				return "", err
			}
			if isMain {
				return filename, nil
			}
		default:
			if utils.IsFileMain(string(content), sdk) {
				return filename, nil
			}
		}

	}

	return utils.TrimExtension(dirEntries[len(dirEntries)-1].Name()), nil
}

func isMainClass(classPath string, className string) (bool, error) {
	cmd := exec.Command(javaDecompilerCommand, "-public", "-classpath", classPath, className)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return false, err
	}

	return strings.Contains(out.String(), javaEntryPointFullName), nil
}
