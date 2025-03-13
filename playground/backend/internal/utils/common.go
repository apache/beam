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

package utils

import (
	"beam.apache.org/playground/backend/internal/logger"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
)

func ReduceWhiteSpacesToSinge(s string) string {
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(s, " ")
}

// ReadYamlFile reads from a yaml file.
func ReadYamlFile(filename string, out interface{}) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Errorf("ReadYamlFile(): error during reading from a yaml file: %s", err.Error())
		return err
	}
	if err = yaml.Unmarshal(buf, out); err != nil {
		logger.Errorf("ReadYamlFile(): error during parsing from a yaml file: %s", err.Error())
		return err
	}
	return nil
}

// CopyFilePreservingName copies a file with fileName from sourceDir to destinationDir.
func CopyFilePreservingName(fileName, sourceDir, destinationDir string) error {
	absSourcePath := filepath.Join(sourceDir, fileName)
	absDestinationPath := filepath.Join(destinationDir, fileName)
	return CopyFile(absSourcePath, absDestinationPath)
}

// CopyFile copies a file from sourcePath to destinationPath
func CopyFile(sourcePath, destinationPath string) error {
	sourceFileStat, err := os.Stat(sourcePath)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", sourcePath)
	}

	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(destinationPath)
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
