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

package validators

import (
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/logger"
	"os"
	"strings"
)

const (
	javaExtension       = ".java"
	javaUnitTestPattern = "@Test"
	javaKatasPattern    = "org.apache.beam.learning.katas"
)

type javaValidator struct {
	filepath string
}

func GetJavaValidator(filepath string) Validator {
	return javaValidator{filepath: filepath}
}

func (v javaValidator) Validate() (map[string]bool, error) {
	var result = make(map[string]bool)
	var err error

	if result["Valid path"], err = fs_tool.CheckPathIsValid(v.filepath, javaExtension); err != nil {
		return result, err
	}
	if result[UnitTestValidatorName], err = checkIsUnitTestJava(v.filepath, javaExtension); err != nil {
		return result, err
	}
	if result[KatasValidatorName], err = checkIsKataJava(v.filepath, javaExtension); err != nil {
		return result, err
	}

	return result, nil
}

// checkIsUnitTestJava checks if the pipeline is a UnitTest
func checkIsUnitTestJava(args ...interface{}) (bool, error) {
	ok, err := checkPipelineType(append(args, javaUnitTestPattern)...)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// checkIsKataJava checks if the pipeline is a kata
func checkIsKataJava(args ...interface{}) (bool, error) {
	ok, err := checkPipelineType(append(args, javaKatasPattern)...)
	if err != nil {
		return false, err
	}
	return ok, nil
}

func checkPipelineType(args ...interface{}) (bool, error) {
	filePath := args[0].(string)
	pattern := args[2].(string)
	code, err := os.ReadFile(filePath)
	if err != nil {
		logger.Errorf("Validation: Error during open file: %s, err: %s\n", filePath, err.Error())
		return false, err
	}
	// check whether s contains substring unit test or katas
	return strings.Contains(string(code), pattern), nil
}
