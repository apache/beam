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
	"beam.apache.org/playground/backend/internal/logger"
	"os"
	"strings"
)

const pyUnitTestPattern = "import unittest"

type pythonValidator struct {
	filepath string
}

func GetPythonValidator(filepath string) Validator {
	return pythonValidator{filepath: filepath}
}

func (v pythonValidator) Validate() (ValidationResult, error) {
	var result = ValidationResult{}
	var err error
	if result.IsUnitTest, err = CheckIsUnitTestPy(v.filepath); err != nil {
		return result, err
	}
	return result, nil
}

func CheckIsUnitTestPy(filePath string) (ValidatorResult, error) {
	code, err := os.ReadFile(filePath)
	if err != nil {
		logger.Errorf("Validation: Error during open file: %s, err: %s\n", filePath, err.Error())
		return Error, err
	}
	// check whether Python code is unit test code
	return resultFromBool(strings.Contains(string(code), pyUnitTestPattern)), nil
}
