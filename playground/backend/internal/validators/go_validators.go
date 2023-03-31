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

const goUnitTestPattern = "*testing.T"

type goValidator struct {
	filepath string
}

func GetGoValidator(filepath string) Validator {
	return goValidator{filepath: filepath}
}

func (v goValidator) Validate() (ValidationResult, error) {
	var result = ValidationResult{}
	var err error
	if result.IsUnitTest, err = CheckIsUnitTestGo(v.filepath); err != nil {
		return result, err
	}
	return result, nil
}

func CheckIsUnitTestGo(filepath string) (ValidationOutcome, error) {
	code, err := os.ReadFile(filepath)
	if err != nil {
		logger.Errorf("Validation: Error during open file: %s, err: %s\n", filepath, err.Error())
		return Error, err
	}
	// check whether Go code is unit test code
	return resultFromBool(strings.Contains(string(code), goUnitTestPattern)), nil
}
