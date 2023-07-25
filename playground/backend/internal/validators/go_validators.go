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
	"io/ioutil"
	"strings"
)

const goUnitTestPattern = "*testing.T"

// GetGoValidators return validators methods that should be applied to Go code
func GetGoValidators(filePath string) *[]Validator {
	validatorArgs := make([]interface{}, 1)
	validatorArgs[0] = filePath
	unitTestValidator := Validator{
		Validator: CheckIsUnitTestGo,
		Args:      validatorArgs,
		Name:      UnitTestValidatorName,
	}
	validators := []Validator{unitTestValidator}
	return &validators
}

func CheckIsUnitTestGo(args ...interface{}) (bool, error) {
	filePath := args[0].(string)
	code, err := ioutil.ReadFile(filePath)
	if err != nil {
		logger.Errorf("Validation: Error during open file: %s, err: %s\n", filePath, err.Error())
		return false, err
	}
	// check whether Go code is unit test code
	return strings.Contains(string(code), goUnitTestPattern), nil
}
