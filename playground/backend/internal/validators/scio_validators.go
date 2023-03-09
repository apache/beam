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

const (
	scalaExtension = ".scala"
)

type scioValidator struct {
	filepath string
}

func GetScioValidator(filepath string) Validator {
	return scioValidator{filepath: filepath}
}

func (v scioValidator) Validate() (ValidationResult, error) {
	var result = ValidationResult{}
	var err error
	if result.IsUnitTest, err = checkIsUnitTestScio(v.filepath, scalaExtension); err != nil {
		return result, err
	}
	return result, nil
}

// checkIsUnitTestScio checks if the pipeline is a UnitTest
func checkIsUnitTestScio(args ...interface{}) (ValidatorResult, error) {
	return No, nil
	//TODO BEAM-13702
}
