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
)

const (
	javaExtension = ".java"
)

// GetJavaValidators return validators methods that should be applied to Java code
func GetJavaValidators(filePath string) *[]Validator {
	validatorArgs := make([]interface{}, 2)
	validatorArgs[0] = filePath
	validatorArgs[1] = javaExtension
	pathCheckerValidator := Validator{
		Validator: fs_tool.CheckPathIsValid,
		Args:      validatorArgs,
	}
	validators := []Validator{pathCheckerValidator}
	return &validators
}
