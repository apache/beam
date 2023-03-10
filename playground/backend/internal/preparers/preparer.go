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

package preparers

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/emulators"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/validators"
	"fmt"
)

// Preparer is used to make preparations with file with code.
type Preparer interface {
	Prepare() error
}

type preparer struct {
	filePath string
}

// GetPreparers returns slice of preparers.Preparer according to sdk
func GetPreparers(sdk pb.Sdk, filepath string, valResults validators.ValidationResult, prepareParams *emulators.EmulatorParameters) (Preparer, error) {
	isUnitTest, err := valResults.IsUnitTest.ToBool()
	if err != nil {
		logger.Errorf("GetPreparers: No information whether example is a unit test or no: check whether the validation step had been ran correctly")
		return nil, fmt.Errorf("GetPreparers: No information whether example is a unit test or no: %s", err)
	}
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		isKatas, err := valResults.IsKatas.ToBool()
		if err != nil {
			logger.Errorf("GetPreparers: No information about katas validation result: check whether the validation step had been ran correctly")
			return nil, fmt.Errorf("GetPreparers:: No information about katas validation result: %s", err.Error())
		}
		return GetJavaPreparer(filepath, isUnitTest, isKatas, prepareParams), nil
	case pb.Sdk_SDK_GO:
		return GetGoPreparer(filepath, isUnitTest), nil
	case pb.Sdk_SDK_PYTHON:
		return GetPythonPreparer(filepath, isUnitTest), nil
	case pb.Sdk_SDK_SCIO:
		return GetScioPreparer(filepath), nil
	default:
		return nil, fmt.Errorf("incorrect sdk: %s", sdk)
	}
}
