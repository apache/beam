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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/preparers"
	"beam.apache.org/playground/backend/internal/validators"
	"fmt"
	"regexp"
	"sync"
)

// GetPreparers returns slice of preparers.Preparer according to sdk
func GetPreparers(sdk pb.Sdk, filepath string, valResults *sync.Map) (*[]preparers.Preparer, error) {
	isUnitTest, ok := valResults.Load(validators.UnitTestValidatorName)
	if !ok {
		return nil, fmt.Errorf("GetPreparers:: No information about unit test validation result")
	}
	builder := preparers.NewPreparersBuilder(filepath)
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		isKata, ok := valResults.Load(validators.KatasValidatorName)
		if !ok {
			return nil, fmt.Errorf("GetPreparers:: No information about katas validation result")
		}
		preparers.GetJavaPreparers(builder, isUnitTest.(bool), isKata.(bool))
	case pb.Sdk_SDK_GO:
		preparers.GetGoPreparers(builder, isUnitTest.(bool))
	case pb.Sdk_SDK_PYTHON:
		preparers.GetPythonPreparers(builder)
	case pb.Sdk_SDK_SCIO:
		preparers.GetScioPreparers(builder)
	default:
		return nil, fmt.Errorf("incorrect sdk: %s", sdk)
	}
	return builder.Build().GetPreparers(), nil
}

// ReplaceSpacesWithEquals prepares pipelineOptions by replacing spaces between option and them value to equals.
func ReplaceSpacesWithEquals(pipelineOptions string) string {
	re := regexp.MustCompile(`(--[A-z0-9]+)\s([A-z0-9]+)`)
	return re.ReplaceAllString(pipelineOptions, "$1=$2")
}
