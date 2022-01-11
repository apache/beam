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
	"beam.apache.org/playground/backend/internal/preparators"
	"beam.apache.org/playground/backend/internal/validators"
	"fmt"
	"regexp"
	"sync"
)

// GetPreparators returns slice of preparators.Preparator according to sdk
func GetPreparators(sdk pb.Sdk, filepath string, valResults *sync.Map) (*[]preparators.Preparator, error) {
	isUnitTest, ok := valResults.Load(validators.UnitTestValidatorName)
	if !ok {
		return nil, fmt.Errorf("GetPreparators:: No information about unit test validation result")
	}
	builder := preparators.NewPreparersBuilder(filepath)
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		isKata, ok := valResults.Load(validators.KatasValidatorName)
		if !ok {
			return nil, fmt.Errorf("GetPreparators:: No information about katas validation result")
		}
		preparators.GetJavaPreparators(builder, isUnitTest.(bool), isKata.(bool))
	case pb.Sdk_SDK_GO:
		preparators.GetGoPreparators(builder, isUnitTest.(bool))
	case pb.Sdk_SDK_PYTHON:
		preparators.GetPythonPreparators(builder)
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
