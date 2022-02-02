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
	"beam.apache.org/playground/backend/internal/validators"
	"fmt"
)

// GetValidators returns slice of validators.Validator according to sdk
func GetValidators(sdk pb.Sdk, filepath string) (*[]validators.Validator, error) {
	var val *[]validators.Validator
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		val = validators.GetJavaValidators(filepath)
	case pb.Sdk_SDK_GO:
		val = validators.GetGoValidators(filepath)
	case pb.Sdk_SDK_PYTHON:
		val = validators.GetPyValidators(filepath)
	case pb.Sdk_SDK_SCIO:
		val = validators.GetScioValidators(filepath)
	default:
		return nil, fmt.Errorf("incorrect sdk: %s", sdk)
	}
	return val, nil
}
