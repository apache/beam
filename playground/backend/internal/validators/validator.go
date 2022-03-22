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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"fmt"
)

const (
	UnitTestValidatorName = "UnitTest"
	KatasValidatorName    = "Katas"
)

type Validator struct {
	Validator func(args ...interface{}) (bool, error)
	Args      []interface{}
	Name      string
}

// GetValidators returns slice of validators.Validator according to sdk
func GetValidators(sdk pb.Sdk, filepath string) (*[]Validator, error) {
	var val *[]Validator
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		val = GetJavaValidators(filepath)
	case pb.Sdk_SDK_GO:
		val = GetGoValidators(filepath)
	case pb.Sdk_SDK_PYTHON:
		val = GetPyValidators(filepath)
	case pb.Sdk_SDK_SCIO:
		val = GetScioValidators(filepath)
	default:
		return nil, fmt.Errorf("incorrect sdk: %s", sdk)
	}
	return val, nil
}
