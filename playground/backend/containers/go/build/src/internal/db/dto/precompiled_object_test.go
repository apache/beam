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

package dto

import (
	"context"
	"testing"
	"time"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/utils"
)

type ExampleDTOTestCase struct {
	name   string
	given  func() *ExampleDTO
	then   func(dto *ExampleDTO) bool
	errMsg string
}

func TestExampleDTO_GetComplexity(t *testing.T) {
	tests := getTestTable([]*ExampleDTOTestCase{
		{
			name: "Getting a complexity in the usual case",
			then: func(dto *ExampleDTO) bool {
				return pb.Complexity_COMPLEXITY_MEDIUM == dto.GetComplexity()
			},
		},
	})
	runTestTable(t, tests)
}

func TestExampleDTO_HasMultiFiles(t *testing.T) {
	tests := getTestTable([]*ExampleDTOTestCase{
		{
			name: "Getting a flag pointing to multiple files in the usual case",
			then: func(dto *ExampleDTO) bool {
				return dto.HasMultiFiles()
			},
		},
	})
	runTestTable(t, tests)
}

func TestExampleDTO_IsDefault(t *testing.T) {
	tests := getTestTable([]*ExampleDTOTestCase{
		{
			name: "Getting a flag pointing to a default example in the usual case",
			then: func(dto *ExampleDTO) bool {
				return !dto.IsDefault()
			},
		},
	})
	runTestTable(t, tests)
}

func TestExampleDTO_GetType(t *testing.T) {
	tests := getTestTable([]*ExampleDTOTestCase{
		{
			name: "Getting a type in the usual case",
			then: func(dto *ExampleDTO) bool {
				return pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE == dto.GetType()
			},
		},
	})
	runTestTable(t, tests)
}

func TestExampleDTO_GetContextLine(t *testing.T) {
	tests := getTestTable([]*ExampleDTOTestCase{
		{
			name: "Getting a context line in the usual case",
			then: func(dto *ExampleDTO) bool {
				return dto.GetContextLine() == 20
			},
		},
	})
	runTestTable(t, tests)
}

func TestExampleDTO_GetSDK(t *testing.T) {
	tests := getTestTable([]*ExampleDTOTestCase{
		{
			name: "Getting SDK in the usual case",
			then: func(dto *ExampleDTO) bool {
				return dto.GetSDK() == pb.Sdk_SDK_JAVA
			},
		},
	})
	runTestTable(t, tests)
}

func TestExampleDTO_GetDatasets(t *testing.T) {
	tests := getTestTable([]*ExampleDTOTestCase{
		{
			name: "Getting datasets in the usual case",
			then: func(dto *ExampleDTO) bool {
				datasets := dto.GetDatasets()
				if len(datasets) != 2 {
					return false
				}
				datasetPath0 := datasets[0].GetDatasetPath()
				datasetPath1 := datasets[1].GetDatasetPath()
				if datasetPath0 != "MOCK_PATH_0" || datasetPath1 != "MOCK_PATH_1" {
					return false
				}
				return true
			},
		},
	})
	runTestTable(t, tests)
}

func getTestTable(cases []*ExampleDTOTestCase) []*ExampleDTOTestCase {
	for _, testCase := range cases {
		if testCase.given == nil {
			testCase.given = getExampleDTO
		}
		if len(testCase.errMsg) == 0 {
			testCase.errMsg = "Unexpected result"
		}
	}
	return cases
}

func runTestTable(t *testing.T, tests []*ExampleDTOTestCase) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dto := tt.given()
			result := tt.then(dto)
			if !result {
				t.Fatal(tt.errMsg)
			}
		})
	}
}

func getExampleDTO() *ExampleDTO {
	example := &entity.ExampleEntity{
		Name:   "MOCK_NAME",
		Sdk:    utils.GetSdkKey(context.Background(), pb.Sdk_SDK_JAVA.String()),
		Descr:  "MOCK_NAME",
		Tags:   nil,
		Cats:   nil,
		Path:   "MOCK_PATH",
		Type:   pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE.String(),
		Origin: constants.ExampleOrigin,
		SchVer: nil,
	}

	snippet := &entity.SnippetEntity{
		Key:           nil,
		OwnerId:       "MOCK_OWNER",
		Sdk:           utils.GetSdkKey(context.Background(), pb.Sdk_SDK_JAVA.String()),
		PipeOpts:      "MOCK_OPTS",
		Created:       time.Time{},
		LVisited:      time.Time{},
		Origin:        constants.ExampleOrigin,
		VisitCount:    0,
		SchVer:        nil,
		NumberOfFiles: 2,
		Complexity:    pb.Complexity_COMPLEXITY_MEDIUM.String(),
	}

	files := []*entity.FileEntity{
		{
			Name:     "MOCK_NAME_0",
			Content:  "MOCK_CONTENT",
			CntxLine: 20,
			IsMain:   true,
		},
		{
			Name:     "MOCK_NAME_1",
			Content:  "MOCK_CONTENT",
			CntxLine: 20,
			IsMain:   false,
		},
	}

	datasets := []*DatasetDTO{
		{
			Path: "MOCK_PATH_0",
			Config: map[string]string{
				"Topic": "MOCK_TOPIC",
			},
			Emulator: pb.EmulatorType_EMULATOR_TYPE_KAFKA,
		},
		{
			Path: "MOCK_PATH_1",
			Config: map[string]string{
				"Topic": "MOCK_TOPIC",
			},
			Emulator: pb.EmulatorType_EMULATOR_TYPE_KAFKA,
		},
	}

	return &ExampleDTO{
		Example:            example,
		Snippet:            snippet,
		Files:              files,
		DefaultExampleName: "MOCK_DEFAULT_EXAMPLE",
		Datasets:           datasets,
	}
}
