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
	"context"
	"testing"

	"beam.apache.org/playground/backend/internal/constants"
)

var ctx = context.Background()

func TestGetExampleKey(t *testing.T) {
	exampleKey := GetExampleKey(ctx, "MOCK_ID")
	if exampleKey.Namespace != constants.Namespace ||
		exampleKey.Parent != nil ||
		exampleKey.Kind != constants.ExampleKind ||
		exampleKey.Name != "MOCK_ID" {
		t.Error("GetExampleKey() unexpected result")
	}
}

func TestGetSdkKey(t *testing.T) {
	sdkKey := GetSdkKey(ctx, "MOCK_ID")
	if sdkKey.Namespace != constants.Namespace ||
		sdkKey.Parent != nil ||
		sdkKey.Kind != constants.SdkKind ||
		sdkKey.Name != "MOCK_ID" {
		t.Error("GetSdkKey() unexpected result")
	}
}

func TestGetFileKey(t *testing.T) {
	sdkKey := GetFileKey(ctx, "MOCK_ID")
	if sdkKey.Namespace != constants.Namespace ||
		sdkKey.Parent != nil ||
		sdkKey.Kind != constants.FileKind ||
		sdkKey.Name != "MOCK_ID" {
		t.Error("GetFileKey() unexpected result")
	}
}

func TestGetSchemaVerKey(t *testing.T) {
	sdkKey := GetSchemaVerKey(ctx, "MOCK_ID")
	if sdkKey.Namespace != constants.Namespace ||
		sdkKey.Parent != nil ||
		sdkKey.Kind != constants.SchemaKind ||
		sdkKey.Name != "MOCK_ID" {
		t.Error("GetSchemaVerKey() unexpected result")
	}
}

func TestGetSnippetKey(t *testing.T) {
	sdkKey := GetSnippetKey(ctx, "MOCK_ID")
	if sdkKey.Namespace != constants.Namespace ||
		sdkKey.Parent != nil ||
		sdkKey.Kind != constants.SnippetKind ||
		sdkKey.Name != "MOCK_ID" {
		t.Error("GetSnippetKey() unexpected result")
	}
}

func TestGetPCObjectKey(t *testing.T) {
	sdkKey := GetPCObjectKey(ctx, "MOCK_ID")
	if sdkKey.Namespace != constants.Namespace ||
		sdkKey.Parent != nil ||
		sdkKey.Kind != constants.PCObjectKind ||
		sdkKey.Name != "MOCK_ID" {
		t.Error("GetPCObjectKey() unexpected result")
	}
}

func TestGetIDWithDelimiter(t *testing.T) {
	tests := []struct {
		name           string
		values         []interface{}
		expectedResult string
	}{
		{
			name:           "Getting ID separated by a delimiter in the usual case",
			values:         []interface{}{"SDK_JAVA", "MOCK_EXAMPLE"},
			expectedResult: "SDK_JAVA_MOCK_EXAMPLE",
		},
		{
			name:           "Getting ID separated by a delimiter when an input data has only one parameter",
			values:         []interface{}{"MOCK_EXAMPLE"},
			expectedResult: "MOCK_EXAMPLE",
		},
		{
			name:           "Getting ID separated by a delimiter when an input data is empty",
			values:         []interface{}{""},
			expectedResult: "",
		},
		{
			name:           "Getting ID separated by a delimiter when an input data is nil",
			values:         nil,
			expectedResult: "",
		},
		{
			name:           "Getting ID separated by a delimiter when an input data has string and integer",
			values:         []interface{}{"MOCK_EXAMPLE", 2},
			expectedResult: "MOCK_EXAMPLE_2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualResult := GetIDWithDelimiter(tt.values...)
			if actualResult != tt.expectedResult {
				t.Errorf("GetIDWithDelimiter() unexpected result")
			}
		})
	}
}
