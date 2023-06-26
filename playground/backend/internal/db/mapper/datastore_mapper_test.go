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

package mapper

import (
	"context"
	"os"
	"testing"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/utils"
)

var testable *DatastoreMapper
var datastoreMapperCtx = context.Background()

func TestMain(m *testing.M) {
	appEnv := environment.NewApplicationEnvs("/app", "", "", "", "", "../../../.", "", "", "", "", "", nil, 0, 0)
	appEnv.SetSchemaVersion(1)
	props, _ := environment.NewProperties(appEnv.PropertyPath())
	testable = NewDatastoreMapper(datastoreMapperCtx, appEnv, props)
	exitValue := m.Run()
	os.Exit(exitValue)
}

func TestEntityMapper_ToSnippet(t *testing.T) {
	tests := []struct {
		name     string
		input    *pb.SaveSnippetRequest
		expected *entity.Snippet
	}{
		{
			name: "Snippet mapper in the usual case",
			input: &pb.SaveSnippetRequest{
				Files:           []*pb.SnippetFile{{Name: "MOCK_NAME", Content: "MOCK_CONTENT"}},
				Sdk:             pb.Sdk_SDK_JAVA,
				PipelineOptions: "MOCK_OPTIONS",
				Complexity:      pb.Complexity_COMPLEXITY_MEDIUM,
			},
			expected: &entity.Snippet{
				IDMeta: &entity.IDMeta{
					Salt:     "Beam playground salt",
					IdLength: 11,
				},
				Snippet: &entity.SnippetEntity{
					SchVer:        utils.GetSchemaVerKey(datastoreMapperCtx, 1),
					Sdk:           utils.GetSdkKey(datastoreMapperCtx, pb.Sdk_SDK_JAVA.String()),
					PipeOpts:      "MOCK_OPTIONS",
					Origin:        constants.UserSnippetOrigin,
					NumberOfFiles: 1,
					Complexity:    pb.Complexity_COMPLEXITY_MEDIUM.String(),
				},
				Files: []*entity.FileEntity{
					{
						Name:     "MOCK_NAME",
						Content:  "MOCK_CONTENT",
						CntxLine: 1,
						IsMain:   true,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := testable.ToSnippet(tt.input)
			if result.IdLength != tt.expected.IdLength ||
				result.Salt != tt.expected.Salt ||
				result.Snippet.PipeOpts != tt.expected.Snippet.PipeOpts ||
				result.Snippet.NumberOfFiles != 1 ||
				result.Snippet.Origin != constants.UserSnippetOrigin ||
				result.Snippet.Complexity != tt.expected.Snippet.Complexity {
				t.Error("Unexpected result")
			}
		})
	}
}

func TestEntityMapper_ToFileEntity(t *testing.T) {
	type args struct {
		info *pb.SaveSnippetRequest
		file *pb.SnippetFile
	}
	tests := []struct {
		name        string
		args        args
		expected    *entity.FileEntity
		expectedErr bool
	}{
		{
			name: "File entity mapper in the usual case",
			args: args{
				info: &pb.SaveSnippetRequest{
					Files:           []*pb.SnippetFile{{Name: "MOCK_NAME", Content: "MOCK_CONTENT"}},
					Sdk:             pb.Sdk_SDK_JAVA,
					PipelineOptions: "MOCK_OPTIONS",
				},
				file: &pb.SnippetFile{
					Name:    "MOCK_NAME",
					Content: "MOCK_CONTENT",
					IsMain:  true,
				},
			},
			expected: &entity.FileEntity{
				Name:     "MOCK_NAME.java",
				Content:  "MOCK_CONTENT",
				CntxLine: 1,
				IsMain:   true,
			},
		},
		{
			name: "File entity mapper when file name and sdk are invalid",
			args: args{
				info: &pb.SaveSnippetRequest{
					Files:           []*pb.SnippetFile{{Name: "MOCK_NAME.scio", Content: "MOCK_CONTENT"}},
					Sdk:             pb.Sdk_SDK_JAVA,
					PipelineOptions: "MOCK_OPTIONS",
					Complexity:      pb.Complexity_COMPLEXITY_MEDIUM,
				},
				file: &pb.SnippetFile{
					Name:    "MOCK_NAME.scio",
					Content: "MOCK_CONTENT",
					IsMain:  true,
				},
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := testable.ToFileEntity(tt.args.info, tt.args.file)
			if (err != nil) != tt.expectedErr {
				t.Errorf("Unexpected error")
				return
			}
			if err == nil {
				if result.IsMain != tt.expected.IsMain ||
					result.Name != tt.expected.Name ||
					result.Content != tt.expected.Content ||
					result.CntxLine != tt.expected.CntxLine {
					t.Error("Unexpected result")
				}
			}
		})
	}
}
