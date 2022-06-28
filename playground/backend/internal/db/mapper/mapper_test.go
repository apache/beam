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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	datastoreDb "beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/utils"
	"testing"
)

var testable *EntityMapper

func TestMain(m *testing.M) {
	appEnv := environment.NewApplicationEnvs("/app", "", "", "", "", "MOCK_SALT", "", "PG_USER", "", nil, 0, "", 1000, 11)
	appEnv.SetSchemaVersion("MOCK_SCHEMA")
	testable = New(appEnv)
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
			},
			expected: &entity.Snippet{
				IDMeta: &entity.IDMeta{
					Salt:     "MOCK_SALT",
					IdLength: 11,
				},
				//OwnerId property will be used in Tour of Beam project
				Snippet: &entity.SnippetEntity{
					SchVer:        utils.GetNameKey(datastoreDb.SchemaKind, "MOCK_SCHEMA", datastoreDb.Namespace, nil),
					Sdk:           utils.GetNameKey(datastoreDb.SdkKind, "SDK_JAVA", datastoreDb.Namespace, nil),
					PipeOpts:      "MOCK_OPTIONS",
					Origin:        entity.Origin(entity.OriginValue["PG_USER"]),
					NumberOfFiles: 1,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := testable.ToSnippet(tt.input)
			if result.IdLength != tt.expected.IdLength ||
				result.Salt != tt.expected.Salt ||
				result.Files[0].IsMain != true ||
				result.Files[0].Content != tt.expected.Files[0].Content ||
				result.Files[0].Name != tt.expected.Files[0].Name {
				t.Error("Unexpected result")
			}
		})
	}
}
