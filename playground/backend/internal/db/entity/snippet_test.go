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

package entity

import (
	"beam.apache.org/playground/backend/internal/utils"
	"testing"
)

func TestSnippet_ID(t *testing.T) {
	tests := []struct {
		name    string
		snip    *Snippet
		want    string
		wantErr bool
	}{
		{
			name: "Snippet ID() in the usual case",
			snip: &Snippet{
				Snippet: &SnippetEntity{
					Sdk:      utils.GetNameKey("pg_sdks", "SDK_GO", "Playground", nil),
					PipeOpts: "MOCK_OPTIONS",
				},
				Files: []*FileEntity{{
					Name:    "MOCK_NAME",
					Content: "MOCK_CONTENT",
					IsMain:  false,
				}},
				IDInfo: &IDInfo{
					IdLength: 11,
					Salt:     "MOCK_SALT",
				},
			},
			want:    "HLCGZQmHrRy",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := tt.snip.ID()
			if (err != nil) != tt.wantErr {
				t.Errorf("ID() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil {
				if len(id) != tt.snip.IdLength {
					t.Error("The ID length is not 11")
				}
				if tt.want != id {
					t.Error("ID is wrong")
				}
			}
		})
	}
}

func TestFile_ID(t *testing.T) {
	file := &FileEntity{
		Name:    "MOCK_NAME",
		Content: "MOCK_CONTENT",
		IsMain:  false,
	}

	tests := []struct {
		name    string
		snip    *Snippet
		file    *FileEntity
		want    string
		wantErr bool
	}{
		{
			name: "FileEntity ID() in the usual case",
			snip: &Snippet{
				Snippet: &SnippetEntity{
					Sdk:      utils.GetNameKey("pg_sdks", "SDK_GO", "Playground", nil),
					PipeOpts: "MOCK_OPTIONS",
				},
				IDInfo: &IDInfo{
					Salt:     "MOCK_SALT",
					IdLength: 11,
				},
				Files: []*FileEntity{file},
			},
			file:    file,
			want:    "OpdzDJYiSbj",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := tt.file.ID(tt.snip)
			if (err != nil) != tt.wantErr {
				t.Errorf("ID() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil {
				if len(id) != tt.snip.IdLength {
					t.Error("The ID length is not 11")
				}
				if tt.want != id {
					t.Error("ID is wrong")
				}
			}
		})
	}
}
