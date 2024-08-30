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

package fileio

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestReadMatches(t *testing.T) {
	tests := []struct {
		name    string
		opts    []ReadOptionFn
		input   []any
		want    []any
		wantErr bool
	}{
		{
			name: "Read matches",
			input: []any{
				FileMetadata{
					Path: "file1.txt",
					Size: 5,
				},
				FileMetadata{
					Path: "file2.txt",
					Size: 0,
				},
			},
			want: []any{
				ReadableFile{
					Metadata: FileMetadata{
						Path: "file1.txt",
						Size: 5,
					},
					Compression: compressionAuto,
				},
				ReadableFile{
					Metadata: FileMetadata{
						Path: "file2.txt",
						Size: 0,
					},
					Compression: compressionAuto,
				},
			},
		},
		{
			name: "Read matches with specified compression",
			opts: []ReadOptionFn{
				ReadGzip(),
			},
			input: []any{
				FileMetadata{
					Path: "file1",
					Size: 5,
				},
				FileMetadata{
					Path: "file2",
					Size: 0,
				},
			},
			want: []any{
				ReadableFile{
					Metadata: FileMetadata{
						Path: "file1",
						Size: 5,
					},
					Compression: compressionGzip,
				},
				ReadableFile{
					Metadata: FileMetadata{
						Path: "file2",
						Size: 0,
					},
					Compression: compressionGzip,
				},
			},
		},
		{
			name: "Read matches and skip directories",
			input: []any{
				FileMetadata{
					Path: "dir/",
					Size: 0,
				},
				FileMetadata{
					Path: "file1.txt",
					Size: 5,
				},
			},
			want: []any{
				ReadableFile{
					Metadata: FileMetadata{
						Path: "file1.txt",
						Size: 5,
					},
					Compression: compressionAuto,
				},
			},
		},
		{
			name: "Error - directories disallowed",
			opts: []ReadOptionFn{
				ReadDirectoryDisallow(),
			},
			input: []any{
				FileMetadata{
					Path: "dir/",
					Size: 0,
				},
				FileMetadata{
					Path: "file1.txt",
					Size: 5,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()

			col := beam.Create(s, tt.input...)
			got := ReadMatches(s, col, tt.opts...)

			passert.Equals(s, got, tt.want...)
			if err := ptest.Run(p); (err != nil) != tt.wantErr {
				t.Errorf("ReadMatches() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_isDirectory(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "Path to directory with forward slash directory separator",
			path: "path/to/",
			want: true,
		},
		{
			name: "Path to directory with backslash directory separator",
			path: "path\\to\\",
			want: true,
		},
		{
			name: "Path to file",
			path: "path/to/file",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDirectory(tt.path); got != tt.want {
				t.Errorf("isDirectory() = %v, want %v", got, tt.want)
			}
		})
	}
}
