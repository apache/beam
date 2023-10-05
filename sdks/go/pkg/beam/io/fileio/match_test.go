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
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/google/go-cmp/cmp"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

type testFile struct {
	filename string
	data     []byte
}

var testFiles = []testFile{
	{
		filename: "file1.txt",
		data:     []byte("test1"),
	},
	{
		filename: "file2.txt",
		data:     []byte(""),
	},
	{
		filename: "file3.csv",
		data:     []byte("test3"),
	},
}

func TestMatchFiles(t *testing.T) {
	dir := t.TempDir()
	testDir := filepath.Join(dir, "testdata")

	for _, tf := range testFiles {
		write(t, filepath.Join(testDir, tf.filename), tf.data)
	}

	fp1 := filepath.Join(testDir, "file1.txt")
	fp2 := filepath.Join(testDir, "file2.txt")

	tests := []struct {
		name string
		glob string
		opts []MatchOptionFn
		want []any
	}{
		{
			name: "Match files",
			glob: filepath.Join(dir, "*", "file*.txt"),
			want: []any{
				FileMetadata{
					Path:         fp1,
					Size:         5,
					LastModified: modTime(t, fp1),
				},
				FileMetadata{
					Path:         fp2,
					Size:         0,
					LastModified: modTime(t, fp2),
				},
			},
		},
		{
			name: "Read matches with specified empty match treatment",
			opts: []MatchOptionFn{
				MatchEmptyAllow(),
			},
			glob: filepath.Join(dir, "non-existent.txt"),
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()

			got := MatchFiles(s, tt.glob, tt.opts...)

			passert.Equals(s, got, tt.want...)
			ptest.RunAndValidate(t, p)
		})
	}
}

func TestMatchAll(t *testing.T) {
	dir := t.TempDir()
	testDir := filepath.Join(dir, "testdata")

	for _, tf := range testFiles {
		write(t, filepath.Join(testDir, tf.filename), tf.data)
	}

	fp1 := filepath.Join(testDir, "file1.txt")
	fp2 := filepath.Join(testDir, "file2.txt")
	fp3 := filepath.Join(testDir, "file3.csv")

	tests := []struct {
		name    string
		opts    []MatchOptionFn
		input   []any
		want    []any
		wantErr bool
	}{
		{
			name: "Match all",
			input: []any{
				filepath.Join(dir, "*", "file*.txt"),
				filepath.Join(dir, "*", "file*.csv"),
			},
			want: []any{
				FileMetadata{
					Path:         fp1,
					Size:         5,
					LastModified: modTime(t, fp1),
				},
				FileMetadata{
					Path:         fp2,
					Size:         0,
					LastModified: modTime(t, fp2),
				},
				FileMetadata{
					Path:         fp3,
					Size:         5,
					LastModified: modTime(t, fp3),
				},
			},
		},
		{
			name: "No matches",
			input: []any{
				filepath.Join(dir, "*", "non-existent.txt"),
			},
			want: nil,
		},
		{
			name:  "No matches for empty glob",
			input: []any{""},
			want:  nil,
		},
		{
			name: "No matches for glob without wildcard and empty matches allowed",
			opts: []MatchOptionFn{
				MatchEmptyAllow(),
			},
			input: []any{
				filepath.Join(dir, "non-existent.txt"),
			},
			want: nil,
		},
		{
			name: "Error - no matches for glob without wildcard",
			input: []any{
				filepath.Join(dir, "non-existent.txt"),
			},
			wantErr: true,
		},
		{
			name: "Error - no matches and empty matches disallowed",
			opts: []MatchOptionFn{
				MatchEmptyDisallow(),
			},
			input: []any{
				filepath.Join(dir, "*", "non-existent.txt"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()

			col := beam.Create(s, tt.input...)
			got := MatchAll(s, col, tt.opts...)

			passert.Equals(s, got, tt.want...)
			if err := ptest.Run(p); (err != nil) != tt.wantErr {
				t.Errorf("MatchAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_allowEmptyMatch(t *testing.T) {
	tests := []struct {
		name      string
		glob      string
		treatment emptyTreatment
		want      bool
	}{
		{
			name:      "Allow for emptyAllow",
			glob:      "path/to/file.txt",
			treatment: emptyAllow,
			want:      true,
		},
		{
			name:      "Disallow for emptyDisallow",
			glob:      "path/to/file.txt",
			treatment: emptyDisallow,
			want:      false,
		},
		{
			name:      "Allow for glob with wildcard and emptyAllowIfWildcard",
			glob:      "path/to/*.txt",
			treatment: emptyAllowIfWildcard,
			want:      true,
		},
		{
			name:      "Disallow for glob without wildcard and emptyAllowIfWildcard",
			glob:      "path/to/file.txt",
			treatment: emptyAllowIfWildcard,
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := allowEmptyMatch(tt.glob, tt.treatment); got != tt.want {
				t.Errorf("allowEmptyMatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_metadataFromFiles(t *testing.T) {
	dir := t.TempDir()
	files := make([]string, len(testFiles))

	for i, tf := range testFiles {
		file := filepath.Join(dir, tf.filename)
		write(t, file, tf.data)
		files[i] = file
	}

	fp1 := filepath.Join(dir, "file1.txt")
	fp2 := filepath.Join(dir, "file2.txt")
	fp3 := filepath.Join(dir, "file3.csv")

	tests := []struct {
		name  string
		files []string
		want  []FileMetadata
	}{
		{
			name:  "Slice of FileMetadata from file paths",
			files: files,
			want: []FileMetadata{
				{
					Path:         fp1,
					Size:         5,
					LastModified: modTime(t, fp1),
				},
				{
					Path:         fp2,
					Size:         0,
					LastModified: modTime(t, fp2),
				},
				{
					Path:         fp3,
					Size:         5,
					LastModified: modTime(t, fp3),
				},
			},
		},
		{
			name:  "Nil when files is empty",
			files: nil,
			want:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fs := local.New(ctx)

			got, err := metadataFromFiles(ctx, fs, tt.files)
			if err != nil {
				t.Fatalf("metadataFromFiles() error = %v, want nil", err)
			}

			if !cmp.Equal(got, tt.want) {
				t.Errorf("metadataFromFiles() got = %v, want %v", got, tt.want)
			}
		})
	}
}
