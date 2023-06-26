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
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"testing/iotest"
)

func TestReadableFile_Open(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "file1.txt"), []byte("test1"))
	writeGzip(t, filepath.Join(dir, "file2.gz"), []byte("test2"))

	tests := []struct {
		name string
		file ReadableFile
		want []byte
	}{
		{
			name: "Open uncompressed file",
			file: ReadableFile{
				Metadata: FileMetadata{
					Path: filepath.Join(dir, "file1.txt"),
				},
				Compression: compressionUncompressed,
			},
			want: []byte("test1"),
		},
		{
			name: "Open file with auto-detection of compression",
			file: ReadableFile{
				Metadata: FileMetadata{
					Path: filepath.Join(dir, "file2.gz"),
				},
				Compression: compressionAuto,
			},
			want: []byte("test2"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			rc, err := tt.file.Open(ctx)
			if err != nil {
				t.Fatalf("Open() error = %v, want nil", err)
			}

			t.Cleanup(func() {
				rc.Close()
			})

			if err := iotest.TestReader(rc, tt.want); err != nil {
				t.Errorf("TestReader() error = %v, want nil", err)
			}
		})
	}
}

func Test_compressionFromExt(t *testing.T) {
	tests := []struct {
		name string
		path string
		want compressionType
	}{
		{
			name: "compressionGzip for gz extension",
			path: "file.gz",
			want: compressionGzip,
		},
		{
			name: "compressionUncompressed for no extension",
			path: "file",
			want: compressionUncompressed,
		},
		{
			name: "compressionUncompressed for unrecognized extension",
			path: "file.unknown",
			want: compressionUncompressed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compressionFromExt(tt.path); got != tt.want {
				t.Errorf("compressionFromExt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newDecompressionReader(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "file1.txt"), []byte("test1"))
	writeGzip(t, filepath.Join(dir, "file2.gz"), []byte("test2"))

	tests := []struct {
		name    string
		path    string
		comp    compressionType
		want    []byte
		wantErr bool
	}{
		{
			name: "Reader for uncompressed file",
			path: filepath.Join(dir, "file1.txt"),
			comp: compressionUncompressed,
			want: []byte("test1"),
		},
		{
			name: "Reader for gzip compressed file",
			path: filepath.Join(dir, "file2.gz"),
			comp: compressionGzip,
			want: []byte("test2"),
		},
		{
			name:    "Error - reader for auto compression not supported",
			path:    filepath.Join(dir, "file2.gz"),
			comp:    compressionAuto,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := openFile(t, tt.path)

			dr, err := newDecompressionReader(rc, tt.comp)
			if (err != nil) != tt.wantErr {
				t.Fatalf("newDecompressionReader() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			t.Cleanup(func() {
				dr.Close()
			})

			if err := iotest.TestReader(dr, tt.want); err != nil {
				t.Errorf("TestReader() error = %v, want nil", err)
			}
		})
	}
}

func TestReadableFile_Read(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "file1.txt"), []byte("test1"))
	writeGzip(t, filepath.Join(dir, "file2.gz"), []byte("test2"))

	tests := []struct {
		name string
		file ReadableFile
		want []byte
	}{
		{
			name: "Read contents from uncompressed file",
			file: ReadableFile{
				Metadata: FileMetadata{
					Path: filepath.Join(dir, "file1.txt"),
				},
				Compression: compressionUncompressed,
			},
			want: []byte("test1"),
		},
		{
			name: "Read contents from gzip compressed file",
			file: ReadableFile{
				Metadata: FileMetadata{
					Path: filepath.Join(dir, "file2.gz"),
				},
				Compression: compressionGzip,
			},
			want: []byte("test2"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			got, err := tt.file.Read(ctx)
			if err != nil {
				t.Fatalf("Read() error = %v, want nil", err)
			}

			if !bytes.Equal(got, tt.want) {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadableFile_ReadString(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "file1.txt"), []byte("test1"))
	writeGzip(t, filepath.Join(dir, "file2.gz"), []byte("test2"))

	tests := []struct {
		name string
		file ReadableFile
		want string
	}{
		{
			name: "Read contents from uncompressed file as string",
			file: ReadableFile{
				Metadata: FileMetadata{
					Path: filepath.Join(dir, "file1.txt"),
				},
				Compression: compressionUncompressed,
			},
			want: "test1",
		},
		{
			name: "Read contents from gzip compressed file as string",
			file: ReadableFile{
				Metadata: FileMetadata{
					Path: filepath.Join(dir, "file2.gz"),
				},
				Compression: compressionGzip,
			},
			want: "test2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			got, err := tt.file.ReadString(ctx)
			if err != nil {
				t.Fatalf("ReadString() error = %v, want nil", err)
			}

			if got != tt.want {
				t.Errorf("ReadString() got = %v, want %v", got, tt.want)
			}
		})
	}
}
