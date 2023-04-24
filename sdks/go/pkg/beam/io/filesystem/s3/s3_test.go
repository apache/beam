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

package s3

import (
	"bytes"
	"context"
	"testing"
	"testing/iotest"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/google/go-cmp/cmp"
)

func TestS3_FilesystemNew(t *testing.T) {
	ctx := context.Background()
	path := "s3://bucket/file.txt"
	fileSystem, err := filesystem.New(ctx, path)
	if err != nil {
		t.Errorf("filesystem.New() error = %v, want %v", err, nil)
	}
	if _, ok := fileSystem.(*fs); !ok {
		t.Errorf("filesystem.New() got type = %T, want %T", fileSystem, &fs{})
	}
	if err := fileSystem.Close(); err != nil {
		t.Errorf("filesystem.Close() error = %v, want %v", err, nil)
	}
}

func Test_fs_List(t *testing.T) {
	tests := []struct {
		name    string
		glob    string
		want    []string
		wantErr bool
	}{
		{
			name:    "List match with full path",
			glob:    "s3://bucket/file-1.txt",
			want:    []string{"s3://bucket/file-1.txt"},
			wantErr: false,
		},
		{
			name:    "List matches with wildcard",
			glob:    "s3://bucket/*.txt",
			want:    []string{"s3://bucket/file-1.txt", "s3://bucket/file-2.txt"},
			wantErr: false,
		},
		{
			name:    "List no matches",
			glob:    "s3://bucket/*.json",
			want:    nil,
			wantErr: false,
		},
		{
			name:    "Error: invalid S3 uri",
			glob:    "bucket/without/scheme",
			wantErr: true,
		},
		{
			name:    "Error: bucket does not exist",
			glob:    "s3://non-existing-bucket/file-1.txt",
			wantErr: true,
		},
		{
			name:    "Error: invalid glob pattern",
			glob:    "s3://bucket/*file-[].txt",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := newServer(t)
			client := newClient(ctx, t, server.URL)

			bucket := "bucket"
			keys := []string{"file-1.txt", "file-2.txt", "file-3.csv"}
			content := []byte("content")
			createBucket(ctx, t, client, bucket)
			for _, key := range keys {
				createObject(ctx, t, client, bucket, key, content)
			}

			fileSystem := &fs{client: client}
			got, err := fileSystem.List(ctx, tt.glob)
			if (err != nil) != tt.wantErr {
				t.Fatalf("List() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("List() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fs_OpenRead(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     []byte
		wantErr  bool
	}{
		{
			name:     "Open and read object",
			filename: "s3://bucket/file.txt",
			wantErr:  false,
		},
		{
			name:     "Error: invalid S3 uri",
			filename: "bucket/without/scheme",
			wantErr:  true,
		},
		{
			name:     "Error: object does not exist",
			filename: "s3://bucket/non-existing-file.txt",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := newServer(t)
			client := newClient(ctx, t, server.URL)

			bucket := "bucket"
			key := "file.txt"
			content := []byte("content")
			createBucket(ctx, t, client, bucket)
			createObject(ctx, t, client, bucket, key, content)

			fileSystem := &fs{client: client}
			reader, err := fileSystem.OpenRead(ctx, tt.filename)
			if (err != nil) != tt.wantErr {
				t.Fatalf("OpenRead() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			defer reader.Close()
			if err := iotest.TestReader(reader, content); err != nil {
				t.Errorf("TestReader() error = %v, want %v", err, nil)
			}
		})
	}
}

func Test_fs_OpenWrite(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{
			name:     "Open and write object",
			filename: "s3://bucket/file.txt",
			wantErr:  false,
		},
		{
			name:     "Error: invalid S3 uri",
			filename: "bucket/without/scheme",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := newServer(t)
			client := newClient(ctx, t, server.URL)

			bucket := "bucket"
			key := "file.txt"
			content := []byte("content")
			createBucket(ctx, t, client, bucket)

			fileSystem := &fs{client: client}
			writer, err := fileSystem.OpenWrite(ctx, tt.filename)
			if (err != nil) != tt.wantErr {
				t.Fatalf("OpenWrite() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			want := len(content)
			if got, err := writer.Write(content); err != nil || got != want {
				t.Errorf("Write() got = (%v, %v), want (%v, %v)", got, err, want, nil)
			}
			if err := writer.Close(); err != nil {
				t.Errorf("Close() error = %v, want %v", err, nil)
			}
			if got := getObject(ctx, t, client, bucket, key); !bytes.Equal(got, content) {
				t.Errorf("getObject() got = %q, want %q", got, content)
			}
		})
	}
}

func Test_fs_Size(t *testing.T) {
	tests := []struct {
		name     string
		content  []byte
		filename string
		want     int64
		wantErr  bool
	}{
		{
			name:     "Object with non-zero size",
			content:  []byte("content"),
			filename: "s3://bucket/file.txt",
			want:     7,
			wantErr:  false,
		},
		{
			name:     "Object with zero size",
			content:  []byte(nil),
			filename: "s3://bucket/file.txt",
			want:     0,
			wantErr:  false,
		},
		{
			name:     "Error: invalid S3 uri",
			filename: "bucket/without/scheme",
			want:     -1,
			wantErr:  true,
		},
		{
			name:     "Error: object does not exist",
			content:  []byte("content"),
			filename: "s3://bucket/non-existing-file.txt",
			want:     -1,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := newServer(t)
			client := newClient(ctx, t, server.URL)

			bucket := "bucket"
			key := "file.txt"
			createBucket(ctx, t, client, bucket)
			createObject(ctx, t, client, bucket, key, tt.content)

			fileSystem := &fs{client: client}
			got, err := fileSystem.Size(ctx, tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("Size() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Size() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fs_LastModified(t *testing.T) {
	tests := []struct {
		name     string
		content  []byte
		filename string
		wantErr  bool
	}{
		{
			name:     "Last modified timestamp",
			content:  []byte("content"),
			filename: "s3://bucket/file.txt",
			wantErr:  false,
		},
		{
			name:     "Error: invalid S3 uri",
			filename: "bucket/without/scheme",
			wantErr:  true,
		},
		{
			name:     "Error: object does not exist",
			content:  []byte("content"),
			filename: "s3://bucket/non-existing-file.txt",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := newServer(t)
			client := newClient(ctx, t, server.URL)

			bucket := "bucket"
			key := "file.txt"
			createBucket(ctx, t, client, bucket)

			// Account for a timestamp resolution of one second.
			t1 := time.Now().Truncate(time.Second)
			createObject(ctx, t, client, bucket, key, tt.content)
			t2 := time.Now()

			fileSystem := &fs{client: client}
			got, err := fileSystem.LastModified(ctx, tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("LastModified() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if got.Before(t1) || got.After(t2) {
				t.Errorf("LastModified() got = %v, want in range [%v, %v]", got, t1, t2)
			}
		})
	}
}

func Test_fs_Remove(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{
			name:     "Remove object",
			filename: "s3://bucket/file.txt",
			wantErr:  false,
		},
		{
			name:     "Error: invalid S3 uri",
			filename: "bucket/without/scheme",
			wantErr:  true,
		},
		{
			name:     "Error: bucket does not exist",
			filename: "s3://non-existing-bucket/file.txt",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := newServer(t)
			client := newClient(ctx, t, server.URL)

			bucket := "bucket"
			key := "file.txt"
			content := []byte("content")
			createBucket(ctx, t, client, bucket)
			createObject(ctx, t, client, bucket, key, content)

			fileSystem := &fs{client: client}
			if err := fileSystem.Remove(ctx, tt.filename); (err != nil) != tt.wantErr {
				t.Fatalf("Remove() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if got := objectExists(ctx, t, client, bucket, key); got {
				t.Errorf("objectExists() got = %v, want %v", got, false)
			}
		})
	}
}

func Test_fs_Copy(t *testing.T) {
	tests := []struct {
		name    string
		oldpath string
		newpath string
		newKey  string
		wantErr bool
	}{
		{
			name:    "Copy object",
			oldpath: "s3://bucket/file.txt",
			newpath: "s3://bucket/file-copy.txt",
			newKey:  "file-copy.txt",
			wantErr: false,
		},
		{
			name:    "Error: invalid source S3 uri",
			oldpath: "bucket/without/scheme",
			newpath: "s3://bucket/file-copy.txt",
			wantErr: true,
		},
		{
			name:    "Error: invalid target S3 uri",
			oldpath: "s3://bucket/file.txt",
			newpath: "bucket/without/scheme",
			wantErr: true,
		},
		{
			name:    "Error: source object does not exist",
			oldpath: "s3://bucket/non-existing-file.txt",
			newpath: "s3://bucket/file-copy.txt",
			wantErr: true,
		},
		{
			name:    "Error: target bucket does not exist",
			oldpath: "s3://bucket/file.txt",
			newpath: "s3://non-existing-bucket/file-copy.txt",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := newServer(t)
			client := newClient(ctx, t, server.URL)

			bucket := "bucket"
			key := "file.txt"
			content := []byte("content")
			createBucket(ctx, t, client, bucket)
			createObject(ctx, t, client, bucket, key, content)

			fileSystem := &fs{client: client}
			if err := fileSystem.Copy(ctx, tt.oldpath, tt.newpath); (err != nil) != tt.wantErr {
				t.Fatalf("Copy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if got := objectExists(ctx, t, client, bucket, tt.newKey); !got {
				t.Fatalf("objectExists() got = %v, want %v", got, true)
			}
			if got := getObject(ctx, t, client, bucket, tt.newKey); !bytes.Equal(got, content) {
				t.Errorf("getObject() got = %q, want %q", got, content)
			}
		})
	}
}
