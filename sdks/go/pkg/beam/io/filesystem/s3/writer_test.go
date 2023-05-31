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
)

func Test_writer_WriteClose(t *testing.T) {
	tests := []struct {
		name     string
		contents [][]byte
		bucket   string
		key      string
		want     []byte
		wantErr  bool
	}{
		{
			name:   "Write non-empty object",
			bucket: "bucket",
			key:    "file.txt",
			contents: [][]byte{
				[]byte("line1\n"),
				[]byte("line2\n"),
			},
			want:    []byte("line1\nline2\n"),
			wantErr: false,
		},
		{
			name:     "Write empty object",
			bucket:   "bucket",
			key:      "file.txt",
			contents: nil,
			want:     nil,
			wantErr:  false,
		},
		{
			name:   "Error: bucket does not exist",
			bucket: "non-existing-bucket",
			key:    "file.txt",
			contents: [][]byte{
				[]byte("line1\n"),
				[]byte("line2\n"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := newServer(t)
			client := newClient(ctx, t, server.URL)
			createBucket(ctx, t, client, "bucket")

			writer := newWriter(ctx, client, tt.bucket, tt.key)
			for _, content := range tt.contents {
				want := len(content)
				if got, err := writer.Write(content); err != nil || got != want {
					t.Errorf("Write() = (%v, %v), want (%v, %v)", got, err, want, nil)
				}
			}
			if err := writer.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}

			wantExists := !tt.wantErr
			if got := objectExists(ctx, t, client, tt.bucket, tt.key); got != wantExists {
				t.Fatalf("objectExists() = %v, want %v", got, wantExists)
			}
			if !wantExists {
				return
			}

			if got := getObject(ctx, t, client, tt.bucket, tt.key); !bytes.Equal(got, tt.want) {
				t.Errorf("getObject() = %q, want %q", got, tt.want)
			}
		})
	}
}
