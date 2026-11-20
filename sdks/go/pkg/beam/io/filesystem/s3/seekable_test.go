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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
)

func TestS3OpenSeekable(t *testing.T) {
	ctx := context.Background()
	server := newServer(t)
	client := newClient(ctx, t, server.URL)
	createBucket(ctx, t, client, "bucket")
	createObject(ctx, t, client, "bucket", "data", []byte("header-payload-footer"))
	fs := &fs{client: client}
	reader, err := fs.OpenSeekable(ctx, "s3://bucket/data")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			p := make([]byte, 6)
			if n, err := reader.ReadAt(p, 15); n != 6 || err != nil || string(p) != "footer" {
				t.Errorf("parallel read=%q,%d,%v", p, n, err)
			}
		})
	}
	wg.Wait()
	p := make([]byte, 10)
	if n, err := reader.ReadAt(p, 18); n != 3 || err != io.EOF || string(p[:n]) != "ter" {
		t.Errorf("partial read=%q,%d,%v", p, n, err)
	}
	if n, err := reader.ReadAt(p, 21); n != 0 || err != io.EOF {
		t.Errorf("EOF=%d,%v", n, err)
	}
	createObject(ctx, t, client, "bucket", "empty", nil)
	empty, err := fs.OpenSeekable(ctx, "s3://bucket/empty")
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if n, err := empty.ReadAt(p, 0); n != 0 || err != io.EOF {
		t.Errorf("empty read=%d,%v", n, err)
	}
	for _, path := range []string{"invalid", "s3://bucket/missing"} {
		if reader, err := fs.OpenSeekable(ctx, path); err == nil {
			reader.Close()
			t.Errorf("opened %s", path)
		}
	}
}

func TestS3SeekableRequests(t *testing.T) {
	const data = "header-payload-footer"
	for _, mode := range []string{"ranges", "gzip", "missing-etag", "bad-range", "denied"} {
		t.Run(mode, func(t *testing.T) {
			var gets atomic.Int32
			var changed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"original"`)
				if r.Method == http.MethodHead {
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
					if mode == "gzip" {
						w.Header().Set("Content-Encoding", "gzip")
					}
					if mode == "missing-etag" {
						w.Header().Del("ETag")
					}
					return
				}
				gets.Add(1)
				if r.Header.Get("If-Match") != `"original"` {
					t.Error("range missing original ETag")
				}
				if changed.Load() {
					http.Error(w, "changed", http.StatusPreconditionFailed)
					return
				}
				if mode == "denied" {
					http.Error(w, "denied", http.StatusForbidden)
					return
				}
				var start, end int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
					t.Error(err)
					http.Error(w, "bad range", 400)
					return
				}
				if mode == "bad-range" {
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
					io.WriteString(w, data)
					return
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
				w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				io.WriteString(w, data[start:end+1])
			}))
			defer server.Close()
			fs := &fs{client: newClient(context.Background(), t, server.URL)}
			reader, err := fs.OpenSeekable(context.Background(), "s3://bucket/data")
			if mode == "gzip" || mode == "missing-etag" {
				if reader != nil {
					reader.Close()
				}
				if err == nil || errors.Is(err, filesystem.ErrNotSeekable) != (mode == "gzip") {
					t.Fatalf("open error=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if gets.Load() != 0 {
				t.Fatal("OpenSeekable downloaded data")
			}
			p := make([]byte, 6)
			n, err := reader.ReadAt(p, 15)
			if mode == "bad-range" || mode == "denied" {
				if err == nil || errors.Is(err, filesystem.ErrNotSeekable) {
					t.Fatalf("range error=%v", err)
				}
				return
			}
			if n != 6 || err != nil || string(p) != "footer" || gets.Load() != 1 {
				t.Fatalf("range read=%q,%d,%v, requests=%d", p, n, err, gets.Load())
			}
			changed.Store(true)
			if n, err := reader.ReadAt(p, 15); n != 0 || err == nil || errors.Is(err, filesystem.ErrNotSeekable) {
				t.Fatalf("read after overwrite=%d,%v", n, err)
			}
		})
	}
}
