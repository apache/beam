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

package gcs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/api/option"
)

type rangeTransport struct {
	base     http.RoundTripper
	requests atomic.Int32
	bytes    atomic.Int64
	t        *testing.T
}

func (tr *rangeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Range") != "" {
		tr.requests.Add(1)
		if req.URL.Query().Get("generation") == "" {
			tr.t.Error("range request not pinned to generation")
		}
	}
	resp, err := tr.base.RoundTrip(req)
	if err == nil && req.Header.Get("Range") != "" {
		resp.Body = countedBody{resp.Body, &tr.bytes}
	}
	return resp, err
}

type countedBody struct {
	io.ReadCloser
	bytes *atomic.Int64
}

func (b countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes.Add(int64(n))
	return n, err
}

func TestGCSOpenSeekable(t *testing.T) {
	ctx := context.Background()
	server := createFakeGCSServer(t)
	const bucket = "beamgogcsfilesystemtest"
	const name = "positional"
	const data = "header-payload-footer"
	server.CreateObject(fakestorage.Object{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: name}, Content: []byte(data)})
	client := server.HTTPClient()
	tr := &rangeTransport{base: client.Transport, t: t}
	client.Transport = tr
	gc, err := storage.NewClient(ctx, option.WithHTTPClient(client), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	fs := &fs{client: gc}
	defer fs.Close()
	reader, err := fs.OpenSeekable(ctx, "gs://"+bucket+"/"+name)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if tr.requests.Load() != 0 {
		t.Fatal("OpenSeekable downloaded content")
	}
	p := make([]byte, 6)
	if n, err := reader.ReadAt(p, 15); n != 6 || err != nil || string(p) != "footer" {
		t.Fatalf("footer=%q,%d,%v", p, n, err)
	}
	if tr.bytes.Load() != 6 {
		t.Fatalf("downloaded %d bytes for 6-byte footer", tr.bytes.Load())
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			p := make([]byte, 6)
			if _, err := reader.ReadAt(p, 15); err != nil || string(p) != "footer" {
				t.Errorf("parallel read=%q,%v", p, err)
			}
		})
	}
	wg.Wait()
	p = make([]byte, 10)
	if n, err := reader.ReadAt(p, 18); n != 3 || err != io.EOF || string(p[:n]) != "ter" {
		t.Errorf("partial read=%q,%d,%v", p, n, err)
	}
	before := tr.requests.Load()
	if n, err := reader.ReadAt(p, 21); n != 0 || err != io.EOF {
		t.Errorf("EOF=%d,%v", n, err)
	}
	if tr.requests.Load() != before {
		t.Error("EOF issued a range request")
	}
	server.CreateObject(fakestorage.Object{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: name}, Content: []byte("replacement-new-value")})
	p = make([]byte, 6)
	n, err := reader.ReadAt(p, 15)
	if err == nil && string(p[:n]) != "footer" {
		t.Fatalf("mixed object generations: %q", p[:n])
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGCSOpenSeekableErrors(t *testing.T) {
	ctx := context.Background()
	server := createFakeGCSServer(t)
	fs := &fs{client: server.Client()}
	defer fs.Close()
	server.CreateObject(fakestorage.Object{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "beamgogcsfilesystemtest", Name: "encoded", ContentEncoding: "gzip"}, Content: []byte("compressed")})
	for _, tc := range []struct {
		path        string
		unsupported bool
	}{
		{"invalid", false}, {"gs://beamgogcsfilesystemtest/missing", false}, {"gs://beamgogcsfilesystemtest/encoded", true},
	} {
		t.Run(tc.path, func(t *testing.T) {
			reader, err := fs.OpenSeekable(ctx, tc.path)
			if reader != nil {
				reader.Close()
			}
			if err == nil || errors.Is(err, filesystem.ErrNotSeekable) != tc.unsupported {
				t.Errorf("error=%v, unsupported=%v", err, tc.unsupported)
			}
		})
	}
	empty, err := fs.OpenSeekable(ctx, "gs://beamgogcsfilesystemtest/stub")
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if n, err := empty.ReadAt(make([]byte, 1), 0); n != 0 || err != io.EOF {
		t.Errorf("empty read=%d,%v", n, err)
	}
	ctx, cancel := context.WithCancel(ctx)
	reader, err := fs.OpenSeekable(ctx, "gs://beamgogcsfilesystemtest/stub")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	cancel()
	if _, err := reader.ReadAt(make([]byte, 1), 0); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled read=%v", err)
	}
}
