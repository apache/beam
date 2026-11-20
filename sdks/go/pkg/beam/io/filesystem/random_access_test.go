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

package filesystem_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/memfs"
)

func TestOpenSeekable(t *testing.T) {
	ctx := context.Background()
	const data = "header-payload-footer"
	for _, backend := range []struct {
		name string
		fs   filesystem.Interface
		path string
	}{
		{"local", local.New(ctx), filepath.Join(t.TempDir(), "data")},
		{"memfs", memfs.New(ctx), "memfs://positional-data"},
	} {
		t.Run(backend.name, func(t *testing.T) {
			defer backend.fs.Close()
			if err := filesystem.Write(ctx, backend.fs, backend.path, []byte(data)); err != nil {
				t.Fatal(err)
			}
			opener, ok := backend.fs.(filesystem.SeekableOpener)
			if !ok {
				t.Fatalf("%T lacks SeekableOpener", backend.fs)
			}
			reader, err := opener.OpenSeekable(ctx, backend.path)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			for _, tc := range []struct {
				off    int64
				length int
				want   string
				eof    bool
			}{
				{15, 6, "footer", false}, {18, 8, "ter", true}, {21, 1, "", true}, {30, 1, "", true}, {0, 0, "", false},
			} {
				p := make([]byte, tc.length)
				n, err := reader.ReadAt(p, tc.off)
				if n != len(tc.want) || string(p[:n]) != tc.want || (err == io.EOF) != tc.eof || (err != nil && err != io.EOF) {
					t.Errorf("ReadAt(%d,%d)=(%q,%v), want %q EOF=%v", tc.off, tc.length, p[:n], err, tc.want, tc.eof)
				}
			}
			if _, err := reader.ReadAt(make([]byte, 1), -1); err == nil {
				t.Error("negative offset accepted")
			}
			var wg sync.WaitGroup
			for i := 0; i < 32; i++ {
				wg.Go(func() {
					for j := 0; j < 10; j++ {
						p := make([]byte, 6)
						if n, err := reader.ReadAt(p, 15); n != 6 || err != nil || string(p) != "footer" {
							t.Errorf("concurrent ReadAt=(%q,%d,%v)", p, n, err)
						}
					}
				})
			}
			wg.Wait()
			if _, err := opener.OpenSeekable(ctx, backend.path+"-missing"); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("missing file error=%v", err)
			}
			if err := filesystem.Write(ctx, backend.fs, backend.path+"-empty", nil); err != nil {
				t.Fatal(err)
			}
			empty, err := opener.OpenSeekable(ctx, backend.path+"-empty")
			if err != nil {
				t.Fatal(err)
			}
			defer empty.Close()
			if n, err := empty.ReadAt(make([]byte, 1), 0); n != 0 || err != io.EOF {
				t.Errorf("empty read=%d,%v", n, err)
			}
		})
	}
}

func TestMemfsSeekableSnapshot(t *testing.T) {
	ctx := context.Background()
	fs := memfs.New(ctx)
	defer fs.Close()
	data := []byte("original")
	const path = "memfs://positional-snapshot"
	memfs.Write(path, data)
	reader, err := fs.(filesystem.SeekableOpener).OpenSeekable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	copy(data, "mutated!")
	memfs.Write(path, []byte("replaced"))
	p := make([]byte, 8)
	if _, err := reader.ReadAt(p, 0); err != nil || string(p) != "original" {
		t.Fatalf("snapshot=%q,%v", p, err)
	}
}
