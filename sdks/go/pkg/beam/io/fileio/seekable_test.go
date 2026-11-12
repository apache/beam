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
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/memfs"
	"github.com/google/go-cmp/cmp"
)

var seekableTestFactory func() filesystem.Interface

func init() {
	filesystem.Register("seekabletest", func(context.Context) filesystem.Interface { return seekableTestFactory() })
}

type sequentialFS struct {
	closes   atomic.Int32
	closeErr error
}

func (f *sequentialFS) Close() error { f.closes.Add(1); return f.closeErr }
func (*sequentialFS) List(context.Context, string) ([]string, error) {
	return []string{"seekabletest://file"}, nil
}
func (*sequentialFS) Size(context.Context, string) (int64, error) { return 4, nil }
func (*sequentialFS) OpenRead(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader([]byte("data"))), nil
}
func (*sequentialFS) OpenWrite(context.Context, string) (io.WriteCloser, error) {
	return nil, errors.New("not supported")
}

type seekableFS struct {
	sequentialFS
	openErr, readerCloseErr error
	readerCloses            atomic.Int32
}

func (f *seekableFS) OpenSeekable(context.Context, string) (filesystem.ReaderAtCloser, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	return &testPositionalReader{fs: f, reader: bytes.NewReader([]byte("data"))}, nil
}

type testPositionalReader struct {
	fs     *seekableFS
	reader *bytes.Reader
}

func (r *testPositionalReader) ReadAt(p []byte, off int64) (int, error) {
	if r.fs.closes.Load() != 0 {
		return 0, errors.New("filesystem closed before reader")
	}
	return r.reader.ReadAt(p, off)
}
func (r *testPositionalReader) Close() error {
	r.fs.readerCloses.Add(1)
	if r.fs.closes.Load() != 0 {
		return errors.New("filesystem closed before reader")
	}
	return r.fs.readerCloseErr
}

func TestReadableFileOpenSeekableOwnership(t *testing.T) {
	sentinel := errors.New("cleanup failed")
	fs := &seekableFS{readerCloseErr: sentinel}
	seekableTestFactory = func() filesystem.Interface { return fs }
	f := ReadableFile{Metadata: FileMetadata{Path: "seekabletest://file", Size: 4, ReadSeekEfficient: true}}
	reader, err := f.OpenSeekable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 2)
	if n, err := reader.ReadAt(p, 2); n != 2 || err != nil || string(p) != "ta" {
		t.Fatalf("reader after open=%q,%d,%v", p, n, err)
	}
	if fs.closes.Load() != 0 {
		t.Fatal("filesystem closed at open")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			if err := reader.Close(); !errors.Is(err, sentinel) {
				t.Errorf("close error=%v", err)
			}
		})
	}
	wg.Wait()
	if fs.closes.Load() != 1 || fs.readerCloses.Load() != 1 {
		t.Errorf("filesystem closes=%d reader closes=%d", fs.closes.Load(), fs.readerCloses.Load())
	}
}

func TestReadableFileOpenSeekableErrors(t *testing.T) {
	sentinel := errors.New("permission denied")
	cleanup := errors.New("cleanup failed")
	for _, tc := range []struct {
		name        string
		fs          filesystem.Interface
		comp        compressionType
		flag        bool
		unsupported bool
		want        error
		closes      int32
	}{
		{"missing-capability", &sequentialFS{}, compressionUncompressed, true, true, nil, 1},
		{"missing-flag", &sequentialFS{}, compressionUncompressed, false, true, nil, 0},
		{"gzip", &seekableFS{}, compressionGzip, true, true, nil, 0},
		{"gzip-auto", &seekableFS{}, compressionAuto, true, true, nil, 0},
		{"open-error", &seekableFS{openErr: sentinel}, compressionUncompressed, true, false, sentinel, 1},
		{"file-not-seekable", &seekableFS{openErr: filesystem.ErrNotSeekable}, compressionUncompressed, true, true, nil, 1},
		{"failed-open-cleanup", &seekableFS{sequentialFS: sequentialFS{closeErr: cleanup}, openErr: sentinel}, compressionUncompressed, true, false, cleanup, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seekableTestFactory = func() filesystem.Interface { return tc.fs }
			path := "seekabletest://file"
			if tc.name == "gzip-auto" {
				path += ".gz"
			}
			file := ReadableFile{Metadata: FileMetadata{Path: path, ReadSeekEfficient: tc.flag}, Compression: tc.comp}
			reader, err := file.OpenSeekable(context.Background())
			if reader != nil {
				reader.Close()
			}
			if err == nil || errors.Is(err, filesystem.ErrNotSeekable) != tc.unsupported || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("error=%v, unsupported=%v", err, tc.unsupported)
			}
			var closes int32
			switch fs := tc.fs.(type) {
			case *sequentialFS:
				closes = fs.closes.Load()
			case *seekableFS:
				closes = fs.closes.Load()
			}
			if closes != tc.closes {
				t.Errorf("filesystem closes=%d,want %d", closes, tc.closes)
			}
		})
	}
	// An unsupported filesystem keeps the existing buffered read available.
	seekableTestFactory = func() filesystem.Interface { return &sequentialFS{} }
	file := ReadableFile{Metadata: FileMetadata{Path: "seekabletest://file"}}
	if _, err := file.OpenSeekable(context.Background()); !errors.Is(err, filesystem.ErrNotSeekable) {
		t.Fatal(err)
	}
	if data, err := file.Read(context.Background()); err != nil || string(data) != "data" {
		t.Fatalf("fallback=%q,%v", data, err)
	}
}

func TestMetadataSeekableCapability(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		fs   filesystem.Interface
		want bool
	}{
		{"unsupported", &sequentialFS{}, false}, {"supported", &seekableFS{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata, err := metadataFromFiles(ctx, tc.fs, []string{"seekabletest://file"})
			if err != nil {
				t.Fatal(err)
			}
			if metadata[0].ReadSeekEfficient != tc.want {
				t.Errorf("ReadSeekEfficient=%v,want %v", metadata[0].ReadSeekEfficient, tc.want)
			}
		})
	}
	// Real matched metadata opens a seekable file, rather than relying on hand-built metadata.
	path := "memfs://fileio-positional"
	memfs.Write(path, []byte("header-footer"))
	fs := memfs.New(ctx)
	defer fs.Close()
	metadata, err := metadataFromFiles(ctx, fs, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := (ReadableFile{Metadata: metadata[0]}).OpenSeekable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	p := make([]byte, 6)
	if _, err := reader.ReadAt(p, 7); err != nil || string(p) != "footer" {
		t.Fatalf("matched file read=%q,%v", p, err)
	}
}

func TestSeekableMetadataCoderRoundTrip(t *testing.T) {
	for _, flag := range []bool{false, true} {
		metadata := FileMetadata{Path: "memfs://file", Size: 17, LastModified: time.Unix(123, 0).UTC(), ReadSeekEfficient: flag}
		for _, want := range []any{metadata, ReadableFile{Metadata: metadata, Compression: compressionUncompressed}} {
			typ := reflect.TypeOf(want)
			var buf bytes.Buffer
			if err := beam.NewElementEncoder(typ).Encode(want, &buf); err != nil {
				t.Fatal(err)
			}
			got, err := beam.NewElementDecoder(typ).Decode(&buf)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("coder round trip (-want,+got):%s", diff)
			}
		}
	}
}
