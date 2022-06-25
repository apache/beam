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

// Package textio contains transforms for reading and writing text files.
package textio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

const testFilePath = "../../../../data/textio_test.txt"

func TestRead(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	lines := Read(s, testFilePath)
	passert.Count(s, lines, "NumLines", 1)

	ptest.RunAndValidate(t, p)
}

func TestReadAll(t *testing.T) {
	p, s, files := ptest.CreateList([]string{testFilePath})
	lines := ReadAll(s, files)
	passert.Count(s, lines, "NumLines", 1)

	ptest.RunAndValidate(t, p)
}

func TestWrite(t *testing.T) {
	out := "text.txt"
	p, s := beam.NewPipelineWithRoot()
	lines := Read(s, testFilePath)
	Write(s, out, lines)

	ptest.RunAndValidate(t, p)

	if _, err := os.Stat(out); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Failed to write %v", out)
	}
	t.Cleanup(func() {
		os.Remove(out)
	})

	outfileContents, _ := os.ReadFile(out)
	infileContents, _ := os.ReadFile(testFilePath)
	if got, want := string(outfileContents), string(infileContents); got != want {
		t.Fatalf("Write() wrote the wrong contents. Got: %v Want: %v", got, want)
	}
}

func TestImmediate(t *testing.T) {
	f, err := os.CreateTemp("", "test2.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file, err: %v", err)
	}
	t.Cleanup(func() {
		os.Remove(f.Name())
	})
	if err := os.WriteFile(f.Name(), []byte("hello\ngo\n"), 0644); err != nil {
		t.Fatalf("Failed to write file %v, err: %v", f, err)
	}

	p, s := beam.NewPipelineWithRoot()
	lines, err := Immediate(s, f.Name())
	if err != nil {
		t.Fatalf("Failed to insert Immediate: %v", err)
	}
	passert.Count(s, lines, "NumLines", 2)

	ptest.RunAndValidate(t, p)
}

// TestReadSdf tests that readSdf successfully reads a test text file, and
// outputs the correct number of lines for it, even for an exceedingly long
// line.
func TestReadSdf(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	lines := ReadSdf(s, testFilePath)
	passert.Count(s, lines, "NumLines", 1)

	if _, err := beam.Run(context.Background(), "direct", p); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}

func TestReadAllSdf(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	files := beam.Create(s, testFilePath)
	lines := ReadAllSdf(s, files)
	passert.Count(s, lines, "NumLines", 1)

	if _, err := beam.Run(context.Background(), "direct", p); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}

func TestReadAllSdfSeeks(t *testing.T) {
	oneKLine := repeatString("x", 999)
	for _, tt := range []struct {
		name          string
		seekable      bool
		contents      []byte
		wantBytesRead int
	}{
		{
			name:     "seekable",
			seekable: true,
			// 300 megabytes - 1 byte total length
			contents: genFile(1000*300, func(_ int) string {
				return oneKLine
			}),
			wantBytesRead: 300010747,
		},
		{
			name:     "seekable",
			seekable: false,
			// 300 megabytes - 1 byte total length
			contents: genFile(1000*300, func(_ int) string {
				return oneKLine
			}),
			wantBytesRead: 971099383,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testFSInstance.reset()
			testFSInstance.wantSeekable = tt.seekable
			testFSInstance.m[normalize("example.txt")] = tt.contents

			p, s := beam.NewPipelineWithRoot()
			files := beam.Create(s, "testfs://example.txt")
			lines := ReadAllSdf(s, files)
			passert.Count(s, lines, "NumLines", 1000*300)

			if _, err := beam.Run(context.Background(), "direct", p); err != nil {
				t.Fatalf("Failed to execute job: %v", err)
			}
			if got, want := testFSInstance.readCounts["testfs://example.txt"], tt.wantBytesRead; got != want {
				t.Errorf("got %d bytes read, want %d", got, want)
			}
		})
	}
}

func genFile(lineCount int, makeLine func(lineNum int) string) []byte {
	var lines []string
	for i := 0; i < lineCount; i++ {
		lines = append(lines, makeLine(i))
	}
	return []byte(strings.Join(lines, "\n"))
}

func repeatString(s string, n int) string {
	out := &strings.Builder{}
	for i := 0; i < n; i++ {
		out.WriteString(s)
	}
	return out.String()
}

// Register a test file system so we can count reads.

func init() {
	filesystem.Register("testfs", New)
}

var testFSInstance = &testFS{m: make(map[string][]byte), readCounts: map[string]int{}}

type testFS struct {
	m            map[string][]byte
	readCounts   map[string]int // key to # of bytes read
	wantSeekable bool
}

// New returns the global memory filesystem.
func New(_ context.Context) filesystem.Interface {
	return testFSInstance
}

func (f *testFS) reset() {
	f.readCounts = make(map[string]int)
	f.m = make(map[string][]byte)
	f.wantSeekable = true
}

func (f *testFS) Close() error {
	return nil
}

func (f *testFS) List(_ context.Context, glob string) ([]string, error) {
	pattern, err := regexp.Compile(glob)
	if err != nil {
		return nil, err
	}

	var ret []string
	for k := range f.m {
		if pattern.MatchString(k) {
			ret = append(ret, k)
		}
	}
	sort.Strings(ret)
	return ret, nil
}

func (f *testFS) OpenRead(_ context.Context, filename string) (io.ReadCloser, error) {
	normalizedKey := normalize(filename)

	if _, ok := f.m[normalizedKey]; !ok {
		return nil, os.ErrNotExist
	}
	if !f.wantSeekable {
		return &bytesReader{instance: f, normalizedKey: normalizedKey}, nil
	}
	return &seekableReader{bytesReader{instance: f, normalizedKey: normalizedKey}}, nil
}

func (f *testFS) OpenWrite(_ context.Context, filename string) (io.WriteCloser, error) {
	return nil, errors.New("unimplemented OpenWrite")
	// Create the file if it does not exist.
}

func (f *testFS) Size(_ context.Context, filename string) (int64, error) {
	if v, ok := f.m[normalize(filename)]; ok {
		return int64(len(v)), nil
	}
	return -1, os.ErrNotExist
}

func normalize(key string) string {
	if strings.HasPrefix(key, "testfs://") {
		return key
	}
	return "testfs://" + key
}

// bytesReader implements io.Reader, io.Seeker, io.Cloer for memfs "files."
type bytesReader struct {
	instance      *testFS
	normalizedKey string
	pos           int64
}

func (r *bytesReader) Read(p []byte) (int, error) {
	currentValue, exists := r.instance.m[r.normalizedKey]
	if !exists {
		return 0, os.ErrNotExist
	}
	if int(r.pos) >= len(currentValue) {
		return 0, io.EOF
	}
	wantEnd := int(r.pos) + len(p)
	if wantEnd > len(currentValue) {
		wantEnd = len(currentValue)
	}
	n := wantEnd - int(r.pos)
	copy(p, currentValue[r.pos:])
	r.pos = int64(wantEnd)
	r.instance.readCounts[r.normalizedKey] += n
	return n, nil
}

func (r *bytesReader) Close() error { return nil }

type seekableReader struct {
	bytesReader
}

var _ io.ReadSeekCloser = (*seekableReader)(nil)

func (r *seekableReader) Seek(offset int64, whence int) (int64, error) {

	currentValue, exists := r.instance.m[r.normalizedKey]
	if !exists {
		return 0, os.ErrNotExist
	}
	currentLen := len(currentValue)

	wantPos := r.pos
	switch whence {
	case io.SeekCurrent:
		wantPos = r.pos + offset
	case io.SeekStart:
		wantPos = offset
	case io.SeekEnd:
		wantPos = int64(currentLen) + offset
	}
	if wantPos < 0 {
		return 0, fmt.Errorf("%w: invalid seek position %d is before start of file", errBadSeek, wantPos)
	}
	if int(wantPos) > currentLen {
		return 0, fmt.Errorf("%w: invalid seek position %d is after end of file", errBadSeek, wantPos)
	}
	r.pos = wantPos
	return r.pos, nil
}

var errBadSeek = errors.New("bad seek")
