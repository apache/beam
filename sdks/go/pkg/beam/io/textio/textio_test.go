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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func init() {
	register.Function2x1(toKV)
}

const testDir = "../../../../data"

var (
	testFilePath   = filepath.Join(testDir, "textio_test.txt")
	testGzFilePath = filepath.Join(testDir, "textio_test.gz")
)

type kv struct {
	K, V string
}

func toKV(k, v string) kv {
	return kv{k, v}
}

func TestRead(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	lines := Read(s, testFilePath)
	passert.Count(s, lines, "NumLines", 1)

	ptest.RunAndValidate(t, p)
}

func TestReadGzip(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	got := Read(s, testGzFilePath, ReadGzip())
	want := []any{"hello", "go"}

	passert.Equals(s, got, want...)
	ptest.RunAndValidate(t, p)
}

func TestReadAll(t *testing.T) {
	p, s, files := ptest.CreateList([]string{testFilePath})
	lines := ReadAll(s, files)
	passert.Count(s, lines, "NumLines", 1)

	ptest.RunAndValidate(t, p)
}

func TestReadAllGzip(t *testing.T) {
	p, s, files := ptest.CreateList([]string{testGzFilePath})
	got := ReadAll(s, files, ReadGzip())
	want := []any{"hello", "go"}

	passert.Equals(s, got, want...)
	ptest.RunAndValidate(t, p)
}

func TestReadWithFilename(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	lines := ReadWithFilename(s, testGzFilePath)
	got := beam.ParDo(s, toKV, lines)

	want := []any{
		kv{K: testGzFilePath, V: "hello"},
		kv{K: testGzFilePath, V: "go"},
	}

	passert.Equals(s, got, want...)
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

	ptest.RunAndValidate(t, p)
}

func TestReadAllSdf(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	files := beam.Create(s, testFilePath)
	lines := ReadAllSdf(s, files)
	passert.Count(s, lines, "NumLines", 1)

	ptest.RunAndValidate(t, p)
}
