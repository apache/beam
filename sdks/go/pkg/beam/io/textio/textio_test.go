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
	"os"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

const testFilePath = "../../../../data/textio_test.txt"

func TestReadFn(t *testing.T) {
	receivedLines := []string{}
	getLines := func(line string) {
		receivedLines = append(receivedLines, line)
	}

	err := readFn(nil, testFilePath, getLines)
	if err != nil {
		t.Fatalf("failed with %v", err)
	}
	want := 1
	if len(receivedLines) != 1 {
		t.Fatalf("received %v lines, want %v", len(receivedLines), want)
	}

}

func TestRead(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	lines := Read(s, testFilePath)
	passert.Count(s, lines, "NumLines", 1)

	if _, err := beam.Run(context.Background(), "direct", p); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}

func TestReadAll(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	files := beam.Create(s, testFilePath)
	lines := ReadAll(s, files)
	passert.Count(s, lines, "NumLines", 1)

	if _, err := beam.Run(context.Background(), "direct", p); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}

func TestWrite(t *testing.T) {
	out := "text.txt"
	p, s := beam.NewPipelineWithRoot()
	lines := Read(s, testFilePath)
	Write(s, out, lines)

	if _, err := beam.Run(context.Background(), "direct", p); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}

	if _, err := os.Stat(out); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Failed to write %v", out)
	}
	defer os.Remove(out)

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
	defer os.Remove(f.Name()) // clean up
	if err := os.WriteFile(f.Name(), []byte("hello\ngo\n"), 0644); err != nil {
		t.Fatalf("Failed to write file %v, err: %v", f, err)
	}

	p, s := beam.NewPipelineWithRoot()
	lines, err := Immediate(s, f.Name())
	if err != nil {
		t.Fatalf("Failed to insert Immediate: %v", err)
	}
	passert.Count(s, lines, "NumLines", 2)

	if _, err := beam.Run(context.Background(), "direct", p); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}
