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

package textio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*writeFileFn)(nil)).Elem())
	beam.RegisterFunction(readFn)
	beam.RegisterFunction(expandFn)
}

// Read reads a set of file and returns the lines as a PCollection<string>. The
// newlines are not part of the lines.
func Read(s beam.Scope, glob string) beam.PCollection {
	s = s.Scope("textio.Read")

	validateScheme(glob)
	return read(s, beam.Create(s, glob))
}

func validateScheme(glob string) {
	if strings.TrimSpace(glob) == "" {
		panic("empty file glob provided")
	}
	scheme := getScheme(glob)
	if _, ok := registry[scheme]; !ok {
		panic(fmt.Sprintf("textio scheme %v not registered", scheme))
	}
}

func getScheme(glob string) string {
	if index := strings.Index(glob, "://"); index > 0 {
		return glob[:index]
	}
	return "default"
}

func newFileSystem(ctx context.Context, glob string) (FileSystem, error) {
	scheme := getScheme(glob)
	mkfs, ok := registry[scheme]
	if !ok {
		return nil, fmt.Errorf("textio scheme %v not registered for %v", scheme, glob)
	}
	return mkfs(ctx), nil
}

// ReadAll expands and reads the filename given as globs by the incoming
// PCollection<string>. It returns the lines of all files as a single
// PCollection<string>. The newlines are not part of the lines.
func ReadAll(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("textio.ReadAll")

	return read(s, col)
}

func read(s beam.Scope, col beam.PCollection) beam.PCollection {
	files := beam.ParDo(s, expandFn, col)
	return beam.ParDo(s, readFn, files)
}

func expandFn(ctx context.Context, glob string, emit func(string)) error {
	if strings.TrimSpace(glob) == "" {
		return nil // ignore empty string elements here
	}

	fs, err := newFileSystem(ctx, glob)
	if err != nil {
		return err
	}
	defer fs.Close()

	files, err := fs.List(ctx, glob)
	if err != nil {
		return err
	}
	for _, filename := range files {
		emit(filename)
	}
	return nil
}

func readFn(ctx context.Context, filename string, emit func(string)) error {
	log.Infof(ctx, "Reading from %v", filename)

	fs, err := newFileSystem(ctx, filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenRead(ctx, filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		emit(scanner.Text())
	}
	return scanner.Err()
}

// TODO(herohde) 7/12/2017: extend Write to write to a series of files
// as well as allow sharding.

// Write writes a PCollection<string> to a file as separate lines. The
// writer add a newline after each element.
func Write(s beam.Scope, filename string, col beam.PCollection) {
	s = s.Scope("textio.Write")

	validateScheme(filename)
	beam.ParDo0(s, &writeFileFn{Filename: filename}, col)
}

type writeFileFn struct {
	Filename string `json:"filename"`

	fs     FileSystem
	fd     io.WriteCloser
	writer *bufio.Writer
}

func (w *writeFileFn) Setup(ctx context.Context) error {
	fs, err := newFileSystem(ctx, w.Filename)
	if err != nil {
		return err
	}
	fd, err := fs.OpenWrite(ctx, w.Filename)
	if err != nil {
		fs.Close()
		return err
	}

	log.Infof(ctx, "Writing to %v", w.Filename)

	w.fs = fs
	w.fd = fd
	w.writer = bufio.NewWriterSize(fd, 1<<20)
	return nil
}

func (w *writeFileFn) ProcessElement(line string) error {
	if _, err := w.writer.WriteString(line); err != nil {
		return err
	}
	_, err := w.writer.Write([]byte{'\n'})
	return err
}

// TODO(herohde) 1/29/2018: we need to write the object as a side input instead
// or similar. We may never call Teardown for non-local runners and FinishBundle
// doesn't have the right granularity.

func (w *writeFileFn) Teardown() error {
	defer w.fs.Close()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.fd.Close()
}

// Immediate reads a local file at pipeline construction-time and embeds the
// data into a I/O-free pipeline source. Should be used for small files only.
func Immediate(s beam.Scope, filename string) (beam.PCollection, error) {
	s = s.Scope("textio.Immediate")

	var data []interface{}

	file, err := os.Open(filename)
	if err != nil {
		return beam.PCollection{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		data = append(data, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return beam.PCollection{}, err
	}
	return beam.Create(s, data...), nil
}
