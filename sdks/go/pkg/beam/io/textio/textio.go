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
	"bufio"
	"context"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/fileio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn4x1[context.Context, *sdf.LockRTracker, fileio.ReadableFile, func(string), error](&readFn{})
	register.Emitter1[string]()

	register.DoFn4x1[context.Context, *sdf.LockRTracker, fileio.ReadableFile, func(string, string), error](&readWNameFn{})
	register.Emitter2[string, string]()

	beam.RegisterType(reflect.TypeOf((*writeFileFn)(nil)).Elem())
	register.DoFn3x1[context.Context, int, func(*string) bool, error](&writeFileFn{})
	register.Iter1[string]()
}

type readOption struct {
	FileOpts []fileio.ReadOptionFn
}

// ReadOptionFn is a function that can be passed to Read or ReadAll to configure options for
// reading files.
type ReadOptionFn func(*readOption)

// ReadAutoCompression specifies that the compression type of files should be auto-detected.
func ReadAutoCompression() ReadOptionFn {
	return func(o *readOption) {
		o.FileOpts = append(o.FileOpts, fileio.ReadAutoCompression())
	}
}

// ReadGzip specifies that files have been compressed using gzip.
func ReadGzip() ReadOptionFn {
	return func(o *readOption) {
		o.FileOpts = append(o.FileOpts, fileio.ReadGzip())
	}
}

// ReadUncompressed specifies that files have not been compressed.
func ReadUncompressed() ReadOptionFn {
	return func(o *readOption) {
		o.FileOpts = append(o.FileOpts, fileio.ReadUncompressed())
	}
}

// Read reads a set of files indicated by the glob pattern and returns
// the lines as a PCollection<string>. The newlines are not part of the lines.
// Read accepts a variadic number of ReadOptionFn that can be used to configure the compression
// type of the file. By default, the compression type is determined by the file extension.
func Read(s beam.Scope, glob string, opts ...ReadOptionFn) beam.PCollection {
	s = s.Scope("textio.Read")

	filesystem.ValidateScheme(glob)
	return read(s, &readFn{}, beam.Create(s, glob), opts...)
}

// ReadAll expands and reads the filename given as globs by the incoming
// PCollection<string>. It returns the lines of all files as a single
// PCollection<string>. The newlines are not part of the lines.
// ReadAll accepts a variadic number of ReadOptionFn that can be used to configure the compression
// type of the files. By default, the compression type is determined by the file extension.
func ReadAll(s beam.Scope, col beam.PCollection, opts ...ReadOptionFn) beam.PCollection {
	s = s.Scope("textio.ReadAll")
	return read(s, &readFn{}, col, opts...)
}

// ReadWithFilename reads a set of files indicated by the glob pattern and returns
// a PCollection<KV<string, string>> of each filename and line. The newlines are not part of the lines.
// ReadWithFilename accepts a variadic number of ReadOptionFn that can be used to configure the compression
// type of the files. By default, the compression type is determined by the file extension.
func ReadWithFilename(s beam.Scope, glob string, opts ...ReadOptionFn) beam.PCollection {
	s = s.Scope("textio.ReadWithFilename")

	filesystem.ValidateScheme(glob)
	return read(s, &readWNameFn{}, beam.Create(s, glob), opts...)
}

// ReadSdf is a variation of Read implemented via SplittableDoFn. This should
// result in increased performance with runners that support splitting.
//
// Deprecated: Use Read instead, which has been migrated to use this SDF implementation.
func ReadSdf(s beam.Scope, glob string) beam.PCollection {
	s = s.Scope("textio.ReadSdf")

	filesystem.ValidateScheme(glob)
	return read(s, &readFn{}, beam.Create(s, glob), ReadUncompressed())
}

// ReadAllSdf is a variation of ReadAll implemented via SplittableDoFn. This
// should result in increased performance with runners that support splitting.
//
// Deprecated: Use ReadAll instead, which has been migrated to use this SDF implementation.
func ReadAllSdf(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("textio.ReadAllSdf")
	return read(s, &readFn{}, col, ReadUncompressed())
}

// read takes a PCollection of globs, finds all matching files, and applies
// the given DoFn on the files.
func read(s beam.Scope, dofn any, col beam.PCollection, opts ...ReadOptionFn) beam.PCollection {
	option := &readOption{}
	for _, opt := range opts {
		opt(option)
	}

	matches := fileio.MatchAll(s, col, fileio.MatchEmptyAllow())
	files := fileio.ReadMatches(s, matches, option.FileOpts...)
	return beam.ParDo(s, dofn, files)
}

// consumer is an interface for consuming a string value.
type consumer interface {
	Consume(value string)
}

// emitter emits a string element.
type emitter struct {
	Emit func(string)
}

func (e *emitter) Consume(value string) {
	e.Emit(value)
}

// kvEmitter emits a KV<string, string> element.
type kvEmitter struct {
	Key  string
	Emit func(string, string)
}

func (e *kvEmitter) Consume(value string) {
	e.Emit(e.Key, value)
}

// readBaseFn implements a number of SDF methods that allows for splitting
// a text file and reading individual lines. A struct that embeds readBaseFn
// and also implements ProcessElement will serve as a complete SDF.
type readBaseFn struct {
}

// CreateInitialRestriction creates an offset range restriction representing
// the file's size in bytes.
func (fn *readBaseFn) CreateInitialRestriction(file fileio.ReadableFile) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   file.Metadata.Size,
	}
}

const (
	// blockSize is the desired size of each block for initial splits.
	blockSize int64 = 64 * 1024 * 1024 // 64 MB
	// tooSmall is the size limit for a block. If the last block is smaller than
	// this, it gets merged with the previous block.
	tooSmall = blockSize / 4
)

// SplitRestriction splits each file restriction into blocks of a predetermined
// size, with some checks to avoid having small remainders.
func (fn *readBaseFn) SplitRestriction(_ fileio.ReadableFile, rest offsetrange.Restriction) []offsetrange.Restriction {
	splits := rest.SizedSplits(blockSize)
	numSplits := len(splits)
	if numSplits > 1 {
		last := splits[numSplits-1]
		if last.End-last.Start <= tooSmall {
			// Last restriction is too small, so merge it with previous one.
			splits[numSplits-2].End = last.End
			splits = splits[:numSplits-1]
		}
	}
	return splits
}

// RestrictionSize returns the size of each restriction as its range.
func (fn *readBaseFn) RestrictionSize(_ fileio.ReadableFile, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker creates sdf.LockRTrackers wrapping offsetRange.Trackers for
// each restriction.
func (fn *readBaseFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// process processes all lines in the file that begin within the paired
// restriction. How each line is output is determined by the consumer.
//
// Note that restrictions may not align perfectly with lines. So lines can begin
// before the restriction and end within it (those are ignored), and lines can
// begin within the restriction and past the restriction (those are entirely
// output, including the portion outside the restriction). In some cases a
// valid restriction might not output any lines.
func (fn *readBaseFn) process(ctx context.Context, rt *sdf.LockRTracker, file fileio.ReadableFile, consumer consumer) error {
	log.Infof(ctx, "Reading from %v", file.Metadata.Path)

	fd, err := file.Open(ctx)
	if err != nil {
		return err
	}
	defer fd.Close()

	rd := bufio.NewReader(fd)

	i := rt.GetRestriction().(offsetrange.Restriction).Start
	if i > 0 {
		// If restriction's starts after 0, we cannot assume a new line starts
		// at the beginning of the restriction, so we must search for the first
		// line beginning at or after restriction.Start. This is done by
		// scanning to the byte just before the restriction and then reading
		// until the next newline, leaving the reader at the start of a new
		// line past restriction.Start.
		i--
		n, err := rd.Discard(int(i)) // Scan to just before restriction.
		if err == io.EOF {
			return errors.Errorf("TextIO restriction lies outside the file being read. "+
				"Restriction begins at %v bytes, but file is only %v bytes.", i+1, n)
		}
		if err != nil {
			return err
		}
		line, err := rd.ReadString('\n') // Read until the first line within the restriction.
		if err == io.EOF {
			// No lines start in the restriction but it's still valid, so
			// finish claiming before returning to avoid errors.
			rt.TryClaim(rt.GetRestriction().(offsetrange.Restriction).End)
			return nil
		}
		if err != nil {
			return err
		}
		i += int64(len(line))
	}

	// Claim each line until we claim a line outside the restriction.
	for rt.TryClaim(i) {
		line, err := rd.ReadString('\n')
		if err == io.EOF {
			if len(line) != 0 {
				consumer.Consume(strings.TrimSuffix(line, "\n"))
			}
			// Finish claiming restriction before breaking to avoid errors.
			rt.TryClaim(rt.GetRestriction().(offsetrange.Restriction).End)
			break
		}
		if err != nil {
			return err
		}
		consumer.Consume(strings.TrimSuffix(line, "\n"))
		i += int64(len(line))
	}
	return nil
}

// readFn is an SDF that emits individual lines from a text file.
type readFn struct {
	readBaseFn
}

func (fn *readFn) ProcessElement(ctx context.Context, rt *sdf.LockRTracker, file fileio.ReadableFile, emit func(string)) error {
	return fn.process(ctx, rt, file, &emitter{Emit: emit})
}

// readWNameFn is an SDF that emits individual lines from a text file with
// the filename as a key.
type readWNameFn struct {
	readBaseFn
}

func (fn *readWNameFn) ProcessElement(ctx context.Context, rt *sdf.LockRTracker, file fileio.ReadableFile, emit func(string, string)) error {
	return fn.process(ctx, rt, file, &kvEmitter{Key: file.Metadata.Path, Emit: emit})
}

// TODO(herohde) 7/12/2017: extend Write to write to a series of files
// as well as allow sharding.

// Write writes a PCollection<string> to a file as separate lines. The
// writer add a newline after each element.
func Write(s beam.Scope, filename string, col beam.PCollection) {
	s = s.Scope("textio.Write")

	filesystem.ValidateScheme(filename)

	// NOTE(BEAM-3579): We may never call Teardown for non-local runners and
	// FinishBundle doesn't have the right granularity. We therefore
	// perform a GBK with a fixed key to get all values in a single invocation.

	// TODO(BEAM-3860) 3/15/2018: use side input instead of GBK.

	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFileFn{Filename: filename}, post)
}

type writeFileFn struct {
	Filename string `json:"filename"`
}

func (w *writeFileFn) ProcessElement(ctx context.Context, _ int, lines func(*string) bool) error {
	fs, err := filesystem.New(ctx, w.Filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenWrite(ctx, w.Filename)
	if err != nil {
		return err
	}
	buf := bufio.NewWriterSize(fd, 1<<20) // use 1MB buffer

	log.Infof(ctx, "Writing to %v", w.Filename)

	var line string
	for lines(&line) {
		if _, err := buf.WriteString(line); err != nil {
			return err
		}
		if _, err := buf.Write([]byte{'\n'}); err != nil {
			return err
		}
	}

	if err := buf.Flush(); err != nil {
		return err
	}
	return fd.Close()
}

// Immediate reads a local file at pipeline construction-time and embeds the
// data into a I/O-free pipeline source. Should be used for small files only.
func Immediate(s beam.Scope, filename string) (beam.PCollection, error) {
	s = s.Scope("textio.Immediate")

	var data []any

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
