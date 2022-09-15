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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*readFn)(nil)).Elem())
	beam.RegisterFunction(sizeFn)
	beam.RegisterType(reflect.TypeOf((*writeFileFn)(nil)).Elem())
	beam.RegisterFunction(expandFn)
}

// Read reads a set of files indicated by the glob pattern and returns
// the lines as a PCollection<string>.
// The newlines are not part of the lines.
func Read(s beam.Scope, glob string) beam.PCollection {
	s = s.Scope("textio.Read")

	filesystem.ValidateScheme(glob)
	return read(s, beam.Create(s, glob))
}

// ReadAll expands and reads the filename given as globs by the incoming
// PCollection<string>. It returns the lines of all files as a single
// PCollection<string>. The newlines are not part of the lines.
func ReadAll(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("textio.ReadAll")
	return read(s, col)
}

// ReadSdf is a variation of Read implemented via SplittableDoFn. This should
// result in increased performance with runners that support splitting.
//
// Deprecated: Use Read instead, which has been migrated to use this SDF implementation.
func ReadSdf(s beam.Scope, glob string) beam.PCollection {
	s = s.Scope("textio.ReadSdf")

	filesystem.ValidateScheme(glob)
	return read(s, beam.Create(s, glob))
}

// ReadAllSdf is a variation of ReadAll implemented via SplittableDoFn. This
// should result in increased performance with runners that support splitting.
//
// Deprecated: Use ReadAll instead, which has been migrated to use this SDF implementation.
func ReadAllSdf(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("textio.ReadAllSdf")
	return read(s, col)
}

// read takes a PCollection of globs and returns a PCollection of lines from
// all files in those globs. Uses an SDF to allow splitting reads of files
// into separate bundles.
func read(s beam.Scope, col beam.PCollection) beam.PCollection {
	files := beam.ParDo(s, expandFn, col)
	sized := beam.ParDo(s, sizeFn, files)
	return beam.ParDo(s, &readFn{}, sized)
}

// expandFn expands a glob pattern into all matching file names.
func expandFn(ctx context.Context, glob string, emit func(string)) error {
	if strings.TrimSpace(glob) == "" {
		return nil // ignore empty string elements here
	}

	fs, err := filesystem.New(ctx, glob)
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

// sizeFn pairs a filename with the size of that file in bytes.
// TODO(https://github.com/apache/beam/issues/20607): Once CreateInitialRestriction supports Context params and
// error return values, this can be done in readSdfFn.CreateInitialRestriction.
func sizeFn(ctx context.Context, filename string) (string, int64, error) {
	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return "", -1, err
	}
	defer fs.Close()

	size, err := fs.Size(ctx, filename)
	if err != nil {
		return "", -1, err
	}
	return filename, size, nil
}

// readFn reads individual lines from a text file, given a filename and a
// size in bytes for that file. Implemented as an SDF to allow splitting
// within a file.
type readFn struct {
}

// CreateInitialRestriction creates an offset range restriction representing
// the file, using the paired size rather than fetching the file's size.
func (fn *readFn) CreateInitialRestriction(_ string, size int64) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   size,
	}
}

const (
	// blockSize is the desired size of each block for initial splits.
	blockSize int64 = 64 * 1024 * 1024 // 64 MB
	// tooSmall is the size limit for a block. If the last block is smaller than
	// this, it gets merged with the previous block.
	tooSmall = blockSize / 4
)

// SplitRestriction splits each file restriction into blocks of a predeterined
// size, with some checks to avoid having small remainders.
func (fn *readFn) SplitRestriction(_ string, _ int64, rest offsetrange.Restriction) []offsetrange.Restriction {
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

// Size returns the size of each restriction as its range.
func (fn *readFn) RestrictionSize(_ string, _ int64, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker creates sdf.LockRTrackers wrapping offsetRange.Trackers for
// each restriction.
func (fn *readFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

// ProcessElement outputs all lines in the file that begin within the paired
// restriction.
//
// Note that restrictions may not align perfectly with lines. So lines can begin
// before the restriction and end within it (those are ignored), and lines can
// begin within the restriction and past the restriction (those are entirely
// output, including the portion outside the restriction). In some cases a
// valid restriction might not output any lines.
func (fn *readFn) ProcessElement(ctx context.Context, rt *sdf.LockRTracker, filename string, _ int64, emit func(string)) error {
	log.Infof(ctx, "Reading from %v", filename)

	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenRead(ctx, filename)
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
				emit(strings.TrimSuffix(line, "\n"))
			}
			// Finish claiming restriction before breaking to avoid errors.
			rt.TryClaim(rt.GetRestriction().(offsetrange.Restriction).End)
			break
		}
		if err != nil {
			return err
		}
		emit(strings.TrimSuffix(line, "\n"))
		i += int64(len(line))
	}
	return nil
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
