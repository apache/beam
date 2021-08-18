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
	"io"
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
	beam.RegisterType(reflect.TypeOf((*readSdfFn)(nil)).Elem())
	beam.RegisterFunction(sizeFn)
}

// ReadSdf is a variation of Read implemented via SplittableDoFn. This should
// result in increased performance with runners that support splitting.
func ReadSdf(s beam.Scope, glob string) beam.PCollection {
	s = s.Scope("textio.ReadSdf")

	filesystem.ValidateScheme(glob)
	return readSdf(s, beam.Create(s, glob))
}

// ReadAllSdf is a variation of ReadAll implemented via SplittableDoFn. This
// should result in increased performance with runners that support splitting.
func ReadAllSdf(s beam.Scope, col beam.PCollection) beam.PCollection {
	s = s.Scope("textio.ReadAllSdf")

	return readSdf(s, col)
}

// readSdf takes a PCollection of globs and returns a PCollection of lines from
// all files in those globs. Unlike textio.read, this version uses an SDF for
// reading files.
func readSdf(s beam.Scope, col beam.PCollection) beam.PCollection {
	files := beam.ParDo(s, expandFn, col)
	sized := beam.ParDo(s, sizeFn, files)
	return beam.ParDo(s, &readSdfFn{}, sized)
}

// sizeFn pairs a filename with the size of that file in bytes.
// TODO(BEAM-11109): Once CreateInitialRestriction supports Context params and
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

// readSdfFn reads individual lines from a text file, given a filename and a
// size in bytes for that file.
type readSdfFn struct {
}

// CreateInitialRestriction creates an offset range restriction representing
// the file, using the paired size rather than fetching the file's size.
func (fn *readSdfFn) CreateInitialRestriction(_ string, size int64) offsetrange.Restriction {
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
func (fn *readSdfFn) SplitRestriction(_ string, _ int64, rest offsetrange.Restriction) []offsetrange.Restriction {
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
func (fn *readSdfFn) RestrictionSize(_ string, _ int64, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker creates sdf.LockRTrackers wrapping offsetRange.Trackers for
// each restriction.
func (fn *readSdfFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
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
func (fn *readSdfFn) ProcessElement(ctx context.Context, rt *sdf.LockRTracker, filename string, _ int64, emit func(string)) error {
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
		i -= 1
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
