/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: splittable-dofn
//   description: Splittable DoFn example
//   multifile: false
//   context_line: 55
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"io"
	"reflect"
	"strings"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*readFn)(nil)).Elem())
	beam.RegisterFunction(sizeFn)
	beam.RegisterFunction(expandFn)
}

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	file := Read(s, "gs://apache-beam-samples/counts-00000-of-00003")

	lines := getLines(s, file)

	debug.Print(s, lines)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func getLines(s beam.Scope, input beam.PCollection) beam.PCollection {
	return filter.Include(s, input, func(element string) bool {
		return element != ""
	})
}

func Read(s beam.Scope, glob string) beam.PCollection {
	s = s.Scope("Read")
	filesystem.ValidateScheme(glob)

	return read(s, beam.Create(s, glob))
}

func read(s beam.Scope, col beam.PCollection) beam.PCollection {
	files := beam.ParDo(s, expandFn, col)
	sized := beam.ParDo(s, sizeFn, files)
	return beam.ParDo(s, &readFn{}, sized)
}

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

type readFn struct {
}

func (fn *readFn) CreateInitialRestriction(_ string, size int64) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   size,
	}
}

const (
	blockSize int64 = 64 * 1024 * 1024 // 64 MB
	tooSmall        = blockSize / 4
)

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

func (fn *readFn) RestrictionSize(_ string, _ int64, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

// CreateTracker creates sdf.LockRTrackers wrapping offsetRange.Trackers for
// each restriction.
func (fn *readFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

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
		i--
		n, err := rd.Discard(int(i)) // Scan to just before restriction.
		if err == io.EOF {
			return fmt.Errorf("TextIO restriction lies outside the file being read. "+
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