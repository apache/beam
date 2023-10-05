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

// Package fileio provides transforms for matching and reading files.
package fileio

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/periodic"
)

func init() {
	register.DoFn3x1[context.Context, string, func(FileMetadata), error](&matchFn{})
	register.DoFn2x0[[]byte, func(string)](&matchContFn{})
	register.DoFn4x1[state.Provider, string, FileMetadata, func(FileMetadata), error](
		&dedupFn{},
	)
	register.DoFn4x1[state.Provider, string, FileMetadata, func(FileMetadata), error](
		&dedupUnmodifiedFn{},
	)
	register.Emitter1[FileMetadata]()
	register.Emitter1[string]()
	register.Function1x2[FileMetadata, string, FileMetadata](keyByPath)
}

// emptyTreatment controls how empty matches of a pattern are treated.
type emptyTreatment int

const (
	// emptyAllow allows empty matches.
	emptyAllow emptyTreatment = iota
	// emptyDisallow disallows empty matches.
	emptyDisallow
	// emptyAllowIfWildcard allows empty matches if the pattern contains a wildcard.
	emptyAllowIfWildcard
)

type matchOption struct {
	EmptyTreatment emptyTreatment
}

// MatchOptionFn is a function that can be passed to MatchFiles or MatchAll to configure options for
// matching files.
type MatchOptionFn func(*matchOption)

// MatchEmptyAllowIfWildcard specifies that empty matches are allowed if the pattern contains a
// wildcard.
func MatchEmptyAllowIfWildcard() MatchOptionFn {
	return func(o *matchOption) {
		o.EmptyTreatment = emptyAllowIfWildcard
	}
}

// MatchEmptyAllow specifies that empty matches are allowed.
func MatchEmptyAllow() MatchOptionFn {
	return func(o *matchOption) {
		o.EmptyTreatment = emptyAllow
	}
}

// MatchEmptyDisallow specifies that empty matches are not allowed.
func MatchEmptyDisallow() MatchOptionFn {
	return func(o *matchOption) {
		o.EmptyTreatment = emptyDisallow
	}
}

// MatchFiles finds all files matching the glob pattern and returns a PCollection<FileMetadata> of
// the matching files. MatchFiles accepts a variadic number of MatchOptionFn that can be used to
// configure the treatment of empty matches. By default, empty matches are allowed if the pattern
// contains a wildcard.
func MatchFiles(s beam.Scope, glob string, opts ...MatchOptionFn) beam.PCollection {
	s = s.Scope("fileio.MatchFiles")

	filesystem.ValidateScheme(glob)
	return MatchAll(s, beam.Create(s, glob), opts...)
}

// MatchAll finds all files matching the glob patterns given by the incoming PCollection<string> and
// returns a PCollection<FileMetadata> of the matching files. MatchAll accepts a variadic number of
// MatchOptionFn that can be used to configure the treatment of empty matches. By default, empty
// matches are allowed if the pattern contains a wildcard.
func MatchAll(s beam.Scope, col beam.PCollection, opts ...MatchOptionFn) beam.PCollection {
	s = s.Scope("fileio.MatchAll")

	option := &matchOption{
		EmptyTreatment: emptyAllowIfWildcard,
	}

	for _, opt := range opts {
		opt(option)
	}

	return beam.ParDo(s, newMatchFn(option), col)
}

type matchFn struct {
	EmptyTreatment emptyTreatment
}

func newMatchFn(option *matchOption) *matchFn {
	return &matchFn{
		EmptyTreatment: option.EmptyTreatment,
	}
}

func (fn *matchFn) ProcessElement(
	ctx context.Context,
	glob string,
	emit func(FileMetadata),
) error {
	if strings.TrimSpace(glob) == "" {
		return nil
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

	if len(files) == 0 {
		if !allowEmptyMatch(glob, fn.EmptyTreatment) {
			return fmt.Errorf("no files matching pattern %q", glob)
		}
		return nil
	}

	metadata, err := metadataFromFiles(ctx, fs, files)
	if err != nil {
		return err
	}

	for _, md := range metadata {
		emit(md)
	}

	return nil
}

func allowEmptyMatch(glob string, treatment emptyTreatment) bool {
	switch treatment {
	case emptyDisallow:
		return false
	case emptyAllowIfWildcard:
		return strings.Contains(glob, "*")
	default:
		return true
	}
}

func metadataFromFiles(
	ctx context.Context,
	fs filesystem.Interface,
	files []string,
) ([]FileMetadata, error) {
	if len(files) == 0 {
		return nil, nil
	}

	metadata := make([]FileMetadata, len(files))

	for i, path := range files {
		size, err := fs.Size(ctx, path)
		if err != nil {
			return nil, err
		}

		mTime, err := lastModified(ctx, fs, path)
		if err != nil {
			return nil, err
		}

		metadata[i] = FileMetadata{
			Path:         path,
			Size:         size,
			LastModified: mTime,
		}
	}

	return metadata, nil
}

func lastModified(ctx context.Context, fs filesystem.Interface, path string) (time.Time, error) {
	lmGetter, ok := fs.(filesystem.LastModifiedGetter)
	if !ok {
		log.Warnf(ctx, "Filesystem %T does not implement filesystem.LastModifiedGetter", fs)
		return time.Time{}, nil
	}

	mTime, err := lmGetter.LastModified(ctx, path)
	if err != nil {
		return time.Time{}, fmt.Errorf("error getting last modified time for %q: %v", path, err)
	}

	return mTime, nil
}

// duplicateTreatment controls how duplicate matches are treated.
type duplicateTreatment int

const (
	// duplicateAllow allows duplicate matches.
	duplicateAllow duplicateTreatment = iota
	// duplicateAllowIfModified allows duplicate matches only if the file has been modified since it
	// was last observed.
	duplicateAllowIfModified
	// duplicateSkip skips duplicate matches.
	duplicateSkip
)

type matchContOption struct {
	Start              time.Time
	End                time.Time
	DuplicateTreatment duplicateTreatment
	ApplyWindow        bool
}

// MatchContOptionFn is a function that can be passed to MatchContinuously to configure options for
// matching files.
type MatchContOptionFn func(*matchContOption)

// MatchStart specifies the start time for matching files.
func MatchStart(start time.Time) MatchContOptionFn {
	return func(o *matchContOption) {
		o.Start = start
	}
}

// MatchEnd specifies the end time for matching files.
func MatchEnd(end time.Time) MatchContOptionFn {
	return func(o *matchContOption) {
		o.End = end
	}
}

// MatchDuplicateAllow specifies that file path matches will not be deduplicated.
func MatchDuplicateAllow() MatchContOptionFn {
	return func(o *matchContOption) {
		o.DuplicateTreatment = duplicateAllow
	}
}

// MatchDuplicateAllowIfModified specifies that file path matches will be deduplicated unless the
// file has been modified since it was last observed.
func MatchDuplicateAllowIfModified() MatchContOptionFn {
	return func(o *matchContOption) {
		o.DuplicateTreatment = duplicateAllowIfModified
	}
}

// MatchDuplicateSkip specifies that file path matches will be deduplicated.
func MatchDuplicateSkip() MatchContOptionFn {
	return func(o *matchContOption) {
		o.DuplicateTreatment = duplicateSkip
	}
}

// MatchApplyWindow specifies that each element will be assigned to an individual window.
func MatchApplyWindow() MatchContOptionFn {
	return func(o *matchContOption) {
		o.ApplyWindow = true
	}
}

// MatchContinuously finds all files matching the glob pattern at the given interval and returns a
// PCollection<FileMetadata> of the matching files. MatchContinuously accepts a variadic number of
// MatchContOptionFn that can be used to configure:
//
//   - Start: start time for matching files. Defaults to the current timestamp
//   - End: end time for matching files. Defaults to the maximum timestamp
//   - DuplicateAllow: allow emitting matches that have already been observed. Defaults to false
//   - DuplicateAllowIfModified: allow emitting matches that have already been observed if the file
//     has been modified since the last observation. Defaults to false
//   - DuplicateSkip: skip emitting matches that have already been observed. Defaults to true
//   - ApplyWindow: assign each element to an individual window with a fixed size equivalent to the
//     interval. Defaults to false, i.e. all elements will reside in the global window
func MatchContinuously(
	s beam.Scope,
	glob string,
	interval time.Duration,
	opts ...MatchContOptionFn,
) beam.PCollection {
	s = s.Scope("fileio.MatchContinuously")

	filesystem.ValidateScheme(glob)

	option := &matchContOption{
		Start:              mtime.Now().ToTime(),
		End:                mtime.MaxTimestamp.ToTime(),
		ApplyWindow:        false,
		DuplicateTreatment: duplicateSkip,
	}

	for _, opt := range opts {
		opt(option)
	}

	imp := periodic.Impulse(s, option.Start, option.End, interval, false)
	globs := beam.ParDo(s, &matchContFn{Glob: glob}, imp)
	matches := MatchAll(s, globs, MatchEmptyAllow())

	out := dedupIfRequired(s, matches, option.DuplicateTreatment)

	if option.ApplyWindow {
		return beam.WindowInto(s, window.NewFixedWindows(interval), out)
	}
	return out
}

func dedupIfRequired(
	s beam.Scope,
	col beam.PCollection,
	treatment duplicateTreatment,
) beam.PCollection {
	if treatment == duplicateAllow {
		return col
	}

	keyed := beam.ParDo(s, keyByPath, col)

	if treatment == duplicateAllowIfModified {
		return beam.ParDo(s, &dedupUnmodifiedFn{}, keyed)
	}

	return beam.ParDo(s, &dedupFn{}, keyed)
}

type matchContFn struct {
	Glob string
}

func (fn *matchContFn) ProcessElement(_ []byte, emit func(string)) {
	emit(fn.Glob)
}

func keyByPath(md FileMetadata) (string, FileMetadata) {
	return md.Path, md
}

type dedupFn struct {
	State state.Value[struct{}]
}

func (fn *dedupFn) ProcessElement(
	sp state.Provider,
	_ string,
	md FileMetadata,
	emit func(FileMetadata),
) error {
	_, ok, err := fn.State.Read(sp)
	if err != nil {
		return fmt.Errorf("error reading state: %v", err)
	}

	if !ok {
		emit(md)
		if err := fn.State.Write(sp, struct{}{}); err != nil {
			return fmt.Errorf("error writing state: %v", err)
		}
	}

	return nil
}

type dedupUnmodifiedFn struct {
	State state.Value[int64]
}

func (fn *dedupUnmodifiedFn) ProcessElement(
	sp state.Provider,
	_ string,
	md FileMetadata,
	emit func(FileMetadata),
) error {
	prevMTime, ok, err := fn.State.Read(sp)
	if err != nil {
		return fmt.Errorf("error reading state: %v", err)
	}

	mTime := md.LastModified.UnixMilli()

	if !ok || mTime > prevMTime {
		emit(md)
		if err := fn.State.Write(sp, mTime); err != nil {
			return fmt.Errorf("error writing state: %v", err)
		}
	}

	return nil
}
