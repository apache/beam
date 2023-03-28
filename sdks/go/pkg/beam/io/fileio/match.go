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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x1[context.Context, string, func(FileMetadata), error](&matchFn{})
	register.Emitter1[FileMetadata]()
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

		metadata[i] = FileMetadata{
			Path: path,
			Size: size,
		}
	}

	return metadata, nil
}
