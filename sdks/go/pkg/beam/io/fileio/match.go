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
	register.DoFn3x1[context.Context, string, func(filesystem.FileMetadata), error](&matchFn{})
	register.Emitter1[filesystem.FileMetadata]()
}

// EmptyMatchTreatment controls how empty matches of a pattern are treated.
type EmptyMatchTreatment int

const (
	// EmptyMatchTreatmentAllow allows empty matches.
	EmptyMatchTreatmentAllow EmptyMatchTreatment = iota
	// EmptyMatchTreatmentDisallow disallows empty matches.
	EmptyMatchTreatmentDisallow
	// EmptyMatchTreatmentAllowIfWildcard allows empty matches if the pattern contains a wildcard.
	EmptyMatchTreatmentAllowIfWildcard
)

type matchOption struct {
	EmptyMatchTreatment EmptyMatchTreatment
}

// MatchOptionFn is a function that can be passed to MatchFiles or MatchAll to configure options for
// matching files.
type MatchOptionFn func(*matchOption) error

// WithEmptyMatchTreatment specifies how empty matches of a pattern should be treated. By default,
// empty matches are allowed if the pattern contains a wildcard.
func WithEmptyMatchTreatment(treatment EmptyMatchTreatment) MatchOptionFn {
	return func(o *matchOption) error {
		o.EmptyMatchTreatment = treatment
		return nil
	}
}

// MatchFiles finds all files matching the glob pattern and returns a
// PCollection<filesystem.FileMetadata> of the matching files.
func MatchFiles(s beam.Scope, glob string, opts ...MatchOptionFn) beam.PCollection {
	s = s.Scope("fileio.MatchFiles")

	filesystem.ValidateScheme(glob)
	return MatchAll(s, beam.Create(s, glob), opts...)
}

// MatchAll finds all files matching the glob patterns given by the incoming PCollection<string> and
// returns a PCollection<filesystem.FileMetadata> of the matching files.
func MatchAll(s beam.Scope, col beam.PCollection, opts ...MatchOptionFn) beam.PCollection {
	s = s.Scope("fileio.MatchAll")

	option := &matchOption{
		EmptyMatchTreatment: EmptyMatchTreatmentAllowIfWildcard,
	}

	for _, opt := range opts {
		if err := opt(option); err != nil {
			panic(fmt.Sprintf("fileio.MatchAll: invalid option: %v", err))
		}
	}

	return beam.ParDo(s, newMatchFn(option), col)
}

type matchFn struct {
	EmptyMatchTreatment EmptyMatchTreatment
}

func newMatchFn(option *matchOption) *matchFn {
	return &matchFn{
		EmptyMatchTreatment: option.EmptyMatchTreatment,
	}
}

func (fn *matchFn) ProcessElement(
	ctx context.Context,
	glob string,
	emit func(filesystem.FileMetadata),
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
		if !allowEmptyMatch(glob, fn.EmptyMatchTreatment) {
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

func allowEmptyMatch(glob string, treatment EmptyMatchTreatment) bool {
	switch treatment {
	case EmptyMatchTreatmentDisallow:
		return false
	case EmptyMatchTreatmentAllowIfWildcard:
		return strings.Contains(glob, "*")
	default:
		return true
	}
}

func metadataFromFiles(
	ctx context.Context,
	fs filesystem.Interface,
	files []string,
) ([]filesystem.FileMetadata, error) {
	if len(files) == 0 {
		return nil, nil
	}

	metadata := make([]filesystem.FileMetadata, len(files))

	for i, path := range files {
		size, err := fs.Size(ctx, path)
		if err != nil {
			return nil, err
		}

		metadata[i] = filesystem.FileMetadata{
			Path: path,
			Size: size,
		}
	}

	return metadata, nil
}
