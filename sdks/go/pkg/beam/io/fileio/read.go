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

package fileio

import (
	"fmt"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn2x1[filesystem.FileMetadata, func(ReadableFile), error](&readFn{})
	register.Emitter1[ReadableFile]()
}

// DirectoryTreatment controls how directories are treated when reading matches.
type DirectoryTreatment int

const (
	// DirectoryTreatmentSkip skips directories.
	DirectoryTreatmentSkip DirectoryTreatment = iota
	// DirectoryTreatmentDisallow disallows directories.
	DirectoryTreatmentDisallow
)

type readOption struct {
	Compression        filesystem.Compression
	DirectoryTreatment DirectoryTreatment
}

// ReadOptionFn is a function that can be passed to ReadMatches to configure options for
// reading files.
type ReadOptionFn func(*readOption) error

// WithReadCompression specifies the compression type of the files that are read. By default,
// the compression type is determined by the file extension.
func WithReadCompression(compression filesystem.Compression) ReadOptionFn {
	return func(o *readOption) error {
		o.Compression = compression
		return nil
	}
}

// WithDirectoryTreatment specifies how directories should be treated when reading files. By
// default, directories are skipped.
func WithDirectoryTreatment(treatment DirectoryTreatment) ReadOptionFn {
	return func(o *readOption) error {
		o.DirectoryTreatment = treatment
		return nil
	}
}

// ReadMatches accepts the result of MatchFiles or MatchAll as a
// PCollection<filesystem.FileMetadata> and converts it to a PCollection<ReadableFile>. The
// ReadableFile can be used to retrieve file metadata, open the file for reading or read the entire
// file into memory. The compression type of the readable files can be specified by passing the
// WithReadCompression option. If no compression type is provided, it will be determined by the file
// extension.
func ReadMatches(s beam.Scope, col beam.PCollection, opts ...ReadOptionFn) beam.PCollection {
	s = s.Scope("fileio.ReadMatches")

	option := &readOption{
		Compression:        filesystem.CompressionAuto,
		DirectoryTreatment: DirectoryTreatmentSkip,
	}

	for _, opt := range opts {
		if err := opt(option); err != nil {
			panic(fmt.Sprintf("fileio.ReadMatches: invalid option: %v", err))
		}
	}

	return beam.ParDo(s, newReadFn(option), col)
}

type readFn struct {
	Compression        filesystem.Compression
	DirectoryTreatment DirectoryTreatment
}

func newReadFn(option *readOption) *readFn {
	return &readFn{
		Compression:        option.Compression,
		DirectoryTreatment: option.DirectoryTreatment,
	}
}

func (fn *readFn) ProcessElement(metadata filesystem.FileMetadata, emit func(ReadableFile)) error {
	if isDirectory(metadata.Path) {
		if fn.DirectoryTreatment == DirectoryTreatmentDisallow {
			return fmt.Errorf("path to directory not allowed: %q", metadata.Path)
		}
		return nil
	}

	file := ReadableFile{
		Metadata:    metadata,
		Compression: fn.Compression,
	}

	emit(file)
	return nil
}

func isDirectory(path string) bool {
	if strings.HasSuffix(path, "/") || strings.HasSuffix(path, "\\") {
		return true
	}
	return false
}
