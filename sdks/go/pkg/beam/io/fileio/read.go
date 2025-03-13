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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn2x1[FileMetadata, func(ReadableFile), error](&readFn{})
	register.Emitter1[ReadableFile]()
}

// directoryTreatment controls how paths to directories are treated when reading matches.
type directoryTreatment int

const (
	// directorySkip skips directories.
	directorySkip directoryTreatment = iota
	// directoryDisallow disallows directories.
	directoryDisallow
)

type readOption struct {
	Compression        compressionType
	DirectoryTreatment directoryTreatment
}

// ReadOptionFn is a function that can be passed to ReadMatches to configure options for
// reading files.
type ReadOptionFn func(*readOption)

// ReadAutoCompression specifies that the compression type of files should be auto-detected.
func ReadAutoCompression() ReadOptionFn {
	return func(o *readOption) {
		o.Compression = compressionAuto
	}
}

// ReadGzip specifies that files have been compressed using gzip.
func ReadGzip() ReadOptionFn {
	return func(o *readOption) {
		o.Compression = compressionGzip
	}
}

// ReadUncompressed specifies that files have not been compressed.
func ReadUncompressed() ReadOptionFn {
	return func(o *readOption) {
		o.Compression = compressionUncompressed
	}
}

// ReadDirectorySkip specifies that directories are skipped.
func ReadDirectorySkip() ReadOptionFn {
	return func(o *readOption) {
		o.DirectoryTreatment = directorySkip
	}
}

// ReadDirectoryDisallow specifies that directories are not allowed.
func ReadDirectoryDisallow() ReadOptionFn {
	return func(o *readOption) {
		o.DirectoryTreatment = directoryDisallow
	}
}

// ReadMatches accepts the result of MatchFiles, MatchAll or MatchContinuously as a
// PCollection<FileMetadata> and converts it to a PCollection<ReadableFile>. The ReadableFile can be
// used to retrieve file metadata, open the file for reading or read the entire file into memory.
// ReadMatches accepts a variadic number of ReadOptionFn that can be used to configure the
// compression type of the files and treatment of directories. By default, the compression type is
// determined by the file extension and directories are skipped.
func ReadMatches(s beam.Scope, col beam.PCollection, opts ...ReadOptionFn) beam.PCollection {
	s = s.Scope("fileio.ReadMatches")

	option := &readOption{
		Compression:        compressionAuto,
		DirectoryTreatment: directorySkip,
	}

	for _, opt := range opts {
		opt(option)
	}

	return beam.ParDo(s, newReadFn(option), col)
}

type readFn struct {
	Compression        compressionType
	DirectoryTreatment directoryTreatment
}

func newReadFn(option *readOption) *readFn {
	return &readFn{
		Compression:        option.Compression,
		DirectoryTreatment: option.DirectoryTreatment,
	}
}

func (fn *readFn) ProcessElement(metadata FileMetadata, emit func(ReadableFile)) error {
	if isDirectory(metadata.Path) {
		if fn.DirectoryTreatment == directoryDisallow {
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
