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
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*FileMetadata)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*ReadableFile)(nil)).Elem())
}

// FileMetadata contains metadata about a file, namely its path, size in bytes and last modified
// time.
type FileMetadata struct {
	Path         string
	Size         int64
	LastModified time.Time
}

// compressionType is the type of compression used to compress a file.
type compressionType int

const (
	// compressionAuto indicates that the compression type should be auto-detected.
	compressionAuto compressionType = iota
	// compressionGzip indicates that the file is compressed using gzip.
	compressionGzip
	// compressionUncompressed indicates that the file is not compressed.
	compressionUncompressed
)

// ReadableFile is a wrapper around a FileMetadata and compressionType that can be used to obtain a
// file descriptor or read the file's contents.
type ReadableFile struct {
	Metadata    FileMetadata
	Compression compressionType
}

// Open opens the file for reading. The compression type is determined by the Compression field of
// the ReadableFile. If Compression is compressionAuto, the compression type is auto-detected from
// the file extension. It is the caller's responsibility to close the returned reader.
func (f ReadableFile) Open(ctx context.Context) (io.ReadCloser, error) {
	fs, err := filesystem.New(ctx, f.Metadata.Path)
	if err != nil {
		return nil, err
	}
	defer fs.Close()

	rc, err := fs.OpenRead(ctx, f.Metadata.Path)
	if err != nil {
		return nil, err
	}

	comp := f.Compression
	if comp == compressionAuto {
		comp = compressionFromExt(f.Metadata.Path)
	}

	return newDecompressionReader(rc, comp)
}

// compressionFromExt detects the compression of a file based on its extension. If the extension is
// not recognized, compressionUncompressed is returned.
func compressionFromExt(path string) compressionType {
	switch filepath.Ext(path) {
	case ".gz":
		return compressionGzip
	default:
		return compressionUncompressed
	}
}

// newDecompressionReader returns an io.ReadCloser that can be used to read uncompressed data from
// reader, based on the specified compression. If the compression is compressionAuto, a non-nil
// error is returned. It is the caller's responsibility to close the returned reader.
func newDecompressionReader(
	reader io.ReadCloser,
	compression compressionType,
) (io.ReadCloser, error) {
	switch compression {
	case compressionAuto:
		return nil, errors.New(
			"compression must be resolved into a concrete type before obtaining a reader",
		)
	case compressionGzip:
		return newGzipReader(reader)
	default:
		return reader, nil
	}
}

// Read reads the entire file into memory and returns the contents.
func (f ReadableFile) Read(ctx context.Context) (data []byte, err error) {
	rc, err := f.Open(ctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		closeErr := rc.Close()
		if err != nil {
			if closeErr != nil {
				log.Errorf(ctx, "error closing reader: %v", closeErr)
			}
			return
		}
		err = closeErr
	}()

	return io.ReadAll(rc)
}

// ReadString reads the entire file into memory and returns the contents as a string.
func (f ReadableFile) ReadString(ctx context.Context) (string, error) {
	data, err := f.Read(ctx)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
