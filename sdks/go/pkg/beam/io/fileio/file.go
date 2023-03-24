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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*FileMetadata)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*ReadableFile)(nil)).Elem())
}

// FileMetadata contains metadata about a file, namely its path and size in bytes.
type FileMetadata struct {
	Path string
	Size int64
}

// Compression is the type of compression used to compress a file.
type Compression int

const (
	// CompressionAuto indicates that the compression type should be auto-detected.
	CompressionAuto Compression = iota
	// CompressionGzip indicates that the file is compressed using gzip.
	CompressionGzip
	// CompressionUncompressed indicates that the file is not compressed.
	CompressionUncompressed
)

// ReadableFile is a wrapper around a FileMetadata and Compression that can be used to obtain a file
// descriptor or read the file's contents.
type ReadableFile struct {
	Metadata    FileMetadata
	Compression Compression
}

// Open opens the file for reading. The compression type is determined by the Compression field of
// the ReadableFile. If Compression is CompressionAuto, the compression type is auto-detected from
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
	if comp == CompressionAuto {
		comp = compressionFromExt(f.Metadata.Path)
	}

	return newDecompressionReader(rc, comp)
}

// compressionFromExt detects the compression of a file based on its extension. If the extension is
// not recognized, CompressionUncompressed is returned.
func compressionFromExt(path string) Compression {
	switch filepath.Ext(path) {
	case ".gz":
		return CompressionGzip
	default:
		return CompressionUncompressed
	}
}

// newDecompressionReader returns an io.ReadCloser that can be used to read uncompressed data from
// reader, based on the specified compression. If the compression is CompressionAuto, a non-nil
// error is returned. It is the caller's responsibility to close the returned reader.
func newDecompressionReader(
	reader io.ReadCloser,
	compression Compression,
) (io.ReadCloser, error) {
	switch compression {
	case CompressionAuto:
		return nil, errors.New(
			"compression must be resolved into a concrete type before obtaining a reader",
		)
	case CompressionGzip:
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
