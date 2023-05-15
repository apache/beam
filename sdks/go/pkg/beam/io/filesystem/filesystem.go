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

// Package filesystem contains an extensible file system abstraction. It allows
// various kinds of storage systems to be used uniformly, notably through textio.
//
// Registered file systems at minimum implement the Interface abstraction, and
// can then optionally implement Remover, Renamer, and Copier to support
// rename operations. Filesystems are only expected to handle their own IO, and
// not cross file system IO. Should cross file system IO be required, additional
// utility methods should be added to this package to support them.
package filesystem

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

var registry = make(map[string]func(context.Context) Interface)

// wellKnownSchemeImportPaths is used for delivering useful error messages when a
// scheme is not found.
var wellKnownSchemeImportPaths = map[string]string{
	"memfs":   "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/memfs",
	"default": "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local",
	"gs":      "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs",
	"s3":      "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/s3",
}

// Register registers a file system backend under the given scheme.  For
// example, "hdfs" would be registered a HFDS file system and HDFS paths used
// transparently.
func Register(scheme string, fs func(context.Context) Interface) {
	if _, ok := registry[scheme]; ok {
		panic(fmt.Sprintf("scheme %v already registered", scheme))
	}
	registry[scheme] = fs
}

// New returns a new Interface for the given file path's scheme.
func New(ctx context.Context, path string) (Interface, error) {
	scheme := getScheme(path)
	mkfs, ok := registry[scheme]
	if !ok {
		return nil, errorForMissingScheme(scheme, path)
	}
	return mkfs(ctx), nil
}

func errorForMissingScheme(scheme, path string) error {
	messageSuffix := ""
	if suggestedImportPath, ok := wellKnownSchemeImportPaths[scheme]; ok {
		messageSuffix = fmt.Sprintf(": Consider adding the following import to your program to register an implementation for %q:\n  import _ %q", scheme, suggestedImportPath)
	}
	return errors.Errorf("file system scheme %q not registered for %q%s", scheme, path, messageSuffix)
}

// Interface is a filesystem abstraction that allows beam io sources and sinks
// to use various underlying storage systems transparently.
type Interface interface {
	io.Closer

	// List expands a pattern to a list of filenames.
	// Returns nil if there are no matching files.
	List(ctx context.Context, glob string) ([]string, error)

	// OpenRead opens a file for reading.
	OpenRead(ctx context.Context, filename string) (io.ReadCloser, error)
	// OpenWrite opens a file for writing. If the file already exist, it will be
	// overwritten. The returned io.WriteCloser should be closed to commit the write.
	OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error)
	// Size returns the size of a file in bytes.
	Size(ctx context.Context, filename string) (int64, error)
}

// The following interfaces are optional for the filesystems, but
// to support more efficient or composite operations when possible.

// LastModifiedGetter is an interface for getting the last modified time
// of a file.
type LastModifiedGetter interface {
	LastModified(ctx context.Context, filename string) (time.Time, error)
}

// Remover is an interface for removing files from the filesystem.
// To be considered for promotion to Interface.
type Remover interface {
	Remove(ctx context.Context, filename string) error
}

// Copier is an interface for copying files in the filesystem.
type Copier interface {
	Copy(ctx context.Context, oldpath, newpath string) error
}

// Renamer is an interface for renaming or moving files in the filesystem.
type Renamer interface {
	Rename(ctx context.Context, oldpath, newpath string) error
}

func getScheme(path string) string {
	if index := strings.Index(path, "://"); index > 0 {
		return path[:index]
	}
	return "default"
}

// ValidateScheme panics if the given path's scheme does not have a
// corresponding file system registered.
func ValidateScheme(path string) {
	if strings.TrimSpace(path) == "" {
		panic("empty file glob provided")
	}
	scheme := getScheme(path)
	if _, ok := registry[scheme]; !ok {
		panic(errorForMissingScheme(scheme, path))
	}
}
