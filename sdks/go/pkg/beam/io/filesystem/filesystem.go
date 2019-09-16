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
package filesystem

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

var registry = make(map[string]func(context.Context) Interface)

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
		return nil, errors.Errorf("file system scheme %v not registered for %v", scheme, path)
	}
	return mkfs(ctx), nil
}

// Interface is a filesystem abstraction that allows beam io sources and sinks
// to use various underlying storage systems transparently.
type Interface interface {
	io.Closer

	// List expands a patten to a list of filenames.
	List(ctx context.Context, glob string) ([]string, error)

	// OpenRead opens a file for reading.
	OpenRead(ctx context.Context, filename string) (io.ReadCloser, error)
	// OpenRead opens a file for writing. If the file already exist, it will be
	// overwritten.
	OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error)
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
		panic(fmt.Sprintf("filesystem scheme %v not registered", scheme))
	}
}
