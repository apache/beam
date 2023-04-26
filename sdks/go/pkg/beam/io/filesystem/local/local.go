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

// Package local contains a local file implementation of the Beam file system.
package local

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
)

func init() {
	filesystem.Register("default", New)
}

type fs struct{}

// New creates a new local filesystem.
func New(_ context.Context) filesystem.Interface {
	return &fs{}
}

func (f *fs) Close() error {
	return nil
}

func (f *fs) List(_ context.Context, glob string) ([]string, error) {
	return filepath.Glob(glob)
}

func (f *fs) OpenRead(_ context.Context, filename string) (io.ReadCloser, error) {
	return os.Open(filename)
}

func (f *fs) OpenWrite(_ context.Context, filename string) (io.WriteCloser, error) {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return nil, err
	}
	return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}

func (f *fs) Size(_ context.Context, filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return -1, err
	}
	info, err := file.Stat()
	if err != nil {
		return -1, err
	}
	return info.Size(), nil
}

// LastModified returns the time at which the file was last modified.
func (f *fs) LastModified(_ context.Context, filename string) (time.Time, error) {
	info, err := os.Stat(filename)
	if err != nil {
		return time.Time{}, err
	}

	return info.ModTime(), nil
}

// Remove the named file from the filesystem.
func (f *fs) Remove(_ context.Context, filename string) error {
	return os.Remove(filename)
}

// Rename the old path to the new path.
func (f *fs) Rename(_ context.Context, oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// Copy copies from oldpath to the newpath.
func (f *fs) Copy(_ context.Context, oldpath, newpath string) error {
	srcFile, err := os.Open(oldpath)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	destFile, err := os.Create(newpath)
	if err != nil {
		return err
	}
	defer destFile.Close()
	_, err = io.Copy(destFile, srcFile)
	return err
}

// Compile time check for interface implementations.
var (
	_ filesystem.LastModifiedGetter = ((*fs)(nil))
	_ filesystem.Copier             = ((*fs)(nil))
	_ filesystem.Remover            = ((*fs)(nil))
	_ filesystem.Renamer            = ((*fs)(nil))
)
