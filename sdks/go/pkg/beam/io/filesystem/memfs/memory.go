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

// Package memfs contains a in-memory Beam filesystem. Useful for testing.
package memfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
)

func init() {
	filesystem.Register("memfs", New)
}

var instance = &fs{m: make(map[string]file)}

type fs struct {
	m  map[string]file
	mu sync.Mutex
}

type file struct {
	Data         []byte
	LastModified time.Time
}

func newFile(data []byte) file {
	cp := make([]byte, len(data))
	copy(cp, data)

	return file{
		Data:         data,
		LastModified: time.Now(),
	}
}

// New returns the global memory filesystem.
func New(_ context.Context) filesystem.Interface {
	return instance
}

func (f *fs) Close() error {
	return nil
}

func (f *fs) List(_ context.Context, glob string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// As with other functions, the memfs:// prefix is optional.
	globNoScheme := strings.TrimPrefix(glob, "memfs://")

	var ret []string
	for k := range f.m {
		matched, err := filesystem.Match(globNoScheme, strings.TrimPrefix(k, "memfs://"))
		if err != nil {
			return nil, fmt.Errorf("invalid glob pattern: %w", err)
		}
		if matched {
			ret = append(ret, k)
		}
	}
	sort.Strings(ret)
	return ret, nil
}

func (f *fs) OpenRead(_ context.Context, filename string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if v, ok := f.m[normalize(filename)]; ok {
		return io.NopCloser(bytes.NewReader(v.Data)), nil
	}
	return nil, os.ErrNotExist
}

func (f *fs) OpenWrite(_ context.Context, filename string) (io.WriteCloser, error) {
	return &commitWriter{key: filename, instance: f}, nil
}

func (f *fs) Size(_ context.Context, filename string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if v, ok := f.m[normalize(filename)]; ok {
		return int64(len(v.Data)), nil
	}
	return -1, os.ErrNotExist
}

// LastModified returns the time at which the file was last modified.
func (f *fs) LastModified(_ context.Context, filename string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.m[normalize(filename)]
	if !ok {
		return time.Time{}, os.ErrNotExist
	}

	return v.LastModified, nil
}

// Remove the named file from the filesystem.
func (f *fs) Remove(_ context.Context, filename string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, filename)
	return nil
}

// Rename the old path to the new path.
func (f *fs) Rename(_ context.Context, oldpath, newpath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[newpath] = f.m[oldpath]
	delete(f.m, oldpath)
	return nil
}

// Copy copies the old path to the new path.
func (f *fs) Copy(_ context.Context, oldpath, newpath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	oldFile, ok := f.m[normalize(oldpath)]
	if !ok {
		return os.ErrNotExist
	}

	f.m[normalize(newpath)] = newFile(oldFile.Data)
	return nil
}

// Compile time check for interface implementations.
var (
	_ filesystem.LastModifiedGetter = ((*fs)(nil))
	_ filesystem.Remover            = ((*fs)(nil))
	_ filesystem.Renamer            = ((*fs)(nil))
	_ filesystem.Copier             = ((*fs)(nil))
)

// write is a helper function for writing to the global store.
func (f *fs) write(key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.m[normalize(key)] = newFile(value)
	return nil
}

// Write stores the given key and value in the global store.
func Write(key string, value []byte) {
	instance.write(key, value)
}

func normalize(key string) string {
	if runtime.GOOS == "windows" {
		key = strings.ReplaceAll(key, "\\", "/")
	}
	if strings.HasPrefix(key, "memfs://") {
		return key
	}
	return "memfs://" + key
}

type commitWriter struct {
	key      string
	buf      bytes.Buffer
	instance *fs
}

func (w *commitWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *commitWriter) Close() error {
	return w.instance.write(w.key, w.buf.Bytes())
}
