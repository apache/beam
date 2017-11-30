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

package local

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
)

func init() {
	textio.RegisterFileSystem("default", New)
}

type fs struct{}

// New creates a new local filesystem.
func New(ctx context.Context) textio.FileSystem {
	return &fs{}
}

func (f *fs) Close() error {
	return nil
}

func (f *fs) List(ctx context.Context, glob string) ([]string, error) {
	return filepath.Glob(glob)
}

func (f *fs) OpenRead(ctx context.Context, filename string) (io.ReadCloser, error) {
	return os.Open(filename)
}

func (f *fs) OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error) {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return nil, err
	}
	return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
}
