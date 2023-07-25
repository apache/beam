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
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// openFile opens a file for reading.
func openFile(t *testing.T, path string) io.ReadCloser {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	return f
}

// createFile creates a file and parent directories if needed.
func createFile(t *testing.T, path string) *os.File {
	t.Helper()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	return file
}

// write writes data to a file.
func write(t *testing.T, path string, data []byte) {
	t.Helper()

	f := createFile(t, path)
	defer f.Close()

	bw := bufio.NewWriter(f)
	if _, err := bw.Write(data); err != nil {
		t.Fatal(err)
	}

	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
}

// writeGzip compresses and writes data to a file using gzip.
func writeGzip(t *testing.T, path string, data []byte) {
	t.Helper()

	f := createFile(t, path)
	defer f.Close()

	zw := gzip.NewWriter(f)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}

	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}

func modTime(t *testing.T, path string) time.Time {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	return info.ModTime()
}
