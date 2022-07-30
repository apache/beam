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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
)

func TestLocal_FilesystemNew(t *testing.T) {
	ctx := context.Background()
	path := "/tmp/"
	c, err := filesystem.New(ctx, path)
	if err != nil {
		t.Fatalf("filesystem.New(ctx, %q) == %v, want nil", path, err)
	}

	if _, ok := c.(*fs); !ok {
		t.Errorf("filesystem.New(ctx, %q) type == %T, want *local.fs", path, c)
	}

	if err := c.Close(); err != nil {
		t.Errorf("c.Close() = %v, want nil", err)
	}
}

func TestLocal_direct(t *testing.T) {
	ctx := context.Background()
	dirPath := filepath.Join(os.TempDir(), "beamgolocalfilesystemtest")
	filePath := filepath.Join(dirPath, "file.txt")
	t.Cleanup(func() {
		os.RemoveAll(dirPath)
	})

	c, err := filesystem.New(ctx, filePath)
	if err != nil {
		t.Fatalf("filesystem.New(ctx, %q) == %v, want nil", filePath, err)
	}
	t.Cleanup(func() {
		c.Close()
	})

	wc, err := c.OpenWrite(ctx, filePath)
	if err != nil {
		t.Fatalf("OpenWrite(ctx, %q) == %v, want nil", filePath, err)
	}
	t.Cleanup(func() {
		wc.Close()
	})
	data := []byte("a meaningless sequence of test words")
	n, err := wc.Write(data)
	if got, want := n, len(data); err != nil || got != want {
		t.Fatalf("Write(data) = %v,%v, want %v, nil", got, err, want)
	}
	wc.Close()

	listGlob := filepath.Join(dirPath, "*.txt")
	files, err := c.List(ctx, listGlob)
	if err != nil {
		t.Fatalf("List(%q) = %v, want nil", listGlob, err)
	}

	if len(files) != 1 || (len(files) == 1 && files[0] != filePath) {
		t.Errorf("List(%v) = %v, want []string{%v}", listGlob, files, filePath)
	}

	size, err := c.Size(ctx, filePath)
	if got, want := size, int64(len(data)); err != nil || got != want {
		t.Errorf("Size(%q) = %v, %v, want %v, nil", filePath, got, err, want)
	}

	rc, err := c.OpenRead(ctx, filePath)
	if err != nil {
		t.Fatalf("OpenRead(ctx, %q) == %v, want nil", filePath, err)
	}
	t.Cleanup(func() {
		c.Close()
	})

	buf, err := io.ReadAll(rc)
	if got, want := n, len(buf); err != nil || got != want {
		t.Fatalf("ReadAll() = %v,%v, want %v, nil", got, err, want)
	}
	if got, want := string(data), string(buf); got != want {
		t.Errorf("ReadAll() = %v, want %v", got, want)
	}
}

func TestLocal_util(t *testing.T) {
	ctx := context.Background()
	dirPath := filepath.Join(os.TempDir(), "beamgolocalfilesystemtest")
	filePath := filepath.Join(dirPath, "file.txt")
	t.Cleanup(func() {
		os.RemoveAll(dirPath)
	})

	c, err := filesystem.New(ctx, filePath)
	if err != nil {
		t.Fatalf("filesystem.New(ctx, %q) == %v, want nil", filePath, err)
	}
	t.Cleanup(func() {
		c.Close()
	})
	data := []byte("a meaningless sequence of test words")
	if err := filesystem.Write(ctx, c, filePath, data); err != nil {
		t.Fatalf("filesystem.Write(ctx, %q) == %v, want nil", filePath, err)
	}

	gotData, err := filesystem.Read(ctx, c, filePath)
	if err != nil {
		t.Fatalf("filesystem.Read(ctx, %q) == %v, want nil", filePath, err)
	}
	if got, want := string(data), string(gotData); got != want {
		t.Errorf("filesystem.Read() = %v, want %v", got, want)
	}
}

func TestLocal_rename(t *testing.T) {
	ctx := context.Background()
	dirPath := filepath.Join(os.TempDir(), "beamgolocalfilesystemtest")
	filePath1 := filepath.Join(dirPath, "file1.txt")
	filePath2 := filepath.Join(dirPath, "file2.txt")
	t.Cleanup(func() {
		os.RemoveAll(dirPath)
	})

	c, err := filesystem.New(ctx, dirPath)
	if err != nil {
		t.Fatalf("filesystem.New(ctx, %q) == %v, want nil", dirPath, err)
	}
	t.Cleanup(func() {
		c.Close()
	})
	data := []byte("a meaningless sequence of test words")
	if err := filesystem.Write(ctx, c, filePath1, data); err != nil {
		t.Fatalf("filesystem.Write(ctx, %q) == %v, want nil", filePath1, err)
	}

	if err := filesystem.Rename(ctx, c, filePath1, filePath2); err != nil {
		t.Fatalf("filesystem.Read(ctx, %q) == %v, want nil", filePath2, err)
	}

	gotData, err := filesystem.Read(ctx, c, filePath2)
	if err != nil {
		t.Fatalf("filesystem.Read(ctx, %q) == %v, want nil", filePath2, err)
	}
	if got, want := string(data), string(gotData); got != want {
		t.Errorf("filesystem.Read() = %v, want %v", got, want)
	}

	// Check that the old file doesn't exist.
	listGlob := filepath.Join(dirPath, "*.txt")
	files, err := c.List(ctx, listGlob)
	if err != nil {
		t.Fatalf("List(%q) = %v, want nil", listGlob, err)
	}

	if len(files) != 1 || (len(files) == 1 && files[0] != filePath2) {
		t.Errorf("List(%v) = %v, want []string{%v}", listGlob, files, filePath2)
	}
}
