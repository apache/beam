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

package filesystem

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
)

// Read fully reads the given file from the file system.
func Read(ctx context.Context, fs Interface, filename string) ([]byte, error) {
	r, err := fs.OpenRead(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

// Write writes the given content to the file system.
func Write(ctx context.Context, fs Interface, filename string, data []byte) error {
	w, err := fs.OpenWrite(ctx, filename)
	if err != nil {
		return err
	}

	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

// Copy replicates the file at oldpath to newpath. Requires the paths to
// be on the same filesystem.
//
// If the file system implements Copier, it uses that, otherwise it does so manually.
func Copy(ctx context.Context, fs Interface, oldpath, newpath string) error {
	if cp, ok := fs.(Copier); ok {
		if err := cp.Copy(ctx, oldpath, newpath); err != nil {
			return err
		}
		return nil
	}
	w, err := fs.OpenWrite(ctx, newpath)
	if err != nil {
		return err
	}

	r, err := fs.OpenRead(ctx, oldpath)
	if err != nil {
		return err
	}
	defer r.Close()

	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	return w.Close()
}

// Rename moves the file at oldpath to newpath. Requires the paths to
// be on the same filesystem.
//
// Rename will use Renamer, Remover, and Copier interfaces if implemented.
// Renamer is tried first, and used if available. Otherwise, Rename requires
// Remover to be implemented, and calls Copy.
func Rename(ctx context.Context, fs Interface, oldpath, newpath string) error {
	if rn, ok := fs.(Renamer); ok {
		return rn.Rename(ctx, oldpath, newpath)
	}

	// Eagerly check if we can remove the temp files.
	rm, ok := fs.(Remover)
	if !ok {
		return &unimplementedError{fs, "Remover", "Rename"}
	}

	if err := Copy(ctx, fs, oldpath, newpath); err != nil {
		return err
	}
	// Clean up the old path.
	if err := rm.Remove(ctx, oldpath); err != nil {
		return err
	}

	return nil
}

// Match is a platform agnostic version of filepath.Match where \ is treated as / on Windows.
func Match(pattern, name string) (bool, error) {
	// Windows accepts / and \ as directory separators. For the sake of consistency with other schemes such as memfs:// we'll convert \ to /.
	if runtime.GOOS == "windows" {
		return filepath.Match(strings.ReplaceAll(pattern, "/", "//"), strings.ReplaceAll(name, "/", "//"))
	}
	return filepath.Match(pattern, name)
}

type unimplementedError struct {
	fs          Interface
	iface, mthd string
}

func (e *unimplementedError) Error() string {
	return fmt.Sprintf("%T doesn't implement filesystem.%v: can't use filesystem.%v", e.fs, e.iface, e.mthd)
}
