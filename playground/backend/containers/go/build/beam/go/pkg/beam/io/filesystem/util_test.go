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
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
)

// A basic test implementation to validate the utility functions.
type testImpl struct {
	m                                           map[string][]byte
	listErr, openReadErr, openWriteErr, sizeErr error
	readerErr, readerCloseErr                   error
	writerErr, writerCloseErr                   error
}

func (fs *testImpl) List(ctx context.Context, glob string) ([]string, error) {
	return nil, fs.listErr
}

func (fs *testImpl) OpenRead(ctx context.Context, filename string) (io.ReadCloser, error) {
	if fs.openReadErr != nil {
		return nil, fs.openReadErr
	}
	if v, ok := fs.m[filename]; ok {
		return errReadCloser{Reader: bytes.NewReader(v), closeErr: fs.readerCloseErr, readErr: fs.readerErr}, nil
	}
	return nil, nil
}

type errReadCloser struct {
	io.Reader
	closeErr, readErr error
}

func (e errReadCloser) Close() error { return e.closeErr }

func (e errReadCloser) Read(p []byte) (n int, err error) {
	if e.readErr != nil {
		return 0, e.readErr
	}
	return e.Reader.Read(p)
}

func (fs *testImpl) OpenWrite(ctx context.Context, filename string) (io.WriteCloser, error) {
	if fs.openWriteErr != nil {
		return nil, fs.openWriteErr
	}
	var buf bytes.Buffer
	return errWriteCloser{
		Writer: &buf,
		closeFn: func() {
			fs.m[filename] = buf.Bytes()
		},
		closeErr: fs.writerCloseErr,
		writeErr: fs.writerErr,
	}, nil
}

type errWriteCloser struct {
	io.Writer
	closeFn            func()
	closeErr, writeErr error
}

func (e errWriteCloser) Close() error {
	if e.closeErr != nil {
		return e.closeErr
	}
	e.closeFn()
	return nil
}

func (e errWriteCloser) Write(p []byte) (n int, err error) {
	if e.writeErr != nil {
		return 0, e.writeErr
	}
	return e.Writer.Write(p)
}

func (fs *testImpl) Size(ctx context.Context, filename string) (int64, error) {
	return -1, fs.sizeErr
}

func (fs *testImpl) Close() error {
	return nil
}

func newTestImpl() *testImpl {
	return &testImpl{m: map[string][]byte{}}
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	filename := "filename"
	data := []byte("arbitrary data")
	setup := func() *testImpl {
		fs := newTestImpl()
		fs.m[filename] = data
		return fs
	}

	t.Run("happypath", func(t *testing.T) {
		fs := setup()
		gotData, err := Read(ctx, fs, filename)
		if err != nil {
			t.Errorf("error on Read() = %v, want nil", err)
		}
		if got, want := string(gotData), string(data); got != want {
			t.Errorf("Read() = %v, want %v", got, want)
		}
	})
	t.Run("openError", func(t *testing.T) {
		fs := setup()
		fs.openReadErr = fmt.Errorf("bad open")
		_, err := Read(ctx, fs, filename)
		if got, want := err, fs.openReadErr; got != want {
			t.Errorf("Read() = %v, want %v", got, want)
		}
	})
	t.Run("closeError", func(t *testing.T) {
		fs := setup()
		fs.readerCloseErr = fmt.Errorf("bad read close")
		gotData, err := Read(ctx, fs, filename)
		// close errors are ignored for Read.
		if err != nil {
			t.Errorf("error on Read() = %v, want nil", err)
		}
		if got, want := string(gotData), string(data); got != want {
			t.Errorf("Read() = %v, want %v", got, want)
		}
	})
	t.Run("readingError", func(t *testing.T) {
		fs := setup()
		fs.readerErr = fmt.Errorf("bad read")
		_, err := Read(ctx, fs, filename)
		if got, want := err, fs.readerErr; got != want {
			t.Errorf("Read() = %v, want %v", got, want)
		}
	})
}

func TestWrite(t *testing.T) {
	ctx := context.Background()
	filename := "filename"
	data := []byte("arbitrary data")
	setup := newTestImpl
	t.Run("happypath", func(t *testing.T) {
		fs := setup()
		if err := Write(ctx, fs, filename, data); err != nil {
			t.Errorf("error on Write() = %v, want nil", err)
		}
		gotData := fs.m[filename]
		if got, want := string(gotData), string(data); got != want {
			t.Errorf("Write() = %v, want %v", got, want)
		}
	})
	t.Run("openError", func(t *testing.T) {
		fs := setup()
		fs.openWriteErr = fmt.Errorf("bad open")
		err := Write(ctx, fs, filename, data)
		if got, want := err, fs.openWriteErr; got != want {
			t.Errorf("Write() = %v, want %v", got, want)
		}
	})
	t.Run("closeError", func(t *testing.T) {
		fs := setup()
		fs.writerCloseErr = fmt.Errorf("bad write close")
		err := Write(ctx, fs, filename, data)
		if got, want := err, fs.writerCloseErr; got != want {
			t.Errorf("Write() = %v, want %v", got, want)
		}
	})
	t.Run("writingError", func(t *testing.T) {
		fs := setup()
		fs.writerErr = fmt.Errorf("bad write")
		err := Write(ctx, fs, filename, data)
		if got, want := err, fs.writerErr; got != want {
			t.Errorf("Write() = %v, want %v", got, want)
		}
	})
}

type testCopyImpl struct {
	*testImpl
	copyErr error
}

func copyImpl(fs *testImpl) *testCopyImpl {
	return &testCopyImpl{testImpl: fs}
}

func (fs *testCopyImpl) Copy(ctx context.Context, oldpath, newpath string) error {
	if fs.copyErr != nil {
		return fs.copyErr
	}
	fs.m[newpath] = fs.m[oldpath]
	return nil
}

var _ Copier = (*testCopyImpl)(nil)

func TestCopy(t *testing.T) {
	ctx := context.Background()
	filename1 := "filename1"
	filename2 := "filename2"
	data := []byte("arbitrary data")
	setup := func() *testImpl {
		fs := newTestImpl()
		fs.m[filename1] = data
		return fs
	}
	// Test if the Copier interface is doing it's job.
	t.Run("happypath_copier", func(t *testing.T) {
		fs := copyImpl(setup())
		if err := Copy(ctx, fs, filename1, filename2); err != nil {
			t.Errorf("error on Copy() = %v, want nil", err)
		}
		gotData := fs.m[filename2]
		if got, want := string(gotData), string(data); got != want {
			t.Errorf("Copy(%q,%q) data = %v, want %v", filename1, filename2, got, want)
		}
		if _, ok := fs.m[filename1]; !ok {
			t.Errorf("Copy(%q,%q) removed old path: %v, should have been kept.", filename1, filename2, filename1)
		}
	})
	t.Run("copyError_copier", func(t *testing.T) {
		fs := copyImpl(setup())
		fs.copyErr = fmt.Errorf("bad copy")
		if got, want := Copy(ctx, fs, filename1, filename2), fs.copyErr; got != want {
			t.Errorf("error on Copy() = %v, want %v", got, want)
		}
	})
	// Test that the backup worked.
	t.Run("happypath", func(t *testing.T) {
		fs := setup()
		if err := Copy(ctx, fs, filename1, filename2); err != nil {
			t.Errorf("error on Copy(%q, %q) = %v, want nil", filename1, filename2, err)
		}
		gotData := fs.m[filename2]
		if got, want := string(gotData), string(data); got != want {
			t.Errorf("Copy(%q, %q) data = %v, want %v", filename1, filename2, got, want)
		}
		if _, ok := fs.m[filename1]; !ok {
			t.Errorf("Copy(%q,%q) removed old path: %v, should have been kept.", filename1, filename2, filename1)
		}
	})
	t.Run("openWriteError", func(t *testing.T) {
		fs := setup()
		fs.openWriteErr = fmt.Errorf("bad write")
		if got, want := Copy(ctx, fs, filename1, filename2), fs.openWriteErr; got != want {
			t.Errorf("wanted error on Copy() = %v, want %v", got, want)
		}
	})
	t.Run("openReadError", func(t *testing.T) {
		fs := setup()
		fs.openReadErr = fmt.Errorf("bad read")
		if got, want := Copy(ctx, fs, filename1, filename2), fs.openReadErr; got != want {
			t.Errorf("wanted error on Copy() = %v, want %v", got, want)
		}
	})
	t.Run("writeError", func(t *testing.T) {
		fs := setup()
		fs.writerErr = fmt.Errorf("bad write")
		if got, want := Copy(ctx, fs, filename1, filename2), fs.writerErr; got != want {
			t.Errorf("wanted error on Copy() = %v, want %v", got, want)
		}
	})
	t.Run("readError", func(t *testing.T) {
		fs := setup()
		fs.readerErr = fmt.Errorf("bad read")
		if got, want := Copy(ctx, fs, filename1, filename2), fs.readerErr; got != want {
			t.Errorf("wanted error on Copy() = %v, want %v", got, want)
		}
	})
}

type testRenameImpl struct {
	*testImpl
	renameErr error
}

func renameImpl(fs *testImpl) *testRenameImpl {
	return &testRenameImpl{testImpl: fs}
}

func (fs *testRenameImpl) Rename(ctx context.Context, oldpath, newpath string) error {
	if fs.renameErr != nil {
		return fs.renameErr
	}
	fs.m[newpath] = fs.m[oldpath]
	delete(fs.m, oldpath)
	return nil
}

var _ Renamer = (*testRenameImpl)(nil)

type testRemoveImpl struct {
	*testImpl
	removeErr error
}

func removeImpl(fs *testImpl) *testRemoveImpl {
	return &testRemoveImpl{testImpl: fs}
}

func (fs *testRemoveImpl) Remove(ctx context.Context, filename string) error {
	if fs.removeErr != nil {
		return fs.removeErr
	}
	delete(fs.m, filename)
	return nil
}

var _ Remover = (*testRemoveImpl)(nil)

func TestRename(t *testing.T) {
	ctx := context.Background()
	filename1 := "filename1"
	filename2 := "filename2"
	data := []byte("arbitrary data")
	setup := func() *testImpl {
		fs := newTestImpl()
		fs.m[filename1] = data
		return fs
	}
	// Test if the Renamer interface is doing it's job.
	t.Run("happypath_renamer", func(t *testing.T) {
		fs := renameImpl(setup())
		if err := Rename(ctx, fs, filename1, filename2); err != nil {
			t.Errorf("error on Rename() = %v, want nil", err)
		}
		gotData := fs.m[filename2]
		if got, want := string(gotData), string(data); got != want {
			t.Errorf("Rename(%q,%q) data = %v, want %v", filename1, filename2, got, want)
		}
		if _, ok := fs.m[filename1]; ok {
			t.Errorf("Rename(%q,%q) did not remove old path: %v", filename1, filename2, filename1)
		}
	})
	t.Run("renameError_renamer", func(t *testing.T) {
		fs := renameImpl(setup())
		fs.renameErr = fmt.Errorf("rename error")
		if got, want := Rename(ctx, fs, filename1, filename2), fs.renameErr; got != want {
			t.Errorf("error on Rename() = %v, want %v", got, want)
		}
	})
	t.Run("removerUnimplemented", func(t *testing.T) {
		fs := setup()
		err := Rename(ctx, fs, filename1, filename2)
		if _, ok := err.(*unimplementedError); !ok {
			t.Errorf("Rename(non-remover) = want unimplementedError, got = %v", err)
		}
	})
	t.Run("happypath", func(t *testing.T) {
		fs := removeImpl(setup())
		if err := Rename(ctx, fs, filename1, filename2); err != nil {
			t.Errorf("error on Rename() = %v, want nil", err)
		}
		gotData := fs.m[filename2]
		if got, want := string(gotData), string(data); got != want {
			t.Errorf("Rename(%q,%q) data = %v, want %v", filename1, filename2, got, want)
		}
		if _, ok := fs.m[filename1]; ok {
			t.Errorf("Rename(%q,%q) did not remove old path: %v", filename1, filename2, filename1)
		}
	})
	t.Run("copyError", func(t *testing.T) {
		fs := removeImpl(setup())
		fs.openReadErr = fmt.Errorf("bad copy")
		if got, want := Rename(ctx, fs, filename1, filename2), fs.openReadErr; got != want {
			t.Errorf("error on Rename() = %v, want %v", got, want)
		}
	})
	t.Run("removeError", func(t *testing.T) {
		fs := removeImpl(setup())
		fs.removeErr = fmt.Errorf("bad remove")
		if got, want := Rename(ctx, fs, filename1, filename2), fs.removeErr; got != want {
			t.Errorf("error on Rename() = %v, want %v", got, want)
		}
	})
}
