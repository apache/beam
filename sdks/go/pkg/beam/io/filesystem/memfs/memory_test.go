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

package memfs

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/google/go-cmp/cmp"
)

// TestReadWrite tests that read and write from the memory filesystem
// works as expected.
func TestReadWrite(t *testing.T) {
	ctx := context.Background()
	fs := &fs{m: make(map[string][]byte)}

	if err := filesystem.Write(ctx, fs, "foo", []byte("foo")); err != nil {
		t.Fatalf("Write(%q) error = %v", "foo", err)
	}
	if err := filesystem.Write(ctx, fs, "bar", []byte("bar")); err != nil {
		t.Fatalf("Write(%q) error = %v", "bar", err)
	}

	foo, err := filesystem.Read(ctx, fs, "foo")
	if err != nil {
		t.Errorf("Read(foo) failed: %v", err)
	}
	if string(foo) != "foo" {
		t.Errorf("Read(foo) = %v, want foo", string(foo))
	}

	bar, err := filesystem.Read(ctx, fs, "bar")
	if err != nil {
		t.Errorf("Read(bar) failed: %v", err)
	}
	if string(bar) != "bar" {
		t.Errorf("Read(bar) = %v, want bar", string(bar))
	}

	if _, err := filesystem.Read(ctx, fs, "baz"); err != os.ErrNotExist {
		t.Errorf("Read(baz) = %v, want os.ErrNotExist", err)
	}
}

// TestDirectWrite tests that that direct writes to the memory filesystem works.
func TestDirectWrite(t *testing.T) {
	ctx := context.Background()
	fs := New(ctx)

	Write("foo2", []byte("foo"))

	foo, err := filesystem.Read(ctx, fs, "foo2")
	if err != nil {
		t.Errorf("Read(foo2) failed: %v", err)
	}
	if string(foo) != "foo" {
		t.Errorf("Read(foo2) = %v, want foo", string(foo))
	}
}

func TestSize(t *testing.T) {
	ctx := context.Background()
	fs := &fs{m: make(map[string][]byte)}

	names := []string{"foo", "foobar"}
	for _, name := range names {
		file := []byte(name)
		if err := filesystem.Write(ctx, fs, name, file); err != nil {
			t.Fatalf("Write(%q) error = %v", name, err)
		}
		size, err := fs.Size(ctx, name)
		if err != nil {
			t.Errorf("Size(%v) failed: %v", name, err)
		}
		if size != int64(len(name)) {
			t.Errorf("Size(%v) incorrect: got %v, want %v", name, size, len(name))
		}
	}
}

func TestList(t *testing.T) {
	ctx := context.Background()
	fs := &fs{m: make(map[string][]byte)}

	names := []string{"fizzbuzz", "foo", "foobar", "baz", "bazfoo"}
	for _, name := range names {
		file := []byte(name)
		if err := filesystem.Write(ctx, fs, name, file); err != nil {
			t.Fatalf("Write(%q) error = %v", name, err)
		}
	}
	glob := "memfs://foo.*"
	got, err := fs.List(ctx, glob)
	if err != nil {
		t.Errorf("error List(%q) = %v", glob, err)
	}

	want := []string{"memfs://foo", "memfs://foobar"}
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("List(%q) = %v, want %v", glob, got, want)
	}
}

func TestRemove(t *testing.T) {
	ctx := context.Background()
	fs := &fs{m: make(map[string][]byte)}

	names := []string{"fizzbuzz", "foobar", "bazfoo"}
	for _, name := range names {
		file := []byte(name)
		if err := filesystem.Write(ctx, fs, name, file); err != nil {
			t.Fatalf("Write(%q) error = %v", name, err)
		}
	}
	toremove := "memfs://foobar"
	if err := fs.Remove(ctx, toremove); err != nil {
		t.Errorf("error Remove(%q) = %v", toremove, err)
	}

	got, err := fs.List(ctx, ".*")
	if err != nil {
		t.Errorf("error List(\".*\") = %v", err)
	}

	want := []string{"memfs://bazfoo", "memfs://fizzbuzz"}
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("After Remove fs.List(\".*\") = %v, want %v", got, want)
	}
}

func TestCopy(t *testing.T) {
	ctx := context.Background()
	fs := &fs{m: make(map[string][]byte)}

	oldpath := "fizzbuzz"
	file := []byte(oldpath)
	if err := filesystem.Write(ctx, fs, oldpath, file); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := filesystem.Copy(ctx, fs, "memfs://fizzbuzz", "memfs://fizzbang"); err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	glob := "memfs://fizz.*"
	got, err := fs.List(ctx, glob)
	if err != nil {
		t.Errorf("error List(%q) = %v", glob, err)
	}

	want := []string{"memfs://fizzbang", "memfs://fizzbuzz"}
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("List(%q) = %v, want %v", glob, got, want)
	}

	read, err := filesystem.Read(ctx, fs, "memfs://fizzbang")
	if err != nil {
		t.Fatalf("Read error = %v", err)
	}
	if got, want := string(read), oldpath; got != want {
		t.Errorf("Copy() error got %q, want %q", got, want)
	}
}

func TestRename(t *testing.T) {
	ctx := context.Background()
	fs := &fs{m: make(map[string][]byte)}

	oldpath := "fizzbuzz"
	file := []byte(oldpath)
	if err := filesystem.Write(ctx, fs, oldpath, file); err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Rename(ctx, fs, "memfs://fizzbuzz", "memfs://fizzbang"); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}
	glob := "memfs://fizz.*"
	got, err := fs.List(ctx, glob)
	if err != nil {
		t.Errorf("error List(%q) = %v", glob, err)
	}

	want := []string{"memfs://fizzbang"}
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("List(%q) = %v, want %v", glob, got, want)
	}

	read, err := filesystem.Read(ctx, fs, "memfs://fizzbang")
	if err != nil {
		t.Fatalf("Read error = %v", err)
	}
	if got, want := string(read), oldpath; got != want {
		t.Errorf("Rename() error got %q, want %q", got, want)
	}
}

// TestWriteAffectsActiveReads tests that that direct writes to the memory filesystem works.
func TestWriteAffectsActiveReads(t *testing.T) {
	ctx := context.Background()
	fs := New(ctx)

	writer1, err := fs.OpenWrite(ctx, "memfs://abc")
	if err != nil {
		t.Fatal(err)
	}

	reader1, err := fs.OpenRead(ctx, "memfs://abc")
	if err != nil {
		t.Fatalf("OpenRead should succeed after OpenWrite for the same path, got err = %v", err)
	}

	writer1.Write([]byte("1234"))

	if got, err := ioutilx.ReadN(reader1, 4); string(got) != "1234" {
		t.Fatalf("read %q after writing 1234 to file, wanted 1234; err = %v", string(got), err)
	}
	if _, err := reader1.Read(make([]byte, 10)); err != io.EOF {
		t.Fatalf("got err = %v, wanted io.EOF", err)
	}

	writer1.Write([]byte("5678"))

	if got, err := ioutilx.ReadN(reader1, 4); string(got) != "5678" {
		t.Fatalf("read %q after writing 1234 to file, wanted 1234; err = %v", string(got), err)
	}
	if _, err := reader1.Read(make([]byte, 10)); err != io.EOF {
		t.Fatalf("got err = %v, wanted io.EOF", err)
	}
}

func TestReadModified(t *testing.T) {
	ctx := context.Background()
	fs := New(ctx)

	Write("abc", []byte("hello world"))

	reader, err := fs.OpenRead(ctx, "memfs://abc")
	if err != nil {
		t.Fatalf("OpenRead should succeed after OpenWrite for the same path, got err = %v", err)
	}

	want := "hello"
	if got, err := ioutilx.ReadN(reader, 5); string(got) != "hello" {
		t.Fatalf("read got %q, want %q; err = %v", string(got), want, err)
	}

	Write("abc", []byte("hello walrus, ocean creature"))

	if got, err := ioutilx.ReadN(reader, 7); string(got) != " walrus" {
		t.Fatalf("read got %q, want %q; err = %v", string(got), want, err)
	}

	Write("abc", []byte("hello"))

	if _, err := ioutilx.ReadN(reader, 1); err != io.EOF {
		t.Fatalf("wanted EOF after reading from past EOF position, got err = %v", err)
	}
}

func TestSeek(t *testing.T) {
	ctx := context.Background()
	fs := New(ctx)

	for _, tt := range []struct {
		name     string
		contents map[string]string
		filename string
		offset   int64
		whence   int
		wantPos  int64
		wantErr  error
		want     string
	}{
		{
			name:     "simple",
			contents: map[string]string{"abc": "12345"},
			filename: "abc",
			offset:   -3,
			whence:   io.SeekEnd,
			wantPos:  2,
			want:     "345",
		},
		{
			name:     "negative",
			contents: map[string]string{"abc": "12345"},
			filename: "abc",
			offset:   -3,
			whence:   io.SeekCurrent,
			wantErr:  errBadSeek,
		},
		{
			name:     "beyond EOF",
			contents: map[string]string{"abc": "12345"},
			filename: "abc",
			offset:   50,
			whence:   io.SeekStart,
			wantErr:  errBadSeek,
		},
		{
			name:     "EOF exactly",
			contents: map[string]string{"abc": "12"},
			filename: "abc",
			offset:   2,
			whence:   io.SeekStart,
			wantPos:  2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reset()
			for k, v := range tt.contents {
				Write(k, []byte(v))
			}

			reader, err := fs.OpenRead(ctx, tt.filename)
			if err != nil {
				t.Fatalf("OpenRead should succeed after OpenWrite for the same path, got err = %v", err)
			}
			seeker, ok := reader.(io.Seeker)
			if !ok {
				t.Fatalf("expected memfs OpenRead to return a seeker")
			}
			gotPos, err := seeker.Seek(tt.offset, tt.whence)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got seek error = %v, wantErr = %v", err, tt.wantErr)
			}
			if gotPos != tt.wantPos {
				t.Fatalf("got seek result %d, want %d", gotPos, tt.wantPos)
			}
			want := []byte(tt.want)
			if len(want) > 0 {
				got, err := ioutilx.ReadN(reader, len(want))
				if err != nil {
					t.Fatalf("error reading after seek: %v", err)
				}
				if string(got) != tt.want {
					t.Fatalf("read after seek got %q, want %q", string(got), want)
				}
			}
		})
	}
}

func reset() {
	instance.m = map[string][]byte{}
}
