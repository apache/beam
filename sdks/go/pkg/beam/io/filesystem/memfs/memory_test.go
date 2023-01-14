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
	"os"
	"path/filepath"
	"testing"

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
	glob := "memfs://foo*"
	got, err := fs.List(ctx, glob)
	if err != nil {
		t.Errorf("error List(%q) = %v", glob, err)
	}

	want := []string{"memfs://foo", "memfs://foobar"}
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("List(%q) = %v, want %v", glob, got, want)
	}
}

func TestListTable(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name    string
		files   []string
		pattern string
		want    []string
		wantErr bool
	}{
		{
			name:    "foo-star",
			files:   []string{"fizzbuzz", "foo", "foobar", "baz", "bazfoo"},
			pattern: "memfs://foo*",
			want:    []string{"memfs://foo", "memfs://foobar"},
		},
		{
			name:    "foo-star-missing-memfs-prefix",
			files:   []string{"fizzbuzz", "foo", "foobar", "baz", "bazfoo"},
			pattern: "foo*",
			want:    []string{"memfs://foo", "memfs://foobar"},
		},
		{
			name:    "bad-pattern",
			files:   []string{"fizzbuzz", "foo", "foobar", "baz", "bazfoo"},
			pattern: "foo[",
			wantErr: true, // invalid glob syntax
		},
		{
			name:    "foo",
			files:   []string{"fizzbuzz", "foo", "foobar", "baz", "bazfoo"},
			pattern: "memfs://foo",
			want:    []string{"memfs://foo"},
		},
		{
			name: "dirs",
			files: []string{
				"fizzbuzz",
				filepath.Join("xyz", "12"),
				filepath.Join("xyz", "1234"),
				filepath.Join("xyz", "1235"),
				"foobar",
				"baz",
				"bazfoo",
			},
			pattern: "memfs://xyz/123*",
			want: []string{
				"memfs://" + filepath.Join("xyz", "1234"),
				"memfs://" + filepath.Join("xyz", "1235"),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fs := &fs{m: make(map[string][]byte)}

			for _, name := range tt.files {
				if err := filesystem.Write(ctx, fs, name, []byte("contents")); err != nil {
					t.Fatalf("Write(%q) error = %v", name, err)
				}
			}
			got, err := fs.List(ctx, tt.pattern)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Errorf("List(%q) got error %v, wantErr = %v", tt.pattern, err, tt.wantErr)
			}
			if err != nil {
				return
			}

			want := tt.want
			if d := cmp.Diff(want, got); d != "" {
				t.Errorf("List(%q) resulted in unexpected diff (-want, +got):\n  %s", tt.pattern, d)
			}
		})
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

	got, err := fs.List(ctx, "memfs://*")
	if err != nil {
		t.Errorf("error List(\"*\") = %v", err)
	}

	want := []string{"memfs://bazfoo", "memfs://fizzbuzz"}
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("After Remove fs.List(\"*\") = %v, want %v", got, want)
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
	glob := "memfs://fizz*"
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
	glob := "memfs://fizz*"
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
