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

package gcs

import (
	"context"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"
)

func TestGCS_FilesystemNew(t *testing.T) {
	ctx := context.Background()
	path := "gs://tmp/"
	c, err := filesystem.New(ctx, path)
	if err != nil {
		t.Errorf("filesystem.New(ctx, %q) = %v, want nil", path, err)
	}
	if _, ok := c.(*fs); !ok {
		t.Errorf("filesystem.New(ctx, %q) type = %T, want *gcs.fs", path, c)
	}
	if err := c.Close(); err != nil {
		t.Errorf("c.Close() = %v, want nil", err)
	}
}

func TestGCS_direct(t *testing.T) {
	testGCS_direct(t)
}

func TestGCS_BillingProjectHookEnable(t *testing.T) {
	billingProject := "whatever"
	RequesterBillingProject(billingProject)
	_, err := hooks.RunInitHooks(context.Background())
	if err != nil {
		t.Errorf("error to init hooks = %v", err)
	}
	projectBillingHook := "beam:go:hook:filesystem:billingproject"
	projectBillingHookIsEnable, hookValue := hooks.IsEnabled(projectBillingHook)
	if !projectBillingHookIsEnable {
		t.Error("project billing hook isn't enable")
	}
	if hookValue[0] != billingProject {
		t.Errorf("projectBillingHook value wrong / want {%s} got {%s}", billingProject, hookValue[0])
	}

}

func testGCS_direct(t *testing.T) {
	ctx := context.Background()
	dirPath := "gs://beamgogcsfilesystemtest"
	filePath := dirPath + "/file.txt"

	server := createFakeGCSServer(t)
	c := &fs{client: server.Client()}

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

	listGlob := dirPath + "/*.txt"
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
	dirPath := "gs://beamgogcsfilesystemtest"
	filePath := dirPath + "/file.txt"

	server := createFakeGCSServer(t)
	c := &fs{client: server.Client()}

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
func TestLocal_listNoMatches(t *testing.T) {
	ctx := context.Background()
	server := createFakeGCSServer(t)
	c := &fs{client: server.Client()}

	glob := "gs://beamgogcsfilesystemtest/foo*"

	got, err := c.List(ctx, glob)
	if err != nil {
		t.Fatalf("List(%q) error = %v, want nil", glob, err)
	}

	want := []string(nil)
	if !cmp.Equal(got, want) {
		t.Errorf("List(%q) = %v, want %v", glob, got, want)
	}
}

func TestGCS_lastModified(t *testing.T) {
	ctx := context.Background()
	server := createFakeGCSServer(t)
	gcsFS := &fs{client: server.Client()}

	filePath := "gs://beamgogcsfilesystemtest/file.txt"

	t1 := time.Now()
	if err := filesystem.Write(ctx, gcsFS, filePath, []byte("")); err != nil {
		t.Fatalf("filesystem.Write(ctx, %q) error = %v, want nil", filePath, err)
	}
	t2 := time.Now()

	got, err := gcsFS.LastModified(ctx, filePath)
	if err != nil {
		t.Fatalf("LastModified(%q) error = %v, want nil", filePath, err)
	}

	if got.Before(t1) || got.After(t2) {
		t.Errorf("LastModified(%q) = %v, want in range [%v, %v]", filePath, got, t1, t2)
	}
}

func TestGCS_rename(t *testing.T) {
	ctx := context.Background()
	dirPath := "gs://beamgogcsfilesystemtest"
	filePath1 := dirPath + "/file1.txt"
	filePath2 := dirPath + "/file2.txt"

	server := createFakeGCSServer(t)
	c := &fs{client: server.Client()}

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
	listGlob := dirPath + "/*.txt"
	files, err := c.List(ctx, listGlob)
	if err != nil {
		t.Fatalf("List(%q) = %v, want nil", listGlob, err)
	}

	if len(files) != 1 || (len(files) == 1 && files[0] != filePath2) {
		t.Errorf("List(%v) = %v, want []string{%v}", listGlob, files, filePath2)
	}
}

func TestGCS_copy(t *testing.T) {
	ctx := context.Background()
	dirPath := "gs://beamgogcsfilesystemtest"
	filePath1 := dirPath + "/file1.txt"
	filePath2 := dirPath + "/file2.txt"

	server := createFakeGCSServer(t)
	c := &fs{client: server.Client()}

	data := []byte("a meaningless sequence of test words")
	if err := filesystem.Write(ctx, c, filePath1, data); err != nil {
		t.Fatalf("filesystem.Write(ctx, %q) == %v, want nil", filePath1, err)
	}

	if err := filesystem.Copy(ctx, c, filePath1, filePath2); err != nil {
		t.Fatalf("filesystem.Copy(ctx, %q) == %v, want nil", filePath2, err)
	}

	gotData, err := filesystem.Read(ctx, c, filePath2)
	if err != nil {
		t.Fatalf("filesystem.Read(ctx, %q) == %v, want nil", filePath2, err)
	}
	if got, want := string(data), string(gotData); got != want {
		t.Errorf("filesystem.Read() = %v, want %v", got, want)
	}

	// Reread old data to make sure that the file still exists.
	gotData, err = filesystem.Read(ctx, c, filePath1)
	if err != nil {
		t.Fatalf("filesystem.Read(ctx, %q) == %v, want nil", filePath1, err)
	}
	if got, want := string(data), string(gotData); got != want {
		t.Errorf("filesystem.Read() = %v, want %v", got, want)
	}

	listGlob := dirPath + "/*.txt"
	files, err := c.List(ctx, listGlob)
	if err != nil {
		t.Fatalf("List(%q) = %v, want nil", listGlob, err)
	}

	// Sort the strings to ensure stable output.
	sort.Strings(files)

	if files[0] != filePath1 {
		t.Errorf("List(%s) = %v, want []string{%s, %s}", listGlob, files, filePath1, filePath2)
	}
	if files[1] != filePath2 {
		t.Errorf("List(%s) = %v, want []string{%s, %s}", listGlob, files, filePath1, filePath2)
	}
}

func TestGlobToRegex(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		// Single * should NOT match / in object names
		{"*.txt", "file.txt", true},
		{"*.txt", "dir/file.txt", false},
		{"prefix*", "prefix123", true},
		{"prefix*", "prefix/subdir", false},

		// ** should match any characters including /
		{"**", "file.txt", true},
		{"**", "dir/file.txt", true},
		{"**", "dir/subdir/file.txt", true},
		{"prefix/**", "prefix/file.txt", true},
		{"prefix/**", "prefix/subdir/file.txt", true},
		{"**/file.txt", "file.txt", true},
		{"**/file.txt", "dir/file.txt", true},
		{"**/file.txt", "dir/subdir/file.txt", true},

		// Mixed patterns
		{"dir/*.txt", "dir/file.txt", true},
		{"dir/*.txt", "dir/subdir/file.txt", false},
		{"dir/**/*.txt", "dir/file.txt", true},
		{"dir/**/*.txt", "dir/subdir/file.txt", true},
		{"dir/**/file.txt", "dir/file.txt", true},
		{"dir/**/file.txt", "dir/a/b/c/file.txt", true},

		// ? should match any single character except /
		{"file?.txt", "file1.txt", true},
		{"file?.txt", "file12.txt", false},
		{"file?.txt", "file/.txt", false}, // ? should not cross /
		{"dir?file.txt", "dir/file.txt", false},

		// Character classes
		{"file[0-9].txt", "file1.txt", true},
		{"file[0-9].txt", "filea.txt", false},
		{"file[!0-9].txt", "filea.txt", true},
		{"file[!0-9].txt", "file1.txt", false},

		// Exact match (no wildcards)
		{"exact.txt", "exact.txt", true},
		{"exact.txt", "notexact.txt", false},

		// Regex special characters should be escaped
		{"file.txt", "file.txt", true},
		{"file.txt", "fileXtxt", false},
		{"file(1).txt", "file(1).txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.name, func(t *testing.T) {
			re, err := globToRegex(tt.pattern)
			if err != nil {
				t.Fatalf("globToRegex(%q) error = %v", tt.pattern, err)
			}
			got := re.MatchString(tt.name)
			if got != tt.want {
				t.Errorf("globToRegex(%q).MatchString(%q) = %v, want %v", tt.pattern, tt.name, got, tt.want)
			}
		})
	}
}

func TestGlobToRegex_errors(t *testing.T) {
	tests := []struct {
		pattern string
		wantErr string
	}{
		{"file[abc.txt", "unclosed '['"},
		{"[invalid", "unclosed '['"},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			_, err := globToRegex(tt.pattern)
			if err == nil {
				t.Errorf("globToRegex(%q) expected error containing %q, got nil", tt.pattern, tt.wantErr)
			} else if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("globToRegex(%q) error = %v, want error containing %q", tt.pattern, err, tt.wantErr)
			}
		})
	}
}

func TestGCS_listWithSlashesInObjectNames(t *testing.T) {
	ctx := context.Background()
	bucket := "beamgogcsfilesystemtest"
	dirPath := "gs://" + bucket

	// Create server with objects that have / in their names
	server := fakestorage.NewServer([]fakestorage.Object{
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: "file.txt"}, Content: []byte("")},
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: "dir/file.txt"}, Content: []byte("")},
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: "dir/subdir/file.txt"}, Content: []byte("")},
		{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: "other.txt"}, Content: []byte("")},
	})
	t.Cleanup(server.Stop)
	c := &fs{client: server.Client()}

	tests := []struct {
		glob string
		want []string
	}{
		// Single * should only match top-level files
		{dirPath + "/*.txt", []string{dirPath + "/file.txt", dirPath + "/other.txt"}},
		// ** should match all files recursively
		{dirPath + "/**", []string{
			dirPath + "/file.txt",
			dirPath + "/dir/file.txt",
			dirPath + "/dir/subdir/file.txt",
			dirPath + "/other.txt",
		}},
		// dir/* should only match immediate children
		{dirPath + "/dir/*", []string{dirPath + "/dir/file.txt"}},
		// dir/** should match all descendants
		{dirPath + "/dir/**", []string{
			dirPath + "/dir/file.txt",
			dirPath + "/dir/subdir/file.txt",
		}},
		// Deeply nested ** matching (core scenario from issue #38059)
		{dirPath + "/dir/subdir/**", []string{
			dirPath + "/dir/subdir/file.txt",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.glob, func(t *testing.T) {
			got, err := c.List(ctx, tt.glob)
			if err != nil {
				t.Fatalf("List(%q) error = %v", tt.glob, err)
			}

			sort.Strings(got)
			sort.Strings(tt.want)

			if !cmp.Equal(got, tt.want) {
				t.Errorf("List(%q) = %v, want %v", tt.glob, got, tt.want)
			}
		})
	}
}

func createFakeGCSServer(tb testing.TB) *fakestorage.Server {
	tb.Helper()

	server := fakestorage.NewServer([]fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "beamgogcsfilesystemtest",
				Name:       "stub",
			},
			Content: []byte(""),
		},
	})
	tb.Cleanup(server.Stop)
	return server
}
