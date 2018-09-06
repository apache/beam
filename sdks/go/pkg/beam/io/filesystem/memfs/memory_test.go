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
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem"
)

// TestReadWrite tests that read and write from the memory filesystem
// works as expected.
func TestReadWrite(t *testing.T) {
	ctx := context.Background()
	fs := New(ctx)

	if err := filesystem.Write(ctx, fs, "foo", []byte("foo")); err != nil {
		t.Error(err)
	}
	if err := filesystem.Write(ctx, fs, "bar", []byte("bar")); err != nil {
		t.Error(err)
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
