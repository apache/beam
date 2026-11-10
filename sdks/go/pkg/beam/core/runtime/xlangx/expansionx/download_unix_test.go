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

//go:build unix

package expansionx

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestExtractJar_NoHandleLeak extracts an archive with more file entries than
// the process is permitted to have open at once. If extractJar held every
// entry's handles until it returned, this would fail with EMFILE.
func TestExtractJar_NoHandleLeak(t *testing.T) {
	const limit = 64
	var orig syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &orig); err != nil {
		t.Skipf("cannot read RLIMIT_NOFILE: %v", err)
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: limit, Max: orig.Max}); err != nil {
		t.Skipf("cannot lower RLIMIT_NOFILE: %v", err)
	}
	t.Cleanup(func() { syscall.Setrlimit(syscall.RLIMIT_NOFILE, &orig) })

	root := t.TempDir()
	dest := filepath.Join(root, "extract")
	jar := filepath.Join(root, "many.jar")
	entries := make([]zipEntry, 0, 4*limit)
	for i := 0; i < 4*limit; i++ {
		entries = append(entries, zipEntry{fmt.Sprintf("file%d.txt", i), "x"})
	}
	writeTestZip(t, jar, entries)

	if err := extractJar(jar, dest); err != nil {
		t.Fatalf("extractJar failed under a low file descriptor limit: %v", err)
	}
	got, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(entries) {
		t.Errorf("extracted %d files, want %d", len(got), len(entries))
	}
}
