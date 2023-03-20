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

package symtab

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestSym2Addr builds and runs this test program.
const testprog = `
package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/symtab"
)

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}

func main() {
	syms, err := symtab.New(os.Args[0])
	if err != nil {
		die("%s: could not read symbols: %v", os.Args[0], err)
	}

	symPC, err := syms.Sym2Addr("main.main")
	if err != nil {
		die("Sym2Addr(%q) failed: %v", "main.main", err)
	}

	runtimePC := fnaddr()
	if symPC != runtimePC {
		die("PC from symbol table %x != runtime PC %x", symPC, runtimePC)
	}
}

// fnaddr returns the entry address of its caller.
func fnaddr() uintptr {
	var pcs [2]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	frame, _ := frames.Next()
	return frame.Func.Entry()
}
`

func TestSym2Addr(t *testing.T) {
	f, err := os.CreateTemp("", "TestSym2Addr*.go")
	if err != nil {
		t.Fatal(err)
	}

	fname := f.Name()
	defer os.Remove(fname)

	if _, err := f.WriteString(testprog); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	bin := strings.TrimSuffix(fname, ".go")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	defer os.Remove(bin)

	gotool := filepath.Join(runtime.GOROOT(), "bin", "go")

	for _, arg := range []string{"-buildmode=exe", "-buildmode=pie"} {
		for _, strip := range []bool{false, true} {
			args := []string{
				gotool,
				"build",
				"-o",
				bin,
				arg,
			}
			if strip {
				args = append(args, "-ldflags=-w")
			}
			args = append(args, fname)
			if out, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
				t.Logf("%s", out)
				t.Errorf("%v failed: %v", args, err)
				continue
			}

			if out, err := exec.Command(bin).CombinedOutput(); err != nil {
				t.Logf("%s", out)
				t.Errorf("test program built with %v failed: %v", args, err)
			}
		}
	}
}
