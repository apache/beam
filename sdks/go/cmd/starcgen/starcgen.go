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

// starcgen is a tool to generate specialized type assertion shims to be
// used in Apache Beam Go SDK pipelines instead of the default reflection shim.
// This is done through static analysis of go sources for the package in question.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/util/starcgenx"
)

var (
	inputs = flag.String("inputs", "", "comma separated list of file with types to create")
	output = flag.String("output", "", "output file with types to create")
	ids    = flag.String("identifiers", "", "comma separated list of package local identifiers for which to generate code")
)

// Generate takes in the ASTs of files and
func Generate(w io.Writer, filename, pkg string, ids []string, fset *token.FileSet, files []*ast.File) error {
	e := starcgenx.NewExtractor(pkg)
	e.Ids = ids

	// Importing from source should work in most cases.
	imp := importer.For("source", nil)
	if err := e.FromAsts(imp, fset, files); err != nil {
		// Always print out the debugging info to the file.
		if _, errw := w.Write(e.Bytes()); errw != nil {
			return fmt.Errorf("error writing debug data to file after err %v:%v", err, errw)
		}
		return fmt.Errorf("error extracting from asts: %v", err)
	}

	e.Print("*/\n")
	data := e.Generate(filename)
	n, err := w.Write(data)
	if err != nil && n < len(data) {
		return fmt.Errorf("short write of data got %d, want %d", n, len(data))
	}
	return err
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %v [options] --inputs=<comma separated of go files>\n", filepath.Base(os.Args[0]))
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	log.SetFlags(log.Lshortfile)
	log.SetPrefix("starcgen: ")

	ipts := strings.Split(*inputs, ",")
	fset := token.NewFileSet()
	var fs []*ast.File
	var pkg string

	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	for _, i := range ipts {
		f, err := parser.ParseFile(fset, i, nil, 0)
		if err != nil {
			err1 := err
			f, err = parser.ParseFile(fset, filepath.Join(dir, i), nil, 0)
			if err != nil {
				log.Print(err1)
				log.Fatal(err) // parse error
			}
		}

		if pkg == "" {
			pkg = f.Name.Name
		} else if pkg != f.Name.Name {
			log.Fatalf("Input file %v has mismatched package path, got %q, want %q", i, f.Name.Name, pkg)
		}
		fs = append(fs, f)
	}
	if pkg == "" {
		log.Fatalf("No package detected in input files: %v", inputs)
	}

	if *output == "" {
		name := pkg
		if len(ipts) == 1 {
			name = filepath.Base(ipts[0])
			if index := strings.Index(name, "."); index > 0 {
				name = name[:index]
			}
		}
		*output = filepath.Join(filepath.Dir(ipts[0]), name+".shims.go")
	}

	f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	if err := Generate(f, *output, pkg, strings.Split(*ids, ","), fset, fs); err != nil {
		log.Fatal(err)
	}
}
