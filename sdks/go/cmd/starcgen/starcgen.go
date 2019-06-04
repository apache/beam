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
//
// The generated type assertion shims have much better performance than the default
// reflection based shims used by beam. Reflection is convenient for development,
// but is an unnecessary expense on pipeline performance.
//
// Using This Tool
//
// This tool is intended for use with `go generate`. The recommended convention
// putting the types and functions used in a separate package from pipeline construction.
// Then, the tool can be used as follows:
//
//   //go:generate go install github.com/apache/beam/sdks/go/cmd/starcgen
//   //go:generate starcgen --package=<mypackagename>
//   //go:generate go fmt
//
// This will generate registrations and shim types for all types and functions
// in the package, in a file `<mypackagename>.shims.go`.
//
// Alternatively, it's possible to specify the specific input files and identifiers within
// the package for generation.
//
//   //go:generate go install github.com/apache/beam/sdks/go/cmd/starcgen
//   //go:generate starcgen --package=<mypackagename> --inputs=foo.go --identifiers=myFn,myStructFn --output=custom.shims.go
//   //go:generate go fmt
//
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
	inputs      = flag.String("inputs", "", "comma separated list of file with types for which to create shims")
	intendedPkg = flag.String("package", "", "a filter on input go files. Required if inputs unset.")
	output      = flag.String("output", "", "output file with types to create")
	ids         = flag.String("identifiers", "", "comma separated list of package local identifiers for which to generate code")
	debug       = flag.Bool("debug", false, "print out a debugging header in the shim file to help diagnose errors")
)

// Generate takes the typechecked inputs, and generates the shim file for the relevant
// identifiers.
func Generate(w io.Writer, filename, pkg string, ids []string, fset *token.FileSet, files []*ast.File) error {
	e := starcgenx.NewExtractor(pkg)
	e.Ids = ids
	e.Debug = *debug

	// Importing from source should work in most cases.
	imp := importer.For("source", nil)
	if err := e.FromAsts(imp, fset, files); err != nil {
		// Always print out the debugging info to the file.
		if _, errw := w.Write(e.Bytes()); errw != nil {
			return fmt.Errorf("error writing debug data to file after err %v:%v", err, errw)
		}
		err = fmt.Errorf("error extracting from asts: %v", err)
		e.Printf("%v", err)
		return err
	}
	data := e.Generate(filename)
	if err := write(w, []byte(license)); err != nil {
		return err
	}
	return write(w, data)
}

func write(w io.Writer, data []byte) error {
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

	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	var ipts []string
	// If inputs are empty, parse all go files in the local directory.
	if len(*inputs) == 0 {
		if *intendedPkg == "" {
			log.Fatal("--package flag is required to be set when --inputs unset")
		}
		globbed, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			log.Fatal(err)
		}
		ipts = globbed
	} else {
		ipts = strings.Split(*inputs, ",")
	}

	// Get an output file for pre-processing if necessary.
	if *output == "" && *intendedPkg != "" {
		*output = *intendedPkg + ".shims.go"
	}

	outputBase := filepath.Base(*output)

	fset := token.NewFileSet()
	var fs []*ast.File
	pkg := *intendedPkg
	for _, i := range ipts {
		// Ignore the existing shim file when re-generating.
		if strings.HasSuffix(i, outputBase) {
			continue
		}

		f, err := parser.ParseFile(fset, i, nil, 0)
		if err != nil {
			log.Fatal(err) // parse error
		}
		if pkg == "" {
			pkg = f.Name.Name
		} else if pkg != f.Name.Name {
			// If we've set an intended package, just filter files outside that package.
			// eg. for pkg_tests in the same directory.
			if *intendedPkg != "" {
				continue
			}
			log.Fatalf("Input file %v has mismatched package path, got %q, want %q", i, f.Name.Name, pkg)
		}
		fs = append(fs, f)
	}
	if pkg == "" {
		log.Fatalf("No package detected in input files: %v", inputs)
	}

	f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("error opening %q: %v", *output, err)
	}
	splitIds := make([]string, 0) // If no ids are specified, we should pass an empty slice.
	if len(*ids) > 0 {
		splitIds = strings.Split(*ids, ",")
	}
	if err := Generate(f, *output, pkg, splitIds, fset, fs); err != nil {
		log.Fatal(err)
	}
}

const license = `// Licensed to the Apache Software Foundation (ASF) under one or more
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

`
