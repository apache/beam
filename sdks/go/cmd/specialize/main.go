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

// specialize is a low-level tool to generate type-specialized code. It is a
// convenience wrapper over text/template suitable for go generate. Unlike
// many other template tools, it does not parse Go code and allows use of
// text/template control within the template itself.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	noheader = flag.Bool("noheader", false, "Omit auto-generated header")
	pack     = flag.String("package", "", "Package name (optional)")
	imports  = flag.String("imports", "", "Comma-separated list of extra imports (optional)")

	x = flag.String("x", "", "Comma-separated list of X types (optional)")
	y = flag.String("y", "", "Comma-separated list of Y types (optional)")
	z = flag.String("z", "", "Comma-separated list of Z types (optional)")

	input  = flag.String("input", "", "Template file.")
	output = flag.String("output", "", "Filename for generated code. If not provided, a file next to the input is generated.")
)

// Top is the top-level struct to be passed to the template.
type Top struct {
	// Name is the base form of the filename: "foo/bar.tmpl" -> "bar".
	Name string
	// Package is the package name.
	Package string
	// Imports is a list of custom imports, if provided.
	Imports []string
	// X is the list of X type values.
	X []*X
}

// X is the concrete type to be iterated over in the user template.
type X struct {
	// Name is the name of X for use as identifier: "int" -> "Int", "[]byte" -> "ByteSlice".
	Name string
	// Type is the textual type of X: "int", "float32", "foo.Baz".
	Type string
	// Y is the list of Y type values for this X.
	Y []*Y
}

// Y is the concrete type to be iterated over in the user template for each X.
// Each combination of X and Y will be present.
type Y struct {
	// Name is the name of Y for use as identifier: "int" -> "Int", "[]byte" -> "ByteSlice".
	Name string
	// Type is the textual type of Y: "int", "float32", "foo.Baz".
	Type string
	// Z is the list of Z type values for this Y.
	Z []*Z
}

// Z is the concrete type to be iterated over in the user template for each Y.
// Each combination of X, Y and Z will be present.
type Z struct {
	// Name is the name of Z for use as identifier: "int" -> "Int", "[]byte" -> "ByteSlice".
	Name string
	// Type is the textual type of Z: "int", "float32", "foo.Baz".
	Type string
}

var (
	integers   = []string{"int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64"}
	floats     = []string{"float32", "float64"}
	primitives = append(append([]string{"bool", "string"}, integers...), floats...)

	macros = map[string][]string{
		"integers":   integers,
		"floats":     floats,
		"primitives": primitives,
		"data":       append([]string{"[]byte"}, primitives...),
		"universals": {"typex.T", "typex.U", "typex.V", "typex.W", "typex.X", "typex.Y", "typex.Z"},
	}

	packageMacros = map[string][]string{
		"typex": {"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"},
	}
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %v [options] --input=<filename.tmpl --x=<types>\n", filepath.Base(os.Args[0]))
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	log.SetFlags(0)
	log.SetPrefix("specialize: ")

	if *input == "" {
		flag.Usage()
		log.Fatalf("no template file")
	}

	name := filepath.Base(*input)
	if index := strings.Index(name, "."); index > 0 {
		name = name[:index]
	}
	if *output == "" {
		*output = filepath.Join(filepath.Dir(*input), name+".go")
	}

	top := Top{Name: name, Package: *pack, Imports: expand(packageMacros, *imports)}
	var ys []*Y
	if *y != "" {
		var zs []*Z
		if *z != "" {
			for _, zt := range expand(macros, *z) {
				zs = append(zs, &Z{Name: makeName(zt), Type: zt})
			}
		}
		for _, yt := range expand(macros, *y) {
			ys = append(ys, &Y{Name: makeName(yt), Type: yt, Z: zs})
		}
	}
	for _, xt := range expand(macros, *x) {
		top.X = append(top.X, &X{Name: makeName(xt), Type: xt, Y: ys})
	}

	tmpl, err := template.New(*input).Funcs(funcMap).ParseFiles(*input)
	if err != nil {
		log.Fatalf("template parse failed: %v", err)
	}
	var buf bytes.Buffer
	if !*noheader {
		buf.WriteString("// File generated by specialize. Do not edit.\n\n")
	}
	if err := tmpl.Funcs(funcMap).Execute(&buf, top); err != nil {
		log.Fatalf("specialization failed: %v", err)
	}
	if err := os.WriteFile(*output, buf.Bytes(), 0644); err != nil {
		log.Fatalf("write failed: %v", err)
	}
}

// expand parses, cleans up and expands macros for a comma-separated list.
func expand(subst map[string][]string, list string) []string {
	var ret []string
	for _, xt := range strings.Split(list, ",") {
		xt = strings.TrimSpace(xt)
		if xt == "" {
			continue
		}
		if exp, ok := subst[strings.ToLower(xt)]; ok {
			for _, t := range exp {
				ret = append(ret, t)
			}
			continue
		}
		ret = append(ret, xt)
	}
	return ret
}

// makeName creates a capitalized identifier from a type.
func makeName(t string) string {
	if strings.HasPrefix(t, "[]") {
		return makeName(t[2:] + "Slice")
	}

	t = strings.Replace(t, ".", "_", -1)
	t = strings.Replace(t, "[", "_", -1)
	t = strings.Replace(t, "]", "_", -1)
	return cases.Title(language.Und, cases.NoLower).String(t)
}

// Useful template functions

var funcMap template.FuncMap = map[string]any{
	"join":                                   strings.Join,
	"upto":                                   upto,
	"mkargs":                                 mkargs,
	"mktuple":                                mktuple,
	"mktuplef":                               mktuplef,
	"add":                                    add,
	"mult":                                   mult,
	"dict":                                   dict,
	"list":                                   list,
	"genericTypingRepresentation":            genericTypingRepresentation,
	"possibleBundleLifecycleParameterCombos": possibleBundleLifecycleParameterCombos,
}

// mkargs(n, type) returns "<fmt.Sprintf(format, 0)>, .., <fmt.Sprintf(format, n-1)> type".
// If n is 0, it returns the empty string.
func mkargs(n int, format, typ string) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%v %v", mktuplef(n, format), typ)
}

// mktuple(n, v) returns "v, v, ..., v".
func mktuple(n int, v string) string {
	var ret []string
	for i := 0; i < n; i++ {
		ret = append(ret, v)
	}
	return strings.Join(ret, ", ")
}

// mktuplef(n, format) returns "<fmt.Sprintf(format, 0)>, .., <fmt.Sprintf(format, n-1)>"
func mktuplef(n int, format string) string {
	var ret []string
	for i := 0; i < n; i++ {
		ret = append(ret, fmt.Sprintf(format, i))
	}
	return strings.Join(ret, ", ")
}

// upto(n) returns []int{0, 1, .., n-1}.
func upto(i int) []int {
	var ret []int
	for k := 0; k < i; k++ {
		ret = append(ret, k)
	}
	return ret
}

func add(i int, j int) int {
	return i + j
}

func mult(i int, j int) int {
	return i * j
}

func dict(values ...any) map[string]any {
	dict := make(map[string]any, len(values)/2)
	if len(values)%2 != 0 {
		panic("Invalid dictionary call")
	}
	for i := 0; i < len(values); i += 2 {
		dict[values[i].(string)] = values[i+1]
	}

	return dict
}

func list(values ...string) []string {
	return values
}

func genericTypingRepresentation(in int, out int, includeType bool) string {
	seenElements := false
	typing := ""
	if in > 0 {
		typing += fmt.Sprintf("[I%v", 0)
		for i := 1; i < in; i++ {
			typing += fmt.Sprintf(", I%v", i)
		}
		seenElements = true
	}
	if out > 0 {
		i := 0
		if !seenElements {
			typing += fmt.Sprintf("[R%v", 0)
			i++
		}
		for i < out {
			typing += fmt.Sprintf(", R%v", i)
			i++
		}
		seenElements = true
	}

	if seenElements {
		if includeType {
			typing += " any"
		}
		typing += "]"
	}

	return typing
}

func possibleBundleLifecycleParameterCombos(numInInterface any, processElementInInterface any) [][]string {
	numIn := numInInterface.(int)
	processElementIn := processElementInInterface.(int)
	orderedKnownParameterOptions := []string{"context.Context", "typex.PaneInfo", "[]typex.Window", "typex.EventTime", "typex.BundleFinalization"}
	// Because of how Bundle lifecycle functions are invoked, all known parameters must precede unknown options and be in order.
	// Once we hit an unknown options, all remaining unknown options must be included since all iters/emitters must be included
	// Therefore, we can generate a powerset of the known options and fill out any remaining parameters with an ordered set of remaining unknown options
	pSetSize := int(math.Pow(2, float64(len(orderedKnownParameterOptions))))
	combos := make([][]string, 0, pSetSize)

	for index := 0; index < pSetSize; index++ {
		var subSet []string

		for j, elem := range orderedKnownParameterOptions {
			// And with the bit representation to get this iteration of the powerset.
			if index&(1<<uint(j)) > 0 {
				subSet = append(subSet, elem)
			}
		}
		// Fill out any remaining parameter slots with consecutive parameters from ProcessElement if there are enough options
		if len(subSet) <= numIn && numIn-len(subSet) <= processElementIn {
			for len(subSet) < numIn {
				nextElement := processElementIn - (numIn - len(subSet))
				subSet = append(subSet, fmt.Sprintf("I%v", nextElement))
			}
			combos = append(combos, subSet)
		}
	}

	return combos
}
