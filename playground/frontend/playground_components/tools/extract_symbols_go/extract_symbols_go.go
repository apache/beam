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

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"gopkg.in/yaml.v2"
)

type ClassSymbols struct {
	Methods    []string `yaml:",omitempty"`
	Properties []string `yaml:",omitempty"`
}

func (cs *ClassSymbols) sort() {
	sort.Strings(cs.Methods)
	cs.Methods = removeDuplicates(cs.Methods)

	sort.Strings(cs.Properties)
	cs.Properties = removeDuplicates(cs.Properties)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run extract_symbols_go.go <SDK path>")
		return
	}

	path := os.Args[1]
	classesMap := getDirSymbolsRecursive(path)
	sortClassSymbolsMap(classesMap)

	yamlData, err := yaml.Marshal(&classesMap)
	if err != nil {
		panic(err)
	}

	fmt.Print(string(yamlData))
}

func getDirSymbolsRecursive(dir string) map[string]*ClassSymbols {
	classesMap := make(map[string]*ClassSymbols)

	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !shouldIncludeFile(path) {
			return nil
		}

		addFileSymbols(classesMap, path)
		return nil
	})

	return classesMap
}

func shouldIncludeFile(path string) bool {
	if !strings.HasSuffix(path, ".go") {
		return false
	}

	if strings.HasSuffix(path, "_test.go") {
		return false
	}

	return true
}

func addFileSymbols(classesMap map[string]*ClassSymbols, filename string) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, filename, nil, parser.SkipObjectResolution)
	if err != nil {
		panic(err)
	}

	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.TypeSpec:
			visitTypeSpec(classesMap, node)
		case *ast.FuncDecl:
			visitFuncDecl(classesMap, node)
		default:
			return true // Go recursive
		}

		return false // No recursion
	})
}

func visitTypeSpec(classesMap map[string]*ClassSymbols, typeSpec *ast.TypeSpec) {
	className := typeSpec.Name.Name
	if !shouldIncludeSymbol(className) {
		return
	}

	if structType, ok := typeSpec.Type.(*ast.StructType); ok {
		visitStructType(classesMap, typeSpec, structType)
		return
	}

	if interfaceType, ok := typeSpec.Type.(*ast.InterfaceType); ok {
		visitInterfaceType(classesMap, typeSpec, interfaceType)
		return
	}
}

func visitStructType(classesMap map[string]*ClassSymbols, typeSpec *ast.TypeSpec, structType *ast.StructType) {
	className := typeSpec.Name.Name
	classSymbols := getOrCreateClassSymbols(classesMap, className)

	for _, field := range (*structType.Fields).List {
		if len(field.Names) == 0 {
			continue
		}

		name := field.Names[0].Name
		if !shouldIncludeSymbol(name) {
			continue
		}

		classSymbols.Properties = append(classSymbols.Properties, name)
	}
}

func visitInterfaceType(classesMap map[string]*ClassSymbols, typeSpec *ast.TypeSpec, interfaceType *ast.InterfaceType) {
	className := typeSpec.Name.Name
	classSymbols := getOrCreateClassSymbols(classesMap, className)

	for _, field := range (*interfaceType.Methods).List {
		if len(field.Names) == 0 {
			continue
		}

		name := field.Names[0].Name
		if !shouldIncludeSymbol(name) {
			continue
		}

		classSymbols.Methods = append(classSymbols.Methods, name)
	}
}

func visitFuncDecl(classesMap map[string]*ClassSymbols, funcDecl *ast.FuncDecl) {
	className := getReceiverClassName(funcDecl)
	if !shouldIncludeSymbol(className) {
		return
	}

	name := funcDecl.Name.Name
	if !shouldIncludeSymbol(name) {
		return
	}

	classSymbols := getOrCreateClassSymbols(classesMap, className)
	classSymbols.Methods = append(classSymbols.Methods, name)
}

func getReceiverClassName(funcDecl *ast.FuncDecl) string {
	if funcDecl.Recv == nil {
		return ""
	}

	return getExpressionClassName(funcDecl.Recv.List[0].Type)
}

// Extracts the class name from nodes like Foo, *Foo, Foo[type] etc.
func getExpressionClassName(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.Ident:
		// Foo
		return expr.Name
	case *ast.IndexExpr:
		// Foo[param]
		if ident, ok := expr.X.(*ast.Ident); ok {
			return ident.Name
		}
	case *ast.IndexListExpr:
		// Foo[param1, param2]
		if ident, ok := expr.X.(*ast.Ident); ok {
			return ident.Name
		}
	case *ast.StarExpr:
		// *Foo, *Foo[param], *Foo[param1, param2]
		return getExpressionClassName(expr.X)
	}

	panic(nil)
}

func getOrCreateClassSymbols(classesMap map[string]*ClassSymbols, name string) *ClassSymbols {
	existing, ok := classesMap[name]
	if ok {
		return existing
	}

	created := ClassSymbols{}

	classesMap[name] = &created
	return &created
}

func shouldIncludeSymbol(symbol string) bool {
	if symbol == "" {
		return true // Special case for globals.
	}

	r := []rune(symbol)[0]
	return unicode.IsUpper(r)
}

func sortClassSymbolsMap(classesMap map[string]*ClassSymbols) {
	// Only sort ClassSymbols objects because the map itself is sorted when serialized.
	// https://github.com/go-yaml/yaml/issues/30#issuecomment-56232269
	for _, v := range classesMap {
		v.sort()
	}
}

func removeDuplicates(arr []string) []string {
	existing := make(map[string]bool)
	result := []string{}

	for _, item := range arr {
		if _, exists := existing[item]; !exists {
			existing[item] = true
			result = append(result, item)
		}
	}

	return result
}
