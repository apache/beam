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

// Package verifies that functions sym2addr and addr2sym work correctly.
package main

import (
	"log"
	"os"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/symtab"
)

const (
	name = "main.Increment"
	arg  = "calling increment function"
)

var symbolTable *symtab.SymbolTable
var counter int64
var t reflect.Type

// Increment is the function that will be executed by its address.
// It increments a global var so we can check that it was indeed called.
func Increment(str string) {
	log.Printf(str)
	counter++
}

func init() {
	// Registers function in symbol table.
	Increment("adding increment function to symbol table")
	t = reflect.FuncOf([]reflect.Type{reflect.TypeOf(arg)}, []reflect.Type{}, false)

	var err error
	// First try the Linux location, since it's the most reliable.
	symbolTable, err = symtab.New("/proc/self/exe")
	if err == nil {
		return
	}
	// For other OS's this works in most cases we need. If it doesn't, log
	// an error and keep going.
	symbolTable, err = symtab.New(os.Args[0])
	if err == nil {
		return
	}
	panic("Can't initialize symbol resolver.")
}

func main() {
	// Translates function symbol to address.
	addr, err := symbolTable.Sym2Addr(name)
	if err != nil {
		log.Fatalf("error translating function name to address: %v", err)
		return
	}

	// Restarts counter and calls increment function by its address.
	counter = 0
	ret, err := funcx.New(reflectx.MakeFunc(reflectx.LoadFunction(addr, t)))
	if err != nil {
		log.Fatalf("error creating function out of address")
		return
	}
	ret.Fn.Call([]interface{}{arg})

	// Checks that function was executed.
	if counter != 1 {
		log.Fatalf("error running function 'increment' through its address")
		return
	}

	// Translates address back to function symbol.
	symbol, err := symbolTable.Addr2Sym(addr)
	if err != nil {
		log.Fatalf("error translating address to function name")
		return
	}

	// Checks that function name is correct.
	if symbol != name {
		log.Fatalf("error verifying translation from address to name")
		return
	}
	log.Printf("success!")
}
