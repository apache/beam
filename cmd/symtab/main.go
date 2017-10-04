// Package verifies that functions sym2addr and addr2sym work correctly.
package main

import (
	"os"
	"log"
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
	ret, err := funcx.New(reflectx.LoadFunction(addr, t))
	if err != nil {
		log.Fatalf("error creating function out of address")
		return
	}
	ret.Fn.Call([]reflect.Value{reflect.ValueOf(arg)})

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