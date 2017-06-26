package graphx

import (
	"os"

	"github.com/apache/beam/sdks/go/pkg/beam/util/symtab"
)

func init() {
	var err error
	// First try the Linux location, since it's the most reliable.
	SymbolResolver, err = symtab.New("/proc/self/exe")
	if err == nil {
		return
	}
	// For other OS's this works in most cases we need. If it doesn't, log
	// an error and keep going.
	SymbolResolver, err = symtab.New(os.Args[0])
	if err == nil {
		return
	}
	SymbolResolver = panicResolver(false)
}

type panicResolver bool

func (p panicResolver) Sym2Addr(name string) (uintptr, error) {
	panic("Attempted call to panicResolver.Sym2Addr(): if this happened from a unit test, you need to supply a fake symbol resolver.")
}
