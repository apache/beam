package runtime

import (
	"os"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/symtab"
)

// TODO(wcn): not happy with these names. if inspiration strikes, changes welcome!

// SymbolResolution is the interface that should be implemented
// in order to override the default behavior for symbol resolution.
type SymbolResolution interface {
	Sym2Addr(string) (uintptr, error)
}

// SymbolResolver is exported to allow unit tests to supply a resolver function.
// This is needed since the default symbol resolution process uses the DWARF
// tables in the executable, which unfortunately are not made available
// by default in "go test" builds. Unit tests that need symbol resolution
// should pass in an appropriate fake.
var SymbolResolver SymbolResolution

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
