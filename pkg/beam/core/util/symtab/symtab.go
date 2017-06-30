package symtab

import (
	"bytes"
	"debug/dwarf"
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"fmt"
	"io/ioutil"
)

// SymbolTable allows for mapping between symbols and their addresses.
type SymbolTable struct {
	data *dwarf.Data
}

// New creates a new symbol table based on the debug info
// read from the specified file.
func New(filename string) (*SymbolTable, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(b)

	// First try ELF
	ef, err := elf.NewFile(r)
	if err == nil {
		d, err := ef.DWARF()
		if err != nil {
			return nil, err
		}
		return &SymbolTable{d}, nil
	}

	// then Mach-O
	mf, err := macho.NewFile(r)
	if err == nil {
		d, err := mf.DWARF()
		if err != nil {
			return nil, fmt.Errorf("No working DWARF: %v", err)
		}
		return &SymbolTable{d}, nil
	}

	// finally try Windows PE format
	pf, err := pe.NewFile(r)
	if err == nil {
		d, err := pf.DWARF()
		if err != nil {
			return nil, err
		}
		return &SymbolTable{d}, nil
	}

	// Give up, we don't recognize it
	return nil, fmt.Errorf("Unknown file format")
}

// Addr2Sym returns the symbol name for the provided address.
func (s *SymbolTable) Addr2Sym(addr uintptr) (string, error) {
	reader := s.data.Reader()
	for {
		e, err := reader.Next()
		if err != nil {
			return "", err
		}

		if e == nil {
			break
		}

		if e.Tag == dwarf.TagSubprogram {
			nf := e.Field[1]
			if nf.Val.(uint64) == uint64(addr) {
				return e.Field[0].Val.(string), nil
			}
		}
	}
	return "", fmt.Errorf("no symbol found at address %x", addr)
}

// Sym2Addr returns the address of the provided symbol name.
func (s *SymbolTable) Sym2Addr(symbol string) (uintptr, error) {
	reader := s.data.Reader()
	for {
		e, err := reader.Next()
		if err != nil {
			return 0, err
		}

		if e == nil {
			break
		}

		if e.Tag == dwarf.TagSubprogram {
			nf := e.Field[0]
			if nf.Attr.String() == "Name" && nf.Val.(string) == symbol {
				addr := e.Field[1].Val.(uint64)
				return uintptr(addr), nil
			}
		}
	}
	return 0, fmt.Errorf("no symbol %q", symbol)
}
