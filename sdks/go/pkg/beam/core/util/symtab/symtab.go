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
	"debug/dwarf"
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"fmt"
	"os"
)

// SymbolTable allows for mapping between symbols and their addresses.
type SymbolTable struct {
	data *dwarf.Data
}

// New creates a new symbol table based on the debug info
// read from the specified file.
func New(filename string) (*SymbolTable, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// The interface contract for the xxx.NewFile() methods takes an
	// io.ReaderAt which suggests the Reader needs to stay alive for the duration
	// of the symbol table.

	// First try ELF
	ef, err := elf.NewFile(f)
	if err == nil {
		d, err := ef.DWARF()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("No working DWARF: %v", err)
		}
		return &SymbolTable{d}, nil
	}

	// then Mach-O
	mf, err := macho.NewFile(f)
	if err == nil {
		d, err := mf.DWARF()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("No working DWARF: %v", err)
		}
		return &SymbolTable{d}, nil
	}

	// finally try Windows PE format
	pf, err := pe.NewFile(f)
	if err == nil {
		d, err := pf.DWARF()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("No working DWARF: %v", err)
		}
		return &SymbolTable{d}, nil
	}

	// Give up, we don't recognize it
	f.Close()
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
