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

// Package symtab allows reading low-level symbol information from the symbol table.
package symtab

import (
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"fmt"
	"os"
	"reflect"
	"runtime"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// SymbolTable allows for mapping between symbols and their addresses.
type SymbolTable struct {
	addr2Sym map[uintptr]string
	sym2Addr map[string]uintptr
	offset   uintptr // offset between file addresses and runtime addresses
}

// New creates a new symbol table based on the debug info
// read from the specified file.
func New(filename string) (*SymbolTable, error) {
	addr2Sym, sym2Addr, err := symbolData(filename)
	if err != nil {
		return nil, err
	}

	sym := &SymbolTable{
		addr2Sym: addr2Sym,
		sym2Addr: sym2Addr,
	}

	// Work out the offset between the file addresses and the
	// runtime addreses, in case this is a position independent
	// executable.
	runtimeAddr := reflect.ValueOf(New).Pointer()
	name := fnname()
	fileAddr, err := sym.Sym2Addr(name)
	if err != nil {
		return nil, fmt.Errorf("failed to reverse lookup known function %s: %v", name, err)
	}
	sym.offset = runtimeAddr - fileAddr

	return sym, nil
}

// symbolData reads the file's symbol table and builds two maps,
// one from symbol to address and one from address to symbol.
// The maps only include function symbols.
func symbolData(filename string) (map[uintptr]string, map[string]uintptr, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	// First try ELF
	ef, err := elf.NewFile(f)
	if err == nil {
		return elfSymbolData(ef)
	}

	// then Mach-O
	mf, err := macho.NewFile(f)
	if err == nil {
		return machoSymbolData(mf)
	}

	// finally try Windows PE format
	pf, err := pe.NewFile(f)
	if err == nil {
		return peSymbolData(pf)
	}

	// Give up, we don't recognize it
	return nil, nil, errors.New("Unknown file format")
}

// elfSymbolData builds function symbol maps from an ELF file.
func elfSymbolData(ef *elf.File) (map[uintptr]string, map[string]uintptr, error) {
	syms, err := ef.Symbols()
	if err != nil {
		return nil, nil, err
	}
	addr2Sym := make(map[uintptr]string)
	sym2Addr := make(map[string]uintptr)
	for _, sym := range syms {
		if elf.ST_TYPE(sym.Info) != elf.STT_FUNC {
			continue
		}
		value := uintptr(sym.Value)
		addr2Sym[value] = sym.Name
		sym2Addr[sym.Name] = value
	}
	return addr2Sym, sym2Addr, nil
}

// machoSymbolData builds function symbol maps from a Mach-O file.
func machoSymbolData(mf *macho.File) (map[uintptr]string, map[string]uintptr, error) {
	addr2Sym := make(map[uintptr]string)
	sym2Addr := make(map[string]uintptr)
	for _, sym := range mf.Symtab.Syms {
		if int(sym.Sect-1) >= len(mf.Sections) {
			continue
		}
		if mf.Sections[sym.Sect-1].Seg != "__TEXT" {
			continue
		}
		value := uintptr(sym.Value)
		addr2Sym[value] = sym.Name
		sym2Addr[sym.Name] = value
	}
	return addr2Sym, sym2Addr, nil
}

// peSymbolData builds function symbol maps from a PE file.
func peSymbolData(pf *pe.File) (map[uintptr]string, map[string]uintptr, error) {
	addr2Sym := make(map[uintptr]string)
	sym2Addr := make(map[string]uintptr)
	for _, sym := range pf.Symbols {
		if sym.SectionNumber <= 0 || int(sym.SectionNumber-1) >= len(pf.Sections) {
			continue
		}
		const text = 0x20
		if pf.Sections[sym.SectionNumber-1].Characteristics&text == 0 {
			continue
		}
		value := uintptr(sym.Value)
		addr2Sym[value] = sym.Name
		sym2Addr[sym.Name] = value
	}
	return addr2Sym, sym2Addr, nil
}

// fnname returns the name of the function that called it.
func fnname() string {
	var pcs [2]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	frame, _ := frames.Next()
	return frame.Func.Name()
}

// Addr2Sym returns the symbol name for the provided address.
func (s *SymbolTable) Addr2Sym(addr uintptr) (string, error) {
	addr -= s.offset
	sym, ok := s.addr2Sym[addr]
	if !ok {
		return "", errors.Errorf("no symbol found at address %x", addr)
	}
	return sym, nil
}

// Sym2Addr returns the address of the provided symbol name.
func (s *SymbolTable) Sym2Addr(symbol string) (uintptr, error) {
	addr, ok := s.sym2Addr[symbol]
	if !ok {
		return 0, errors.Errorf("no symbol %q", symbol)
	}
	return addr + s.offset, nil
}
