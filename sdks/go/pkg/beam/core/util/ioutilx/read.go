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

// Package ioutilx contains additional io utilities.
package ioutilx

import (
	"io"
	"unsafe"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// ReadN reads exactly N bytes from the reader. Fails otherwise.
func ReadN(r io.Reader, n int) ([]byte, error) {
	ret := make([]byte, n)
	index := 0

	for {
		i, err := r.Read(ret[index:])
		if i+index == n {
			return ret, nil
		}
		if err != nil {
			return nil, err
		}
		if i == 0 {
			return nil, errors.New("No data")
		}

		index += i
	}
}

// ReadNBufUnsafe reads exactly cap(buf) bytes from the reader. Fails otherwise.
// Uses the unsafe package unsafely to convince escape analysis that the passed
// in []byte doesn't escape this function through the io.Reader.
// Intended for use with small, fixed sized, stack allocated buffers that have
// no business being allocated to the heap.
// If the io.Reader somehow retains the passed in []byte, then this should not
// be used, and ReadN preferred.
func ReadNBufUnsafe(r io.Reader, b []byte) error {
	// Use with parameter retaining readers at your own peril.
	ret := *(*[]byte)(noescape(unsafe.Pointer(&b)))
	index := 0
	n := len(ret)
	for {
		i, err := r.Read(ret[index:])
		if i+index == n {
			return nil
		}
		if err != nil {
			return err
		}
		if i == 0 {
			return errors.New("No data")
		}

		index += i
	}
}

// ReadUnsafe is an unsafe version of read that breaks escape analysis
// to allow the passed in []byte to be allocated to the stack.
// Intended for use with small, fixed sized, stack allocated buffers that have
// no business being allocated to the heap.
// If the io.Reader somehow retains the passed in []byte, then this should not
// be used, and ReadN preferred.
func ReadUnsafe(r io.Reader, b []byte) (int, error) {
	ret := *(*[]byte)(noescape(unsafe.Pointer(&b)))
	return r.Read(ret)
}

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input. noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
// This was copied from the runtime; see issues 23382 and 7921.
//
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	//lint:ignore SA4016 see function comment
	return unsafe.Pointer(x ^ 0)
}
