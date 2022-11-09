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

// Package resource supports Beam resource hints to specify scoped hints or annotations
// to pipelines.
//
// See https://beam.apache.org/documentation/runtime/resource-hints/ for more information.
package resource

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/dustin/go-humanize"
)

// Hints contains a list of hints for a given scope.
type Hints struct {
	h map[string]Hint
}

// MergeWithOuter produces a new list of Hints from this Hints, and the Hints from the outer scope.
// Semantics are defined per hint urn, but by default the "inner" hint will be prefered over the
// outer hint if both scopes have the same urn.
func (hs Hints) MergeWithOuter(outer Hints) Hints {
	if len(outer.h) == 0 {
		return hs
	}
	if len(hs.h) == 0 {
		return outer
	}
	merged := Hints{h: map[string]Hint{}}
	for k, o := range outer.h {
		if h, ok := hs.h[k]; ok {
			merged.h[k] = h.MergeWithOuter(o)
		} else {
			merged.h[k] = o
		}
	}
	// Always include any from the base, not already merged from outer.
	for k, h := range hs.h {
		if _, ok := outer.h[k]; !ok {
			merged.h[k] = h
		}
	}
	return merged
}

// Equal checks if two sets of hints are identical. A hint is identical to another if their payloads
// are the same for a given URN.
func (hs Hints) Equal(other Hints) bool {
	if len(hs.h) != len(other.h) {
		return false
	}
	for k, h := range hs.h {
		o, ok := other.h[k]
		if !ok {
			return false
		}
		if !bytes.Equal(h.Payload(), o.Payload()) {
			return false
		}
	}

	return true
}

// Payloads retuns a map from all hint URNs to the serialized byte representation of their payloads.
func (hs Hints) Payloads() map[string][]byte {
	p := map[string][]byte{}
	for k, h := range hs.h {
		p[k] = h.Payload()
	}
	return p
}

// NewHints produces a hints map from a list of hints. If there are multiple hints
// with the same URN, the last one in the list is used.
func NewHints(hs ...Hint) Hints {
	hints := Hints{h: map[string]Hint{}}
	for _, h := range hs {
		hints.h[h.URN()] = h
	}
	return hints
}

// Hint contains all the information about a given resource hint.
type Hint interface {
	// URN returns the name for this hint.
	URN() string
	// Payload returns the serialized version of this payload.
	Payload() []byte
	// MergeWithOuter an outer scope hint.
	MergeWithOuter(outer Hint) Hint
}

// MinRAMBytes hints that this scope should be put in a machine with at least this many bytes of memory.
//
// Hints are advisory only and runners may not respect them.
//
// See https://beam.apache.org/documentation/runtime/resource-hints/ for more information about
// resource hints.
func MinRAMBytes(v uint64) Hint {
	return minRAMHint{value: int64(v)}
}

// ParseMinRAM converts various byte units, including MB, GB, MiB, and GiB into a hint.
// An invalid byte size format will cause ParseMinRAM to panic.
//
// Hints are advisory only and runners may not respect them.
//
// See https://beam.apache.org/documentation/runtime/resource-hints/ for more information about
// resource hints.
func ParseMinRAM(v string) Hint {
	b, err := humanize.ParseBytes(v)
	if err != nil {
		panic(fmt.Sprintf("resource.ParseMinRAM: unable to parse %q: %v", v, err))
	}
	return MinRAMBytes(b)
}

type minRAMHint struct {
	value int64
}

func (minRAMHint) URN() string {
	return "beam:resources:min_ram_bytes:v1"
}

func (h minRAMHint) Payload() []byte {
	// Go strings are utf8, and if the string is ascii,
	// byte conversion handles that directly.
	return []byte(strconv.FormatInt(h.value, 10))
}

// MergeWith an outer minRAMHints by keeping the maximum of the two byte amounts.
func (h minRAMHint) MergeWithOuter(outer Hint) Hint {
	// Intentional runtime panic from type assertion to catch hint merge errors.
	if outer.(minRAMHint).value > h.value {
		return outer
	}
	return h
}

func (h minRAMHint) String() string {
	return fmt.Sprintf("min_ram=%v", humanize.Bytes(uint64(h.value)))
}

// Accelerator hints that this scope should be put in a machine with a given accelerator.
//
// Hints for accelerators will have formats that are runner specific.
// For example, the following is valid accelerator syntax for the Dataflow runner:
//
//	accelerator="type:<type>;count:<n>;<options>"
//
// Hints are advisory only and runners may not respect them.
//
// See https://beam.apache.org/documentation/runtime/resource-hints/ for more information about
// resource hints.
func Accelerator(v string) Hint {
	return acceleratorHint{value: v}
}

type acceleratorHint struct {
	value string
}

func (acceleratorHint) URN() string {
	return "beam:resources:accelerator:v1"
}

func (h acceleratorHint) Payload() []byte {
	// Go strings are utf8, and if the string is ascii,
	// byte conversion handles that directly.
	return []byte(h.value)
}

// MergeWithOuter an outer acceleratorHint by keeping this hint.
func (h acceleratorHint) MergeWithOuter(outer Hint) Hint {
	return h
}

func (h acceleratorHint) String() string {
	return fmt.Sprintf("accelerator=%v", h.value)
}
