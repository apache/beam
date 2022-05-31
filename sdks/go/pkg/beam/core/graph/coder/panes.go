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

package coder

import (
	"fmt"
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
)

// EncodePane encodes a typex.PaneInfo.
func EncodePane(v typex.PaneInfo, w io.Writer) error {
	// Encoding: typex.PaneInfo

	pane := byte(0)
	if v.IsFirst {
		pane |= 0x02
	}
	if v.IsLast {
		pane |= 0x01
	}
	pane |= byte(v.Timing << 2)

	switch {
	case (v.Index == 0 && v.NonSpeculativeIndex == 0) || v.Timing == typex.PaneUnknown:
		// The entire pane info is encoded as a single byte
		paneByte := []byte{pane}
		w.Write(paneByte)
	case v.Index == v.NonSpeculativeIndex || v.Timing == typex.PaneEarly:
		// The pane info is encoded as this byte plus a single VarInt encoded integer
		if v.Timing == typex.PaneEarly && v.NonSpeculativeIndex != -1 {
			return fmt.Errorf("error encoding pane %v: non-speculative index value must be equal to -1 if the pane timing is early", v)
		}
		paneByte := []byte{pane | 1<<4}
		w.Write(paneByte)
		EncodeVarInt(v.Index, w)
	default:
		// The pane info is encoded as this byte plus two VarInt encoded integer
		paneByte := []byte{pane | 2<<4}
		w.Write(paneByte)
		EncodeVarInt(v.Index, w)
		EncodeVarInt(v.NonSpeculativeIndex, w)
	}
	return nil
}

// NewPane initializes the PaneInfo from a given byte.
// By default, PaneInfo is assigned to NoFiringPane.
func NewPane(b byte) typex.PaneInfo {
	pn := typex.NoFiringPane()

	if !(b&0x02 == 2) {
		pn.IsFirst = false
	}
	if !(b&0x01 == 1) {
		pn.IsLast = false
	}

	pn.Timing = typex.PaneTiming((b >> 2) & 0x03)
	return pn
}

// DecodePane decodes a single byte.
func DecodePane(r io.Reader) (typex.PaneInfo, error) {
	// Decoding: typex.PaneInfo

	var data [1]byte
	if err := ioutilx.ReadNBufUnsafe(r, data[:]); err != nil {
		return typex.PaneInfo{}, err
	}

	pn := NewPane(data[0] & 0x0f)

	switch data[0] >> 4 {
	case 0:
		// Result encoded in only one pane.
		return pn, nil
	case 1:
		// Result encoded in one pane plus a VarInt encoded integer.
		data, err := DecodeVarInt(r)
		if err != nil {
			return typex.PaneInfo{}, err
		}

		pn.Index = data
		if pn.Timing == typex.PaneEarly {
			pn.NonSpeculativeIndex = -1
		} else {
			pn.NonSpeculativeIndex = pn.Index
		}
	case 2:
		// Result encoded in one pane plus two VarInt encoded integer.
		data, err := DecodeVarInt(r)
		if err != nil {
			return typex.PaneInfo{}, err
		}

		pn.Index = data
		pn.NonSpeculativeIndex, err = DecodeVarInt(r)
		if err != nil {
			return typex.PaneInfo{}, err
		}
	}
	return pn, nil
}
