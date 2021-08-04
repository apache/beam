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
	"io"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
)

const (
	FIRST     int = 0
	ONE_INDEX int = 1
	TWO_INDEX int = 2
)

func chooseEncoding(v typex.PaneInfo) int {
	if (v.Index == 0 || v.NonSpeculativeIndex == 0) || v.Timing == typex.UNKNOWN {
		return FIRST
	} else if v.Index == v.NonSpeculativeIndex || v.Timing == typex.EARLY {
		return ONE_INDEX
	} else {
		return TWO_INDEX
	}
}

func timing(v typex.Timing) int {
	if v == typex.EARLY {
		return 0
	} else if v == typex.ON_TIME {
		return 1
	} else if v == typex.LATE {
		return 2
	} else {
		return 3
	}
}

// EncodePane encodes a single byte.
func EncodePane(v typex.PaneInfo, w io.Writer) error {
	// Encoding: typex.PaneInfo

	pane := 0
	if v.IsFirst {
		pane |= 1
	}
	if v.IsLast {
		pane |= 2
	}
	pane |= timing(v.Timing) << 2

	switch chooseEncoding(v) {
	case FIRST:
		paneByte := []byte{byte(pane)}
		w.Write(paneByte)
	case ONE_INDEX:
		paneByte := []byte{byte(pane | (ONE_INDEX)<<4)}
		w.Write(paneByte)
		EncodeVarInt(v.Index, w)
	case TWO_INDEX:
		paneByte := []byte{byte(pane | (TWO_INDEX)<<4)}
		w.Write(paneByte)
		EncodeVarInt(v.Index, w)
		EncodeVarInt(v.NonSpeculativeIndex, w)
	}
	return nil
}

func encodingType(b byte) int64 {
	return int64(b >> 4)
}

func NewPane(b byte) typex.PaneInfo {
	pn := typex.PaneInfo{}
	if b&0x01 == 1 {
		pn.IsFirst = true
	}
	if b&0x02 == 2 {
		pn.IsLast = true
	}
	switch int64((b >> 2) & 0x03) {
	case 0:
		pn.Timing = typex.EARLY
	case 1:
		pn.Timing = typex.ON_TIME
	case 2:
		pn.Timing = typex.LATE
	case 3:
		pn.Timing = typex.UNKNOWN
	}

	return pn
}

// DecodePane decodes a single byte.
func DecodePane(r io.Reader) (typex.PaneInfo, error) {
	// Decoding: typex.PaneInfo
	var data [1]byte
	if err := ioutilx.ReadNBufUnsafe(r, data[:]); err != nil { // NO_FIRING pane
		return typex.PaneInfo{}, err
	}
	pn := NewPane(data[0] & 0x0f)
	switch encodingType(data[0]) {
	case 0:
		return pn, nil
	case 1:
		data, err := DecodeVarInt(r)
		if err != nil {
			return pn, err
		}
		pn.Index = data
		if pn.Timing == typex.EARLY {
			pn.NonSpeculativeIndex = -1
		} else {
			pn.NonSpeculativeIndex = pn.Index
		}
	case 2:
		data, err := DecodeVarInt(r)
		if err != nil {
			return pn, err
		}
		pn.Index = data
		pn.NonSpeculativeIndex, err = DecodeVarInt(r)
		if err != nil {
			return pn, err
		}
	}
	return pn, nil
}
