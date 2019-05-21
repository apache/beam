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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// ErrVarIntTooLong indicates a data corruption issue that needs special
// handling by callers of decode. TODO(herohde): have callers perform
// this special handling.
var ErrVarIntTooLong = errors.New("varint too long")

// EncodeVarUint64 encodes an uint64.
func EncodeVarUint64(value uint64, w io.Writer) error {
	ret := make([]byte, 0, 8)
	for {
		// Encode next 7 bits + terminator bit
		bits := value & 0x7f
		value >>= 7

		var mask uint64
		if value != 0 {
			mask = 0x80
		}
		ret = append(ret, (byte)(bits|mask))
		if value == 0 {
			_, err := ioutilx.WriteUnsafe(w, ret)
			return err
		}
	}
}

// Variable-length encoding for integers.
//
// Takes between 1 and 10 bytes. Less efficient for negative or large numbers.
// All negative ints are encoded using 5 bytes, longs take 10 bytes. We use
// uint64 (over int64) as the primitive form to get logical bit shifts.

// TODO(herohde) 5/16/2017: figure out whether it's too slow to read one byte
// at a time here. If not, we may need a more sophisticated reader than
// io.Reader with lookahead, say.

// DecodeVarUint64 decodes an uint64.
func DecodeVarUint64(r io.Reader) (uint64, error) {
	var ret uint64
	var shift uint

	var data [1]byte
	for {
		// Get 7 bits from next byte
		if n, err := ioutilx.ReadUnsafe(r, data[:]); n < 1 {
			return 0, err
		}

		b := data[0]
		bits := (uint64)(b & 0x7f)

		if shift >= 64 || (shift == 63 && bits > 1) {
			return 0, ErrVarIntTooLong
		}

		ret |= bits << shift
		shift += 7

		if (b & 0x80) == 0 {
			return ret, nil
		}
	}
}

// EncodeVarInt encodes an int64.
func EncodeVarInt(value int64, w io.Writer) error {
	return EncodeVarUint64((uint64)(value), w)
}

// DecodeVarInt decodes an int64.
func DecodeVarInt(r io.Reader) (int64, error) {
	ret, err := DecodeVarUint64(r)
	if err != nil {
		return 0, err
	}
	return (int64)(ret), nil
}
