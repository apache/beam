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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// EncodeByte encodes a single byte.
func EncodeByte(v byte, w io.Writer) error {
	// Encoding: raw byte.
	if _, err := ioutilx.WriteUnsafe(w, []byte{v}); err != nil {
		return fmt.Errorf("error encoding byte: %v", err)
	}
	return nil
}

// DecodeByte decodes a single byte.
func DecodeByte(r io.Reader) (byte, error) {
	// Encoding: raw byte
	var b [1]byte
	if err := ioutilx.ReadNBufUnsafe(r, b[:]); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, errors.Wrap(err, "error decoding byte")
	}
	return b[0], nil
}

// EncodeBytes encodes a []byte with a length prefix per the beam protocol.
func EncodeBytes(v []byte, w io.Writer) error {
	// Encoding: size (varint) + raw data
	size := len(v)
	if err := EncodeVarInt((int64)(size), w); err != nil {
		return err
	}
	_, err := w.Write(v)
	return err

}

// DecodeBytes decodes a length prefixed []byte according to the beam protocol.
func DecodeBytes(r io.Reader) ([]byte, error) {
	// Encoding: size (varint) + raw data
	size, err := DecodeVarInt(r)
	if err != nil {
		return nil, err
	}
	return ioutilx.ReadN(r, (int)(size))
}
