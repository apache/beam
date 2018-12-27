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
	"encoding/binary"
	"io"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
)

// EncodeUint64 encodes an uint64 in big endian format.
func EncodeUint64(value uint64, w io.Writer) error {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], value)
	_, err := ioutilx.WriteUnsafe(w, data[:])
	return err
}

// DecodeUint64 decodes an uint64 in big endian format.
func DecodeUint64(r io.Reader) (uint64, error) {
	var data [8]byte
	if err := ioutilx.ReadNBufUnsafe(r, data[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(data[:]), nil
}

// EncodeUint32 encodes an uint32 in big endian format.
func EncodeUint32(value uint32, w io.Writer) error {
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], value)
	_, err := ioutilx.WriteUnsafe(w, data[:])
	return err
}

// DecodeUint32 decodes an uint32 in big endian format.
func DecodeUint32(r io.Reader) (uint32, error) {
	var data [4]byte
	if err := ioutilx.ReadNBufUnsafe(r, data[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(data[:]), nil
}

// EncodeInt32 encodes an int32 in big endian format.
func EncodeInt32(value int32, w io.Writer) error {
	return EncodeUint32(uint32(value), w)
}

// DecodeInt32 decodes an int32 in big endian format.
func DecodeInt32(r io.Reader) (int32, error) {
	ret, err := DecodeUint32(r)
	if err != nil {
		return 0, err
	}
	return int32(ret), nil
}
