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
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
)

const bufCap = 64

// EncodeStringUTF8 encodes a UTF8 string with a length prefix.
func EncodeStringUTF8(s string, w io.Writer) error {
	if err := EncodeVarInt(int64(len(s)), w); err != nil {
		return err
	}
	return encodeStringUTF8(s, w)
}

// encodeStringUTF8 encodes a string.
func encodeStringUTF8(s string, w io.Writer) error {
	l := len(s)
	var b [bufCap]byte
	i := 0
	for l-i > bufCap {
		n := i + bufCap
		copy(b[:], s[i:n])
		if _, err := ioutilx.WriteUnsafe(w, b[:]); err != nil {
			return err
		}
		i = n
	}
	n := l - i
	copy(b[:n], s[i:])
	ioutilx.WriteUnsafe(w, b[:n])
	return nil
}

// decodeStringUTF8 reads l bytes and produces a string.
func decodeStringUTF8(l int64, r io.Reader) (string, error) {
	var builder strings.Builder
	var b [bufCap]byte
	i := l
	for i > bufCap {
		err := ioutilx.ReadNBufUnsafe(r, b[:])
		if err != nil {
			return "", err
		}
		i -= bufCap
		builder.Write(b[:])
	}
	err := ioutilx.ReadNBufUnsafe(r, b[:i])
	if err != nil {
		return "", err
	}
	builder.Write(b[:i])

	return builder.String(), nil
}

// DecodeStringUTF8 decodes a length prefixed UTF8 string.
func DecodeStringUTF8(r io.Reader) (string, error) {
	l, err := DecodeVarInt(r)
	if err != nil {
		return "", err
	}
	return decodeStringUTF8(l, r)
}
