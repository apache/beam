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
	"math"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
)

// EncodeDouble encodes a float64 in big endian format.
func EncodeDouble(value float64, w io.Writer) error {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], math.Float64bits(value))
	_, err := ioutilx.WriteUnsafe(w, data[:])
	return err
}

// DecodeDouble decodes a float64 in big endian format.
func DecodeDouble(r io.Reader) (float64, error) {
	var data [8]byte
	if err := ioutilx.ReadNBufUnsafe(r, data[:]); err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(data[:])), nil
}
