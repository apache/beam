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

package coderx

import (
	"encoding/binary"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// Fixed-sized custom coders for integers.
var (
	Uint32 *coder.CustomCoder
	Int32  *coder.CustomCoder
	Uint64 *coder.CustomCoder
	Int64  *coder.CustomCoder
)

func init() {
	var err error
	Uint32, err = coder.NewCustomCoder("uint32", reflectx.Uint32, encUint32, decUint32)
	if err != nil {
		panic(err)
	}
	Int32, err = coder.NewCustomCoder("int32", reflectx.Int32, encInt32, decInt32)
	if err != nil {
		panic(err)
	}
	Uint64, err = coder.NewCustomCoder("uint64", reflectx.Uint64, encUint64, decUint64)
	if err != nil {
		panic(err)
	}
	Int64, err = coder.NewCustomCoder("int64", reflectx.Int64, encInt64, decInt64)
	if err != nil {
		panic(err)
	}
}

func encUint32(v uint32) []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, v)
	return ret
}

func decUint32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func encInt32(v int32) []byte {
	return encUint32(uint32(v))
}

func decInt32(data []byte) int32 {
	return int32(decUint32(data))
}

func encUint64(v uint64) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, v)
	return ret
}

func decUint64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func encInt64(v int64) []byte {
	return encUint64(uint64(v))
}

func decInt64(data []byte) int64 {
	return int64(decUint64(data))
}
