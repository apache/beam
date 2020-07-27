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

package exec

import (
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// Infrastructure for hashing values for lifted combines.

type elementHasher interface {
	Hash(element interface{}) (uint64, error)
}

func makeElementHasher(c *coder.Coder) elementHasher {
	// TODO(lostluck): move to a faster hashing library once we can take dependencies easily.
	hasher := fnv.New64a()
	switch c.Kind {
	case coder.Bytes:
		return &bytesHasher{hash: hasher}

	case coder.VarInt:
		return &numberHasher{}

	case coder.String:
		return &stringHasher{hash: hasher}

	case coder.Custom:
		// Shortcut for primitives where we know we can do better.
		switch c.Custom.Type {
		case reflectx.Int, reflectx.Int8, reflectx.Int16, reflectx.Int32, reflectx.Int64,
			reflectx.Uint, reflectx.Uint8, reflectx.Uint16, reflectx.Uint32, reflectx.Uint64,
			reflectx.Float32, reflectx.Float64:
			return &numberHasher{}
		}
		// TODO(lostluck): 2019.02.07 - consider supporting encoders that
		// take in a io.Writer instead.
		return &customEncodedHasher{
			hash:  hasher,
			t:     c.Custom.Type,
			coder: makeEncoder(c.Custom.Enc.Fn),
		}
	default:
		panic(fmt.Sprintf("Unexpected coder for hashing: %v", c))
	}
}

type bytesHasher struct {
	hash hash.Hash64
}

func (h *bytesHasher) Hash(element interface{}) (uint64, error) {
	h.hash.Reset()
	h.hash.Write(element.([]byte))
	return h.hash.Sum64(), nil
}

type stringHasher struct {
	hash hash.Hash64
}

func (h *stringHasher) Hash(element interface{}) (uint64, error) {
	h.hash.Reset()
	s := element.(string)
	var b [64]byte
	l := len(s)
	i := 0
	for len(s)-i > 64 {
		n := i + 64
		copy(b[:], s[i:n])
		h.hash.Write(b[:])
		i = n
	}
	n := l - i
	copy(b[:], s[i:])
	h.hash.Write(b[:n])
	return h.hash.Sum64(), nil
}

type numberHasher struct {
}

func (h *numberHasher) Hash(element interface{}) (uint64, error) {
	var val uint64
	switch n := element.(type) {
	case int:
		val = uint64(n)
	case int8:
		val = uint64(n)
	case int16:
		val = uint64(n)
	case int32:
		val = uint64(n)
	case int64:
		val = uint64(n)
	case uint:
		val = uint64(n)
	case uint8:
		val = uint64(n)
	case uint16:
		val = uint64(n)
	case uint32:
		val = uint64(n)
	case uint64:
		val = n
	case float64:
		val = math.Float64bits(n)
	case float32:
		val = uint64(math.Float64bits(float64(n)))
	default:
		panic(fmt.Sprintf("received unknown value type: want a number:, got %T", n))
	}
	return val, nil
}

type customEncodedHasher struct {
	hash  hash.Hash64
	t     reflect.Type
	coder Encoder
}

func (h *customEncodedHasher) Hash(element interface{}) (uint64, error) {
	h.hash.Reset()
	b, err := h.coder.Encode(h.t, element)
	if err != nil {
		return 0, err
	}
	h.hash.Write(b)
	return h.hash.Sum64(), nil
}
