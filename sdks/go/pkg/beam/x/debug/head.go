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

package debug

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// Head returns the first "n" elements it sees, it doesn't enforce any logic
// as to what elements they will be.
func Head(s beam.Scope, col beam.PCollection, n int) beam.PCollection {
	s = s.Scope("debug.Head")

	switch {
	case typex.IsKV(col.Type()):
		return beam.ParDo(s, &headKVFn{N: n}, beam.Impulse(s), beam.SideInput{Input: col})
	default:
		return beam.ParDo(s, &headFn{N: n}, beam.Impulse(s), beam.SideInput{Input: col})
	}
}

type headFn struct {
	N int `json:"n"`
}

func (h *headFn) ProcessElement(_ []byte, iter func(*beam.T) bool, emit func(beam.T)) {
	i := 0
	var val beam.T
	for iter(&val) && i < h.N {
		emit(val)
		i++
	}
}

type headKVFn struct {
	N int `json:"n"`
}

func (h *headKVFn) ProcessElement(_ []byte, iter func(*beam.X, *beam.Y) bool, emit func(beam.X, beam.Y)) {
	i := 0
	var x beam.X
	var y beam.Y
	for iter(&x, &y) && i < h.N {
		emit(x, y)
		i++
	}
}
