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

package primitives

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

// TruncateFn is an SDF.
type valueStateFn struct {
	State1 state.Value[int]
}

func (f *valueStateFn) ProcessElement(s state.Provider, w string, c int) string {
	i, ok, err := f.State1.Read(s)
	if err != nil {
		panic(err)
	}
	if !ok {
		err = f.State1.Write(s, 1)
		if err != nil {
			panic(err)
		}
	} else {
		f.State1.Write(s, i+1)
		if err != nil {
			panic(err)
		}
	}
	return fmt.Sprintf("%s: %v", w, i)
}

// ValueStateParDo tests a DoFn that uses value state.
func ValueStateParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, func(w string, emit func(string, int)) {
		emit(w, 1)
	}, in)
	counts := beam.ParDo(s, &valueStateFn{State1: state.MakeValueState[int]("key1")}, keyed)
	passert.Equals(s, counts, "apple: 1", "pear: 1", "peach: 1", "apple: 2", "apple: 3", "pear: 2")

	return p
}
