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

package passert

import (
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// Count verifies the given PCollection<T> has the specified number of elements.
func Count(s beam.Scope, col beam.PCollection, name string, count int) {
	s = s.Scope(fmt.Sprintf("passert.Count(%v)", name))

	if typex.IsKV(col.Type()) {
		col = beam.DropKey(s, col)
	}
	counted := beam.Combine(s, &elmCountCombineFn{}, col)
	beam.ParDo0(s, &errFn{Name: name, Count: count}, counted)
}

type elmCountCombineFn struct {
}

func (f *elmCountCombineFn) CreateAccumulator() int {
	return 0
}

func (f *elmCountCombineFn) AddInput(a int, _ beam.T) int {
	return a + 1
}

func (f *elmCountCombineFn) MergeAccumulators(a, b int) int {
	return a + b
}

func (f *elmCountCombineFn) ExtractOutput(a int) int {
	return a
}

type errFn struct {
	Name  string `json:"name,omitempty"`
	Count int    `json:"count,omitempty"`
}

func (f *errFn) ProcessElement(count int) error {
	if f.Count != count {
		return errors.Errorf("passert.Count(%v) = %v, want %v", f.Name, count, f.Count)
	}
	return nil
}
