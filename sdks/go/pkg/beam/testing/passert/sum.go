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
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// Sum validates that the sum and count of elements in the incoming PCollection<int> is
// the same as the given sum and count, under coder equality. Sum is a specialized version of Equals
// that avoids a lot of machinery for testing.
func Sum(s beam.Scope, col beam.PCollection, name string, size, value int) {
	s = s.Scope(fmt.Sprintf("passert.Sum(%v)", name))

	keyed := beam.AddFixedKey(s, col)
	grouped := beam.GroupByKey(s, keyed)
	beam.ParDo0(s, &sumFn{Name: name, Size: size, Sum: value}, grouped)
}

type sumFn struct {
	Name string `json:"name,omitempty"`
	Size int    `json:"size,omitempty"`
	Sum  int    `json:"sum,omitempty"`
}

func (f *sumFn) ProcessElement(_ int, values func(*int) bool) error {
	var sum, count, i int
	for values(&i) {
		count++
		sum += i
	}

	if f.Sum != sum || f.Size != count {
		return errors.Errorf("passert.Sum(%v) = {%v, size: %v}, want {%v, size:%v}", f.Name, sum, count, f.Sum, f.Size)
	}
	return nil
}
