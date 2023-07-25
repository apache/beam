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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestHead(t *testing.T) {
	p, s, sequence := ptest.CreateList([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	headSequence := Head(s, sequence, 5)
	passert.Count(s, headSequence, "NumElements", 5)
	passert.Equals(s, headSequence, 1, 2, 3, 4, 5)

	ptest.RunAndValidate(t, p)
}

func TestHead_KV(t *testing.T) {
	p, s, sequence := ptest.CreateList([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	kvSequence := beam.AddFixedKey(s, sequence)
	headKvSequence := Head(s, kvSequence, 5)
	headSequence := beam.DropKey(s, headKvSequence)
	passert.Count(s, headSequence, "NumElements", 5)
	passert.Equals(s, headSequence, 1, 2, 3, 4, 5)

	ptest.RunAndValidate(t, p)
}
