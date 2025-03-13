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
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

// Flatten tests flatten.
func Flatten(s beam.Scope) {
	a := beam.Create(s, 1, 2, 3)
	b := beam.Create(s, 4, 5, 6)
	c := beam.Create(s, 7, 8, 9)

	flat := beam.Flatten(s, a, b, c)
	passert.Sum(s, flat, "flat", 9, 45)
}

// FlattenDups tests flatten with the same input multiple times.
func FlattenDup(s beam.Scope) {
	a := beam.Create(s, 1, 2, 3)
	b := beam.Create(s, 4, 5, 6)

	flat := beam.Flatten(s, a, b, a)
	passert.Sum(s, flat, "flat", 9, 27)
}
