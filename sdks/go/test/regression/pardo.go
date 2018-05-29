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

// Package regression contains pipeline regression tests.
package regression

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

func directCountFn(_ int, values func(*int) bool) (int, error) {
	sum := 0
	var i int
	for values(&i) {
		sum += i
	}
	return sum, nil
}

func emitCountFn(_ int, values func(*int) bool, emit func(int)) error {
	sum := 0
	var i int
	for values(&i) {
		sum += i
	}
	emit(sum)
	return nil
}

// DirectParDoAfterGBK generates a pipeline with a direct-form
// ParDo after a GBK. See: BEAM-3978 and BEAM-4175.
func DirectParDoAfterGBK() *beam.Pipeline {
	p, s, col := ptest.Create([]interface{}{1, 2, 3, 4})

	keyed := beam.GroupByKey(s, beam.AddFixedKey(s, col))
	sum := beam.ParDo(s, directCountFn, keyed)
	passert.Equals(s, beam.DropKey(s, beam.AddFixedKey(s, sum)), 10)

	return p
}

// EmitParDoAfterGBK generates a pipeline with a emit-form
// ParDo after a GBK. See: BEAM-3978 and BEAM-4175.
func EmitParDoAfterGBK() *beam.Pipeline {
	p, s, col := ptest.Create([]interface{}{1, 2, 3, 4})

	keyed := beam.GroupByKey(s, beam.AddFixedKey(s, col))
	sum := beam.ParDo(s, emitCountFn, keyed)
	passert.Equals(s, beam.DropKey(s, beam.AddFixedKey(s, sum)), 10)

	return p
}
