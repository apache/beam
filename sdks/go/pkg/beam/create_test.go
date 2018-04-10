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

package beam_test

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

type wc struct {
	K string
	V int
}

func TestCreate(t *testing.T) {
	tests := []struct {
		values []interface{}
	}{
		{[]interface{}{1, 2, 3}},
		{[]interface{}{"1", "2", "3"}},
		{[]interface{}{wc{"a", 23}, wc{"b", 42}, wc{"c", 5}}},
	}

	for _, test := range tests {
		p, s, c := ptest.Create(test.values)
		passert.Equals(s, c, test.values...)

		if err := ptest.Run(p); err != nil {
			t.Errorf("beam.Create(%v) failed: %v", test.values, err)
		}
	}
}
