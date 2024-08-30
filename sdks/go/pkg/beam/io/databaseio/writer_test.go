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

package databaseio

import (
	"fmt"
	"testing"
)

func TestValueTemplateGenerator_generate(t *testing.T) {
	tests := []struct {
		generator   *valueTemplateGenerator
		rowCount    int
		columnCount int
		expected    string
	}{
		{
			generator:   &valueTemplateGenerator{"postgres"},
			rowCount:    4,
			columnCount: 3,
			expected:    "($1,$2,$3),($4,$5,$6),($7,$8,$9),($10,$11,$12)",
		},
		{
			generator:   &valueTemplateGenerator{"postgres"},
			rowCount:    0,
			columnCount: 10,
			expected:    "",
		},
		{
			generator:   &valueTemplateGenerator{"pgx"},
			rowCount:    4,
			columnCount: 3,
			expected:    "($1,$2,$3),($4,$5,$6),($7,$8,$9),($10,$11,$12)",
		},
		{
			generator:   &valueTemplateGenerator{"mysql"},
			rowCount:    4,
			columnCount: 3,
			expected:    "(?,?,?),(?,?,?),(?,?,?),(?,?,?)",
		},
		{
			generator:   &valueTemplateGenerator{"mysql"},
			rowCount:    0,
			columnCount: 10,
			expected:    "",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("(generator[%v], rowCount = %v, columnCount = %v)",
			test.generator.driver, test.rowCount, test.columnCount), func(t *testing.T) {
			result := test.generator.generate(test.rowCount, test.columnCount)
			if result != test.expected {
				t.Fatalf("generator[%v] generated unexpected values string. got \"%v\", want \"%v\"",
					test.generator.driver, result, test.expected)
			}
		})
	}
}
