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

package wordcount

import (
	"strings"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/memfs"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

func TestWordCount(t *testing.T) {
	tests := []struct {
		lines []string
		words int
		hash  string
	}{
		{
			[]string{
				"foo",
			},
			1,
			"6zZtmVTet7aIhR3wmPE8BA==",
		},
		{
			[]string{
				"foo foo foo",
				"foo foo",
				"foo",
			},
			1,
			"jAk8+k4BOH7vQDUiUZdfWg==",
		},
		{
			[]string{
				"bar bar foo bar foo foo",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==",
		},
		{
			[]string{
				"foo bar foo bar foo bar",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==", // ordering doesn't matter: same hash as above
		},
		{
			[]string{
				"",
				"bar foo bar",
				"  \t ",
				" \n\n\n ",
				"foo bar",
				"       foo",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==", // whitespace doesn't matter: same hash as above
		},
	}

	for _, test := range tests {
		const filename = "memfs://input"
		memfs.Write(filename, []byte(strings.Join(test.lines, "\n")))

		p := WordCount(filename, test.hash, test.words)
		if err := ptest.Run(p); err != nil {
			t.Errorf("WordCount(\"%v\") failed: %v", strings.Join(test.lines, "|"), err)
		}
	}
}
