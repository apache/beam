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

package test

import (
	"beam.apache.org/learning/katas/io/textio/read/pkg/task"
	"beam.apache.org/learning/katas/io/textio/read/testdata"
	"github.com/apache/beam/sdks/go/pkg/beam"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"testing"
)

var filePath = testdata.Path("countries.txt")

func TestRead(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	tests := []struct {
		input string
		want  []interface{}
	}{
		{
			input: filePath,
			want: []interface{}{
				"Singapore",
				"United States",
				"Australia",
				"England",
				"France",
				"China",
				"Indonesia",
				"Mexico",
				"Germany",
				"Japan",
			},
		},
	}
	for _, tt := range tests {
		got := task.Read(s, tt.input)

		passert.Equals(s, got, tt.want...)

		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	tests := []struct {
		input beam.PCollection
		want []interface{}
	} {
		{
			input: textio.Read(s, filePath),
			want:  []interface{}{
				"SINGAPORE",
				"UNITED STATES",
				"AUSTRALIA",
				"ENGLAND",
				"FRANCE",
				"CHINA",
				"INDONESIA",
				"MEXICO",
				"GERMANY",
				"JAPAN",
			},
		},
	}
	for _, tt := range tests {
		got := task.ApplyTransform(s, tt.input)
		passert.Equals(s, got, tt.want...)
		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}