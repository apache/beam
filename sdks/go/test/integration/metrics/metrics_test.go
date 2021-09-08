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

package metrics

import (
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/memfs"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
	"github.com/apache/beam/sdks/v2/go/test/integration/wordcount"
)

func TestMetrics(t *testing.T) {
	tests := []struct {
		lines    []string
		words    int
		hash     string
		name     string
		counters int
	}{
		{
			[]string{
				"bar bar foo bar foo foo",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==",
			"smallWords",
			1,
		},
		{
			[]string{
				"bar bar foo bar foo foo",
				"",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==",
			"words",
			0,
		},
	}

	for _, test := range tests {
		integration.CheckFilters(t)
		const filename = "memfs://input"
		memfs.Write(filename, []byte(strings.Join(test.lines, "\n")))

		p := wordcount.WordCount(filename, test.hash, test.words)
		pr, err := ptest.RunWithMetrics(p)
		if err != nil {
			t.Errorf("Wordcount with Metrics failed (\"%v\") failed: %v", strings.Join(test.lines, "|"), err)
		}

		qr := pr.Metrics().Query(func(sr metrics.SingleResult) bool {
			return sr.Name() == test.name
		})
		if len(qr.Counters()) != test.counters {
			t.Errorf("Metrics filtering with Name failed.\nGot %d counters \n Want %d counters", len(qr.Counters()), test.counters)
		}
	}
}

func TestMain(m *testing.M) {
	ptest.Main(m)
}
