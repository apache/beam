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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestWordCount(t *testing.T) {
	tests := []struct {
		lines                                                             []string
		words                                                             int
		hash                                                              string
		smallWords                                                        string
		lineLen                                                           string
		smallWordsCount, lineLenCount, lineLenSum, lineLenMin, lineLenMax int64
	}{
		{
			[]string{
				"foo",
			},
			1,
			"6zZtmVTet7aIhR3wmPE8BA==",
			"smallWords",
			"lineLenDistro",
			1,
			1,
			3,
			3,
			3,
		},
		{
			[]string{
				"foo foo foo",
				"foo foo",
				"foo",
			},
			1,
			"jAk8+k4BOH7vQDUiUZdfWg==",
			"smallWords",
			"lineLenDistro",
			6,
			3,
			21,
			3,
			11,
		},
		{
			[]string{
				"bar bar foo bar foo foo",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==",
			"smallWords",
			"lineLenDistro",
			6,
			1,
			23,
			23,
			23,
		},
		{
			[]string{
				"foo bar foo bar foo bar",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==", // ordering doesn't matter: same hash as above
			"smallWords",
			"lineLenDistro",
			6,
			1,
			23,
			23,
			23,
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
			"smallWords",
			"lineLenDistro",
			6,
			6,
			37,
			0,
			11,
		},
	}

	for _, test := range tests {
		integration.CheckFilters(t)
		p, s := beam.NewPipelineWithRoot()
		lines := beam.CreateList(s, test.lines)
		WordCountFromPCol(s, lines, test.hash, test.words)
		pr, err := ptest.RunWithMetrics(p)
		if err != nil {
			t.Errorf("WordCount(\"%v\") failed: %v", strings.Join(test.lines, "|"), err)
		}
		qr := pr.Metrics().Query(func(sr metrics.SingleResult) bool {
			return sr.Name() == test.smallWords
		})
		if qr.Counters()[0].Result() != test.smallWordsCount {
			t.Errorf("Metrics().Query(by Name) failed. Got %d counters, Want %d counters", qr.Counters()[0].Result(), test.smallWordsCount)
		}
		qr = pr.Metrics().Query(func(sr metrics.SingleResult) bool {
			return sr.Name() == test.lineLen
		})
		distributonValue := qr.Distributions()[0].Result()
		if distributonValue.Count != test.lineLenCount || distributonValue.Sum != test.lineLenSum || distributonValue.Max != test.lineLenMax || distributonValue.Min != test.lineLenMin {
			t.Errorf("Metrics().Query(by Name) failed. Got {%d %d %d %d} distributions, Want {%d %d %d %d} distribution", distributonValue.Count, distributonValue.Sum, distributonValue.Min, distributonValue.Max, test.lineLenCount, test.lineLenSum, test.lineLenMin, test.lineLenMax)
		}
	}
}

func TestMain(m *testing.M) {
	ptest.Main(m)
}
