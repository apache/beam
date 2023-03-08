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
		lines           []string
		words           int
		hash            string
		smallWordsCount int64
		lineLen         metrics.DistributionValue
		transform       string
		transformCount  int
	}{
		{
			[]string{
				"foo",
			},
			1,
			"6zZtmVTet7aIhR3wmPE8BA==",
			1,
			metrics.DistributionValue{Count: 1, Sum: 3, Min: 3, Max: 3},
			"wordcount.extractFn",
			1,
		},
		{
			[]string{
				"foo foo foo",
				"foo foo",
				"foo",
			},
			1,
			"jAk8+k4BOH7vQDUiUZdfWg==",
			6,
			metrics.DistributionValue{Count: 3, Sum: 21, Min: 3, Max: 11},
			"extractFn",
			1,
		},
		{
			[]string{
				"bar bar foo bar foo foo",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==",
			6,
			metrics.DistributionValue{Count: 1, Sum: 23, Min: 23, Max: 23},
			"CountFn",
			1,
		},
		{
			[]string{
				"foo bar foo bar foo bar",
			},
			2,
			"Nz70m/sn3Ep9o484r7MalQ==", // ordering doesn't matter: same hash as above
			6,
			metrics.DistributionValue{Count: 1, Sum: 23, Min: 23, Max: 23},
			"extract",
			1,
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
			6,
			metrics.DistributionValue{Count: 6, Sum: 37, Min: 0, Max: 11},
			"CreateFn",
			0,
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

		qr := pr.Metrics().Query(func(mr beam.MetricResult) bool {
			return mr.Name() == "smallWords"
		})
		counter := metrics.CounterResult{}
		if len(qr.Counters()) != 0 {
			counter = qr.Counters()[0]
		}
		if counter.Result() != test.smallWordsCount {
			t.Errorf("Metrics().Query(by Name) failed. Got %d counters, Want %d counters", counter.Result(), test.smallWordsCount)
		}

		qr = pr.Metrics().Query(func(mr beam.MetricResult) bool {
			return mr.Name() == "lineLenDistro"
		})
		distribution := metrics.DistributionResult{}
		if len(qr.Distributions()) != 0 {
			distribution = qr.Distributions()[0]
		}
		if distribution.Result() != test.lineLen {
			t.Errorf("Metrics().Query(by Name) failed. Got %v distribution, Want %v distribution", distribution.Result(), test.lineLen)
		}
	}
}

func TestMain(m *testing.M) {
	ptest.Main(m)
}
