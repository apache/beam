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

// This is GroupByKey load test with Synthetic Source.

package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/synthetic"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/test/load"
)

var (
	fanout = flag.Int(
		"fanout",
		1,
		"A number of GroupByKey operations to perform in parallel.")
	iterations = flag.Int(
		"iterations",
		1,
		"A number of reiterations over per-key-grouped values to perform.")
	syntheticConfig = flag.String(
		"input_options",
		"",
		"A JSON object that describes the configuration for synthetic source.")
)

func parseSyntheticConfig() synthetic.SourceConfig {
	if *syntheticConfig == "" {
		panic("--input_options not provided")
	} else {
		encoded := []byte(*syntheticConfig)
		return synthetic.DefaultSourceConfig().BuildFromJSON(encoded)
	}
}

func init() {
	register.DoFn2x2[[]byte, func(*[]byte) bool, []byte, []byte]((*ungroupAndReiterateFn)(nil))
	register.Iter1[[]byte]()
}

// ungroupAndReiterateFn reiterates given number of times over GBK's output.
type ungroupAndReiterateFn struct {
	Iterations int
}

// TODO use re-iterators once supported.

func (fn *ungroupAndReiterateFn) ProcessElement(key []byte, values func(*[]byte) bool) ([]byte, []byte) {
	var value []byte
	for i := 0; i < fn.Iterations; i++ {
		for values(&value) {
			if i == fn.Iterations-1 {
				return key, value
			}
		}
	}
	return key, []byte{0}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()
	src := synthetic.SourceSingle(s, parseSyntheticConfig())
	src = beam.ParDo(s, &load.RuntimeMonitor{}, src)
	for i := 0; i < *fanout; i++ {
		pcoll := beam.GroupByKey(s, src)
		pcoll = beam.ParDo(s, &ungroupAndReiterateFn{*iterations}, pcoll)
		pcoll = beam.ParDo(s, &load.RuntimeMonitor{}, pcoll)
		_ = pcoll
	}

	presult, err := beamx.RunWithMetrics(ctx, p)
	if err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}

	if presult != nil {
		metrics := presult.Metrics().AllMetrics()
		load.PublishMetrics(metrics)
	}
}
