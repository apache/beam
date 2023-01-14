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
	iterations = flag.Int(
		"iterations",
		1,
		"A number of reiterations over per-key-grouped values to be performed.")
	syntheticConfig = flag.String(
		"input_options",
		"",
		"A JSON object that describes the configuration for the first synthetic source.")
	coSyntheticConfig = flag.String(
		"co_input_options",
		"",
		"A JSON object that describes the configuration for the second synthetic source.")
)

func init() {
	register.DoFn4x0[[]byte, func(*[]byte) bool, func(*[]byte) bool, func([]byte, []byte)]((*ungroupAndReiterateFn)(nil))
	register.Emitter2[[]byte, []byte]()
	register.Iter1[[]byte]()
}

// ungroupAndReiterateFn reiterates given number of times over CoGBK's output.
type ungroupAndReiterateFn struct {
	Iterations int
}

// TODO use re-iterators once supported.

func (fn *ungroupAndReiterateFn) ProcessElement(key []byte, p1values, p2values func(*[]byte) bool, emit func([]byte, []byte)) {
	var value []byte
	for i := 0; i < fn.Iterations; i++ {
		for p1values(&value) {
			// emit output only once
			if i == fn.Iterations-1 {
				emit(key, value)
			}
		}
		for p2values(&value) {
			if i == fn.Iterations-1 {
				emit(key, value)
			}
		}
	}
}

func parseSyntheticConfig(config string) synthetic.SourceConfig {
	if config == "" {
		panic("--input_options and --co_input_options not provided")
	} else {
		encoded := []byte(config)
		return synthetic.DefaultSourceConfig().BuildFromJSON(encoded)
	}
}

func main() {
	flag.Parse()
	beam.Init()
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	src1 := synthetic.SourceSingle(s, parseSyntheticConfig(*syntheticConfig))
	pc1 := beam.ParDo(s, &load.RuntimeMonitor{}, src1)

	src2 := synthetic.SourceSingle(s, parseSyntheticConfig(*coSyntheticConfig))
	pc2 := beam.ParDo(s, &load.RuntimeMonitor{}, src2)

	joined := beam.CoGroupByKey(s, pc1, pc2)
	pc := beam.ParDo(s, &ungroupAndReiterateFn{Iterations: *iterations}, joined)
	beam.ParDo(s, &load.RuntimeMonitor{}, pc)

	presult, err := beamx.RunWithMetrics(ctx, p)
	if err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}

	if presult != nil {
		metrics := presult.Metrics().AllMetrics()
		load.PublishMetrics(metrics)
	}
}
