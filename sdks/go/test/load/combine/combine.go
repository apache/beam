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
	"bytes"
	"context"
	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/synthetic"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/test/load"
)

var (
	fanout = flag.Int(
		"fanout",
		1,
		"A number of combine operations to perform in parallel.")
	topCount = flag.Int(
		"top_count",
		20,
		"A number of greatest elements to extract from the PCollection.")
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
	register.Function2x1(compareLess)
	register.Function3x0(getElement)
	register.Emitter2[[]byte, []byte]()
}

func compareLess(key []byte, value []byte) bool {
	return bytes.Compare(key, value) < 0
}

func getElement(key []byte, value [][]byte, emit func([]byte, []byte)) {
	emit(key, value[0])
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()
	src := synthetic.SourceSingle(s, parseSyntheticConfig())
	src = beam.ParDo(s, &load.RuntimeMonitor{}, src)
	for i := 0; i < *fanout; i++ {
		pcoll := top.LargestPerKey(s, src, *topCount, compareLess)
		pcoll = beam.ParDo(s, getElement, pcoll)
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
