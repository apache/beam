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

// This is GroupByKey load test with Synthetic Source. Besides of the standard
// input options there are additional options:
// * fanout (optional) - number of GBK operations to run in parallel
// * iterations (optional) - number of reiterations over per-key-grouped
// values to perform
// input_options - options for Synthetic Sources.

// Example test run:

// go run sdks/go/test/load/group_by_key_test/group_by_key.go  \
// --fanout=1
// --iterations=1
// --input_options='{
// \"num_records\": 300,
// \"key_size\": 5,
// \"value_size\": 15,
// \"num_hot_keys\": 30,
// \"hot_key_fraction\": 0.5
// }'"

package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/synthetic"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/test/load"
)

var (
	fanout = flag.Int(
		"fanout",
		1,
		"Fanout")
	iterations = flag.Int(
		"iterations",
		1,
		"A number of subsequent ParDo transforms to be performed")
	syntheticSourceConfig = flag.String(
		"input_options",
		"",
		"A JSON object that describes the configuration for synthetic source")
)

func parseSyntheticSourceConfig() synthetic.SourceConfig {
	if *syntheticSourceConfig == "" {
		panic("--input_options not provided")
	} else {
		encoded := []byte(*syntheticSourceConfig)
		return synthetic.DefaultSourceConfig().BuildFromJSON(encoded)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()
	src := synthetic.SourceSingle(s, parseSyntheticSourceConfig())
	pcoll := beam.ParDo(s, &load.RuntimeMonitor{}, src)
	for i := 0; i < *fanout; i++ {
		pcoll = beam.GroupByKey(s, src)
		beam.ParDo(s, func(key []uint8, values func(*[]uint8) bool) ([]uint8, []uint8) {
			for i := 0; i < *iterations; i++ {
				var value []uint8
				for values(&value) {
					if i == *iterations-1 {
						return key, value
					}
				}
			}
			return key, []uint8{0}
		}, pcoll)
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
