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

func init() {
	register.DoFn4x0[[]byte, []byte, func(*[]byte, *[]byte) bool, func([]byte, []byte)]((*iterSideInputFn)(nil))
	register.Emitter2[[]byte, []byte]()
	register.Iter2[[]byte, []byte]()
	register.Function2x0(impToKV)
}

var (
	accessPercentage = flag.Int(
		"access_percentage",
		100,
		"Specifies the percentage of elements in the side input to be accessed.")
	syntheticSourceConfig = flag.String(
		"input_options",
		"",
		"A JSON object that describes the configuration for synthetic source")
)

func parseSyntheticConfig() synthetic.SourceConfig {
	if *syntheticSourceConfig == "" {
		panic("--input_options not provided")
	} else {
		encoded := []byte(*syntheticSourceConfig)
		return synthetic.DefaultSourceConfig().BuildFromJSON(encoded)
	}
}

// impToKV just turns an impulse signal into a KV instead of
// adding a single value input version of RuntimeMonitor
func impToKV(imp []byte, emit func([]byte, []byte)) {
	emit(imp, imp)
}

type iterSideInputFn struct {
	ElementsToAccess int64
}

func (fn *iterSideInputFn) ProcessElement(_, _ []byte, values func(*[]byte, *[]byte) bool, emit func([]byte, []byte)) {
	var key, value []byte
	var i int64
	for values(&key, &value) {
		if i >= fn.ElementsToAccess {
			break
		}
		emit(key, value)
		i++
	}
}

func main() {
	flag.Parse()
	beam.Init()
	ctx := context.Background()
	p, s := beam.NewPipelineWithRoot()

	syntheticConfig := parseSyntheticConfig()
	elementsToAccess := syntheticConfig.NumElements * int64(float64(*accessPercentage)/float64(100))

	src := synthetic.SourceSingle(s, syntheticConfig)

	imp := beam.Impulse(s)
	impKV := beam.ParDo(s, impToKV, imp)
	monitored := beam.ParDo(s, &load.RuntimeMonitor{}, impKV)

	useSide := beam.ParDo(
		s,
		&iterSideInputFn{ElementsToAccess: elementsToAccess},
		monitored,
		beam.SideInput{Input: src})

	beam.ParDo(s, &load.RuntimeMonitor{}, useSide)

	presult, err := beamx.RunWithMetrics(ctx, p)
	if err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}

	if presult != nil {
		metrics := presult.Metrics().AllMetrics()
		load.PublishMetrics(metrics)
	}
}
