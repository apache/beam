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
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/synthetic"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/test/load"
)

func init() {
	beam.RegisterDoFn(reflect.TypeOf((*doFn)(nil)))
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

type doFn struct {
	ElementsToAccess int
}

func (fn *doFn) ProcessElement(_ []byte, values func(*[]byte, *[]byte) bool, emit func([]byte, []byte)) {
	var key []byte
	var value []byte
	i := 0
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
	elementsToAccess := syntheticConfig.NumElements * *accessPercentage / 100

	src := synthetic.SourceSingle(s, syntheticConfig)
	src = beam.ParDo(s, &load.RuntimeMonitor{}, src)

	src = beam.ParDo(
		s,
		&doFn{ElementsToAccess: elementsToAccess},
		beam.Impulse(s),
		beam.SideInput{Input: src})

	beam.ParDo(s, &load.RuntimeMonitor{}, src)

	presult, err := beamx.RunWithMetrics(ctx, p)
	if err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}

	if presult != nil {
		metrics := presult.Metrics().AllMetrics()
		load.PublishMetrics(metrics)
	}
}
