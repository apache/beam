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

package beam_test

import (
	"context"
	"regexp"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
)

// A beam_test global context var to improve how the examples look.
var ctx = context.Background()

func ctxWithPtransformID(id string) context.Context {
	ctx := context.Background()
	ctx = metrics.SetBundleID(ctx, "exampleBundle")
	ctx = metrics.SetPTransformID(ctx, id)
	return ctx
}

var (
	wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
)

func Example_metricsDeclaredAnywhere() {

	// Metrics can be declared outside DoFns, and used inside..
	outside := beam.NewCounter("example.namespace", "count")

	extractWordsDofn := func(ctx context.Context, line string, emit func(string)) {
		// They can be defined at time of use within a DoFn, if necessary.
		inside := beam.NewDistribution("example.namespace", "characters")
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
			outside.Inc(ctx, 1)
			inside.Update(ctx, int64(len(word)))
		}
	}
	ctx := ctxWithPtransformID("example")
	extractWordsDofn(ctx, "this has six words in it", func(string) {})
	extractWordsDofn(ctx, "this has seven words in it, see?", func(string) {})

	metrics.DumpToOutFromContext(ctx)
	// Output: PTransformID: "example"
	//	example.namespace.characters - count: 13 sum: 43 min: 2 max: 5
	//	example.namespace.count - value: 13
}

func Example_metricsReusable() {

	// Metric proxies can be used in multiple DoFns
	c := beam.NewCounter("example.reusable", "count")

	extractWordsDofn := func(ctx context.Context, line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
			c.Inc(ctx, 1)
		}
	}

	extractRunesDofn := func(ctx context.Context, line string, emit func(rune)) {
		for _, r := range line {
			emit(r)
			c.Inc(ctx, 1)
		}
	}
	ctx = metrics.SetBundleID(ctx, "exampleBundle")
	extractWordsDofn(metrics.SetPTransformID(ctx, "extract1"), "this has six words in it", func(string) {})

	extractRunesDofn(metrics.SetPTransformID(ctx, "extract2"), "seven thousand", func(rune) {})

	metrics.DumpToOutFromContext(ctx)
	// Output: PTransformID: "extract1"
	//	example.reusable.count - value: 6
	// PTransformID: "extract2"
	//	example.reusable.count - value: 14
}

func ExampleCounter_Inc() {
	c := beam.NewCounter("example", "size")
	c.Inc(ctx, int64(len("foobar")))
}

func ExampleCounter_Dec() {
	c := beam.NewCounter("example", "size")
	c.Dec(ctx, int64(len("foobar")))
}

func ExampleDistribution_Update() {
	t := time.Millisecond * 42
	d := beam.NewDistribution("example", "latency_micros")
	d.Update(ctx, int64(t/time.Microsecond))
}

func ExampleGauge_Set() {
	g := beam.NewGauge("example", "progress")
	g.Set(ctx, 42)
}
