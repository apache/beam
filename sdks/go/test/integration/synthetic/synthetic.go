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

// Package synthetic contains pipelines for testing synthetic steps and sources.
package synthetic

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/synthetic"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
)

// SimplePipeline creates a very simple synthetic pipeline to test that basic
// synthetic pipelines work.
func SimplePipeline() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()
	const size = 100

	src := synthetic.SourceSingle(s,
		synthetic.DefaultSourceConfig().NumElements(size).Build())
	step := synthetic.Step(s, synthetic.DefaultStepConfig().Build(), src)
	passert.Count(s, step, "out", size)

	return p
}

// SplittablePipeline creates a simple synthetic pipeline that exercises
// splitting-related behavior.
func SplittablePipeline() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()
	const srcSize1 = 50
	const srcSize2 = 10
	const stepMult = 500
	const outCount = (srcSize1 + srcSize2) * stepMult

	configs := beam.Create(s,
		synthetic.DefaultSourceConfig().NumElements(srcSize1).InitialSplits(3).Build(),
		synthetic.DefaultSourceConfig().NumElements(srcSize2).InitialSplits(3).Build())
	src := synthetic.Source(s, configs)
	step := synthetic.Step(
		s,
		synthetic.
			DefaultStepConfig().
			OutputPerInput(stepMult).
			Splittable(true).
			InitialSplits(8).
			Build(),
		src)
	passert.Count(s, step, "out", outCount)

	return p
}
