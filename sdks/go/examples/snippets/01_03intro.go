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

package snippets

import (
	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
)

// PipelineConstruction contains snippets for the initial sections of
// the Beam Programming Guide, from initializing to submitting a
// pipeline.
func PipelineConstruction() {
	// [START pipeline_options]
	// If beamx or Go flags are used, flags must be parsed first,
	// before beam.Init() is called.
	flag.Parse()
	// [END pipeline_options]

	// [START pipelines_constructing_creating]
	// beam.Init() is an initialization hook that must be called
	// near the beginning of main(), before creating a pipeline.
	beam.Init()

	// Create the Pipeline object and root scope.
	pipeline, scope := beam.NewPipelineWithRoot()
	// [END pipelines_constructing_creating]

	// [START pipelines_constructing_reading]
	// Read the file at the URI 'gs://some/inputData.txt' and return
	// the lines as a PCollection<string>.
	// Notice the scope as the first variable when calling
	// the method as is needed when calling all transforms.
	lines := textio.Read(scope, "gs://some/inputData.txt")

	// [END pipelines_constructing_reading]

	_ = []any{pipeline, scope, lines}
}

// Create demonstrates using beam.CreateList.
func Create() {
	// [START model_pcollection]
	lines := []string{
		"To be, or not to be: that is the question: ",
		"Whether 'tis nobler in the mind to suffer ",
		"The slings and arrows of outrageous fortune, ",
		"Or to take arms against a sea of troubles, ",
	}

	// Create the Pipeline object and root scope.
	// It's conventional to use p as the Pipeline variable and
	// s as the scope variable.
	p, s := beam.NewPipelineWithRoot()

	// Pass the slice to beam.CreateList, to create the pcollection.
	// The scope variable s is used to add the CreateList transform
	// to the pipeline.
	linesPCol := beam.CreateList(s, lines)
	// [END model_pcollection]
	_ = []any{p, linesPCol}
}

// PipelineOptions shows basic pipeline options using flags.
func PipelineOptions() {
	// [START pipeline_options_define_custom]
	// Use standard Go flags to define pipeline options.
	var (
		input  = flag.String("input", "", "")
		output = flag.String("output", "", "")
	)
	// [END pipeline_options_define_custom]

	_ = []any{input, output}
}

// PipelineOptionsCustom shows slightly less basic pipeline options using flags.
func PipelineOptionsCustom() {
	// [START pipeline_options_define_custom_with_help_and_default]
	var (
		input  = flag.String("input", "gs://my-bucket/input", "Input for the pipeline")
		output = flag.String("output", "gs://my-bucket/output", "Output for the pipeline")
	)
	// [END pipeline_options_define_custom_with_help_and_default]

	_ = []any{input, output}
}
