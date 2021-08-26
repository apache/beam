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
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
)

func Example_gettingStarted() {
	// In order to start creating the pipeline for execution, a Pipeline object is needed.
	p := beam.NewPipeline()
	s := p.Root()

	// The pipeline object encapsulates all the data and steps in your processing task.
	// It is the basis for creating the pipeline's data sets as PCollections and its operations
	// as transforms.

	// The PCollection abstraction represents a potentially distributed,
	// multi-element data set. You can think of a PCollection as “pipeline” data;
	// Beam transforms use PCollection objects as inputs and outputs. As such, if
	// you want to work with data in your pipeline, it must be in the form of a
	// PCollection.

	// Transformations are applied in a scoped fashion to the pipeline. The scope
	// can be obtained from the pipeline object.

	// Start by reading text from an input files, and receiving a PCollection.
	lines := textio.Read(s, "protocol://path/file*.txt")

	// Transforms are added to the pipeline so they are part of the work to be
	// executed.  Since this transform has no PCollection as an input, it is
	// considered a 'root transform'

	// A pipeline can have multiple root transforms
	moreLines := textio.Read(s, "protocol://other/path/file*.txt")

	// Further transforms can be applied, creating an arbitrary, acyclic graph.
	// Subsequent transforms (and the intermediate PCollections they produce) are
	// attached to the same pipeline.
	all := beam.Flatten(s, lines, moreLines)
	wordRegexp := regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	words := beam.ParDo(s, func(line string, emit func(string)) {
		for _, word := range wordRegexp.FindAllString(line, -1) {
			emit(word)
		}
	}, all)
	formatted := beam.ParDo(s, strings.ToUpper, words)
	textio.Write(s, "protocol://output/path", formatted)

	// Applying a transform adds it to the pipeline, rather than executing it
	// immediately.  Once the whole pipeline of transforms is constructed, the
	// pipeline can be executed by a PipelineRunner.  The direct runner executes the
	// transforms directly, sequentially, in this one process, which is useful for
	// unit tests and simple experiments:
	if _, err := direct.Execute(context.Background(), p); err != nil {
		fmt.Printf("Pipeline failed: %v", err)
	}
}

var (
	s = beam.Scope{}
)

func ExampleCreate() {
	beam.Create(s, 5, 6, 7, 8, 9)               // PCollection<int>
	beam.Create(s, []int{5, 6}, []int{7, 8, 9}) // PCollection<[]int>
	beam.Create(s, []int{5, 6, 7, 8, 9})        // PCollection<[]int>
	beam.Create(s, "a", "b", "c")               // PCollection<string>
}

func ExampleCreateList() {
	beam.CreateList(s, []int{5, 6, 7, 8, 9}) // PCollection<int>
}

func ExampleExplode() {
	d := beam.Create(s, []int{1, 2, 3, 4, 5}) // PCollection<[]int>
	beam.Explode(s, d)                        // PCollection<int>
}

func ExampleFlatten() {
	a := textio.Read(s, "...some file path...") // PCollection<string>
	b := textio.Read(s, "...some other file path...")
	c := textio.Read(s, "...some third file path...")

	beam.Flatten(s, a, b, c) // PCollection<String>
}

func ExampleImpulse() {
	beam.Impulse(s) // PCollection<[]byte>
}

func ExampleImpulseValue() {
	beam.ImpulseValue(s, []byte{}) // PCollection<[]byte>
}

func ExampleSideInput() {
	// words and sample are PCollection<string>
	var words, sample beam.PCollection
	// analyzeFn emits values from the primary based on the singleton side input.
	analyzeFn := func(primary string, side string, emit func(string)) {}
	// Use beam.SideInput to declare that the sample PCollection is the side input.
	beam.ParDo(s, analyzeFn, words, beam.SideInput{Input: sample})
}

func ExampleSeq() {
	a := textio.Read(s, "...some file path...") // PCollection<string>

	beam.Seq(s, a,
		strconv.Atoi, // string to int
		func(i int) float64 { return float64(i) }, // int to float64
		math.Signbit, // float64 to bool
	) // PCollection<bool>
}

func ExampleGroupByKey() {
	type Doc struct{}
	var urlDocPairs beam.PCollection             // PCollection<KV<string, Doc>>
	urlToDocs := beam.GroupByKey(s, urlDocPairs) // PCollection<CoGBK<string, Doc>>

	// CoGBK parameters receive an iterator function with all values associated
	// with the same key.
	beam.ParDo0(s, func(key string, values func(*Doc) bool) {
		var cur Doc
		for values(&cur) {
			// ... process all docs having that url ...
		}
	}, urlToDocs) // PCollection<KV<string, []Doc>>
}

// Optionally, a ParDo transform can produce zero or multiple output
// PCollections. Note the use of ParDo2 to specify 2 outputs.
func ExampleParDo_additionalOutputs() {
	var words beam.PCollection  // PCollection<string>
	var cutoff beam.PCollection // Singleton PCollection<int>
	small, big := beam.ParDo2(s, func(word string, cutoff int, small, big func(string)) {
		if len(word) < cutoff {
			small(word)
		} else {
			big(word)
		}
	}, words, beam.SideInput{Input: cutoff})

	_, _ = small, big
}
