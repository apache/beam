package main

import (
	"beam.apache.org/learning/katas/core_transforms/map/pardo_onetomany/pkg/task"
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

func main() {
	p, s := beam.NewPipelineWithRoot()

    // List of elements
	input := beam.Create(s, "Hello Beam", "It is awesome")

	// The applyTransform() converts [input] to [output]
	output := applyTransform(s, input)

	debug.Print(s, output)

	err := beamx.Run(context.Background(), p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// The applyTransform() using ParDo with tokenize function
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, tokenizeFn, input)
}

// The tokenizeFn() divides a sentence into an array of words
func tokenizeFn(input string, emit func(out string)) {
	tokens := strings.Split(input, " ")
	for _, k := range tokens {
		emit(k)
	}
}

