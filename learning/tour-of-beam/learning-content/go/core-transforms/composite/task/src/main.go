
package main

import (
	"beam.apache.org/learning/katas/core_transforms/composite/composite/pkg/common"
	"beam.apache.org/learning/katas/core_transforms/composite/composite/pkg/task"
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	// Getting a list of items
	input := createLines(s)

    // The applyTransform() converts [input] to [output]
    output := applyTransform(s, input)

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// Divides sentences into words and returns their numbers
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	s = s.Scope("CountCharacters")
	characters := extractNonSpaceCharacters(s, input)
	return stats.Count(s, characters)
}

// Nested logic that collects characters
func extractNonSpaceCharacters(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(string)){
		for _, k := range line {
			char := string(k)
			if char != " " {
				emit(char)
			}
		}
	}, input)
}

// Function for create list of elements
func CreateLines(s beam.Scope) beam.PCollection {
	return beam.Create(s,
		"Apache Beam is an open source unified programming model",
		"to define and execute data processing pipelines")
}
