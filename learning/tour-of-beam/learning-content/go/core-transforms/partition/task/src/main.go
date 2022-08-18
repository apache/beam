package main

import (
	"beam.apache.org/learning/katas/core_transforms/partition/partition/pkg/task"
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

    // List of elements
    input := beam.Create(s, 1, 2, 3, 4, 5, 100, 110, 150, 250)

    // The applyTransform() converts [input] to [output[]]
	output := applyTransform(s, input)

	debug.Printf(s, "Number > 100: %v", output[0])
	debug.Printf(s, "Number <= 100: %v", output[1])

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// The applyTransform accepts PCollection and returns the PCollection array
func applyTransform(s beam.Scope, input beam.PCollection) []beam.PCollection {
	return beam.Partition(s, 2, func(element int) int {
		if element > 100 {
			return 0
		}
		return 1
	}, input)
}