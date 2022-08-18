
package main

import (
	"beam.apache.org/learning/katas/core_transforms/additional_outputs/additional_outputs/pkg/task"
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
	input := beam.Create(s, 10, 50, 120, 20, 200, 0)

    // The applyTransform() converts [input] to [numBelow100] and [numAbove100]
	numBelow100, numAbove100 := applyTransform(s, input)

	debug.Printf(s, "Number <= 100: %v", numBelow100)
	debug.Printf(s, "Number > 100: %v", numAbove100)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// The function has multiple outputs, numbers above 100 and below
func applyTransform(s beam.Scope, input beam.PCollection) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, func(element int, numBelow100, numAbove100 func(int)) {
		if element <= 100 {
			numBelow100(element)
			return
		}
		numAbove100(element)
	}, input)
}
