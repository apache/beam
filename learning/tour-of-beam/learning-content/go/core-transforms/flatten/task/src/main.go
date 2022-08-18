package main

import (
	"beam.apache.org/learning/katas/core_transforms/flatten/flatten/pkg/task"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
	"context"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

    // List of elements start with a
    aWords := beam.Create(s, "apple", "ant", "arrow")

    // List of elements start with b
	bWords := beam.Create(s, "ball", "book", "bow")


	// The applyTransform() converts [aWords] and [bWords] to [output]
	output := applyTransform(s, aWords, bWords)

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// The applyTransform two PCollection data types are the same combines and returns one PCollection
func applyTransform(s beam.Scope, aInputs beam.PCollection, bInputs beam.PCollection) beam.PCollection {
	return beam.Flatten(s, aInputs, bInputs)
}