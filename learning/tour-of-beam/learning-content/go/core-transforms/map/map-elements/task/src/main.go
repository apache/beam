package main

import (
	"beam.apache.org/learning/katas/common_transforms/filter/filter/pkg/task"
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
    "github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	input := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    // The [input] need to be filtered with the function you wrote
	output := input

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// Write here function ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection

