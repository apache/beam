package main

import (
	"context"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
    "github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

func main() {
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	input := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	output := ApplyTransform(s, input)

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}

func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return stats.Min(s, input)
}