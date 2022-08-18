package main

import (
	"beam.apache.org/learning/katas/core_transforms/groupbykey/groupbykey/pkg/task"
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
	input := beam.Create(s, "apple", "ball", "car", "bear", "cheetah", "ant")

    // The applyTransform() converts [input] to [output]
	output := applyTransform(s, input)

	debug.Print(s, output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// The method returns a map which key will be the first letter, and the values are a list of words
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	kv := beam.ParDo(s, func(element string) (string, string) {
		return string(element[0]), element
	}, input)
	return beam.GroupByKey(s, kv)
}
