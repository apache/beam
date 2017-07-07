package main

import (
	"context"
	"flag"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dot"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

var (
	n     = flag.Int("count", 2, "Number of trees")
	depth = flag.Int("depth", 3, "Depth of each tree")

	runner = flag.String("runner", "local", "Pipeline runner.")
)

func tree(p *beam.Pipeline, depth int) beam.PCollection {
	if depth <= 0 {
		return leaf(p)
	}
	a := tree(p, depth-1)
	b := tree(p, depth-1)
	c := tree(p, depth-2)
	return beam.Flatten(p, a, b, c)
}

var count = 0

func leaf(p *beam.Pipeline) beam.PCollection {
	count++
	return beam.Create(p, count) // singleton PCollection<int>
}

func main() {
	flag.Parse()
	beam.Init()

	log.Print("Running forest")

	// Build a forest of processing nodes with flatten "branches".
	p := beam.NewPipeline()
	for i := 0; i < *n; i++ {
		t := tree(p, *depth)
		debug.Print(p, t)
	}

	if err := beam.Run(context.Background(), *runner, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
