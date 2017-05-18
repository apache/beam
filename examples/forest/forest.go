package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/count"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/debug"
	"log"
	"os"
	"strings"
)

var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
	n     = flag.Int("count", 2, "Number of trees")
	depth = flag.Int("depth", 3, "Depth of each tree")
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

func leaf(p *beam.Pipeline) beam.PCollection {
	lines, err := textio.Immediate(p, *input)
	if err != nil {
		log.Fatalf("Failed to read %v: %v", *input, err)
	}
	return beam.ParDo(p, strings.ToUpper, lines)
}

func main() {
	flag.Parse()
	ctx := context.Background()
	beamexec.Init(ctx)

	log.Print("Running forest")

	// Build a forest of processing nodes with flatten "branches".
	p := beam.NewPipeline()
	for i := 0; i < *n; i++ {
		t := tree(p, *depth)
		debug.Print(p, count.Dedup(p, t))
	}

	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
