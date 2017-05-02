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

// Forest builds a forest of processing nodes with flatten "branches".
func Forest(p *beam.Pipeline) error {
	for i := 0; i < *n; i++ {
		t, err := tree(p, *depth)
		if err != nil {
			return err
		}
		deduped, err := count.Dedup(p, t)
		if err != nil {
			return err
		}
		if err := debug.Print0(p, deduped); err != nil {
			return err
		}
	}
	return nil
}

func tree(p *beam.Pipeline, depth int) (beam.PCollection, error) {
	if depth <= 0 {
		return leaf(p)
	}
	a, err := tree(p, depth-1)
	if err != nil {
		return beam.PCollection{}, err
	}
	b, err := tree(p, depth-1)
	if err != nil {
		return beam.PCollection{}, err
	}
	c, err := tree(p, depth-2)
	if err != nil {
		return beam.PCollection{}, err
	}
	return beam.Flatten(p, a, b, c)
}

func leaf(p *beam.Pipeline) (beam.PCollection, error) {
	lines, err := textio.Immediate(p, *input)
	if err != nil {
		return beam.PCollection{}, err
	}
	return beam.ParDo(p, strings.ToUpper, lines)
}

func main() {
	flag.Parse()
	ctx := context.Background()
	beamexec.Init(ctx)

	log.Print("Running forest")

	p := beam.NewPipeline()
	if err := Forest(p); err != nil {
		log.Fatalf("Failed to construct job: %v", err)
	}
	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
