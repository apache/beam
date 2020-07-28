package main

import (
	"context"
	"flag"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

// Concept #2: Defining your own configuration options. Pipeline options can
// just be standard Go flags (or be obtained any other way). Defining and
// configuring the pipeline is normal Go code.
var (
	// By default, this example reads from a public dataset containing the text of
	// King Lear. Set this option to choose a different input file or glob.
	input = flag.String("input", "./in", "File(s) to read.")

	// Set this required option to specify where to write the output.
	output = flag.String("output", "./out", "Output file (required).")
)

func main() {
	// If beamx or Go flags are used, flags must be parsed first.
	flag.Parse()
	// beam.Init() is an initialization hook that must be called on startup. On
	// distributed runners, it is used to intercept control.
	beam.Init()

	// Input validation is done as usual. Note that it must be after Init().
	if *output == "" {
		log.Fatal("No output provided")
	}

	// Concepts #3 and #4: The pipeline uses the named transform and DoFn.
	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)

	external := beam.ExternalTransform{
		In:  []beam.PCollection{lines},
		Urn: "beam:transforms:xlang:count",
	}
	beam.CrossLanguage(s, external)
	// textio.Write(s, *output, formatted)

	// Concept #1: The beamx.Run convenience wrapper allows a number of
	// pre-defined runners to be used via the --runner flag.
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
