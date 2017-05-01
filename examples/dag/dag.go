package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/debug"
	"log"
	"os"
	"regexp"
)

var (
	input  = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/shakespeare/kinglear.txt"), "Files to read.")
	output = flag.String("output", "/tmp/dag/out.", "Prefix of output.")
)

// DAG constructs a pipeline with side-input and multiple outout.
func DAG(p *beam.Pipeline) error {
	// Local source.

	lines, err := textio.Read(p, *input)
	if err != nil {
		return err
	}
	words, err := beam.ParDo(p, extractFn, lines)
	if err != nil {
		return err
	}

	avg, err := beam.ParDo(p, avgFn, debug.Tick(p), beam.SideInput{Input: words})
	if err != nil {
		return err
	}

	// Pre-computed side input as singleton. Multiple outputs.

	small, big, err := beam.ParDo2(p, multiFn, words, beam.SideInput{Input: avg})
	if err != nil {
		return err
	}

	// Local sink.

	if err := textio.Write(p, *output, small); err != nil {
		return err
	}
	return textio.Write(p, *output, big)
}

// TODO: side input processing into start bundle, once supported, instead of the
// side input trick.

func avgFn(_ string, sample func(*string) bool) (int, error) {
	count := 0
	size := 0

	var w string
	for sample(&w) {
		count++
		size += len(w)
	}
	if count == 0 {
		return 0, fmt.Errorf("Empty sample")
	}
	avg := size / count

	log.Printf("Sample size: %v, avg: %v", count, avg)
	return avg, nil
}

func multiFn(word string, avg int, small, big func(string)) error {
	if len(word) < avg {
		small(word)
	} else {
		big(word)
	}
	return nil
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()
	beamexec.Init(ctx)

	log.Print("Running dag")

	p := beam.NewPipeline()
	if err := DAG(p); err != nil {
		log.Fatalf("Failed to construct job: %v", err)
	}
	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
