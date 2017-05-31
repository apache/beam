package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"log"
	"os"
	"regexp"
	"reflect"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/graphx"
)

var (
	input  = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/shakespeare/kinglear.txt"), "Files to read.")
	output = flag.String("output", "/tmp/dag/out.", "Prefix of output.")
)

func init() {
	graphx.Register(reflect.TypeOf((*average)(nil)).Elem())
}

// TODO(herohde) 5/22/2017: maybe make it more convenient to use the side
// input form here? The below version looks worse for this use-case.

// average computes the average length of a bundle of strings.
type average struct {
	size int
	count int
}

func (a *average) StartBundle() {
	a.size = 0
	a.count = 0
}

func (a *average) ProcessElement(w string, emit func (int) /* emit needed by signature */) {
	a.count++
	a.size += len(w)
}

func (a *average) FinishBundle() (int, error) {
	if a.count == 0 {
		return 0, fmt.Errorf("Empty sample")
	}
	avg := a.size / a.count

	log.Printf("Sample size: %v, average: %v", a.count, avg)
	return avg, nil
}

func splitFn(word string, avg int, small, big func(string)) {
	if len(word) < avg {
		small(word)
	} else {
		big(word)
	}
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

	// Construct a pipeline with side-input and multiple outout.
	p := beam.NewPipeline()

	// Local source.
	lines := textio.Read(p, *input)
	words := beam.ParDo(p, extractFn, lines)

	avg := beam.ParDo(p, &average{}, words)

	// Pre-computed side input as singleton. Multiple outputs.
	small, big := beam.ParDo2(p, splitFn, words, beam.SideInput{Input: avg})

	// Local sinks.
	textio.Write(p, *output, small)
	textio.Write(p, *output, big)

	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
