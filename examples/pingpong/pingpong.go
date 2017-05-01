package main

import (
	"context"
	"errors"
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
	input  = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
	output = flag.String("output", "/tmp/pingpong/out.", "Prefix of output.")
)

// PingPong constructs a convoluted pipeline with two "cyclic" composites.
func PingPong(p *beam.Pipeline) error {
	lines, err := textio.Read(p, *input)
	if err != nil {
		return err
	}
	words, err := beam.ParDo(p, extractFn, lines)
	if err != nil {
		return err
	}

	// Run baseline and stitch; then compare them.

	small, big, err := beam.ParDo2(p, multiFn, words, beam.SideInput{Input: words})
	if err != nil {
		return err
	}
	small2, big2, err := stitch(p, words)
	if err != nil {
		return err
	}

	if err := subset(p, small, small2); err != nil {
		return err
	}
	if err := subset(p, big2, big); err != nil {
		return err
	}

	if err := textio.Write(p, *output, small2); err != nil {
		return err
	}
	return textio.Write(p, *output, big2)
}

// stitch constructs two composite PTranformations that provide input to each other. It
// is a (deliberately) complex DAG to show what kind of structures are possible.
func stitch(p *beam.Pipeline, words beam.PCollection) (beam.PCollection, beam.PCollection, error) {
	ping := p.Composite("ping")
	pong := ping // p.Composite("pong")

	// TODO(herohde) 2/23/2017: Dataflow UX seems to have limited support for composite
	// structures. Fails to display a graph if "pong" above is used.

	small1, big1, err := beam.ParDo2(ping, multiFn, words, beam.SideInput{Input: words}) // self-sample (ping)
	if err != nil {
		return beam.PCollection{}, beam.PCollection{}, err
	}
	small2, big2, err := beam.ParDo2(pong, multiFn, words, beam.SideInput{Input: big1}) // big-sample  (pong). More words are small.
	if err != nil {
		return beam.PCollection{}, beam.PCollection{}, err
	}
	_, big3, err := beam.ParDo2(ping, multiFn, big2, beam.SideInput{Input: small1}) // small-sample big (ping). All words are big.
	if err != nil {
		return beam.PCollection{}, beam.PCollection{}, err
	}
	small4, _, err := beam.ParDo2(pong, multiFn, small2, beam.SideInput{Input: big3}) // big-sample small (pong). All words are small.
	if err != nil {
		return beam.PCollection{}, beam.PCollection{}, err
	}
	return small4, big3, nil
}

// Slice side input.

func multiFn(word string, sample []string, small, big func(string)) error {
	// TODO: side input processing into start bundle, once supported.

	count := 0
	size := 0
	for _, w := range sample {
		count++
		size += len(w)
	}
	if count == 0 {
		return errors.New("Empty sample")
	}
	avg := size / count

	if len(word) < avg {
		small(word)
	} else {
		big(word)
	}
	return nil
}

func subset(p *beam.Pipeline, a, b beam.PCollection) error {
	return beam.ParDo0(p, subsetFn, debug.Tick(p), beam.SideInput{Input: a}, beam.SideInput{Input: b})
}

func subsetFn(_ string, a, b func(*string) bool) error {
	larger := make(map[string]bool)
	var elm string
	for b(&elm) {
		larger[elm] = true
	}
	for a(&elm) {
		if !larger[elm] {
			return fmt.Errorf("Extra element: %v", elm)
		}
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

	log.Print("Running pingpong")

	p := beam.NewPipeline()
	if err := PingPong(p); err != nil {
		log.Fatalf("Failed to construct job: %v", err)
	}
	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
