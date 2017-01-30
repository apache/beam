package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"log"
	"os"
	"regexp"
	"strings"
)

var (
	input  = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/shakespeare/kinglear.txt"), "File to read.")
	output = flag.String("output", "/tmp/pingpong/out.", "Prefix of output.")
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

// Extract processes a bundle at a time.
func Extract(lines <-chan string, out chan<- string) error {
	for line := range lines {
		for _, word := range wordRE.FindAllString(line, -1) {
			out <- strings.ToLower(word)
		}
	}
	return nil
}

func Multi(words, sample <-chan string, small, big chan<- string) error {
	count := 0
	size := 0
	for word := range sample {
		count++
		size += len(word)
	}
	if count == 0 {
		return fmt.Errorf("Empty sample")
	}

	avg := size / count
	log.Printf("Sample size: %v, avg: %v", count, avg)

	for word := range words {
		if len(word) < avg {
			small <- word
		} else {
			big <- word
		}
	}
	return nil
}

func Subset(a, b <-chan string) {
	larger := make(map[string]bool)
	for elm := range b {
		larger[elm] = true
	}

	for elm := range a {
		if !larger[elm] {
			panic(fmt.Sprintf("Extra element: %v", elm))
		}
	}
}

func subset(p *beam.Pipeline, a, b beam.PCollection) error {
	return beam.ParDo0(p, Subset, a, beam.SideInput{Input: b})
}

func Drop(elms <-chan string) {
	i := 0
	for range elms {
		i++
	}
	log.Printf("Dropped: %v", i)
}

func drop(p *beam.Pipeline, empty beam.PCollection) error {
	return beam.ParDo0(p, Drop, empty)
}

// stitch constructs two composite PTranformations that provide input to each other. It
// is a (deliberately) complex DAG to show what kind of structures are possible.
func stitch(p *beam.Pipeline, words beam.PCollection) (beam.PCollection, beam.PCollection) {
	ping := p.Composite("ping")
	pong := p.Composite("pong")

	small1, big1, _ := beam.ParDo2(ping, Multi, words, beam.SideInput{Input: words})   // self-sample (ping)
	small2, big2, _ := beam.ParDo2(pong, Multi, words, beam.SideInput{Input: big1})    // big-sample  (pong). More words are small.
	empty3, big3, _ := beam.ParDo2(ping, Multi, big2, beam.SideInput{Input: small1})   // small-sample big (ping). All words are big.
	small4, empty4, _ := beam.ParDo2(pong, Multi, small2, beam.SideInput{Input: big3}) // big-sample small (pong). All words are small.

	drop(p, empty3)
	drop(p, empty4)
	drop(p, small2) // Force buffering of small2. Workaround for inadequate buffering in local.

	return small4, big3
}

func build(p *beam.Pipeline) error {
	lines, _ := textio.Read(p, *input)
	words, _ := beam.ParDo1(p, Extract, lines)

	small, big, _ := beam.ParDo2(p, Multi, words, beam.SideInput{Input: words})
	small2, big2 := stitch(p, words)

	subset(p, small, small2)
	subset(p, big2, big)

	if err := textio.Write(p, *output, small2); err != nil {
		return err
	}
	return textio.Write(p, *output, big2)
}

func main() {
	flag.Parse()

	p := beam.NewPipeline()
	if err := build(p); err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	if err := local.Execute(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
