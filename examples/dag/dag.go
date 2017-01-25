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
	input  = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/shakespeare/kinglear.txt"), "Files to read.")
	output = flag.String("output", "/tmp/dag/out.", "Prefix of output.")
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

func build(p *beam.Pipeline) error {
	lines, err := textio.Read(p, *input)
	if err != nil {
		return err
	}
	words, err := beam.ParDo1(p, Extract, lines)
	if err != nil {
		return err
	}

	small, big, err := beam.ParDo2(p, Multi, words, beam.SideInput{Input: words})
	if err != nil {
		return err
	}

	if err := textio.Write(p, *output, small); err != nil {
		return err
	}
	return textio.Write(p, *output, big)
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
