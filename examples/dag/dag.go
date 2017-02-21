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

func main() {
	flag.Parse()
	ctx := context.Background()
	beamexec.Init(ctx)

	p := beam.NewPipeline()

	lines := textio.Read(p, *input)
	words := beam.ParDo(p, Extract, lines)

	small, big := beam.ParDo2(p, Multi, words, beam.SideInput{Input: words})

	textio.Write(p, *output, small)
	textio.Write(p, *output, big)

	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
