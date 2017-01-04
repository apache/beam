package main

import (
	"context"
	"log"
	"regexp"
	"strings"

	"beam"
	"dataflow"
	"textio"
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func Extract(lines <-chan string, emit chan<- string) error {
	for line := range lines {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit <- strings.ToLower(word)
		}
	}
	return nil
}

func main() {
	p := &beam.Pipeline{}

	// GOOD: error handling is idiomatic. Transformations are
	// straightforward function calls.

	lines, err := textio.Read(p, "foo.txt")
	if err != nil {
		log.Fatalf("Bad pipeline: %v", err)
	}
	col, err := beam.ParDo(p, Extract, lines)
	if err != nil {
		log.Fatalf("Bad pipeline: %v", err)
	}
	if err := textio.Write(p, "bar.txt", col); err != nil {
		log.Fatalf("Bad pipeline: %v", err)
	}

	dataflow.Execute(context.Background(), p)
}

func main2() {
	p := &beam.Pipeline{}

	// NEUTRAL: Users can always ignore errors for
	// compactness. We'd fail in p.Run, if pure collection
	// propagation. Given that *PCollection are concrete types,
	// users can add assert helpers (although not idiomatic).

	lines, _ := textio.Read(p, "foo.txt")
	col, _ := beam.ParDo(p, Extract, lines)
	textio.Write(p, "bar.txt", col)

	dataflow.Execute(context.Background(), p)
}

func main3() {
	p := &beam.Pipeline{}

	// GOOD: PCollections can be passed around like any other value.

	var cols []*beam.PCollection
	for i := 0; i < 20; i++ {
		lines, _ := textio.Read(p, "foo.txt")
		cols = append(cols, lines)
	}

	flat, _ := beam.Flatten(p, cols...)
	col, _ := beam.ParDo(p, Extract, flat)
	textio.Write(p, "bar.txt", col)

	dataflow.Execute(context.Background(), p)
}
