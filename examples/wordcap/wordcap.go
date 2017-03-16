package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"log"
	"os"
	"regexp"
	"strings"
)

var input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func Extract(lines <-chan string, out chan<- string) {
	for line := range lines {
		for _, word := range wordRE.FindAllString(line, -1) {
			out <- word
		}
	}
}

func Cap(words <-chan string, out chan<- string) {
	for word := range words {
		out <- strings.ToUpper(word)
	}
}

func Drop(elms <-chan string) {
	i := 0
	for elm := range elms {
		log.Printf("Word[%v]: %s", i, elm)
		i++
	}
	log.Printf("Processed: %v", i)
}

func main() {
	flag.Parse()
	ctx := context.Background()
	beamexec.Init(ctx)

	log.Print("Running wordcap")

	p := beam.NewPipeline()

	// WordCap is an I/O-free, linear pipeline.

	lines, _ := textio.Immediate(p, *input)
	words := beam.ParDo(p, Extract, lines)
	cap := beam.ParDo(p, Cap, words)
	beam.ParDo0(p, Drop, cap)

	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
