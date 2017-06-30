package main

import (
	"context"
	"flag"
	"log"
	"os"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

// TODO(herohde) 5/30/2017: fully implement https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/AutoComplete.java

var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
	n     = flag.Int("top", 3, "Number of completions")
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

func main() {
	flag.Parse()
	beamexec.Init()

	log.Print("Running autocomplete")

	p := beam.NewPipeline()
	lines, err := textio.Immediate(p, *input)
	if err != nil {
		log.Fatalf("Failed to read %v: %v", *input, err)
	}
	words := beam.ParDo(p, extractFn, lines)

	hits := top.Globally(p, words, *n, func(a, b string) bool {
		return len(a) < len(b)
	})
	debug.Print(p, hits)

	if err := beamexec.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
