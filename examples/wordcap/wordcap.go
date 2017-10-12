package main

import (
	"context"
	"flag"
	"os"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
	short = flag.Bool("short", false, "Filter out long words.")
)

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	log.Info(ctx, "Running wordcap")

	// Construct an I/O-free, linear pipeline.
	p := beam.NewPipeline()

	lines, err := textio.Immediate(p, *input) // Embedded data. Go flags as parameters.
	if err != nil {
		log.Exitf(ctx, "Failed to read %v: %v", *input, err)
	}
	words := beam.ParDo(p, extractFn, lines)     // Named function.
	cap := beam.ParDo(p, strings.ToUpper, words) // Library function.
	if *short {
		// Conditional pipeline construction. Function literals.
		cap = filter.Include(p, cap, func(s string) bool {
			return len(s) < 5
		})
	}
	debug.Print(p, cap) // Debug helper.

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
