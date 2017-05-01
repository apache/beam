package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/debug"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
	"log"
	"os"
	"regexp"
	"strings"
)

var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
	short = flag.Bool("short", false, "Filter out long words.")
)

// WordCap construts an I/O-free, linear pipeline.
func WordCap(p *beam.Pipeline) error {
	// Embedded data. Go flags as parameters.

	lines, err := textio.Immediate(p, *input)
	if err != nil {
		return err
	}

	// Named function.

	words, err := beam.ParDo(p, extractFn, lines)
	if err != nil {
		return err
	}

	// Library function.

	cap, err := beam.ParDo(p, strings.ToUpper, words)
	if err != nil {
		return err
	}

	if *short {
		// Conditional pipeline construction. Function literals.

		filtered, err := filter.Filter(p, cap, func(s string) bool {
			return len(s) < 5
		})
		if err != nil {
			return err
		}
		cap = filtered
	}

	// Debug helper.

	return debug.Print0(p, cap)
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

	log.Print("Running wordcap")

	p := beam.NewPipeline()
	if err := WordCap(p); err != nil {
		log.Fatalf("Failed to construct job: %v", err)
	}
	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
