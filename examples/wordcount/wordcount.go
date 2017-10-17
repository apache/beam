package main

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java

import (
	"context"
	"flag"
	"fmt"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	output = flag.String("output", "", "Output file (required).")
)

// CountWords is a composite transform.
func CountWords(p *beam.Pipeline, lines beam.PCollection) beam.PCollection {
	p = p.Scope("CountWords")

	col := beam.ParDo(p, extractFn, lines)
	return stats.Count(p, col)
}

func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

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

	// Input validation is done as usual. Note that it must be after Init().
	if *output == "" {
		log.Exit(ctx, "No output provided")
	}

	log.Info(ctx, "Running wordcount")

	// Construct a pipeline to count words.
	p := beam.NewPipeline()
	lines := textio.Read(p, *input)
	counted := CountWords(p, lines)
	formatted := beam.ParDo(p, formatFn, counted)
	textio.Write(p, *output, formatted)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
