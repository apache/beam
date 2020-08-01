package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"

	// Imports to enable correct filesystem access and runner setup in LOOPBACK mode
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/universal"
)

var (
	// Set this option to choose a different input file or glob.
	input = flag.String("input", "./input", "File(s) to read.")

	// Set this required option to specify where to write the output.
	output = flag.String("output", "./output", "Output file (required).")
)

var (
	wordRE  = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty   = beam.NewCounter("extract", "emptyLines")
	lineLen = beam.NewDistribution("extract", "lineLenDistro")
)

// extractFn is a DoFn that emits the words in a given line.
func extractFn(ctx context.Context, line string, emit func(string)) {
	lineLen.Update(ctx, int64(len(line)))
	if len(strings.TrimSpace(line)) == 0 {
		empty.Inc(ctx, 1)
	}
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

// formatFn is a DoFn that formats a word and its count as a string.
func formatFn(w string, c int64) string {
	fmt.Println(w, c)
	return fmt.Sprintf("%s: %v", w, c)
}

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(formatFn)
}

func main() {
	// If beamx or Go flags are used, flags must be parsed first.
	flag.Parse()
	// beam.Init() is an initialization hook that must be called on startup. On
	// distributed runners, it is used to intercept control.
	beam.Init()

	// Input validation is done as usual. Note that it must be after Init().
	if *output == "" {
		log.Fatal("No output provided")
	}

	// Concepts #3 and #4: The pipeline uses the named transform and DoFn.
	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	// Convert lines of text into individual words.
	col := beam.ParDo(s, extractFn, lines)

	// Using Cross-language Count from Python's test expansion service
	// TODO(pskevin): Cleaner using-face API
	outputType := typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int64))
	external := &beam.ExternalTransform{
		In:            []beam.PCollection{col},
		Urn:           "beam:transforms:xlang:count",
		ExpansionAddr: "localhost:8118",
		Out:           []typex.FullType{outputType},
		Bounded:       true, // TODO(pskevin): Infer this value from output PCollection(s) part of the expanded tranform
	}
	counted := beam.CrossLanguage(s, p, external) // TODO(pskevin): Add external transform to Pipeline without passing it to the transform

	formatted := beam.ParDo(s, formatFn, counted[0])
	textio.Write(s, *output, formatted)

	// Concept #1: The beamx.Run convenience wrapper allows a number of
	// pre-defined runners to be used via the --runner flag.
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
