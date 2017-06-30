package main

// See: https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples/blob/master/src/main/java/com/google/cloud/dataflow/examples/WordCount.java

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
)

// CountWords is a composite transform.
func CountWords(p *beam.Pipeline, lines beam.PCollection) beam.PCollection {
	p = p.Composite("CountWords")

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
	beamexec.Init()

	log.Print("Running wordcount")

	// Construct a pipeline to count words.
	p := beam.NewPipeline()
	lines, err := textio.Immediate(p, *input)
	if err != nil {
		log.Fatalf("Failed to read %v: %v", *input, err)
	}
	counted := CountWords(p, lines)
	formatted := beam.ParDo(p, formatFn, counted)
	debug.Print(p, formatted)

	if err := beamexec.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
