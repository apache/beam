package main

// See: https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples/blob/master/src/main/java/com/google/cloud/dataflow/examples/WordCount.java

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/count"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/debug"
	"log"
	"os"
	"regexp"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/haiku/old_pond.txt"), "Files to read.")
)

// WordCount constructs a pipeline to count words.
func WordCount(p *beam.Pipeline) error {
	lines, err := textio.Immediate(p, *input)
	if err != nil {
		return err
	}
	counted, err := CountWords(p, lines)
	if err != nil {
		return err
	}
	formatted, err := beam.ParDo(p, formatFn, counted)
	if err != nil {
		return err
	}
	return debug.Print0(p, formatted)
}

// CountWords is a composite transform.
func CountWords(p *beam.Pipeline, lines beam.PCollection) (beam.PCollection, error) {
	p = p.Composite("CountWords")

	col, err := beam.ParDo(p, extractFn, lines)
	if err != nil {
		return beam.PCollection{}, err
	}
	return count.PerElement(p, col)
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
	ctx := context.Background()
	beamexec.Init(ctx)

	log.Print("Running wordcount")

	p := beam.NewPipeline()
	if err := WordCount(p); err != nil {
		log.Fatalf("Failed to construct job: %v", err)
	}
	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
