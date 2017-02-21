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
	"log"
	"os"
	"regexp"
	"strings"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input  = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/shakespeare/kinglear.txt"), "Files to read.")
	output = flag.String("output", "/tmp/wordcount/out.", "Prefix of output.")
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

// KV is a concretely-typed KV<string,int>.
type KV struct {
	Word  string `beam:"key"`
	Count int    `beam:"value"`
}

// Extract processes one KV element at a time (and can't fail). It's the
// equivalent of a SimpleFunction.
func Format(wc KV) string {
	return fmt.Sprintf("%s: %v", wc.Word, wc.Count)
}

func Format2(in <-chan KV, out chan<- string) {
	for kv := range in {
		out <- Format(kv)
	}
}

// CountWords is a composite transform.
func CountWords(p *beam.Pipeline, lines beam.PCollection) beam.PCollection {
	return beam.Composite(p, "CountWords", func(p *beam.Pipeline) beam.PCollection {
		col := beam.ParDo(p, Extract, lines)
		return count.PerElement(p, col)
	})
}

func main() {
	flag.Parse()
	ctx := context.Background()
	beamexec.Init(ctx)

	log.Print("Running wordcount")

	// (1) build pipeline

	p := beam.NewPipeline()

	lines := textio.Read(p, *input)
	counted := CountWords(p, lines)
	formatted := beam.ParDo(p, Format2, counted)
	textio.Write(p, os.ExpandEnv(*output), formatted)

	// (2) execute it locally

	if err := beamexec.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
	log.Print("Success!")
}
