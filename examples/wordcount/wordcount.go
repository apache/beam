package main

// See: https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples/blob/master/src/main/java/com/google/cloud/dataflow/examples/WordCount.java

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
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
func CountWords(p *beam.Pipeline, lines beam.PCollection) (beam.PCollection, error) {
	return beam.Composite1(p, "CountWords", func(p *beam.Pipeline) (beam.PCollection, error) {
		col, err := beam.ParDo1(p, Extract, lines)
		if err != nil {
			return beam.PCollection{}, err
		}
		return count.PerElement(p, col)
	})
}

func Wordcount(p *beam.Pipeline) error {
	lines, err := textio.Read(p, *input)
	if err != nil {
		return err
	}
	counted, err := CountWords(p, lines)
	if err != nil {
		return err
	}
	formatted, err := beam.ParDo1(p, Format2, counted)
	if err != nil {
		return err
	}
	return textio.Write(p, os.ExpandEnv(*output), formatted)
}

func main() {
	flag.Parse()
	log.Print("Running wordcount")

	p := beam.NewPipeline()

	// (1) build pipeline

	if err := Wordcount(p); err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}

	// (2) execute it locally

	if err := local.Execute(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
	log.Print("Success!")
}
