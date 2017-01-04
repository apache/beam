package main

// See: https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples/blob/master/src/main/java/com/google/cloud/dataflow/examples/WordCount.java

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"beam"
	"count"
	"dataflow"
	"textio"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/*", "Files to read.")
	output = flag.String("output", "gs://foo/wordcount", "Prefix of output.")
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

// WordCount is a concretely-typed KV<string,int>.
type WordCount struct {
	Word  string `beam:"key"`
	Count int    `beam:"value"`
}

// Extract processes one KV element at a time (and can't fail). It's the
// equivalent of a SimpleFunction.
func Format(wc WordCount) string {
	return fmt.Sprintf("%s: %v", wc.Word, wc.Count)
}

// CountWords is a composite transform.
func CountWords(p *beam.Pipeline, lines *beam.PCollection) (*beam.PCollection, error) {
	col, err := beam.ParDo(p, Extract, lines)
	if err != nil {
		return nil, err
	}
	return count.PerElement(p, col)
}

func main() {
	p := &beam.Pipeline{}

	// (1) build pipeline

	lines, err := textio.Read(p, *input)
	if err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	counted, err := CountWords(p, lines)
	if err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	formatted, err := beam.ParDo(p, Format, counted)
	if err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	if err := textio.Write(p, os.ExpandEnv(*output), formatted); err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}

	// (2) execute it on Dataflow

	if err := dataflow.Execute(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
	log.Print("Success!")
}
