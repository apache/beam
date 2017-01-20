package main

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
	"runtime/pprof"
	"strings"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input = flag.String("input", os.ExpandEnv("$GOPATH/src/github.com/apache/beam/sdks/go/data/shakespeare/kinglear.txt"), "Files to read.")

	profile    = flag.String("profile", "wordcount.pprof", "Profile output.")
	iterations = flag.Int("n", 1000, "Number of iterations.")
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

func Drop(in <-chan string) {
	for _ = range in {
		// nop
	}
}

func build(p *beam.Pipeline) error {
	lines, err := textio.Immediate(p, *input)
	if err != nil {
		return err
	}
	col, err := beam.ParDo1(p, Extract, lines)
	if err != nil {
		return err
	}
	counted, err := count.PerElement(p, col)
	if err != nil {
		return err
	}
	formatted, err := beam.ParDo1(p, Format2, counted)
	if err != nil {
		return err
	}
	return beam.ParDo0(p, Drop, formatted)
}

func run(p *beam.Pipeline, n int) error {
	f, err := os.Create(*profile)
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		return err
	}
	defer pprof.StopCPUProfile()

	for i := 0; i < n; i++ {
		if err := local.Execute(context.Background(), p); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.Parse()

	log.Printf("Running wordcount x%v", *iterations)

	p := beam.NewPipeline()
	if err := build(p); err != nil {
		log.Fatalf("Failed to constuct pipeline: %v", err)
	}
	if err := run(p, *iterations); err != nil {
		log.Fatalf("Failed to run pipeline: %v", err)
	}
	log.Print("Success!")
}
