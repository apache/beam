package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	search = flag.String("search", "", "Only return words that contain this substring.")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*includeFn)(nil)).Elem())
}

// FilterWords returns PCollection<KV<word,count>> with (up to) 10 matching words.
func FilterWords(p *beam.Pipeline, lines beam.PCollection) beam.PCollection {
	p = p.Scope("FilterWords")
	words := beam.ParDo(p, extractFn, lines)
	filtered := beam.ParDo(p, &includeFn{Search: *search}, words)
	counted := stats.Count(p, filtered)
	return debug.Head(p, counted, 10)
}

var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

func extractFn(line string, emit func(string)) {
	for _, w := range wordRE.FindAllString(line, -1) {
		emit(w)
	}
}

// includeFn outputs (word) iif the word contains substring Search.
type includeFn struct {
	Search string `json:"search"`
}

func (f *includeFn) ProcessElement(s string, emit func(string)) {
	if strings.Contains(s, f.Search) {
		emit(s)
	}
}

func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *search == "" {
		log.Exit(ctx, "No search string provided")
	}

	log.Info(ctx, "Running contains")

	// Construct a pipeline that only keeps 10 words that contain the provided search string.
	p := beam.NewPipeline()
	lines := textio.Read(p, *input)
	filtered := FilterWords(p, lines)
	formatted := beam.ParDo(p, formatFn, filtered)
	debug.Print(p, formatted)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
