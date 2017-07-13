package main

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/CombinePerKeyExamples.java

import (
	"context"
	"flag"
	"log"
	"reflect"

	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", "publicdata:samples.shakespeare", "Shakespeare plays BQ table.")
	output = flag.String("output", "", "Output BQ table.")

	minLength = flag.Int("min_length", 9, "Minimum word length")
)

type WordRow struct {
	Corpus string `bigquery:"corpus"`
	Word   string `bigquery:"word"`
}

type PlaysRow struct {
	Word  string `bigquery:"word"`
	Plays string `bigquery:"plays"`
}

// PlaysForWords generates a string containing the list of play names
// in which that word appears. It takes a PCollection<WordRow> and
// returns a PCollection<PlaysRow>.
func PlaysForWords(p *beam.Pipeline, rows beam.PCollection) beam.PCollection {
	p = p.Composite("PlaysForWords")

	words := beam.ParDo(p, &extractFn{MinLength: *minLength}, rows)
	keyed := beam.Combine(p, concatFn, words)
	return beam.ParDo(p, formatFn, keyed)
}

// extractFn outputs (word, play) iif the word is longer than the minimum length.
type extractFn struct {
	MinLength int `json:"min_length"`
}

func (f *extractFn) ProcessElement(row WordRow, emit func(string, string)) {
	if len(row.Word) >= f.MinLength {
		emit(row.Word, row.Corpus)
	}
	// TODO(herohde) 7/14/2017: increment counter for "small words"
}

// TODO(herohde) 7/14/2017: the choice of a string (instead of []string) for the
// output makes the combiner simpler. Seems hokey.

func concatFn(a, b string) string {
	return fmt.Sprintf("%v,%v", a, b)
}

func formatFn(word, plays string) PlaysRow {
	return PlaysRow{Word: word, Plays: plays}
}

func main() {
	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("no output table specified")
	}

	// TODO(herohde) 7/14/2017: hack to grab the Dataflow flag. We should reconcile
	// such information somehow. Maybe add a wrapper to get-or-define flags?
	project := flag.Lookup("project").Value.String()

	log.Print("Running combine")

	p := beam.NewPipeline()
	rows := bigqueryio.Read(p, project, *input, reflect.TypeOf(WordRow{}))
	out := PlaysForWords(p, rows)
	bigqueryio.Write(p, project, *output, out)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
