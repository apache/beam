package main

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java

import (
	"context"
	"flag"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", "clouddataflow-readonly:samples.weather_stations", "Weather data BQ table.")
	output = flag.String("output", "", "Output BQ table.")
)

type WeatherDataRow struct {
	Tornado bool `bigquery:"tornado"`
	Month   int  `bigquery:"month"`
}

type TornadoRow struct {
	Month int `bigquery:"month"`
	Count int `bigquery:"tornado_count"`
}

// CountTornadoes computes the number of tornadoes pr month. It takes a
// PCollection<WeatherDataRow> and returns a PCollection<TornadoRow>.
func CountTornadoes(p *beam.Pipeline, rows beam.PCollection) beam.PCollection {
	p = p.Composite("CountTornadoes")

	months := beam.ParDo(p, extractFn, rows)
	counted := stats.Count(p, months)
	return beam.ParDo(p, formatFn, counted)
}

// extractFn outputs the month iff a tornado happened.
func extractFn(row WeatherDataRow, emit func(int)) {
	if row.Tornado {
		emit(row.Month)
	}
}

func formatFn(month, count int) TornadoRow {
	return TornadoRow{Month: month, Count: count}
}

func main() {
	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("No output table specified. Use --output=<table>")
	}
	project := gcpopts.GetProject()

	log.Print("Running tornadoes")

	p := beam.NewPipeline()
	rows := bigqueryio.Read(p, project, *input, reflect.TypeOf(WeatherDataRow{}))
	out := CountTornadoes(p, rows)
	bigqueryio.Write(p, project, *output, out)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
