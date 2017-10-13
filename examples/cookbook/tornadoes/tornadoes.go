package main

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java

import (
	"context"
	"flag"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", "clouddataflow-readonly:samples.weather_stations", "Weather data BQ table.")
	output = flag.String("output", "", "Output BQ table.")
)

// Month is represented as 'int' in BQ. A Go type definition allows
// us to write more type-safe transformations.
type Month int

// WeatherDataRow defines a BQ schema using field annotations.
// It is used as a projection to extract data from a table.
type WeatherDataRow struct {
	Tornado bool  `bigquery:"tornado"`
	Month   Month `bigquery:"month"`
}

// TornadoRow defines the output BQ schema.
type TornadoRow struct {
	Month Month `bigquery:"month"`
	Count int   `bigquery:"tornado_count"`
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
func extractFn(row WeatherDataRow, emit func(Month)) {
	if row.Tornado {
		emit(row.Month)
	}
}

// formatFn converts a KV<Month, int> to a TornadoRow.
func formatFn(month Month, count int) TornadoRow {
	return TornadoRow{Month: month, Count: count}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *output == "" {
		log.Exit(ctx, "No output table specified. Use --output=<table>")
	}
	project := gcpopts.GetProject(ctx)

	log.Info(ctx, "Running tornadoes")

	p := beam.NewPipeline()
	rows := bigqueryio.Read(p, project, *input, reflect.TypeOf(WeatherDataRow{}))
	out := CountTornadoes(p, rows)
	bigqueryio.Write(p, project, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
