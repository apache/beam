package main

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/MaxPerKeyExamples.java

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

type WeatherDataRow struct {
	Month    int     `bigquery:"month"`
	MeanTemp float64 `bigquery:"mean_temp"`
}

type MaxMeanTempRow struct {
	Month       int     `bigquery:"month"`
	MaxMeanTemp float64 `bigquery:"max_mean_temp"`
}

// MaxMeanTemp finds the max mean_temp for each month. It takes a
// PCollection<WeatherDataRow> and returns a PCollection<MaxMeanTempRow>.
func MaxMeanTemp(p *beam.Pipeline, rows beam.PCollection) beam.PCollection {
	p = p.Composite("MaxMeanTemp")

	keyed := beam.ParDo(p, extractFn, rows)
	maxTemps := stats.Max(p, keyed)
	return beam.ParDo(p, formatFn, maxTemps)
}

func extractFn(row WeatherDataRow) (int, float64) {
	return row.Month, row.MeanTemp
}

func formatFn(month int, temp float64) MaxMeanTempRow {
	return MaxMeanTempRow{Month: month, MaxMeanTemp: temp}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *output == "" {
		log.Exit(ctx, "No output table specified. Use --output=<table>")
	}
	project := gcpopts.GetProject(ctx)

	log.Info(ctx, "Running max")

	p := beam.NewPipeline()
	rows := bigqueryio.Read(p, project, *input, reflect.TypeOf(WeatherDataRow{}))
	out := MaxMeanTemp(p, rows)
	bigqueryio.Write(p, project, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
