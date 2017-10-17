package main

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/FilterExamples.java

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
	month  = flag.Int("month_filter", 7, "Numerical month to analyze")
)

type WeatherDataRow struct {
	Year     int     `bigquery:"year"`
	Month    int     `bigquery:"month"`
	Day      int     `bigquery:"day"`
	MeanTemp float64 `bigquery:"mean_temp"`
}

// BelowGlobalMean computes the rows for the given month below the global mean. It takes a
// PCollection<WeatherDataRow> and returns a PCollection<WeatherDataRow>.
func BelowGlobalMean(p *beam.Pipeline, m int, rows beam.PCollection) beam.PCollection {
	p = p.Scope("BelowGlobalMean")

	// Find the global mean of all the mean_temp readings in the weather data.
	globalMeanTemp := stats.Mean(p, beam.ParDo(p, extractMeanTempFn, rows))

	// Rows filtered to remove all but a single month
	filtered := beam.ParDo(p, &filterMonthFn{Month: m}, rows)

	// Then, use the global mean as a side input, to further filter the weather data.
	// By using a side input to pass in the filtering criteria, we can use a value
	// that is computed earlier in pipeline execution. We'll only output readings with
	// temperatures below this mean.
	return beam.ParDo(p, filterBelowMeanFn, filtered, beam.SideInput{Input: globalMeanTemp})
}

type filterMonthFn struct {
	Month int `json:"month"`
}

func (f *filterMonthFn) ProcessElement(row WeatherDataRow, emit func(WeatherDataRow)) {
	if row.Month == f.Month {
		emit(row)
	}
}

func extractMeanTempFn(row WeatherDataRow) float64 {
	return row.MeanTemp
}

func filterBelowMeanFn(row WeatherDataRow, globalMeanTemp float64, emit func(WeatherDataRow)) {
	if row.MeanTemp < globalMeanTemp {
		emit(row)
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	if *output == "" {
		log.Exit(ctx, "No output table specified. Use --output=<table>")
	}
	project := gcpopts.GetProject(ctx)

	log.Info(ctx, "Running filter")

	p := beam.NewPipeline()
	rows := bigqueryio.Read(p, project, *input, reflect.TypeOf(WeatherDataRow{}))
	out := BelowGlobalMean(p, *month, rows)
	bigqueryio.Write(p, project, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
