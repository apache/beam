// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/FilterExamples.java

import (
	"context"
	"flag"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	input  = flag.String("input", "clouddataflow-readonly:samples.weather_stations", "Weather data BQ table.")
	output = flag.String("output", "", "Output BQ table.")
	month  = flag.Int("month_filter", 7, "Numerical month to analyze")
)

func init() {
	register.Function1x1(extractMeanTempFn)
	register.Function3x0(filterBelowMeanFn)
	register.DoFn2x0[WeatherDataRow, func(WeatherDataRow)](&filterMonthFn{})
	register.Emitter1[WeatherDataRow]()
}

type WeatherDataRow struct {
	Year     int     `bigquery:"year"`
	Month    int     `bigquery:"month"`
	Day      int     `bigquery:"day"`
	MeanTemp float64 `bigquery:"mean_temp"`
}

// BelowGlobalMean computes the rows for the given month below the global mean. It takes a
// PCollection<WeatherDataRow> and returns a PCollection<WeatherDataRow>.
func BelowGlobalMean(s beam.Scope, m int, rows beam.PCollection) beam.PCollection {
	s = s.Scope("BelowGlobalMean")

	// Find the global mean of all the mean_temp readings in the weather data.
	globalMeanTemp := stats.Mean(s, beam.ParDo(s, extractMeanTempFn, rows))

	// Rows filtered to remove all but a single month
	filtered := beam.ParDo(s, &filterMonthFn{Month: m}, rows)

	// Then, use the global mean as a side input, to further filter the weather data.
	// By using a side input to pass in the filtering criteria, we can use a value
	// that is computed earlier in pipeline execution. We'll only output readings with
	// temperatures below this mean.
	return beam.ParDo(s, filterBelowMeanFn, filtered, beam.SideInput{Input: globalMeanTemp})
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
	s := p.Root()
	rows := bigqueryio.Read(s, project, *input, reflect.TypeOf(WeatherDataRow{}))
	out := BelowGlobalMean(s, *month, rows)
	bigqueryio.Write(s, project, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
