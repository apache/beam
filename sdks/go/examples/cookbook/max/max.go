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

// See: https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/MaxPerKeyExamples.java

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
)

func init() {
	register.Function2x1(formatFn)
	register.Function1x2(extractFn)
}

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
func MaxMeanTemp(s beam.Scope, rows beam.PCollection) beam.PCollection {
	s = s.Scope("MaxMeanTemp")

	keyed := beam.ParDo(s, extractFn, rows)
	maxTemps := stats.MaxPerKey(s, keyed)
	return beam.ParDo(s, formatFn, maxTemps)
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
	s := p.Root()
	rows := bigqueryio.Read(s, project, *input, reflect.TypeOf(WeatherDataRow{}))
	out := MaxMeanTemp(s, rows)
	bigqueryio.Write(s, project, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
