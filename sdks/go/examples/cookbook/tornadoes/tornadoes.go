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

// tornadoes is an example that reads the public samples of weather data from
// BigQuery, counts the number of tornadoes that occur in each month, and
// writes the results to BigQuery.
//
// Concepts: Reading/writing BigQuery; Using Go types for better type-safety.
//
// Note: Before running this example, you must create a BigQuery dataset to
// contain your output table as described here:
//
//   https://cloud.google.com/bigquery/docs/tables#create-table
//
// To execute this pipeline locally, specify the BigQuery table for the output
// with the form:
//
//   --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
//
// The BigQuery input table defaults to clouddataflow-readonly:samples.weather_stations
// and can be overridden with {@code --input}.
package main

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
	input  = flag.String("input", "clouddataflow-readonly:samples.weather_stations", "BigQuery table with weather data to read from, specified as <project_id>:<dataset_id>.<table_id>")
	output = flag.String("output", "", "BigQuery table to write to, specified as <project_id>:<dataset_id>.<table_id>. The dataset must already exist")
)

// Month is represented as 'int' in BQ. A Go type definition allows
// us to write more type-safe transformations.
type Month int

// WeatherDataRow defines a BQ schema using field annotations.
// It is used as a projection to extract rows from a table.
type WeatherDataRow struct {
	Tornado bool  `bigquery:"tornado"`
	Month   Month `bigquery:"month"`
}

// TornadoRow defines the output BQ schema. Each row in the output dataset
// conforms to this schema. A TornadoRow value represents a concrete row.
type TornadoRow struct {
	Month Month `bigquery:"month"`
	Count int   `bigquery:"tornado_count"`
}

// CountTornadoes computes the number of tornadoes pr month. It takes a
// PCollection<WeatherDataRow> and returns a PCollection<TornadoRow>.
func CountTornadoes(s beam.Scope, rows beam.PCollection) beam.PCollection {
	s = s.Scope("CountTornadoes")

	// row... => month...
	months := beam.ParDo(s, extractFn, rows)
	// month... => <month,count>...
	counted := stats.Count(s, months)
	// <month,count>... => row...
	return beam.ParDo(s, formatFn, counted)
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
	s := p.Root()
	rows := bigqueryio.Read(s, project, *input, reflect.TypeOf(WeatherDataRow{}))
	out := CountTornadoes(s, rows)
	bigqueryio.Write(s, project, *output, out)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
