/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

// beam-playground:
//   name: read-table
//   description: BigQueryIO read table example.
//   multifile: false
//   context_line: 42
//   never_run: true
//   always_run: true
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"

	"cloud.google.com/go/bigquery"
	internal_log "log"
	"reflect"
)

type Game struct {
	GameID     bigquery.NullString `bigquery:"gameId"`
	GameNumber bigquery.NullInt64  `bigquery:"gameNumber"`
	SeasonID   bigquery.NullString `bigquery:"seasonId"`
	Year       bigquery.NullInt64  `bigquery:"year"`
	Type       bigquery.NullString `bigquery:"type"`
	DayNight   bigquery.NullString `bigquery:"dayNight"`
	Duration   bigquery.NullString `bigquery:"duration"`
}

func main() {
	internal_log.Println("Running Task")

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()
	project := "apache-beam-testing"

	// Build a PCollection<Game> by querying BigQuery.
	rows := bigqueryio.Read(s, project, "bigquery-public-data:baseball.schedules", reflect.TypeOf(Game{}))

	fixedSizeLines := top.Largest(s, rows, 5, less)

	debug.Print(s, fixedSizeLines)
	// Now that the pipeline is fully constructed, we execute it.
	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
func less(a, b Game) bool {
	return true
}
