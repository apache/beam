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
//   name: read-query
//   description: BigQuery read query example.
//   multifile: false
//   context_line: 40
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam
package main

import (
	_ "context"
	_ "flag"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	internal_log "log"
	_ "reflect"
)

// Define the data model: The CommentRow struct is defined, which models one row of HackerNews comments.
//The bigquery tag in the struct field is used to map the struct field to the BigQuery column.
type CommentRow struct {
	Text string `bigquery:"text"`
}

// Construct the BigQuery query: A constant query is defined that selects the text column
// from the bigquery-public-data.hacker_news.comments table for a certain time range.
const query = `SELECT text
FROM ` + "`bigquery-public-data.hacker_news.comments`" + `
WHERE time_ts BETWEEN '2013-01-01' AND '2014-01-01'
LIMIT 1000
`

func main() {
	internal_log.Println("Running Task")
	/*
		ctx := context.Background()
		p := beam.NewPipeline()
		s := p.Root()
		project := "tess-372508"

		// Build a PCollection<CommentRow> by querying BigQuery.
		rows := bigqueryio.Query(s, project, query,
			reflect.TypeOf(CommentRow{}), bigqueryio.UseStandardSQL())

		debug.Print(s, rows)

		// Now that the pipeline is fully constructed, we execute it.
		if err := beamx.Run(ctx, p); err != nil {
			log.Exitf(ctx, "Failed to execute job: %v", err)
		}*/
}
