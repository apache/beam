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
	"time"
)

type Comment struct {
	ID      int       `bigquery:"id"`
	By      string    `bigquery:"by"`
	Author  string    `bigquery:"author"`
	Time    int       `bigquery:"time"`
	TimeTS  time.Time `bigquery:"time_ts"`
	Text    string    `bigquery:"text"`
	Parent  int       `bigquery:"parent"`
	Deleted bool      `bigquery:"deleted"`
	Dead    bool      `bigquery:"dead"`
	Ranking float64   `bigquery:"ranking"`
}

// rows := bigqueryio.Read(s, project, "bigquery-public-data:hacker_news.comments", reflect.TypeOf(Comment{}))
// reads data from the specified BigQuery table and produces a PCollection where each element is a Comment.
// The reflect.TypeOf(Comment{}) is used to tell BigQuery the schema of the data.

// debug.Print(s, rows) prints the elements of the PCollection to stdout for debugging purposes.

func main() {
	internal_log.Println("Running Task")
	/*
		ctx := context.Background()
		p := beam.NewPipeline()
		s := p.Root()
		project := "tess-372508"

		// Build a PCollection<CommentRow> by querying BigQuery.
		rows := bigqueryio.Read(s, project, "bigquery-public-data:hacker_news.comments", reflect.TypeOf(Comment{}))

		debug.Print(s, rows)

		// Now that the pipeline is fully constructed, we execute it.
		if err := beamx.Run(ctx, p); err != nil {
			log.Exitf(ctx, "Failed to execute job: %v", err)
		}*/
}
