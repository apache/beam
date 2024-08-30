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
//   name: write-table-schema
//   description: BigQueryIO table-schema example.
//   multifile: false
//   context_line: 40
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
  "log"
    /*
    "context"
    beam_log "github.com/apache/beam/sdks/v2/go/pkg/beam/log"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
    "cloud.google.com/go/bigquery"
    "github.com/apache/beam/sdks/v2/go/pkg/beam"
    "github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
    */)

type User struct {
  ID   int32  `bigquery:"id"`
  Name string `bigquery:"name"`
  Age  int32  `bigquery:"age"`
}

var(projectID = "project-id"
    datasetID = "dataset"
    tableID = "table")

func main() {
  log.Println("Running Task")

/*
  ctx := context.Background()


  // set up pipeline
  p := beam.NewPipeline()
  s := p.Root()
  s = s.Scope("ReadFromBigQuery")

  // Reads from the BigQuery table specified by the projectID, datasetID, and tableID, with the schema defined by the User struct, and stores the result in rows.
  rows := bigqueryio.Read(s, bigquery.TableReference{ProjectID: projectID, DatasetID: datasetID, TableID: tableID},
    beam.WithSchema(User{}))

  beam.ParDo0(s, &logOutput{}, rows)

  if err := beam.Run(ctx, p); err != nil {
    log.Fatalf("Failed to execute job: %v", err)
  }
  */
}

type logOutput struct{}

func (l *logOutput) ProcessElement(row User, emit func(User)) {
  log.Printf("Processing element: %v", row)
  emit(row)
}