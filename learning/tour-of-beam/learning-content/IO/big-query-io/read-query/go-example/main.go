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
//   description: BigQuery read beam-schema example.
//   multifile: false
//   context_line: 40
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"context"
	"log"

	"github.com/apache/beam/sdk/io/gcp/bigquery"
	"github.com/apache/beam/sdk/options/pipelineoptions"
	"github.com/apache/beam/sdk/pipeline"
)

func main() {
	log.Println("Running Task")

	options := pipelineoptions.NewPipelineOptions(nil)
	options.SetTempLocation("gs://btestq")
	options.SetProject("tess-372508")

	p := pipeline.NewPipeline(options)

	pCollection, err := bigquery.NewClient(context.Background(), options).Read(p,
		bigquery.Query("SELECT max_temperature FROM `tess-372508.fir.xasw`"),
		bigquery.WithCoder(bigquery.Float64()))
	if err != nil {
		log.Fatalf("Failed to read from BigQuery: %v", err)
	}

	pCollection.Apply("Log words", ParDo(func(el float64, emit func(float64)) {
		log.Printf("Processing element: %v", el)
		emit(el)
	}, bigquery.Float64()))

	if err := p.Run(); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
