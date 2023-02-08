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
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: rest-api-io
//   description: REST-API BigQueryIO example.
//   multifile: false
//   default_example: false
//   context_line: 72
//   categories:
//     - Emulated Data Source
//     - IO
//   complexity: ADVANCED

package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
)

func main() {
	// Create a client for BigQuery.
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "tess-372508")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Define the schema for the data to be written.
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "age", Type: bigquery.IntegerFieldType, Required: true},
	}

	// Create the table in BigQuery.
	tableRef := client.Dataset("fir").Table("xasw")
	table := tableRef.Create(ctx, schema, &bigquery.TableMetadata{})
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Write the data to the table.
	data := []bigquery.Value{
		{"1", "John Doe", 30},
		{"2", "Jane Doe", 25},
		{"3", "Jim Smith", 40},
	}
	upl := table.Uploader()
	err = upl.Put(ctx, data)
	if err != nil {
		log.Fatalf("Failed to write data: %v", err)
	}

	fmt.Printf("Data written to table %s\n", tableRef.ID())
}
