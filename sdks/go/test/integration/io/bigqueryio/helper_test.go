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

package bigqueryio

import (
	"context"
	"fmt"
	"os/exec"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

// newTempTable creates a new BigQuery table using BigQuery's Data Definition Language (DDL) and the
// "bq query" console command. Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
// The tables created are set to expire after a day.
//
// newTable takes the name of a BigQuery dataset and a DDL schema for the data,
// and generates that table with a unique suffix and an expiration time of a day later.
func newTempTable(t *testing.T, name string, schema string) {
	t.Helper()

	query := fmt.Sprintf("CREATE TABLE `%s`(%s) OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))", name, schema)
	cmd := exec.Command("bq", "query", "--use_legacy_sql=false", query)
	_, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("error creating BigQuery table: %v", err)
	}
	t.Logf("Created BigQuery table %v", name)
}

// deleteTable deletes a BigQuery table using BigQuery's Data Definition Language (DDL) and the
// "bq query" console command. Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
func deleteTempTable(t *testing.T, table string) {
	t.Helper()

	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)
	cmd := exec.Command("bq", "query", "--use_legacy_sql=false", query)
	t.Logf("Deleting BigQuery table %v", table)
	_, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Error deleting BigQuery table: %v", err)
	}
}

func checkTableExistsAndNonEmpty(ctx context.Context, t *testing.T, project, tableID string) {
	t.Helper()

	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		t.Fatalf("error creating BigQuery client: %v", err)
	}
	defer client.Close()

	tableRef := client.Dataset(*integration.BigQueryDataset).Table(tableID)
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		t.Fatalf("unable to find table: %v", err)
	}
	streamingBuffer := metadata.StreamingBuffer
	if streamingBuffer == nil {
		t.Fatalf("there's no streaming buffer for the table")
	}
	if streamingBuffer.EstimatedRows != inputSize {
		t.Fatalf("streamingBuffer.EstimatedRows = %v, want %v", streamingBuffer.EstimatedRows, inputSize)
	}
}
