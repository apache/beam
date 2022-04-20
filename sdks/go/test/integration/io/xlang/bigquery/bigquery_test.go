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

package bigquery

import (
	"flag"
	"log"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

var expansionAddr string // Populate with expansion address labelled "gcpio".

func checkFlags(t *testing.T) {
	if *integration.BigQueryDataset == "" {
		t.Skip("No BigQuery dataset provided.")
	}
}

// TestBigQueryIO_BasicWriteRead runs a pipeline that generates semi-randomized elements, writes
// them to a BigQuery table and then reads from that table, and checks that the result matches the
// original inputs. This requires a pre-existing table to be created.
func TestBigQueryIO_BasicWriteRead(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	// Create a table before running the pipeline
	table, err := newTempTable(*integration.BigQueryDataset, "go_bqio_it", ddlTestRowSchema)
	if err != nil {
		t.Fatalf("error creating BigQuery table: %v", err)
	}
	t.Logf("Created BigQuery table %v", table)

	write := WritePipeline(expansionAddr, table, createTestRows)
	ptest.RunAndValidate(t, write)
	read := ReadPipeline(expansionAddr, table, createTestRows)
	ptest.RunAndValidate(t, read)

	t.Logf("Deleting BigQuery table %v", table)
	err = deleteTempTable(table)
	if err != nil {
		t.Logf("Error deleting BigQuery table: %v", err)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	services := integration.NewExpansionServices()
	defer func() { services.Shutdown() }()
	addr, err := services.GetAddr("gcpio")
	if err != nil {
		log.Printf("skipping missing expansion service: %v", err)
	} else {
		expansionAddr = addr
	}

	ptest.MainRet(m)
}
