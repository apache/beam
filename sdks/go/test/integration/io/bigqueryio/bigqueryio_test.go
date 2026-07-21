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
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func init() {
	register.DoFn2x0[[]byte, func(TestRow)](&CreateTestRowsFn{})
	register.Emitter1[TestRow]()
}

func checkFlags(t *testing.T) {
	gcpProjectIsNotSet := gcpopts.Project == nil || *gcpopts.Project == ""
	if gcpProjectIsNotSet {
		t.Skip("GCP project flag is not set.")
	}
	if *integration.BigQueryDataset == "" {
		t.Skip("No BigQuery dataset provided.")
	}
}

const (
	// A text to shuffle to get random words.
	text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas eget nulla nec " +
		"velit hendrerit placerat. Donec eu odio ultricies, fermentum arcu at, mollis lectus. " +
		"Vestibulum porttitor pharetra sem vitae feugiat. Mauris facilisis neque in mauris " +
		"feugiat rhoncus. Donec eu ipsum at nibh lobortis euismod. Nam at hendrerit felis. " +
		"Vivamus et orci ex. Nam dui nisl, rutrum ac pretium eget, vehicula in tortor. Class " +
		"aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. " +
		"Phasellus ante lorem, pharetra blandit dapibus et, tempus nec purus. Maecenas in " +
		"posuere sem, vel pharetra nisl. Pellentesque habitant morbi tristique senectus et netus " +
		"et malesuada fames ac turpis egestas. Donec nec facilisis ex. Praesent euismod commodo " +
		"efficitur. Fusce in nisi nunc."
	// Number of random elements to create for test. Must be less than number of words in text.
	inputSize = 50
)

// TestRow is a sample row to write and read from that is expected to contain enough deterministic
// and random data in different data types to provide a reasonable signal that reading and writing
// works at a basic level.
type TestRow struct {
	Counter  int64    `bigquery:"counter"`   // A deterministic counter, increments for each row generated.
	RandData RandData `bigquery:"rand_data"` // An inner struct containing randomized data.
}

func shuffleText() []string {
	words := strings.Fields(text)
	rand.Shuffle(len(words), func(i, j int) { words[i], words[j] = words[j], words[i] })
	return words
}

func generateTestRows(seed int64, size int) []TestRow {
	rand.Seed(seed)
	words := shuffleText()
	rows := make([]TestRow, size)
	for i := 0; i < size; i++ {
		rows[i] = TestRow{
			Counter: int64(i),
			RandData: RandData{
				Flip: rand.Int63n(2) != 0,
				Num:  rand.Int63(),
				Word: words[i],
			},
		}
	}
	return rows
}

// RandData is a struct of various types of random data.
type RandData struct {
	Flip         bool                `bigquery:"flip"`          // Flip is a bool with a random chance of either result (a coin flip).
	Num          int64               `bigquery:"num"`           // Num is a random int64.
	Word         string              `bigquery:"word"`          // Word is a randomly selected word from a sample text.
	NullableWord bigquery.NullString `bigquery:"nullable_word"` // Word is a randomly selected word from a sample text.
}

// ddlSchema is a string for BigQuery data definition language that corresponds to TestRow.
const ddlTestRowSchema = "counter INT64 NOT NULL, " +
	"rand_data STRUCT<" +
	"flip BOOL NOT NULL," +
	"num INT64 NOT NULL," +
	"word STRING NOT NULL," +
	"nullable_word STRING" +
	"> NOT NULL"

// CreateTestRowsFn is a DoFn that creates randomized TestRows based on a seed.
type CreateTestRowsFn struct {
	seed int64
}

// ProcessElement creates a number of TestRows, populating the randomized data.
func (fn *CreateTestRowsFn) ProcessElement(_ []byte, emit func(TestRow)) {
	rows := generateTestRows(fn.seed, inputSize)
	for _, row := range rows {
		emit(row)
	}
}

func TestBigQueryIO_Query(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	ctx := context.Background()
	// Get the GCP project
	// this assumes dataflow is running in the same project as the project in which the bigquery dataset
	// is located
	project := gcpopts.GetProject(ctx)
	bigqueryDataset := *integration.BigQueryDataset

	tests := []struct {
		name          string
		query         string
		queryOptions  []func(*bigqueryio.QueryOptions) error
		preCreate     bool
		insertData    bool
		expectedCount int
		wantErr       bool
	}{
		{
			name:  "Query without parameters",
			query: "SELECT * FROM `%s`",
			queryOptions: []func(*bigqueryio.QueryOptions) error{
				bigqueryio.UseStandardSQL(),
			},
			preCreate:     true,
			insertData:    true,
			expectedCount: inputSize,
			wantErr:       false,
		},
		{
			name:  "Query with parameters",
			query: "SELECT * FROM `%s` WHERE counter = @key1 and rand_data = @key2",
			queryOptions: []func(*bigqueryio.QueryOptions) error{
				bigqueryio.UseStandardSQL(),
				bigqueryio.WithQueryParameters([]bigquery.QueryParameter{
					bigquery.QueryParameter{
						Name:  "key1",
						Value: -1, // to ensure expectedCount is 0
					},
					bigquery.QueryParameter{
						Name: "key2",
						Value: &bigquery.QueryParameterValue{
							Type: bigquery.StandardSQLDataType{
								StructType: &bigquery.StandardSQLStructType{
									Fields: []*bigquery.StandardSQLField{
										{
											Name: "flip",
											Type: &bigquery.StandardSQLDataType{
												TypeKind: "BOOL",
											},
										},
										{
											Name: "num",
											Type: &bigquery.StandardSQLDataType{
												TypeKind: "INT64",
											},
										},
										{
											Name: "word",
											Type: &bigquery.StandardSQLDataType{
												TypeKind: "STRING",
											},
										},
										{
											Name: "nullable_word",
											Type: &bigquery.StandardSQLDataType{
												TypeKind: "STRING",
											},
										},
									},
								},
							},
							StructValue: map[string]bigquery.QueryParameterValue{
								"flip": {
									Value: true,
								},
								"num": {
									Value: int64(1),
								},
								"word": {
									Value: "ipsum",
								},
								"nullable_word": {
									Value: bigquery.NullString{
										StringVal: "nullable",
										Valid:     true,
									},
								},
							},
						},
					},
				}...),
			},
			preCreate:     true,
			insertData:    true,
			expectedCount: 0,
			wantErr:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableID := fmt.Sprintf("%s_temp_%v", "go_bqio_it", time.Now().UnixNano())
			tableName := fmt.Sprintf("%s.%s", bigqueryDataset, tableID)
			fullyQualifiedTableName := fmt.Sprintf("%s.%s.%s", project, bigqueryDataset, tableID)
			if tt.preCreate {
				newTempTable(t, tableName, ddlTestRowSchema, project)
			}
			testRows := generateTestRows(time.Now().UnixNano(), inputSize)
			if tt.insertData {
				insertTestRows(ctx, t, project, bigqueryDataset, tableID, testRows)
				waitForRows(ctx, t, project, bigqueryDataset, tableID, len(testRows))
			}
			t.Cleanup(func() {
				deleteTempTable(t, tableName, project)
			})

			p, s := beam.NewPipelineWithRoot()

			q := fmt.Sprintf(tt.query, fullyQualifiedTableName)
			queryType := reflect.TypeOf((*TestRow)(nil)).Elem()
			pcol := bigqueryio.Query(s, project, q, queryType, tt.queryOptions...)

			passert.Count(s, pcol, fmt.Sprintf("Should have %d rows", tt.expectedCount), tt.expectedCount)
			if err := ptest.Run(p); (err != nil) != tt.wantErr {
				t.Fatalf("ptest.Run() err = %v, wantErr %v", err, tt.wantErr)
			} else if err != nil {
				// Pipeline failed as expected, return early
				return
			}
		})
	}
}

func TestBigQueryIO_Write(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	ctx := context.Background()

	tests := []struct {
		name              string
		preCreate         bool
		createDisposition bigquery.TableCreateDisposition
		wantErr           bool
	}{
		{
			name:              "CreateNever table create disposition with preexisting table",
			preCreate:         true,
			createDisposition: bigquery.CreateNever,
			wantErr:           false,
		},
		{
			name:              "CreateIfNeeded table create disposition with preexisting table",
			preCreate:         true,
			createDisposition: bigquery.CreateIfNeeded,
			wantErr:           false,
		},
		{
			name:              "CreateNever table create disposition with no preexisting table",
			preCreate:         false,
			createDisposition: bigquery.CreateNever,
			wantErr:           true,
		},
		{
			name:              "CreateIfNeeded table create disposition with no preexisting table",
			preCreate:         false,
			createDisposition: bigquery.CreateIfNeeded,
			wantErr:           false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Get the GCP project
			// this assumes dataflow is running in the same project as the project in which the bigquery dataset
			// is located
			project := gcpopts.GetProject(ctx)
			tableID := fmt.Sprintf("%s_temp_%v", "go_bqio_it", time.Now().UnixNano())
			tableName := fmt.Sprintf("%s.%s", *integration.BigQueryDataset, tableID)
			if tt.preCreate {
				newTempTable(t, tableName, ddlTestRowSchema, project)
			}
			t.Cleanup(func() {
				deleteTempTable(t, tableName, project)
			})
			createTestRows := &CreateTestRowsFn{seed: time.Now().UnixNano()}
			p, s := beam.NewPipelineWithRoot()

			// Generate elements and write to table.
			rows := beam.ParDo(s, createTestRows, beam.Impulse(s))
			table := fmt.Sprintf("%s:%s", project, tableName)
			bigqueryio.Write(s, project, table, rows, bigqueryio.WithCreateDisposition(tt.createDisposition))

			if err := ptest.Run(p); (err != nil) != tt.wantErr {
				t.Fatalf("ptest.Run() err = %v, wantErr %v", err, tt.wantErr)
			} else if err != nil {
				// Pipeline failed as expected, return early
				return
			}
			checkTableExistsAndNonEmpty(ctx, t, project, tableID)
		})
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	ptest.MainRet(m)
}
