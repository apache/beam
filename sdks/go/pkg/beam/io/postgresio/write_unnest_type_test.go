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

package postgresio

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/lib/pq"
)

func TestWrite_UNNESTTypeBinding(t *testing.T) {
	type complexRow struct {
		DateCol  string `db:"date_col"`
		ArrCol   string `db:"array_col"`
		TextCol  string `db:"text_col"`
		JsonbCol string `db:"jsonb_col"`
	}

	fn := &writeFn{
		Table:   `"orders"`,
		Options: NewWriteOptions(WithWriteMode(WriteModeInsert)),
		Type:    beam.EncodedType{T: reflect.TypeOf(complexRow{})},
		columns: []string{"date_col", "array_col", "text_col", "jsonb_col"},
		colTypes: map[string]string{
			"date_col":  "DATE",
			"array_col": "_DATE", // array of date
			"text_col":  "",      // fallback to TEXT
			"jsonb_col": "JSONB",
		},
	}

	batch := []any{
		complexRow{DateCol: "2020-01-01", ArrCol: "{2020-01-01}", TextCol: "foo", JsonbCol: "{}"},
	}

	query, _, err := fn.buildUnnestQueryForColumns(batch, fn.columns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedUnnest := `UNNEST($1::DATE[], $2::text[], $3::TEXT[], $4::JSONB[]) AS t(col0, col1, col2, col3)`
	if !strings.Contains(query, expectedUnnest) {
		t.Errorf("expected query to contain %q, got %q", expectedUnnest, query)
	}
	expectedSelect := `SELECT t.col0, t.col1::DATE[], t.col2, t.col3 FROM UNNEST`
	if !strings.Contains(query, expectedSelect) {
		t.Errorf("expected query to contain %q, got %q", expectedSelect, query)
	}
}

func TestWrite_StagingTableUniquePerWorker(t *testing.T) {
	qualifiedTable := "public.orders"
	worker1 := "w1"
	worker2 := "w2"

	name1 := stagingTableName(qualifiedTable, worker1)
	name2 := stagingTableName(qualifiedTable, worker2)

	if name1 == name2 {
		t.Errorf("expected different staging table names for different workers, got %q", name1)
	}

	if !strings.HasSuffix(name1, "_w1") || !strings.HasSuffix(name2, "_w2") {
		t.Errorf("expected staging table names to include worker ID, got %q, %q", name1, name2)
	}
}

func TestWrite_ErrorWrapping(t *testing.T) {
	copyErr := &pq.Error{Code: "23514", Message: "no partition of relation"}
	fallbackErr := &pq.Error{Code: "42804", Message: "expression is of type text"}

	finalErr := fmt.Errorf("postgresio: write batch fallback failed with sqlstate %s: %w (original COPY error: %v)", extractSqlState(fallbackErr), fallbackErr, copyErr)

	errStr := finalErr.Error()
	if !strings.Contains(errStr, "23514") || !strings.Contains(errStr, "no partition") {
		t.Errorf("missing original COPY error: %s", errStr)
	}
	if !strings.Contains(errStr, "42804") || !strings.Contains(errStr, "expression is of type text") {
		t.Errorf("missing fallback error: %s", errStr)
	}
}
