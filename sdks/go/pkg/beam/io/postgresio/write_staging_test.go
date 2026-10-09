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
	"strings"
	"testing"
)

// --- P2-1: COPY statement construction ---

// TestBuildCopyStatementDoesNotDoubleQuote is the regression test for the
// identifier bug that pq.CopyIn caused.
//
// SanitizeTableIdentifier already returns a quoted identifier. pq.CopyIn
// quoted it again, producing COPY """public"".""orders""" which PostgreSQL
// parses as a single table literally named `"public"."orders"`.
func TestBuildCopyStatementDoesNotDoubleQuote(t *testing.T) {
	table, err := SanitizeTableIdentifier("public.orders")
	if err != nil {
		t.Fatalf("SanitizeTableIdentifier() error: %v", err)
	}

	col, err := SanitizeIdentifier("order_id")
	if err != nil {
		t.Fatalf("SanitizeIdentifier() error: %v", err)
	}

	got, err := buildCopyStatement(table, []string{col})
	if err != nil {
		t.Fatalf("buildCopyStatement() error: %v", err)
	}

	want := `COPY "public"."orders" ("order_id") FROM STDIN`
	if got != want {
		t.Errorf("buildCopyStatement() =\n %q\nwant %q", got, want)
	}

	// A tripled quote is the specific signature of the double-quoting bug.
	if strings.Contains(got, `"""`) {
		t.Errorf("COPY statement contains a triple quote, indicating the identifier was quoted twice: %q", got)
	}
}

// TestBuildCopyStatementQuotesEveryColumn ensures no column is interpolated raw.
func TestBuildCopyStatementQuotesEveryColumn(t *testing.T) {
	cols := make([]string, 0, 3)
	for _, name := range []string{"id", "total_amount", "created_at"} {
		q, err := SanitizeIdentifier(name)
		if err != nil {
			t.Fatalf("SanitizeIdentifier(%q) error: %v", name, err)
		}
		cols = append(cols, q)
	}

	got, err := buildCopyStatement(`"public"."orders"`, cols)
	if err != nil {
		t.Fatalf("buildCopyStatement() error: %v", err)
	}

	want := `COPY "public"."orders" ("id", "total_amount", "created_at") FROM STDIN`
	if got != want {
		t.Errorf("buildCopyStatement() =\n %q\nwant %q", got, want)
	}
}

// TestBuildCopyStatementRejectsEmptyInput keeps a malformed statement from
// reaching the server.
func TestBuildCopyStatementRejectsEmptyInput(t *testing.T) {
	if _, err := buildCopyStatement("", []string{`"a"`}); err == nil {
		t.Error("buildCopyStatement() accepted an empty table name")
	}
	if _, err := buildCopyStatement(`"t"`, nil); err == nil {
		t.Error("buildCopyStatement() accepted an empty column list")
	}
}

// --- P1-4: staging table reuse ---

// TestStagingTableNameIsStableAcrossCalls is what makes reuse possible: the
// name must not change between batches, otherwise every flush creates a new
// table and the catalog churn returns.
func TestStagingTableNameIsStableAcrossCalls(t *testing.T) {
	const table = `"public"."orders"`
	const workerID = "w1"

	first := stagingTableName(table, workerID)
	for i := 0; i < 100; i++ {
		if got := stagingTableName(table, workerID); got != first {
			t.Fatalf("stagingTableName() is not deterministic: call %d returned %q, first call returned %q", i, got, first)
		}
	}
}

// TestStagingTableNameDiffersPerTargetTable prevents a worker that writes to
// two tables from reusing one staging table with the wrong column layout.
func TestStagingTableNameDiffersPerTargetTable(t *testing.T) {
	orders := stagingTableName(`"public"."orders"`, "w1")
	payments := stagingTableName(`"public"."payments"`, "w1")

	if orders == payments {
		t.Errorf("two different target tables produced the same staging table name %q", orders)
	}
}

// TestStagingTableNameIsAValidIdentifier ensures the generated name needs no
// quoting and cannot collide with user tables in a surprising way.
func TestStagingTableNameIsAValidIdentifier(t *testing.T) {
	name := stagingTableName(`"public"."orders"`, "w1")

	if !strings.HasPrefix(name, "beam_stage_") {
		t.Errorf("staging table name %q does not carry the beam_stage_ prefix", name)
	}
	if _, err := SanitizeIdentifier(name); err != nil {
		t.Errorf("staging table name %q is not a valid unquoted identifier: %v", name, err)
	}
}
