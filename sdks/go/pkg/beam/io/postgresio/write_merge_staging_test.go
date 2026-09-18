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
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

// mergeStagingRow is the shape a CDC source produces for a MERGE sink: the
// stored columns plus an operation column describing the change.
type mergeStagingRow struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
	Op   string `db:"op"`
}

func TestStagingOpColumnDDL(t *testing.T) {
	cases := []struct {
		name     string
		mode     WriteMode
		opColumn string
		want     string
		wantErr  bool
	}{
		{
			name:     "merge with op column emits the ALTER",
			mode:     WriteModeMerge,
			opColumn: "op",
			want:     `ALTER TABLE stage ADD COLUMN IF NOT EXISTS "op" text`,
		},
		{
			name:     "merge without an op column emits nothing",
			mode:     WriteModeMerge,
			opColumn: "",
			want:     "",
		},
		{
			name:     "upsert never touches the staging schema",
			mode:     WriteModeUpsert,
			opColumn: "op",
			want:     "",
		},
		{
			name:     "insert never touches the staging schema",
			mode:     WriteModeInsert,
			opColumn: "op",
			want:     "",
		},
		{
			name:     "a hostile op column name is rejected, not interpolated",
			mode:     WriteModeMerge,
			opColumn: `op" text; DROP TABLE users; --`,
			wantErr:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := stagingOpColumnDDL("stage", tc.mode, tc.opColumn)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("stagingOpColumnDDL(%q) = %q, want an error", tc.opColumn, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("stagingOpColumnDDL returned an unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("stagingOpColumnDDL = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestMergeStagingCoversEveryCopiedColumn is the regression test for the
// staged-COPY MERGE failure.
//
// Two decisions have to agree and are made in different functions. The staging
// table is created LIKE the target, and buildMergeQuery deliberately keeps the
// operation column out of the target column lists because it describes the
// change rather than the stored row. The consequence is that the operation
// column exists on no target and therefore on no clone, while the COPY that
// loads the staging table names every column of the Go row, including it. The
// COPY failed with 42703 on every MERGE batch.
//
// The test states the invariant the two functions have to satisfy jointly:
// every column the COPY names must exist on the staging table. It also asserts
// the pre-fix condition still fails, so that it cannot pass vacuously if the
// operation column later stops being excluded from the target column list.
func TestMergeStagingCoversEveryCopiedColumn(t *testing.T) {
	const (
		target = `"public"."accounts"`
		opCol  = "op"
	)

	fn := &writeFn{
		Table: target,
		Options: NewWriteOptions(
			WithWriteMode(WriteModeMerge),
			WithWriteMethod(WriteMethodStagedCopy),
			WithOpColumn(opCol),
			WithDeleteOpValue("D"),
		),
		Type:           beam.EncodedType{T: reflect.TypeOf(mergeStagingRow{})},
		PrimaryKeyCols: []string{"id"},
	}
	fn.inspectColumns(reflect.TypeOf(mergeStagingRow{}))

	if !includesColumn(fn.columns, opCol) {
		t.Fatalf("precondition failed: the COPY column list %v does not include the operation column %q", fn.columns, opCol)
	}

	// Columns the staging table inherits from the target. buildMergeQuery is the
	// authority on which columns the connector considers part of the target, so
	// the set is read out of the statement it generates rather than assumed.
	// target is in the form Write produces, so the sanitized-input variant is
	// the one the sink itself calls.
	mergeSQL, err := buildMergeQueryForSanitizedTables(target, `"stage"`, fn.columns, fn.PrimaryKeyCols, opCol, "D")
	if err != nil {
		t.Fatalf("buildMergeQueryForSanitizedTables failed: %v", err)
	}
	clonedCols := insertColumnsOf(t, mergeSQL)

	if includesColumn(clonedCols, opCol) {
		t.Fatalf("the operation column %q is now part of the target column list %v; "+
			"this test's premise no longer holds and the staging ALTER may be redundant", opCol, clonedCols)
	}

	// Negative control: without the ALTER the COPY names a column the staging
	// table does not have. This is exactly the pre-fix behaviour, and asserting
	// it keeps the check below from passing for the wrong reason.
	if missing := missingFrom(fn.columns, clonedCols); len(missing) == 0 {
		t.Fatalf("negative control failed: a staging table cloned from the target already covers every copied column %v, "+
			"so this test would pass with the fix reverted", fn.columns)
	} else if !reflect.DeepEqual(missing, []string{opCol}) {
		t.Fatalf("negative control: expected only %q to be missing from the clone, got %v", opCol, missing)
	}

	// The fix: the staging table gains the operation column before the COPY.
	alterSQL, err := stagingOpColumnDDL(`"stage"`, fn.Options.WriteMode, fn.Options.OpColumn)
	if err != nil {
		t.Fatalf("stagingOpColumnDDL failed: %v", err)
	}
	if alterSQL == "" {
		t.Fatal("stagingOpColumnDDL emitted nothing for a MERGE sink with an operation column")
	}

	stagingCols := append(append([]string{}, clonedCols...), addedColumnOf(t, alterSQL))
	if missing := missingFrom(fn.columns, stagingCols); len(missing) != 0 {
		t.Errorf("the COPY names %v, which the staging table (%v) does not have; the COPY will fail with 42703", missing, stagingCols)
	}
}

// TestMergeQueryUsesTheSanitizationStageTheSinkIsAt is the regression test for
// the second half of the staged-COPY MERGE failure.
//
// Write sanitizes the table name once and stores the quoted result on writeFn,
// and every statement executeStagedCopy builds interpolates that stored value
// directly. The MERGE builder was the one exception: it sanitized its arguments
// itself, so the stored value went through SanitizeTableIdentifier a second
// time and was rejected, because a quotation mark already present in the input
// is indistinguishable from an injected one.
//
// The test asserts both directions, so it keeps biting if either the storage
// convention or the builder's contract drifts.
func TestMergeQueryUsesTheSanitizationStageTheSinkIsAt(t *testing.T) {
	const raw = "public.accounts"

	stored, err := SanitizeTableIdentifier(raw)
	if err != nil {
		t.Fatalf("SanitizeTableIdentifier(%q) failed: %v", raw, err)
	}
	if stored == raw {
		t.Fatalf("precondition failed: sanitization left %q unchanged, so double sanitization would be harmless", raw)
	}

	cols := []string{"id", "name"}
	pks := []string{"id"}

	if _, err := buildMergeQuery(stored, "stage", cols, pks, "", ""); err == nil {
		t.Error("buildMergeQuery accepted an already-sanitized table name; " +
			"if it is now idempotent, the sink no longer needs the sanitized-input variant")
	}

	query, err := buildMergeQueryForSanitizedTables(stored, `"stage"`, cols, pks, "", "")
	if err != nil {
		t.Fatalf("buildMergeQueryForSanitizedTables rejected the name the sink actually holds (%q): %v", stored, err)
	}
	if !strings.Contains(query, "MERGE INTO "+stored+" AS target") {
		t.Errorf("generated MERGE does not target %s:\n%s", stored, query)
	}
	if strings.Contains(query, `""`) {
		t.Errorf("generated MERGE contains doubled quotes:\n%s", query)
	}
}

func TestWritePartitionMergeRefusesUnnestFallback(t *testing.T) {
	fn := &writeFn{
		Table: `"public"."accounts"`,
		Options: NewWriteOptions(
			WithWriteMode(WriteModeMerge),
			WithWriteMethod(WriteMethodUnnest),
			WithOpColumn("op"),
			WithDeleteOpValue("D"),
		),
		Type:           beam.EncodedType{T: reflect.TypeOf(mergeStagingRow{})},
		PrimaryKeyCols: []string{"id"},
	}
	fn.inspectColumns(reflect.TypeOf(mergeStagingRow{}))

	part := writePartition{
		Columns: fn.columns,
		Rows:    []any{mergeStagingRow{ID: 1, Name: "a", Op: "D"}},
	}

	var succeeded int
	var failed []FailedRow
	// fn.db is nil: reaching the database at all would panic, which is itself
	// the assertion that the guard returns before any statement is issued.
	err := fn.writePartition(context.Background(), part,
		func(beam.X) { succeeded++ },
		func(fr FailedRow) { failed = append(failed, fr) })

	if err == nil {
		t.Fatal("writePartition accepted a MERGE batch on the UNNEST write method; deletes would have been silently dropped")
	}
	if !strings.Contains(err.Error(), "cannot express deletes") {
		t.Errorf("error does not explain the cause: %v", err)
	}
	if succeeded != 0 {
		t.Errorf("emitted %d rows as written", succeeded)
	}
	if len(failed) != len(part.Rows) {
		t.Errorf("routed %d of %d rows to the failed output; every row of a rejected batch must be recoverable", len(failed), len(part.Rows))
	}
}

func TestWritePartitionMergeRefusesPartialRows(t *testing.T) {
	fn := &writeFn{
		Table: `"public"."accounts"`,
		Options: NewWriteOptions(
			WithWriteMode(WriteModeMerge),
			WithWriteMethod(WriteMethodStagedCopy),
			WithOpColumn("op"),
			WithDeleteOpValue("D"),
		),
		Type:           beam.EncodedType{T: reflect.TypeOf(mergeStagingRow{})},
		PrimaryKeyCols: []string{"id"},
	}
	fn.inspectColumns(reflect.TypeOf(mergeStagingRow{}))

	// A row that omits a column, as a CDC update of an unmodified TOASTed value
	// does. The staging table carries every column, so streaming this row into
	// it would write a zero value over the stored one.
	part := writePartition{
		Columns:          []string{"id", "op"},
		Rows:             []any{mergeStagingRow{ID: 1, Op: "U"}},
		UnchangedColumns: []string{"name"},
	}

	var succeeded int
	var failed []FailedRow
	err := fn.writePartition(context.Background(), part,
		func(beam.X) { succeeded++ },
		func(fr FailedRow) { failed = append(failed, fr) })

	if err == nil {
		t.Fatal("writePartition accepted a partial row in MERGE mode")
	}
	if !strings.Contains(err.Error(), "name") {
		t.Errorf("error does not name the omitted column: %v", err)
	}
	if succeeded != 0 {
		t.Errorf("emitted %d rows as written", succeeded)
	}
	if len(failed) != len(part.Rows) {
		t.Errorf("routed %d of %d rows to the failed output", len(failed), len(part.Rows))
	}
}

// insertColumnsOf returns the unquoted column names from the INSERT clause of a
// generated MERGE statement.
func insertColumnsOf(t *testing.T, mergeSQL string) []string {
	t.Helper()

	_, after, found := strings.Cut(mergeSQL, "INSERT (")
	if !found {
		t.Fatalf("generated MERGE has no INSERT clause:\n%s", mergeSQL)
	}
	list, _, found := strings.Cut(after, ")")
	if !found {
		t.Fatalf("generated MERGE has an unterminated INSERT column list:\n%s", mergeSQL)
	}

	var out []string
	for _, col := range strings.Split(list, ",") {
		out = append(out, strings.Trim(strings.TrimSpace(col), `"`))
	}
	return out
}

// addedColumnOf returns the unquoted column name from a generated ALTER TABLE
// ... ADD COLUMN statement. Reading it back out of the statement, rather than
// assuming it, keeps the test honest about what would actually be executed.
func addedColumnOf(t *testing.T, alterSQL string) string {
	t.Helper()

	_, after, found := strings.Cut(alterSQL, "ADD COLUMN IF NOT EXISTS ")
	if !found {
		t.Fatalf("not an ADD COLUMN statement: %q", alterSQL)
	}
	name, _, found := strings.Cut(after, " ")
	if !found {
		t.Fatalf("ADD COLUMN statement has no type: %q", alterSQL)
	}
	return strings.Trim(name, `"`)
}

// includesColumn reports whether a column list contains a name. The package
// has an equivalent helper, but it is introduced by a later commit in the
// stack, and a test file may not reference a symbol its own commit does not
// yet define.
func includesColumn(list []string, name string) bool {
	for _, c := range list {
		if c == name {
			return true
		}
	}
	return false
}

// missingFrom returns the members of want that are absent from has.
func missingFrom(want, has []string) []string {
	var out []string
	for _, w := range want {
		if !includesColumn(has, w) {
			out = append(out, w)
		}
	}
	return out
}
