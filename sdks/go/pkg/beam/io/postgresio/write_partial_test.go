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
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

// partialDoc models a CDC row whose large TOASTed column may be absent.
type partialDoc struct {
	ID      int64  `db:"id"`
	Title   string `db:"title"`
	Body    string `db:"body"`
	Version int64  `db:"version"`

	unchanged []string
}

func (d partialDoc) UnchangedColumns() []string { return d.unchanged }

// completeDoc has the same shape but never declares absent columns, standing in
// for an ordinary row type that does not implement PartialRow.
type completeDoc struct {
	ID      int64  `db:"id"`
	Title   string `db:"title"`
	Body    string `db:"body"`
	Version int64  `db:"version"`
}

var testDocColumns = []string{"id", "title", "body", "version"}

func newPartialWriteFn(t *testing.T) *writeFn {
	t.Helper()
	fn := &writeFn{
		Table:          `"public"."docs"`,
		PrimaryKeyCols: []string{"id"},
		Options:        NewWriteOptions(WithWriteMode(WriteModeUpsert)),
		columns:        testDocColumns,
	}
	fn.Type = beam.EncodedType{T: reflect.TypeOf(partialDoc{})}
	return fn
}

// mustPartition partitions a batch that is expected to be well formed. The
// tests below exercise partition layout, so the error return is checked once
// here rather than at every call site.
func mustPartition(t *testing.T, batch []any, allColumns []string) []writePartition {
	t.Helper()
	parts, err := partitionBatchByUnchangedColumns(batch, allColumns)
	if err != nil {
		t.Fatalf("partitionBatchByUnchangedColumns(%v) returned an unexpected error: %v", batch, err)
	}
	return parts
}

// --- interface detection ---

func TestRowUnchangedColumnsIgnoresNonPartialRows(t *testing.T) {
	if got := rowUnchangedColumns(completeDoc{ID: 1}); got != nil {
		t.Errorf("a type not implementing PartialRow must be treated as complete, got %v", got)
	}
	if got := rowUnchangedColumns(partialDoc{ID: 1}); got != nil {
		t.Errorf("a PartialRow returning nil must be treated as complete, got %v", got)
	}
	if got := rowUnchangedColumns(partialDoc{unchanged: []string{}}); got != nil {
		t.Errorf("an empty slice must be treated as complete, got %v", got)
	}
}

// TestRowUnchangedColumnsNormalizes ensures two rows omitting the same columns
// land in the same partition regardless of declaration order or duplicates.
func TestRowUnchangedColumnsNormalizes(t *testing.T) {
	a := rowUnchangedColumns(partialDoc{unchanged: []string{"body", "title"}})
	b := rowUnchangedColumns(partialDoc{unchanged: []string{"title", "body", "title", ""}})

	if !reflect.DeepEqual(a, b) {
		t.Errorf("normalization mismatch: %v vs %v; these rows would be written by two statements "+
			"when one would do", a, b)
	}
	if !reflect.DeepEqual(a, []string{"body", "title"}) {
		t.Errorf("expected sorted de-duplicated names, got %v", a)
	}
}

// --- partitioning ---

// TestPartitionKeepsCompleteBatchWhole asserts the common path is untouched:
// a batch with no partial rows produces exactly one partition holding the
// original slice.
func TestPartitionKeepsCompleteBatchWhole(t *testing.T) {
	batch := []any{completeDoc{ID: 1}, completeDoc{ID: 2}, completeDoc{ID: 3}}

	parts := mustPartition(t, batch, testDocColumns)
	if len(parts) != 1 {
		t.Fatalf("got %d partitions, want 1", len(parts))
	}
	if len(parts[0].Rows) != 3 {
		t.Errorf("got %d rows, want 3", len(parts[0].Rows))
	}
	if !parts[0].IsComplete() {
		t.Error("partition must report complete so it keeps the staged-COPY fast path")
	}
	if !reflect.DeepEqual(parts[0].Columns, testDocColumns) {
		t.Errorf("columns = %v, want all columns", parts[0].Columns)
	}
}

func TestPartitionGroupsBySignature(t *testing.T) {
	batch := []any{
		partialDoc{ID: 1},
		partialDoc{ID: 2, unchanged: []string{"body"}},
		partialDoc{ID: 3, unchanged: []string{"body"}},
		partialDoc{ID: 4, unchanged: []string{"body", "title"}},
		partialDoc{ID: 5},
	}

	parts := mustPartition(t, batch, testDocColumns)
	if len(parts) != 3 {
		t.Fatalf("got %d partitions, want 3 (complete, body, body+title)", len(parts))
	}

	// The complete partition must come first.
	if !parts[0].IsComplete() {
		t.Errorf("first partition must be the complete one, got unchanged=%v", parts[0].UnchangedColumns)
	}
	if len(parts[0].Rows) != 2 {
		t.Errorf("complete partition has %d rows, want 2", len(parts[0].Rows))
	}

	bySig := map[string]writePartition{}
	for _, p := range parts {
		bySig[strings.Join(p.UnchangedColumns, ",")] = p
	}

	bodyPart, ok := bySig["body"]
	if !ok {
		t.Fatalf("no partition for the body signature; got %v", keysOf(bySig))
	}
	if len(bodyPart.Rows) != 2 {
		t.Errorf("body partition has %d rows, want 2", len(bodyPart.Rows))
	}
	if !reflect.DeepEqual(bodyPart.Columns, []string{"id", "title", "version"}) {
		t.Errorf("body partition columns = %v, want id/title/version", bodyPart.Columns)
	}

	bothPart, ok := bySig["body,title"]
	if !ok {
		t.Fatalf("no partition for the body+title signature; got %v", keysOf(bySig))
	}
	if !reflect.DeepEqual(bothPart.Columns, []string{"id", "version"}) {
		t.Errorf("body+title partition columns = %v, want id/version", bothPart.Columns)
	}
}

// TestPartitionPreservesEveryRow is the conservation property: partitioning
// must neither drop nor duplicate a row, or the sink would silently lose writes.
func TestPartitionPreservesEveryRow(t *testing.T) {
	batch := []any{
		partialDoc{ID: 1},
		partialDoc{ID: 2, unchanged: []string{"body"}},
		partialDoc{ID: 3, unchanged: []string{"title"}},
		partialDoc{ID: 4, unchanged: []string{"body", "title"}},
		partialDoc{ID: 5, unchanged: []string{"body"}},
		partialDoc{ID: 6},
		partialDoc{ID: 7, unchanged: []string{"version"}},
	}

	parts := mustPartition(t, batch, testDocColumns)

	seen := map[int64]int{}
	total := 0
	for _, p := range parts {
		for _, r := range p.Rows {
			seen[r.(partialDoc).ID]++
			total++
		}
	}

	if total != len(batch) {
		t.Errorf("partitions hold %d rows, batch had %d", total, len(batch))
	}
	for id := int64(1); id <= 7; id++ {
		if seen[id] != 1 {
			t.Errorf("row %d appears %d times across partitions, want exactly 1", id, seen[id])
		}
	}
}

// TestPartitionPreservesRowOrderWithinPartition matters because the compactor
// has already sorted the batch by primary key to avoid deadlocks between
// concurrent writers. Reordering inside a partition would defeat that.
func TestPartitionPreservesRowOrderWithinPartition(t *testing.T) {
	batch := []any{
		partialDoc{ID: 1, unchanged: []string{"body"}},
		partialDoc{ID: 2},
		partialDoc{ID: 3, unchanged: []string{"body"}},
		partialDoc{ID: 4},
		partialDoc{ID: 5, unchanged: []string{"body"}},
	}

	parts := mustPartition(t, batch, testDocColumns)
	for _, p := range parts {
		var last int64
		for _, r := range p.Rows {
			id := r.(partialDoc).ID
			if id < last {
				t.Errorf("rows out of order within partition %v: %d follows %d",
					p.UnchangedColumns, id, last)
			}
			last = id
		}
	}
}

// TestPartitionOrderIsDeterministic guards the retry path: a batch retried
// after a deadlock must issue the same statements in the same order, or the two
// attempts can deadlock against each other.
func TestPartitionOrderIsDeterministic(t *testing.T) {
	batch := []any{
		partialDoc{ID: 1, unchanged: []string{"version"}},
		partialDoc{ID: 2, unchanged: []string{"body"}},
		partialDoc{ID: 3},
		partialDoc{ID: 4, unchanged: []string{"title"}},
		partialDoc{ID: 5, unchanged: []string{"body", "title"}},
	}

	var reference []string
	for trial := 0; trial < 50; trial++ {
		parts := mustPartition(t, batch, testDocColumns)
		order := make([]string, len(parts))
		for i, p := range parts {
			order[i] = strings.Join(p.UnchangedColumns, ",")
		}
		if reference == nil {
			reference = order
			continue
		}
		if !reflect.DeepEqual(order, reference) {
			t.Fatalf("partition order varies between calls: %v vs %v", order, reference)
		}
	}
}

func TestPartitionHandlesEmptyBatch(t *testing.T) {
	if parts := mustPartition(t, nil, testDocColumns); parts != nil {
		t.Errorf("expected nil for a nil batch, got %v", parts)
	}
	if parts := mustPartition(t, []any{}, testDocColumns); parts != nil {
		t.Errorf("expected nil for an empty batch, got %v", parts)
	}
}

func TestSubtractColumns(t *testing.T) {
	all := []string{"id", "title", "body", "version"}

	if got := subtractColumns(all, nil); !reflect.DeepEqual(got, all) {
		t.Errorf("subtracting nothing changed the list: %v", got)
	}
	if got := subtractColumns(all, []string{"body"}); !reflect.DeepEqual(got, []string{"id", "title", "version"}) {
		t.Errorf("got %v", got)
	}
	// Order must survive, because UNNEST arguments are positional.
	if got := subtractColumns(all, []string{"version", "title"}); !reflect.DeepEqual(got, []string{"id", "body"}) {
		t.Errorf("got %v, want id/body in original order", got)
	}
	// An unknown name must be ignored rather than shifting the list.
	if got := subtractColumns(all, []string{"nonexistent"}); !reflect.DeepEqual(got, all) {
		t.Errorf("got %v", got)
	}
}

// --- generated SQL ---

// TestPartialUpdateOmitsUnchangedColumnFromSetClause is the property the whole
// change exists for. The omitted column must appear in neither the INSERT
// column list nor the SET clause, so the stored value survives.
func TestPartialUpdateOmitsUnchangedColumnFromSetClause(t *testing.T) {
	fn := newPartialWriteFn(t)
	batch := []any{partialDoc{ID: 1, Title: "new title", Version: 2, unchanged: []string{"body"}}}

	parts := mustPartition(t, batch, fn.columns)
	if len(parts) != 1 {
		t.Fatalf("got %d partitions, want 1", len(parts))
	}

	query, args, err := fn.buildUnnestQueryForColumns(parts[0].Rows, parts[0].Columns)
	if err != nil {
		t.Fatalf("buildUnnestQueryForColumns: %v", err)
	}

	if strings.Contains(query, `"body"`) {
		t.Errorf("the unchanged column appears in the statement, so the stored value "+
			"would be overwritten with a zero value:\n%s", query)
	}
	for _, col := range []string{`"id"`, `"title"`, `"version"`} {
		if !strings.Contains(query, col) {
			t.Errorf("column %s missing from statement:\n%s", col, query)
		}
	}
	if !strings.Contains(query, `"title" = EXCLUDED."title"`) {
		t.Errorf("changed column is not updated on conflict:\n%s", query)
	}
	if !strings.Contains(query, `ON CONFLICT ("id") DO UPDATE SET`) {
		t.Errorf("expected an upsert:\n%s", query)
	}

	// One argument array per emitted column, not per declared column.
	if len(args) != 3 {
		t.Errorf("got %d argument arrays, want 3; placeholders and arguments must agree", len(args))
	}
}

// TestCompleteRowStatementIsUnchanged pins the common path so this change
// cannot silently alter the SQL for ordinary rows.
func TestCompleteRowStatementIsUnchanged(t *testing.T) {
	fn := newPartialWriteFn(t)
	batch := []any{partialDoc{ID: 1, Title: "t", Body: "b", Version: 1}}

	viaPartition := mustPartition(t, batch, fn.columns)
	partQuery, partArgs, err := fn.buildUnnestQueryForColumns(viaPartition[0].Rows, viaPartition[0].Columns)
	if err != nil {
		t.Fatalf("partitioned build: %v", err)
	}

	directQuery, directArgs, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("direct build: %v", err)
	}

	if partQuery != directQuery {
		t.Errorf("partitioning changed the statement for a complete row:\n got: %s\nwant: %s",
			partQuery, directQuery)
	}
	if len(partArgs) != len(directArgs) {
		t.Errorf("argument count differs: %d vs %d", len(partArgs), len(directArgs))
	}
}

// TestPartialUpdateValuesAlignWithColumns catches the positional error this
// refactor most easily introduces: values must line up with the reduced column
// list, not the original one.
func TestPartialUpdateValuesAlignWithColumns(t *testing.T) {
	fn := newPartialWriteFn(t)
	batch := []any{partialDoc{
		ID: 77, Title: "the title", Body: "SHOULD NOT APPEAR", Version: 5,
		unchanged: []string{"body"},
	}}

	parts := mustPartition(t, batch, fn.columns)
	query, args, err := fn.buildUnnestQueryForColumns(parts[0].Rows, parts[0].Columns)
	if err != nil {
		t.Fatalf("buildUnnestQueryForColumns: %v", err)
	}

	// Column list order must be id, title, version; placeholders are numbered
	// in that same order.
	idxID := strings.Index(query, `"id"`)
	idxTitle := strings.Index(query, `"title"`)
	idxVersion := strings.Index(query, `"version"`)
	if !(idxID < idxTitle && idxTitle < idxVersion) {
		t.Errorf("column order in statement is not id, title, version:\n%s", query)
	}

	if len(args) != 3 {
		t.Fatalf("got %d args, want 3", len(args))
	}
	// The omitted value must not have been passed as a parameter at all.
	for i, a := range args {
		if strings.Contains(strings.ToUpper(toStringForTest(a)), "SHOULD NOT APPEAR") {
			t.Errorf("argument %d carries the omitted column's value: %v", i, a)
		}
	}
}

// TestPartialUpdateWithAllNonKeyColumnsUnchanged covers the degenerate case
// where only the primary key remains. There is nothing to update, so the
// statement must not emit an empty SET clause.
func TestPartialUpdateWithAllNonKeyColumnsUnchanged(t *testing.T) {
	fn := newPartialWriteFn(t)
	batch := []any{partialDoc{ID: 1, unchanged: []string{"title", "body", "version"}}}

	parts := mustPartition(t, batch, fn.columns)
	query, _, err := fn.buildUnnestQueryForColumns(parts[0].Rows, parts[0].Columns)
	if err != nil {
		t.Fatalf("buildUnnestQueryForColumns: %v", err)
	}

	if strings.Contains(query, "DO UPDATE SET ,") || strings.Contains(query, "SET  ") {
		t.Errorf("malformed SET clause:\n%s", query)
	}
	if !strings.Contains(query, "DO NOTHING") {
		t.Errorf("with no updatable columns the statement must DO NOTHING:\n%s", query)
	}
}

func TestBuildUnnestQueryRejectsEmptyColumnSet(t *testing.T) {
	fn := newPartialWriteFn(t)
	if _, _, err := fn.buildUnnestQueryForColumns([]any{partialDoc{ID: 1}}, nil); err == nil {
		t.Error("expected an error for an empty column set")
	}
}

// --- validation of declared names ---

// TestPartitionRejectsUnknownUnchangedColumn covers a name that matches no
// column. subtractColumns removes nothing for it, so the row kept its full
// column set while still counting as partial: the staged-COPY fast path was
// silently skipped, and under MERGE the write failed naming a column the
// target does not have. Neither symptom points back at the row type.
func TestPartitionRejectsUnknownUnchangedColumn(t *testing.T) {
	batch := []any{
		partialDoc{ID: 1, unchanged: []string{"body"}},
		partialDoc{ID: 2, unchanged: []string{"bdoy"}}, // transposed
	}

	_, err := partitionBatchByUnchangedColumns(batch, testDocColumns)
	if err == nil {
		t.Fatal("a misspelled column name was accepted; the row would be written by a statement " +
			"that silently disagrees with the one its signature implies")
	}
	if !strings.Contains(err.Error(), "bdoy") {
		t.Errorf("error does not name the offending column: %v", err)
	}
	for _, col := range testDocColumns {
		if !strings.Contains(err.Error(), col) {
			t.Errorf("error does not list the valid column %q, so the caller cannot see the "+
				"intended spelling: %v", col, err)
		}
	}
}

// TestPartitionAcceptsCompleteBatchWithoutValidation confirms the validation
// does not run when no row is partial, keeping the common path allocation-free.
func TestPartitionAcceptsCompleteBatchWithoutValidation(t *testing.T) {
	batch := []any{completeDoc{ID: 1}}

	// An empty column list would fail validation if it ran, but a batch with
	// no partial rows never consults it.
	parts, err := partitionBatchByUnchangedColumns(batch, nil)
	if err != nil {
		t.Fatalf("a complete batch must not be validated against the column list: %v", err)
	}
	if len(parts) != 1 {
		t.Errorf("got %d partitions, want 1", len(parts))
	}
}

func TestPartitionRejectsUnknownColumnEvenWhenOthersAreValid(t *testing.T) {
	batch := []any{partialDoc{ID: 1, unchanged: []string{"body", "not_a_column"}}}

	if _, err := partitionBatchByUnchangedColumns(batch, testDocColumns); err == nil {
		t.Error("a signature mixing a valid and an invalid name was accepted")
	}
}

// --- helpers ---

func keysOf(m map[string]writePartition) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func toStringForTest(v any) string {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return ""
	}
	return strings.ToUpper(reflect.Indirect(rv).String())
}
