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
	"sort"
	"strings"
)

// PartialRow is implemented by row types that carry values for only some of
// their columns.
//
// The motivating case is a CDC UPDATE of a row with a large TOASTed column the
// statement did not modify. PostgreSQL does not retransmit such a value, so the
// connector has no value to write. Writing the Go zero value would overwrite
// the stored one, and writing NULL would erase it; the only correct action is
// to leave the column out of the statement entirely.
//
// Row types that always carry every column do not need to implement this. The
// sink treats a type that does not implement it as carrying a full row, which
// keeps the common path unchanged.
type PartialRow interface {
	// UnchangedColumns returns the names of columns this row carries no value
	// for. Names must match those the sink derives from the struct's db, beam
	// or json tags. Returning nil or an empty slice means the row is complete.
	UnchangedColumns() []string
}

// rowUnchangedColumns returns the columns a row declares it has no value for,
// normalized to a sorted, de-duplicated slice so that two rows omitting the
// same columns produce the same signature regardless of ordering.
func rowUnchangedColumns(elem any) []string {
	pr, ok := elem.(PartialRow)
	if !ok {
		return nil
	}
	raw := pr.UnchangedColumns()
	if len(raw) == 0 {
		return nil
	}

	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, c := range raw {
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// writePartition is a group of rows that share the same set of present columns
// and can therefore be written by a single statement.
type writePartition struct {
	// Columns is the ordered subset of the sink's columns these rows carry.
	Columns []string
	// Rows are the batch members belonging to this partition.
	Rows []any
	// UnchangedColumns is the sorted set of columns omitted from Columns,
	// retained for logging and metrics.
	UnchangedColumns []string
}

// IsComplete reports whether this partition carries every column, which is the
// case the fast staged-COPY path can handle.
func (p writePartition) IsComplete() bool {
	return len(p.UnchangedColumns) == 0
}

// partitionBatchByUnchangedColumns groups a batch so that every group can be
// written with one statement.
//
// A batch shares a single SQL statement, but rows in it may omit different
// columns, and a statement can only name one column list. Grouping by the set
// of omitted columns is the smallest partitioning that makes each group
// expressible as one statement.
//
// The cost is one statement per distinct signature rather than one per batch.
// In practice the count is small: a signature corresponds to a combination of
// TOASTed columns left unmodified by a statement, and real workloads produce a
// handful of these, most commonly just the empty signature.
//
// Order is deterministic. The complete partition, when present, is returned
// first so the common case is written before any partial ones; the remainder
// follow in lexicographic signature order. Determinism matters because a retry
// must issue the same statements in the same order to avoid introducing a new
// deadlock ordering between attempts.
// An unrecognized name is rejected rather than ignored. subtractColumns
// removes nothing for a name that matches no column, so a typo left the row
// carrying its full column set while still marking it partial: the staged-COPY
// fast path was silently skipped, and under MERGE the write failed naming a
// column the target does not have. Neither symptom points at the row type that
// produced the name.
func partitionBatchByUnchangedColumns(batch []any, allColumns []string) ([]writePartition, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	// Fast path: scan once and avoid all allocation when no row is partial,
	// which is the overwhelmingly common case.
	anyPartial := false
	for _, item := range batch {
		if len(rowUnchangedColumns(item)) > 0 {
			anyPartial = true
			break
		}
	}
	if !anyPartial {
		return []writePartition{{Columns: allColumns, Rows: batch}}, nil
	}

	known := make(map[string]bool, len(allColumns))
	for _, c := range allColumns {
		known[c] = true
	}

	groups := make(map[string]*writePartition, 4)
	for _, item := range batch {
		unchanged := rowUnchangedColumns(item)
		for _, c := range unchanged {
			if !known[c] {
				return nil, fmt.Errorf("postgresio: UnchangedColumns reported %q, which is not a column of this row type; "+
					"names must match the columns derived from the db, beam or json struct tags (%s)",
					c, strings.Join(allColumns, ", "))
			}
		}
		signature := strings.Join(unchanged, "\x00")

		g, ok := groups[signature]
		if !ok {
			g = &writePartition{
				Columns:          subtractColumns(allColumns, unchanged),
				UnchangedColumns: unchanged,
			}
			groups[signature] = g
		}
		g.Rows = append(g.Rows, item)
	}

	signatures := make([]string, 0, len(groups))
	for sig := range groups {
		signatures = append(signatures, sig)
	}
	sort.Strings(signatures)

	out := make([]writePartition, 0, len(groups))
	if g, ok := groups[""]; ok {
		out = append(out, *g)
	}
	for _, sig := range signatures {
		if sig == "" {
			continue
		}
		out = append(out, *groups[sig])
	}
	return out, nil
}

// subtractColumns returns all columns not present in remove, preserving the
// original column order. Order is preserved because the value arrays passed to
// UNNEST are positional and must line up with the generated column list.
func subtractColumns(all []string, remove []string) []string {
	if len(remove) == 0 {
		return all
	}
	removeSet := make(map[string]bool, len(remove))
	for _, c := range remove {
		removeSet[c] = true
	}

	out := make([]string, 0, len(all))
	for _, c := range all {
		if !removeSet[c] {
			out = append(out, c)
		}
	}
	return out
}
