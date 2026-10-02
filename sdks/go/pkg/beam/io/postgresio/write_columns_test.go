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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

// Tests for the mapping from a Go struct to a PostgreSQL column list. Getting
// this wrong is not a crash; it is a column that quietly reads NULL or a name
// the server rejects with an error that points at the sanitizer rather than at
// the struct, so each case below names the symptom it prevents.

func TestColumnNameFromTag(t *testing.T) {
	tests := []struct {
		name     string
		tag      string
		wantName string
		wantExcl bool
		wantOK   bool
	}{
		{name: "absent tag", tag: "", wantOK: false},
		{name: "plain name", tag: "customer_id", wantName: "customer_id", wantOK: true},
		{
			name:     "name with options",
			tag:      "customer_id,omitempty",
			wantName: "customer_id",
			wantOK:   true,
		},
		{
			name:     "multiple options",
			tag:      "customer_id,omitempty,string",
			wantName: "customer_id",
			wantOK:   true,
		},
		{
			// `json:",omitempty"` sets an option without renaming, so the
			// next tag, or the field name, should decide.
			name:   "options only",
			tag:    ",omitempty",
			wantOK: false,
		},
		{name: "excluded", tag: "-", wantExcl: true, wantOK: true},
		{name: "excluded with options", tag: "-,omitempty", wantExcl: true, wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotName, gotExcl, gotOK := columnNameFromTag(tt.tag)
			if gotName != tt.wantName || gotExcl != tt.wantExcl || gotOK != tt.wantOK {
				t.Errorf("columnNameFromTag(%q) = (%q, %v, %v), want (%q, %v, %v)",
					tt.tag, gotName, gotExcl, gotOK, tt.wantName, tt.wantExcl, tt.wantOK)
			}
		})
	}
}

func TestToSnakeCase(t *testing.T) {
	tests := []struct{ in, want string }{
		{"ID", "id"},
		{"Amount", "amount"},
		{"CustomerID", "customer_id"},
		{"customerID", "customer_id"},
		{"HTTPServer", "http_server"},
		{"OrderLineItem", "order_line_item"},
		{"Line1", "line1"},
		{"Address2Line", "address2_line"},
		{"already_snake", "already_snake"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := toSnakeCase(tt.in); got != tt.want {
				t.Errorf("toSnakeCase(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// taggedRow exercises every tag shape a real row type mixes.
type taggedRow struct {
	ID         int64  `db:"id"`
	CustomerID string `json:"customer_id,omitempty"`
	Region     string `beam:"region_code" json:"region"`
	Note       string `json:",omitempty"`
	Internal   string `db:"-"`
	// Never read by design: this field exists so the test covers the case of
	// inspectColumns skipping unexported fields. Its absence from the wanted
	// column list below is the assertion.
	//lint:ignore U1000 deliberately unread fixture field; see comment above
	unexported string
}

func TestInspectColumnsDerivesUsableNames(t *testing.T) {
	fn := &writeFn{}
	fn.inspectColumns(reflect.TypeOf(taggedRow{}))

	want := []string{
		"id",          // db tag
		"customer_id", // json tag with its options stripped
		"region_code", // beam outranks json
		"note",        // an options-only tag names nothing, so the field name is used
	}
	if !reflect.DeepEqual(fn.columns, want) {
		t.Errorf("columns = %v, want %v", fn.columns, want)
	}
}

// TestInspectColumnsRejectedNothingBefore documents the concrete breakage: the
// derived names must survive the identifier sanitizer that every statement
// builder runs them through. A name of `customer_id,omitempty` or `-` does not.
func TestInspectColumnsProducesSanitizableNames(t *testing.T) {
	fn := &writeFn{}
	fn.inspectColumns(reflect.TypeOf(taggedRow{}))

	for _, col := range fn.columns {
		if _, err := SanitizeIdentifier(col); err != nil {
			t.Errorf("derived column %q is not a usable identifier: %v", col, err)
		}
	}
}

// untaggedRow stands in for a row type that relies on the field-name fallback.
type untaggedRow struct {
	ID         int64
	CustomerID string
	OrderTotal float64
}

func TestInspectColumnsFallsBackToSnakeCase(t *testing.T) {
	fn := &writeFn{}
	fn.inspectColumns(reflect.TypeOf(untaggedRow{}))

	want := []string{"id", "customer_id", "order_total"}
	if !reflect.DeepEqual(fn.columns, want) {
		t.Errorf("columns = %v, want %v; lowercasing alone yields names like \"customerid\", "+
			"which match no column in a conventionally named table", fn.columns, want)
	}
}

// TestExtractRowValuesMatchesTaggedColumns closes the loop. inspectColumns and
// resolveColumn have to read the tags the same way: when they disagreed, the
// column derived from `json:"customer_id,omitempty"` never matched the field
// it came from, and the row was written with that column NULL.
func TestExtractRowValuesMatchesTaggedColumns(t *testing.T) {
	fn := &writeFn{Type: beam.EncodedType{T: reflect.TypeOf(taggedRow{})}}
	fn.inspectColumns(fn.Type.T)

	row := taggedRow{ID: 7, CustomerID: "cust-1", Region: "us-east", Note: "hello"}
	vals := fn.extractRowValues(row)

	if len(vals) != len(fn.columns) {
		t.Fatalf("got %d values for %d columns", len(vals), len(fn.columns))
	}
	want := []any{int64(7), "cust-1", "us-east", "hello"}
	for i, col := range fn.columns {
		if vals[i] != want[i] {
			t.Errorf("column %q = %v, want %v", col, vals[i], want[i])
		}
	}
}
