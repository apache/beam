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
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/lib/pq"
)

func TestBuildUnnestQueryUpsert(t *testing.T) {
	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeUpsert), WithPrimaryKeyColumns("id")),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: []string{"id"},
		columns:        []string{"id", "region", "amount"},
		colTypes: map[string]string{
			"id":     "INT8",
			"region": "TEXT",
			"amount": "FLOAT8",
		},
	}

	batch := []any{
		TestOrder{ID: 1, Region: "US", Amount: 100.0},
		TestOrder{ID: 2, Region: "EU", Amount: 200.0},
	}

	query, args, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error building unnest query: %v", err)
	}

	expectedPrefix := `INSERT INTO "public"."orders" ("id", "region", "amount") SELECT t.col0, t.col1, t.col2 FROM UNNEST($1::INT8[], $2::TEXT[], $3::FLOAT8[]) AS t(col0, col1, col2)`
	if !strings.HasPrefix(query, expectedPrefix) {
		t.Errorf("expected query to start with %q, got %q", expectedPrefix, query)
	}

	expectedConflict := `ON CONFLICT ("id") DO UPDATE SET "region" = EXCLUDED."region", "amount" = EXCLUDED."amount"`
	if !strings.Contains(query, expectedConflict) {
		t.Errorf("expected conflict clause %q in query: %q", expectedConflict, query)
	}

	if len(args) != 3 {
		t.Errorf("expected 3 array arguments for 3 columns, got %d", len(args))
	}
}

func TestBuildUnnestQueryUpsert_WithUpdateFields(t *testing.T) {
	fn := &writeFn{
		Table: `"public"."orders"`,
		Options: NewWriteOptions(
			WithWriteMode(WriteModeUpsert),
			WithPrimaryKeyColumns("id"),
			WithUpdateFields("amount"),
		),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: []string{"id"},
		columns:        []string{"id", "region", "amount"},
		colTypes: map[string]string{
			"id":     "INT8",
			"region": "TEXT",
			"amount": "FLOAT8",
		},
	}

	batch := []any{
		TestOrder{ID: 1, Region: "US", Amount: 100.0},
	}

	query, _, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error building unnest query: %v", err)
	}

	expectedConflict := `ON CONFLICT ("id") DO UPDATE SET "amount" = EXCLUDED."amount"`
	if !strings.Contains(query, expectedConflict) {
		t.Errorf("expected conflict clause %q in query: %q", expectedConflict, query)
	}
	if strings.Contains(query, "EXCLUDED.\"region\"") {
		t.Errorf("expected query NOT to update region when UpdateFields is set to [amount], got %q", query)
	}
}

func TestBuildUnnestQueryInsertOnly(t *testing.T) {
	fn := &writeFn{
		Table:          `"orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeInsert)),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: nil,
		columns:        []string{"id", "region", "amount"},
		colTypes: map[string]string{
			"id":     "INT8",
			"region": "TEXT",
			"amount": "FLOAT8",
		},
	}

	batch := []any{
		TestOrder{ID: 1, Region: "US", Amount: 50.0},
	}

	query, args, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Contains(query, "ON CONFLICT") {
		t.Errorf("expected no ON CONFLICT clause for WriteModeInsert, got %q", query)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestBuildUnnestQueryUpdate(t *testing.T) {
	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeUpdate), WithPrimaryKeyColumns("id")),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: []string{"id"},
		columns:        []string{"id", "region", "amount"},
		colTypes: map[string]string{
			"id":     "INT8",
			"region": "TEXT",
			"amount": "FLOAT8",
		},
	}

	batch := []any{
		TestOrder{ID: 1, Region: "US", Amount: 50.0},
	}

	query, args, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.HasPrefix(query, `UPDATE "public"."orders" AS target SET`) {
		t.Errorf("expected UPDATE statement, got %q", query)
	}
	if !strings.Contains(query, `FROM (SELECT t.col0 AS "id", t.col1 AS "region", t.col2 AS "amount" FROM UNNEST`) {
		t.Errorf("expected UNNEST subquery, got %q", query)
	}
	if !strings.Contains(query, `WHERE target."id" = source."id"`) {
		t.Errorf("expected WHERE clause on primary key, got %q", query)
	}
	if !strings.Contains(query, `"region" = source."region"`) || !strings.Contains(query, `"amount" = source."amount"`) {
		t.Errorf("expected SET clause on non-pk columns, got %q", query)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestBuildUnnestQueryUpdate_WithUpdateFields(t *testing.T) {
	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeUpdate), WithPrimaryKeyColumns("id"), WithUpdateFields("amount")),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: []string{"id"},
		columns:        []string{"id", "region", "amount"},
		colTypes: map[string]string{
			"id":     "INT8",
			"region": "TEXT",
			"amount": "FLOAT8",
		},
	}

	batch := []any{
		TestOrder{ID: 1, Region: "US", Amount: 50.0},
		TestOrder{ID: 2, Region: "EU", Amount: 75.0},
	}

	query, args, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.HasPrefix(query, `UPDATE "public"."orders" AS target SET`) {
		t.Errorf("expected UPDATE statement, got %q", query)
	}
	if !strings.Contains(query, `"amount" = source."amount"`) {
		t.Errorf("expected SET clause on amount, got %q", query)
	}
	if strings.Contains(query, `"region" = source."region"`) {
		t.Errorf("expected region to NOT be in SET clause when update_fields only names amount, got %q", query)
	}
	if strings.Contains(query, `"id" = source."id"`) && !strings.Contains(query, `WHERE target."id" = source."id"`) {
		t.Errorf("expected primary key to be in WHERE clause, not SET, got %q", query)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d", len(args))
	}
}

func TestBuildStagedCopyMergeQueryUpdate(t *testing.T) {
	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeUpdate), WithPrimaryKeyColumns("id"), WithUpdateFields("amount")),
		PrimaryKeyCols: []string{"id"},
		columns:        []string{"id", "region", "amount"},
	}

	query, err := fn.buildStagedCopyMergeQuery("beam_stage_w1_test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantPrefix := `UPDATE "public"."orders" SET "amount" = beam_stage_w1_test."amount" FROM beam_stage_w1_test WHERE "public"."orders"."id" = beam_stage_w1_test."id"`
	if query != wantPrefix {
		t.Errorf("buildStagedCopyMergeQuery() =\n  %q\nwant:\n  %q", query, wantPrefix)
	}

	// Without UpdateFields, all non-PK columns are updated
	fnNoFilter := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeUpdate), WithPrimaryKeyColumns("id")),
		PrimaryKeyCols: []string{"id"},
		columns:        []string{"id", "region", "amount"},
	}

	queryAll, err := fnNoFilter.buildStagedCopyMergeQuery("beam_stage_w1_test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(queryAll, `"region" = beam_stage_w1_test."region"`) || !strings.Contains(queryAll, `"amount" = beam_stage_w1_test."amount"`) {
		t.Errorf("expected all non-pk columns in SET clause, got %q", queryAll)
	}
	if strings.Contains(queryAll, `"id" = beam_stage_w1_test."id"`) && !strings.Contains(queryAll, `WHERE "public"."orders"."id" = beam_stage_w1_test."id"`) {
		t.Errorf("expected id in WHERE, not SET, got %q", queryAll)
	}

	// WriteModeUpdate requires PKs
	fnNoPK := &writeFn{
		Table:   `"public"."orders"`,
		Options: NewWriteOptions(WithWriteMode(WriteModeUpdate)),
		columns: []string{"id", "region", "amount"},
	}
	if _, err := fnNoPK.buildStagedCopyMergeQuery("beam_stage_w1_test"); err == nil {
		t.Error("expected error when WriteModeUpdate has no primary keys, got nil")
	}
}

func TestBuildUnnestQueryNullArray(t *testing.T) {
	type ArrayOrder struct {
		ID   int64    `db:"id"`
		Tags []string `db:"tags"`
	}

	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(WithWriteMode(WriteModeInsert)),
		Type:           beam.EncodedType{T: reflect.TypeOf(ArrayOrder{})},
		PrimaryKeyCols: nil,
		columns:        []string{"id", "tags"},
		colTypes: map[string]string{
			"id":   "INT8",
			"tags": "_TEXT",
		},
	}

	batch := []any{
		ArrayOrder{ID: 1, Tags: []string{"electronics"}},
		ArrayOrder{ID: 2, Tags: nil}, // nil array
	}

	query, args, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(query, `SELECT t.col0, t.col1::TEXT[] FROM UNNEST`) {
		t.Errorf("expected select to cast t.col1::TEXT[], got %q", query)
	}

	if len(args) != 2 {
		t.Fatalf("expected 2 arguments, got %d", len(args))
	}

	arrayVal, err := args[1].(driver.Valuer).Value()
	if err != nil {
		t.Fatalf("failed to get driver value from array arg: %v", err)
	}
	strVal, ok := arrayVal.(string)
	if !ok {
		t.Fatalf("expected string from driver.Valuer, got %T", arrayVal)
	}
	if strings.Contains(strVal, `,""`) || strings.Contains(strVal, `""`) {
		t.Fatalf("array literal contains empty string instead of NULL: %s", strVal)
	}
	if !strings.Contains(strVal, "NULL") {
		t.Fatalf("expected array literal to contain NULL for nil slice, got %s", strVal)
	}
}

func TestBuildMergeQueryNullOp(t *testing.T) {
	sql, err := buildMergeQuery("public.orders", "pg_temp.orders_staging", []string{"id", "val"}, []string{"id"}, "op", "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "WHEN NOT MATCHED AND (source.\"op\" IS DISTINCT FROM 'd') THEN\n  INSERT (\"id\", \"val\")"
	if !strings.Contains(sql, expected) {
		t.Errorf("expected IS DISTINCT FROM clause, got:\n%s", sql)
	}
}

func TestWriteTransformPipelineConstruction(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	orders := []TestOrder{
		{ID: 1, Region: "US", Amount: 10.5},
		{ID: 2, Region: "EU", Amount: 25.0},
	}

	col := beam.CreateList(s, orders)
	opts := NewWriteOptions(
		WithHost("localhost"),
		WithDatabase("testdb"),
		WithPrimaryKeyColumns("id"),
	)

	result := Write(s, "public.orders", opts, col)

	if !result.SuccessfulRows.IsValid() {
		t.Errorf("expected valid SuccessfulRows PCollection")
	}
	if !result.FailedRows.IsValid() {
		t.Errorf("expected valid FailedRows PCollection")
	}

	// Verify pipeline graph builds
	if _, _, err := p.Build(); err != nil {
		t.Fatalf("pipeline build failed: %v", err)
	}
}

func TestWriteRejectsInvalidTableNameAtGraphConstruction(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on invalid table name, but none occurred")
		}
	}()

	_, s := beam.NewPipelineWithRoot()
	col := beam.CreateList(s, []TestOrder{{ID: 1}})
	opts := NewWriteOptions()

	// Malicious table name should panic at graph construction time
	Write(s, `public.orders"; DROP TABLE users; --`, opts, col)
}

func TestStagedCopyExecutionDirect(t *testing.T) {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=beam_test password=beam_test dbname=postgres sslmode=disable search_path=pg_catalog,pg_temp")
	if err != nil {
		t.Skipf("skipping db integration test: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("skipping db integration test: %v", err)
	}

	type StagedRecord struct {
		ID       int64     `db:"id"`
		Code     string    `db:"code"`
		Amount   float64   `db:"amount"`
		Region   string    `db:"region"`
		Tier     string    `db:"tier"`
		LoadedAt time.Time `db:"loaded_at"`
	}

	fn := &writeFn{
		Table:          `"public"."migrated_transactions"`,
		Options:        NewWriteOptions(WithWriteMethod(WriteMethodStagedCopy), WithWriteMode(WriteModeUpsert)),
		Type:           beam.EncodedType{T: reflect.TypeOf(StagedRecord{})},
		PrimaryKeyCols: []string{"id"},
		db:             db,
	}
	fn.inspectColumns(reflect.TypeOf(StagedRecord{}))

	batch := make([]any, 25000)
	now := time.Now().UTC()
	for i := 0; i < 25000; i++ {
		batch[i] = StagedRecord{
			ID:       int64(900000 + i),
			Code:     fmt.Sprintf("STAGED-%d", i),
			Amount:   123.45,
			Region:   "us-west1",
			Tier:     "ENTERPRISE",
			LoadedAt: now,
		}
	}

	ctx := context.Background()
	start := time.Now()
	if err := fn.executeStagedCopy(ctx, batch); err != nil {
		t.Fatalf("executeStagedCopy failed: %v", err)
	}
	dur := time.Since(start)
	t.Logf("executeStagedCopy inserted %d rows in %v (Rate: %.2f rows/sec)", len(batch), dur, float64(len(batch))/dur.Seconds())
}

// isDSNSpace reports whether c is whitespace for the purposes of libpq
// keyword/value connection string parsing.
func isDSNSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// parseLibpqDSN implements the keyword/value parsing rules libpq applies in
// conninfo_parse, so a test can assert what the server would actually be asked
// for rather than asserting on the shape of the string the sink produced.
//
// Within a value a backslash escapes the following character, a single quote
// opens or closes a quoted run, and unquoted whitespace terminates the value.
func parseLibpqDSN(dsn string) (map[string]string, error) {
	out := make(map[string]string)
	i, n := 0, len(dsn)

	for {
		for i < n && isDSNSpace(dsn[i]) {
			i++
		}
		if i >= n {
			return out, nil
		}

		start := i
		for i < n && dsn[i] != '=' && !isDSNSpace(dsn[i]) {
			i++
		}
		key := dsn[start:i]

		for i < n && isDSNSpace(dsn[i]) {
			i++
		}
		if i >= n || dsn[i] != '=' {
			return nil, fmt.Errorf("keyword %q is not followed by '='", key)
		}
		i++
		for i < n && isDSNSpace(dsn[i]) {
			i++
		}

		var value strings.Builder
		quoted := false
		for i < n {
			c := dsn[i]
			if c == '\\' && i+1 < n {
				value.WriteByte(dsn[i+1])
				i += 2
				continue
			}
			if c == '\'' {
				quoted = !quoted
				i++
				continue
			}
			if !quoted && isDSNSpace(c) {
				break
			}
			value.WriteByte(c)
			i++
		}
		if quoted {
			return nil, fmt.Errorf("unterminated quoted value for keyword %q", key)
		}
		out[key] = value.String()
	}
}

// TestBuildWriteDSNRejectsKeywordInjection is the regression test for the
// unescaped-DSN finding.
//
// libpq ends an unquoted value at the first space and reads the remainder as
// further keywords. An unquoted password therefore authenticated with only the
// text before the first space, and the discarded remainder became connection
// settings. sslrootcert is the damaging case: it appears nowhere else in the
// DSN, so nothing overrides it, and moving the trust anchor defeats
// certificate verification even though sslmode is still verify-full.
//
// The test asserts both halves, so that it keeps biting if the oracle drifts:
// the naive concatenation really is exploitable, and the sink's output is not.
func TestBuildWriteDSNRejectsKeywordInjection(t *testing.T) {
	const payload = "hunter2 sslrootcert=/tmp/attacker-ca.crt"

	naive := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s search_path=pg_catalog,pg_temp",
		"db.example.com", 5432, "orders", "beam", payload, "verify-full")
	naiveFields, err := parseLibpqDSN(naive)
	if err != nil {
		t.Fatalf("parsing the unescaped DSN failed: %v", err)
	}
	if got := naiveFields["sslrootcert"]; got != "/tmp/attacker-ca.crt" {
		t.Fatalf("the oracle no longer detects the injection: unescaped DSN parsed sslrootcert=%q, want %q",
			got, "/tmp/attacker-ca.crt")
	}
	if got := naiveFields["password"]; got != "hunter2" {
		t.Fatalf("the oracle no longer detects the truncation: unescaped DSN parsed password=%q, want %q",
			got, "hunter2")
	}

	fields, err := parseLibpqDSN(buildWriteDSN("db.example.com", 5432, "orders", "beam", payload, "verify-full", ""))
	if err != nil {
		t.Fatalf("parsing the sink DSN failed: %v", err)
	}
	if got, ok := fields["sslrootcert"]; ok {
		t.Errorf("password injected the sslrootcert keyword with value %q", got)
	}
	if got := fields["password"]; got != payload {
		t.Errorf("password was truncated: got %q, want %q", got, payload)
	}
	if got := fields["sslmode"]; got != "verify-full" {
		t.Errorf("sslmode was disturbed: got %q, want %q", got, "verify-full")
	}
}

// TestBuildWriteDSNEmitsSecuritySettingsLast locks in the keyword ordering that
// backs up the escaping.
//
// libpq lets a later duplicate keyword win. Emitting sslmode and the pinned
// search_path after every configurable value means that even if a value were
// to escape quoting in future, it could not displace the transport security
// setting or reopen CVE-2018-1058. Reordering the format string would remove
// that second layer silently, so it is asserted rather than left to review.
func TestBuildWriteDSNEmitsSecuritySettingsLast(t *testing.T) {
	dsn := buildWriteDSN("db.example.com", 5432, "orders", "beam", "pw", "verify-full", "/etc/ssl/certs/root.crt")

	sslmode := strings.Index(dsn, "sslmode=")
	searchPath := strings.Index(dsn, "search_path=")
	if sslmode < 0 || searchPath < 0 {
		t.Fatalf("DSN is missing sslmode or search_path: %s", dsn)
	}

	for _, keyword := range []string{"host=", "port=", "dbname=", "user=", "password=", "sslrootcert="} {
		at := strings.Index(dsn, keyword)
		if at < 0 {
			t.Fatalf("DSN is missing %s: %s", keyword, dsn)
		}
		if at > sslmode {
			t.Errorf("%s is emitted after sslmode, so an injected value could override it", keyword)
		}
		if at > searchPath {
			t.Errorf("%s is emitted after search_path, so an injected value could reopen CVE-2018-1058", keyword)
		}
	}
	if searchPath < sslmode {
		t.Error("search_path is emitted before sslmode; the pinned search_path must be last")
	}
}

// TestBuildWriteDSNRoundTripsHostileValues checks that every configurable value
// survives libpq parsing unchanged, and that none of them can dislodge the
// pinned search_path that closes CVE-2018-1058.
func TestBuildWriteDSNRoundTripsHostileValues(t *testing.T) {
	hostile := []struct {
		name  string
		value string
	}{
		{"embedded space", "two words"},
		{"single quote", "o'brien"},
		{"backslash", `back\slash`},
		{"quote and backslash", `\'; DROP`},
		{"empty", ""},
		{"keyword injection", "x sslmode=disable search_path=public"},
		{"trailing backslash", `trailing\`},
	}

	for _, tc := range hostile {
		t.Run(tc.name, func(t *testing.T) {
			fields, err := parseLibpqDSN(buildWriteDSN(tc.value, 5432, tc.value, tc.value, tc.value, "verify-full", ""))
			if err != nil {
				t.Fatalf("parsing DSN failed: %v", err)
			}
			for _, key := range []string{"host", "dbname", "user", "password"} {
				if got := fields[key]; got != tc.value {
					t.Errorf("%s did not round-trip: got %q, want %q", key, got, tc.value)
				}
			}
			if got := fields["sslmode"]; got != "verify-full" {
				t.Errorf("sslmode was overridden: got %q, want %q", got, "verify-full")
			}
			if got := fields["search_path"]; got != "pg_catalog,pg_temp" {
				t.Errorf("search_path was overridden: got %q, want %q", got, "pg_catalog,pg_temp")
			}
		})
	}
}

func TestBuildWriteDSN_SSLRootCert(t *testing.T) {
	// 1. Empty sslRootCert -> omitted from DSN
	dsnEmpty := buildWriteDSN("db.example.com", 5432, "orders", "beam", "pw", "verify-full", "")
	fieldsEmpty, err := parseLibpqDSN(dsnEmpty)
	if err != nil {
		t.Fatalf("parseLibpqDSN failed: %v", err)
	}
	if _, ok := fieldsEmpty["sslrootcert"]; ok {
		t.Errorf("expected no sslrootcert in DSN when unset, got %q", fieldsEmpty["sslrootcert"])
	}

	// 2. Whitespace-only sslRootCert -> omitted from DSN
	dsnWhitespace := buildWriteDSN("db.example.com", 5432, "orders", "beam", "pw", "verify-full", "   ")
	fieldsWhitespace, err := parseLibpqDSN(dsnWhitespace)
	if err != nil {
		t.Fatalf("parseLibpqDSN failed: %v", err)
	}
	if _, ok := fieldsWhitespace["sslrootcert"]; ok {
		t.Errorf("expected no sslrootcert in DSN for whitespace, got %q", fieldsWhitespace["sslrootcert"])
	}

	// 3. Configured sslRootCert -> properly quoted and emitted
	certPath := "/etc/ssl/certs/pg root CA's.crt"
	dsnWithCert := buildWriteDSN("db.example.com", 5432, "orders", "beam", "pw", "verify-full", certPath)
	fieldsWithCert, err := parseLibpqDSN(dsnWithCert)
	if err != nil {
		t.Fatalf("parseLibpqDSN failed: %v", err)
	}
	if got := fieldsWithCert["sslrootcert"]; got != certPath {
		t.Errorf("sslrootcert = %q, want %q", got, certPath)
	}
	if got := fieldsWithCert["sslmode"]; got != "verify-full" {
		t.Errorf("sslmode = %q, want %q", got, "verify-full")
	}
}

func TestWriteOptions_SSLRootCert(t *testing.T) {
	opts := NewWriteOptions(
		WithHost("localhost"),
		WithDatabase("testdb"),
		WithSSLRootCert("/etc/ssl/ca.pem"),
	)
	if opts.SSLRootCert != "/etc/ssl/ca.pem" {
		t.Errorf("WithSSLRootCert failed: got %q, want %q", opts.SSLRootCert, "/etc/ssl/ca.pem")
	}

	optsAlias := NewWriteOptions(
		WithHost("localhost"),
		WithDatabase("testdb"),
		WithSSLRootCerts("/etc/ssl/ca-alias.pem"),
	)
	if optsAlias.SSLRootCert != "/etc/ssl/ca-alias.pem" {
		t.Errorf("WithSSLRootCerts failed: got %q, want %q", optsAlias.SSLRootCert, "/etc/ssl/ca-alias.pem")
	}
}

func TestWriteOptions_DefaultSSLMode(t *testing.T) {
	opts := NewWriteOptions(
		WithHost("localhost"),
		WithDatabase("testdb"),
	)
	if opts.SSLMode != DefaultSSLMode {
		t.Errorf("default SSLMode = %q, want %q", opts.SSLMode, DefaultSSLMode)
	}

	// Caller can explicitly lower to "require"
	optsRequire := NewWriteOptions(
		WithHost("localhost"),
		WithDatabase("testdb"),
		WithSSLMode("require"),
	)
	if optsRequire.SSLMode != "require" {
		t.Errorf("WithSSLMode(require) = %q, want require", optsRequire.SSLMode)
	}
}

// TestWriteRejectsUnqualifiedTableName covers the schema-qualification finding.
//
// Because the sink pins search_path to pg_catalog,pg_temp, an unqualified name
// cannot resolve to a user table. Without this check the pipeline builds
// successfully and then fails on a worker with "relation does not exist", which
// gives the operator no indication that the schema prefix is the fix.
func TestWriteRejectsUnqualifiedTableName(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, TestOrder{ID: 1, Region: "US", Amount: 100.0})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Write accepted the unqualified table name \"orders\", which cannot resolve under the pinned search_path")
		}
		msg := fmt.Sprint(r)
		if !strings.Contains(msg, "schema-qualified") {
			t.Errorf("panic message does not state the requirement: %s", msg)
		}
		if !strings.Contains(msg, "public.orders") {
			t.Errorf("panic message does not name the fix: %s", msg)
		}
	}()

	Write(s, "orders", NewWriteOptions(WithPrimaryKeyColumns("id")), col)
}

// TestWriteAcceptsSchemaQualifiedTableName confirms the guard does not reject
// the supported form.
func TestWriteAcceptsSchemaQualifiedTableName(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Write rejected a schema-qualified table name: %v", r)
		}
	}()

	_, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, TestOrder{ID: 1, Region: "US", Amount: 100.0})
	Write(s, "public.orders", NewWriteOptions(
		WithHost("localhost"),
		WithDatabase("testdb"),
		WithPrimaryKeyColumns("id"),
	), col)
}

func TestBuildMergeQuery(t *testing.T) {
	t.Run("merge with op column conditional delete", func(t *testing.T) {
		query, err := buildMergeQuery(
			"public.orders",
			"pg_temp.stage_orders",
			[]string{"order_id", "customer_id", "amount", "_op_type"},
			[]string{"order_id"},
			"_op_type",
			"D",
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !strings.Contains(query, `MERGE INTO "public"."orders" AS target`) {
			t.Errorf("missing MERGE INTO target, got:\n%s", query)
		}
		if !strings.Contains(query, `USING "pg_temp"."stage_orders" AS source`) {
			t.Errorf("missing USING source, got:\n%s", query)
		}
		if !strings.Contains(query, `ON target."order_id" = source."order_id"`) {
			t.Errorf("missing ON condition, got:\n%s", query)
		}
		if !strings.Contains(query, `WHEN MATCHED AND source."_op_type" = 'D' THEN`+"\n"+`  DELETE`) {
			t.Errorf("missing conditional DELETE, got:\n%s", query)
		}
		if !strings.Contains(query, `WHEN MATCHED THEN`+"\n"+`  UPDATE SET`+"\n"+`    "customer_id" = source."customer_id",`+"\n"+`    "amount" = source."amount"`) {
			t.Errorf("missing UPDATE SET, got:\n%s", query)
		}
		if !strings.Contains(query, `WHEN NOT MATCHED AND (source."_op_type" IS DISTINCT FROM 'D') THEN`+"\n"+`  INSERT ("order_id", "customer_id", "amount")`+"\n"+`  VALUES (source."order_id", source."customer_id", source."amount")`) {
			t.Errorf("missing INSERT clause, got:\n%s", query)
		}
	})

	t.Run("merge without op column", func(t *testing.T) {
		query, err := buildMergeQuery(
			"public.users",
			"pg_temp.stage_users",
			[]string{"user_id", "email"},
			[]string{"user_id"},
			"",
			"",
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.Contains(query, "DELETE") {
			t.Errorf("query should not contain DELETE when opCol is empty")
		}
		if !strings.Contains(query, `WHEN NOT MATCHED THEN`+"\n"+`  INSERT ("user_id", "email")`) {
			t.Errorf("missing standard WHEN NOT MATCHED THEN INSERT clause")
		}
	})

	t.Run("missing primary keys returns error", func(t *testing.T) {
		_, err := buildMergeQuery("public.orders", "pg_temp.stage", []string{"id", "data"}, nil, "", "")
		if err == nil {
			t.Fatalf("expected error for empty pkCols")
		}
	})
}

func TestExplainPlanParsingAndSampling(t *testing.T) {
	// rand.Float64 returns a value in [0,1), so a rate of 1.0 always samples.
	if !shouldSampleExplain(1.0) {
		t.Errorf("shouldSampleExplain(1.0) = false, want true")
	}

	// An explicit 0.0 falls back to the 0.001 default, which is probabilistic
	// and so has no deterministic assertion; the call exercises that branch.
	shouldSampleExplain(0.0)

	planJSON := []byte(`[
		{
			"Plan": {
				"Node Type": "Merge",
				"Relation Name": "orders",
				"Shared Hit Blocks": 4200,
				"Shared Read Blocks": 150,
				"Actual Rows": 500
			},
			"Planning Time": 1.25,
			"Execution Time": 285.50
		}
	]`)

	// Ensure recordExplainTelemetry runs without panic or error
	recordExplainTelemetry(context.Background(), planJSON, "public.orders")

	var results []ExplainPlanResult
	if err := json.Unmarshal(planJSON, &results); err != nil {
		t.Fatalf("failed to unmarshal plan JSON: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ExecutionTime != 285.50 {
		t.Errorf("expected Execution Time 285.50, got %f", results[0].ExecutionTime)
	}
	if results[0].Plan.SharedHitBlocks != 4200 {
		t.Errorf("expected Shared Hit Blocks 4200, got %d", results[0].Plan.SharedHitBlocks)
	}
}

func TestWriteReturnsErrorOnQueryFailure(t *testing.T) {
	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: []string{"id"},
		columns:        []string{""}, // Empty column name causes SanitizeIdentifier to fail
	}

	ctx := context.Background()
	fn.compactor = NewBatchCompactor(10, 1024, 0)

	// A batch that cannot be turned into a statement is a configuration fault,
	// not a data fault: every later batch would fail identically. The bundle
	// must fail rather than silently draining the input into the dead-letter
	// output. Nothing is emitted there, because a runner discards a failed
	// bundle's pending outputs, so an emit on this path produces rows that no
	// downstream transform ever observes.
	var failedRows []FailedRow
	emitFailed := func(f FailedRow) { failedRows = append(failedRows, f) }

	_ = fn.ProcessElement(ctx, TestOrder{ID: 1}, func(v beam.X) {}, emitFailed)
	err := fn.FinishBundle(ctx, func(v beam.X) {}, emitFailed)

	if err == nil {
		t.Fatalf("expected FinishBundle to return error when query building fails, got nil")
	}
	if !strings.Contains(err.Error(), "identifier cannot be empty") {
		t.Errorf("expected error to contain 'identifier cannot be empty', got: %v", err)
	}
	if len(failedRows) != 0 {
		t.Errorf("emitted %d rows to the failed output on a bundle-failing path; those outputs are discarded with the bundle", len(failedRows))
	}
}

// failingConn is a database/sql connection whose every statement execution
// fails with a fixed error, so the sink's execution-failure path can be
// exercised without a live PostgreSQL server.
type failingConn struct{ err error }

func (c failingConn) Prepare(string) (driver.Stmt, error) { return nil, c.err }
func (c failingConn) Close() error                        { return nil }
func (c failingConn) Begin() (driver.Tx, error)           { return nil, c.err }
func (c failingConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, c.err
}

type failingDriver struct{ err error }

func (d failingDriver) Open(string) (driver.Conn, error) { return failingConn(d), nil }

type failingConnector struct{ err error }

func (c failingConnector) Connect(context.Context) (driver.Conn, error) {
	return failingConn(c), nil
}
func (c failingConnector) Driver() driver.Driver { return failingDriver(c) }

// stmtRecorder captures every statement issued against the fake driver, so a
// test can assert on both the statements and their transactional framing.
type stmtRecorder struct {
	mu    sync.Mutex
	stmts []string
	args  [][]driver.NamedValue
}

func (r *stmtRecorder) record(s string) { r.recordWithArgs(s, nil) }

func (r *stmtRecorder) recordWithArgs(s string, args []driver.NamedValue) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stmts = append(r.stmts, s)
	r.args = append(r.args, args)
}

func (r *stmtRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.stmts...)
}

func (r *stmtRecorder) indexOf(substr string) int {
	for i, s := range r.all() {
		if strings.Contains(s, substr) {
			return i
		}
	}
	return -1
}

// argsAt returns the bound parameters recorded for statement i.
func (r *stmtRecorder) argsAt(i int) []driver.NamedValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i < 0 || i >= len(r.args) {
		return nil
	}
	return r.args[i]
}

type recordingConn struct{ rec *stmtRecorder }

func (c recordingConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c recordingConn) Close() error                        { return nil }

func (c recordingConn) Begin() (driver.Tx, error) {
	c.rec.record("BEGIN")
	return recordingTx(c), nil
}

func (c recordingConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.rec.recordWithArgs(query, args)
	return driver.RowsAffected(0), nil
}

type recordingTx struct{ rec *stmtRecorder }

func (t recordingTx) Commit() error   { t.rec.record("COMMIT"); return nil }
func (t recordingTx) Rollback() error { t.rec.record("ROLLBACK"); return nil }

type recordingDriver struct{ rec *stmtRecorder }

func (d recordingDriver) Open(string) (driver.Conn, error) { return recordingConn(d), nil }

type recordingConnector struct{ rec *stmtRecorder }

func (c recordingConnector) Connect(context.Context) (driver.Conn, error) {
	return recordingConn(c), nil
}
func (c recordingConnector) Driver() driver.Driver { return recordingDriver(c) }

// newRecordingWriteFn builds a sink on the UNNEST path backed by the recording
// driver, which reaches the parameterized execution path without needing the
// staged COPY fast path or a real server.
func newRecordingWriteFn(rec *stmtRecorder, opts ...Option) *writeFn {
	fn := &writeFn{
		Table:   `"public"."orders"`,
		Options: NewWriteOptions(append([]Option{WithWriteMode(WriteModeInsert), WithWriteMethod(WriteMethodUnnest)}, opts...)...),
		Type:    beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		columns: []string{"id", "region", "amount"},
		colTypes: map[string]string{
			"id":     "INT8",
			"region": "TEXT",
			"amount": "FLOAT8",
		},
		db: sql.OpenDB(recordingConnector{rec: rec}),
	}
	fn.compactor = NewBatchCompactor(10, 1024, 0)
	return fn
}

func drainRecordingWriteFn(t *testing.T, fn *writeFn) {
	t.Helper()
	ctx := context.Background()
	noopSuccess := func(beam.X) {}
	noopFailed := func(FailedRow) {}

	if err := fn.ProcessElement(ctx, TestOrder{ID: 1, Region: "US", Amount: 10}, noopSuccess, noopFailed); err != nil {
		t.Fatalf("ProcessElement: %v", err)
	}
	if err := fn.FinishBundle(ctx, noopSuccess, noopFailed); err != nil {
		t.Fatalf("FinishBundle: %v", err)
	}
}

func TestRecordingWriteFn_WriteModeUpdate_Execution(t *testing.T) {
	rec := &stmtRecorder{}
	fn := newRecordingWriteFn(rec,
		WithWriteMode(WriteModeUpdate),
		WithPrimaryKeyColumns("id"),
		WithUpdateFields("amount"),
	)
	fn.PrimaryKeyCols = []string{"id"}

	ctx := context.Background()
	var successes []any
	var failures []FailedRow
	emitSuccess := func(x beam.X) { successes = append(successes, x) }
	emitFailed := func(fr FailedRow) { failures = append(failures, fr) }

	if err := fn.ProcessElement(ctx, TestOrder{ID: 10, Region: "US", Amount: 99.5}, emitSuccess, emitFailed); err != nil {
		t.Fatalf("ProcessElement failed: %v", err)
	}
	if err := fn.FinishBundle(ctx, emitSuccess, emitFailed); err != nil {
		t.Fatalf("FinishBundle failed: %v", err)
	}

	if len(failures) != 0 {
		t.Fatalf("expected 0 failures, got %d", len(failures))
	}
	if len(successes) != 1 {
		t.Fatalf("expected 1 success, got %d", len(successes))
	}

	stmts := rec.all()
	if len(stmts) == 0 {
		t.Fatalf("expected executed statements, got none")
	}

	updateIdx := rec.indexOf("UPDATE")
	if updateIdx == -1 {
		t.Fatalf("expected UPDATE statement, recorded: %v", stmts)
	}
	updateStmt := stmts[updateIdx]
	if !strings.Contains(updateStmt, `"amount" = source."amount"`) {
		t.Errorf("expected UPDATE statement to set amount, got: %s", updateStmt)
	}
	if strings.Contains(updateStmt, `"region" = source."region"`) {
		t.Errorf("expected UPDATE statement to omit region when update_fields=[amount], got: %s", updateStmt)
	}
	if !strings.Contains(updateStmt, `WHERE target."id" = source."id"`) {
		t.Errorf("expected UPDATE statement to have WHERE on primary key, got: %s", updateStmt)
	}
}

// TestConnectorSelectsReplicationOrigin covers the mechanism that stamps
// outgoing writes. pg_replication_origin_session_setup is the only function
// that names an origin; the code previously called
// pg_replication_origin_xact_setup, which takes (origin_lsn, origin_timestamp)
// and was being passed the origin name where an LSN belongs. Every tagged
// write therefore failed, and loop prevention never worked.
func TestConnectorSelectsReplicationOrigin(t *testing.T) {
	rec := &stmtRecorder{}
	conn := recordingConn{rec: rec}

	if err := selectReplicationOrigin(context.Background(), conn, "beam_sink"); err != nil {
		t.Fatalf("selectReplicationOrigin: %v", err)
	}

	at := rec.indexOf("pg_replication_origin_session_setup")
	if at < 0 {
		t.Fatalf("the origin was not selected; statements: %v", rec.all())
	}
	if rec.indexOf("pg_replication_origin_xact_setup") >= 0 {
		t.Errorf("xact_setup cannot name an origin and errors without a session origin; statements: %v", rec.all())
	}

	// Passed as a bound parameter rather than interpolated, so the statement
	// is not a function of the configured name.
	args := rec.argsAt(at)
	if len(args) != 1 {
		t.Fatalf("got %d bound parameters, want 1; the origin name must not be interpolated", len(args))
	}
	if got, ok := args[0].Value.(string); !ok || got != "beam_sink" {
		t.Errorf("bound parameter = %v, want %q", args[0].Value, "beam_sink")
	}
}

func TestConnectorSkipsOriginWhenUnset(t *testing.T) {
	rec := &stmtRecorder{}

	if err := selectReplicationOrigin(context.Background(), recordingConn{rec: rec}, ""); err != nil {
		t.Fatalf("selectReplicationOrigin: %v", err)
	}
	if len(rec.all()) != 0 {
		t.Errorf("no origin was configured, so no statement should have been issued; got %v", rec.all())
	}
}

// failingOriginConn rejects the origin selection the way a server would for a
// role without the privilege, or for an origin that does not exist.
type failingOriginConn struct{ recordingConn }

func (c failingOriginConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, &pq.Error{Code: "42501", Message: "permission denied for function pg_replication_origin_session_setup"}
}

// TestConnectorFailsClosedWhenOriginCannotBeSelected pins the failure mode.
// Continuing here would hand back a working connection whose writes are
// unstamped, and a bidirectional peer would replay every one of them back --
// the loop the origin exists to break, appearing only in production.
func TestConnectorFailsClosedWhenOriginCannotBeSelected(t *testing.T) {
	err := selectReplicationOrigin(context.Background(), failingOriginConn{}, "beam_sink")
	if err == nil {
		t.Fatal("a connection that could not select the origin was accepted")
	}
	for _, want := range []string{"beam_sink", "pg_replication_origin_create"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q, so the operator cannot act on it: %v", want, err)
		}
	}
}

// TestUnnestPathStaysInAutocommit confirms the hot path keeps its single round
// trip. The origin is session state selected when the connection is opened, so
// no transaction has to be opened to carry it.
func TestUnnestPathStaysInAutocommit(t *testing.T) {
	for _, origin := range []string{"", "beam_sink"} {
		name := "without origin"
		if origin != "" {
			name = "with origin"
		}
		t.Run(name, func(t *testing.T) {
			rec := &stmtRecorder{}
			opts := []Option{}
			if origin != "" {
				opts = append(opts, WithReplicationOriginName(origin))
			}
			fn := newRecordingWriteFn(rec, opts...)
			defer fn.db.Close()

			drainRecordingWriteFn(t, fn)

			if rec.indexOf("INSERT INTO") < 0 {
				t.Fatalf("expected the write to be issued; statements: %v", rec.all())
			}
			if rec.indexOf("BEGIN") >= 0 {
				t.Errorf("the parameterized path must stay in autocommit; statements: %v", rec.all())
			}
		})
	}
}

// TestWriteRoutesRejectedRowsToDLQOnExecFailure covers the case the dead-letter
// queue exists for: the statement is well-formed and the server rejects the
// data. Such a batch must not fail the bundle, because failing it would discard
// the dead-letter output that carries the rejected rows, and the next attempt
// would hit the same rejection forever.
func TestWriteRoutesRejectedRowsToDLQOnExecFailure(t *testing.T) {
	execErr := &pq.Error{Code: "23505", Message: "duplicate key value violates unique constraint"}

	fn := &writeFn{
		Table: `"public"."orders"`,
		// The UNNEST method reaches the execution path directly, without the
		// staged COPY fast path that would need a real transaction.
		Options: NewWriteOptions(
			WithWriteMode(WriteModeInsert),
			WithWriteMethod(WriteMethodUnnest),
		),
		Type:    beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		columns: []string{"id", "region", "amount"},
		colTypes: map[string]string{
			"id":     "INT8",
			"region": "TEXT",
			"amount": "FLOAT8",
		},
		db: sql.OpenDB(failingConnector{err: execErr}),
	}
	defer fn.db.Close()

	ctx := context.Background()
	fn.compactor = NewBatchCompactor(10, 1024, 0)

	var succeeded []any
	var failedRows []FailedRow

	_ = fn.ProcessElement(ctx, TestOrder{ID: 1, Region: "US", Amount: 10},
		func(v beam.X) { succeeded = append(succeeded, v) },
		func(f FailedRow) { failedRows = append(failedRows, f) })
	err := fn.FinishBundle(ctx,
		func(v beam.X) { succeeded = append(succeeded, v) },
		func(f FailedRow) { failedRows = append(failedRows, f) })

	if err != nil {
		t.Fatalf("expected FinishBundle to return nil so the dead-letter output is committed, got: %v", err)
	}
	if len(succeeded) != 0 {
		t.Errorf("emitted %d rows as written despite the statement failing", len(succeeded))
	}
	if len(failedRows) != 1 {
		t.Fatalf("expected 1 row on the dead-letter output, got %d", len(failedRows))
	}
	if failedRows[0].SqlState != "23505" {
		t.Errorf("expected SqlState 23505 to be propagated, got %q", failedRows[0].SqlState)
	}
	if !strings.Contains(failedRows[0].ErrorMessage, "duplicate key") {
		t.Errorf("expected the server message to be preserved, got %q", failedRows[0].ErrorMessage)
	}
	if got, ok := failedRows[0].Row.(TestOrder); !ok || got.ID != 1 {
		t.Errorf("expected the original row to be recoverable from the dead-letter output, got %#v", failedRows[0].Row)
	}
}

func TestWriteOptionsPasswordRedaction(t *testing.T) {
	opts := NewWriteOptions(
		WithHost("db.example.com"),
		WithUsername("beam_writer"),
		WithPassword("super_secret_write_pwd"),
	)
	str := opts.String()
	if strings.Contains(str, "super_secret_write_pwd") {
		t.Fatalf("WriteOptions string leaked raw password: %s", str)
	}
	if !strings.Contains(str, "<redacted>") {
		t.Fatalf("WriteOptions string missing <redacted> token: %s", str)
	}
}

// validWriteOpts returns options that satisfy every construction-time check,
// so a test can introduce exactly one fault and attribute the panic to it.
func validWriteOpts() WriteOptions {
	return NewWriteOptions(
		WithHost("localhost"),
		WithDatabase("testdb"),
		WithPrimaryKeyColumns("id"),
	)
}

// writePanic runs Write with the given options and reports the panic message,
// or "" when the call returned normally.
func writePanic(t *testing.T, opts WriteOptions) string {
	t.Helper()

	var msg string
	func() {
		defer func() {
			if r := recover(); r != nil {
				msg = fmt.Sprint(r)
			}
		}()
		_, s := beam.NewPipelineWithRoot()
		col := beam.Create(s, TestOrder{ID: 1, Region: "US", Amount: 1})
		Write(s, "public.orders", opts, col)
	}()
	return msg
}

// TestWriteRejectsConflictModesWithoutPrimaryKeys covers the case the reviewer
// called the exact opposite of what upsert promises: with no key, the
// statement degrades to ON CONFLICT DO NOTHING and conflicting rows are
// dropped while the pipeline reports success.
func TestWriteRejectsConflictModesWithoutPrimaryKeys(t *testing.T) {
	for _, mode := range []WriteMode{WriteModeUpsert, WriteModeMerge, WriteModeUpdate} {
		t.Run(mode.String(), func(t *testing.T) {
			opts := validWriteOpts()
			opts.WriteMode = mode
			opts.PrimaryKeyCols = nil

			msg := writePanic(t, opts)
			if msg == "" {
				t.Fatalf("%v without primary keys was accepted; conflicting rows would be silently discarded", mode)
			}
			if !strings.Contains(msg, "PrimaryKeyCols") {
				t.Errorf("panic does not name the option to set: %s", msg)
			}
		})
	}
}

func TestWriteRejectsIncompleteConnectionTarget(t *testing.T) {
	t.Run("empty host", func(t *testing.T) {
		opts := validWriteOpts()
		opts.Host = ""

		msg := writePanic(t, opts)
		if msg == "" {
			t.Fatal("Write accepted an empty Host; the failure would surface on a worker as a bare libpq error")
		}
		if !strings.Contains(msg, "Host") {
			t.Errorf("panic does not name the empty option: %s", msg)
		}
	})

	t.Run("empty database", func(t *testing.T) {
		opts := validWriteOpts()
		opts.Database = ""

		msg := writePanic(t, opts)
		if msg == "" {
			t.Fatal("Write accepted an empty Database")
		}
		if !strings.Contains(msg, "Database") {
			t.Errorf("panic does not name the empty option: %s", msg)
		}
	})
}

// TestWriteRejectsInvalidSSLMode confirms validateSSLMode now guards the
// struct-literal path, not just the option builders.
func TestWriteRejectsInvalidSSLMode(t *testing.T) {
	opts := validWriteOpts()
	opts.SSLMode = "verify_full" // underscore rather than hyphen

	msg := writePanic(t, opts)
	if msg == "" {
		t.Fatal("Write accepted an invalid sslmode; every worker would fail at connect time instead")
	}
	if !strings.Contains(msg, "verify_full") {
		t.Errorf("panic does not quote the rejected value: %s", msg)
	}
}

// TestWriteAcceptsEmptySSLModeAsDefault guards against the validation
// rejecting the common case of leaving the mode unset, which is normalized to
// DefaultSSLMode rather than treated as invalid.
func TestWriteAcceptsEmptySSLModeAsDefault(t *testing.T) {
	opts := validWriteOpts()
	opts.SSLMode = ""

	if msg := writePanic(t, opts); msg != "" {
		t.Fatalf("Write rejected an unset SSLMode instead of applying the default: %s", msg)
	}
}

// TestWriteRejectsUnknownUpdateField covers the typo the reviewer described:
// WithUpdateFields("emial") is a valid identifier, so SanitizeIdentifier passes
// it through and PostgreSQL rejects the batch at flush time with `column
// "emial" of relation "orders" does not exist`. The name is checkable against
// the element type at construction, which is where every other write option is
// checked.
func TestWriteRejectsUnknownUpdateField(t *testing.T) {
	for _, mode := range []WriteMode{WriteModeUpsert, WriteModeUpdate} {
		t.Run(mode.String(), func(t *testing.T) {
			opts := validWriteOpts()
			opts.WriteMode = mode
			opts.UpdateFields = []string{"emial"}

			msg := writePanic(t, opts)
			if msg == "" {
				t.Fatal("Write accepted an update field naming no column; the batch would fail on a worker")
			}
			if !strings.Contains(msg, "emial") {
				t.Errorf("panic does not quote the rejected field: %s", msg)
			}
			// The known set is what tells a reader what to write instead.
			if !strings.Contains(msg, "amount") {
				t.Errorf("panic does not list the known columns: %s", msg)
			}
		})
	}
}

// TestWriteRejectsCaseMismatchedUpdateField pins the case-sensitivity contract.
// The compactor's primary key lookup falls back to a case-insensitive match, so
// a caller could reasonably expect "Amount" to select amount. It does not, and
// the error has to say so rather than just report an unknown column.
func TestWriteRejectsCaseMismatchedUpdateField(t *testing.T) {
	opts := validWriteOpts()
	opts.WriteMode = WriteModeUpsert
	opts.UpdateFields = []string{"Amount"}

	msg := writePanic(t, opts)
	if msg == "" {
		t.Fatal("Write accepted an update field differing from the column only by case")
	}
	if !strings.Contains(msg, "case-sensitive") {
		t.Errorf("panic does not explain that matching is case-sensitive: %s", msg)
	}
	if !strings.Contains(msg, `"amount"`) {
		t.Errorf("panic does not name the column the caller probably meant: %s", msg)
	}
}

// TestWriteRejectsUpdateFieldsForModesWithoutSetClause covers the silent-ignore
// case: Insert has no SET clause and MERGE builds its own, so UpdateFields is
// read by nothing under either. Accepting it would let a pipeline that meant to
// restrict which columns are overwritten run as though it had said nothing.
func TestWriteRejectsUpdateFieldsForModesWithoutSetClause(t *testing.T) {
	for _, mode := range []WriteMode{WriteModeInsert, WriteModeMerge} {
		t.Run(mode.String(), func(t *testing.T) {
			opts := validWriteOpts()
			opts.WriteMode = mode
			opts.UpdateFields = []string{"amount"}

			msg := writePanic(t, opts)
			if msg == "" {
				t.Fatalf("Write silently ignored UpdateFields under %v", mode)
			}
			if !strings.Contains(msg, "UpdateFields") {
				t.Errorf("panic does not name the ignored option: %s", msg)
			}
		})
	}
}

// TestWriteAcceptsValidUpdateFields guards against the new checks rejecting the
// cases they are meant to allow.
func TestWriteAcceptsValidUpdateFields(t *testing.T) {
	t.Run("upsert subset", func(t *testing.T) {
		opts := validWriteOpts()
		opts.WriteMode = WriteModeUpsert
		opts.UpdateFields = []string{"amount", "region"}

		if msg := writePanic(t, opts); msg != "" {
			t.Fatalf("Write rejected update fields that name real columns: %s", msg)
		}
	})

	// A primary key column is dropped from the SET clause rather than rejected,
	// so naming one must still be accepted.
	t.Run("primary key column", func(t *testing.T) {
		opts := validWriteOpts()
		opts.WriteMode = WriteModeUpsert
		opts.UpdateFields = []string{"id", "amount"}

		if msg := writePanic(t, opts); msg != "" {
			t.Fatalf("Write rejected a primary key column in UpdateFields: %s", msg)
		}
	})

	t.Run("unset", func(t *testing.T) {
		opts := validWriteOpts()
		opts.WriteMode = WriteModeMerge

		if msg := writePanic(t, opts); msg != "" {
			t.Fatalf("Write rejected MERGE with no UpdateFields: %s", msg)
		}
	})
}

// TestValidateUpdateFields exercises the checker directly, including the shapes
// Write cannot reach: a non-struct element type yields no columns to check
// against, and must not be turned into a rejection.
func TestValidateUpdateFields(t *testing.T) {
	columns := []string{"id", "region", "amount"}

	tests := []struct {
		name         string
		updateFields []string
		columns      []string
		wantErr      bool
	}{
		{name: "empty update fields", updateFields: nil, columns: columns},
		{name: "all known", updateFields: []string{"region", "amount"}, columns: columns},
		{name: "unknown", updateFields: []string{"amount", "emial"}, columns: columns, wantErr: true},
		{name: "case mismatch", updateFields: []string{"Amount"}, columns: columns, wantErr: true},
		{name: "no columns resolved", updateFields: []string{"anything"}, columns: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateUpdateFields(tc.updateFields, tc.columns)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

// TestSetupRejectsUnknownUpdateField covers the backstop for a writeFn built
// directly rather than through Write.
func TestSetupRejectsUnknownUpdateField(t *testing.T) {
	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        NewWriteOptions(WithHost("localhost"), WithDatabase("testdb"), WithPrimaryKeyColumns("id"), WithUpdateFields("emial")),
		Type:           beam.EncodedType{T: reflect.TypeOf(TestOrder{})},
		PrimaryKeyCols: []string{"id"},
	}

	err := fn.Setup(context.Background())
	if err == nil {
		t.Fatal("Setup accepted an update field naming no column")
	}
	if !strings.Contains(err.Error(), "emial") {
		t.Errorf("error does not quote the rejected field: %v", err)
	}
}

// TestExtractRowValuesFromMap covers the panic the reviewer found:
// ExtractPrimaryKeys accepted maps, but extractRowValues called FieldByName
// unconditionally, so a map-shaped element took down the bundle.
func TestExtractRowValuesFromMap(t *testing.T) {
	fn := &writeFn{columns: []string{"id", "region", "amount", "missing"}}

	vals := fn.extractRowValues(map[string]any{
		"id":     int64(7),
		"region": "US",
		"amount": 12.5,
	})

	if len(vals) != 4 {
		t.Fatalf("expected one value per column, got %d", len(vals))
	}
	if vals[0] != int64(7) {
		t.Errorf("id: got %#v, want int64(7)", vals[0])
	}
	if vals[1] != "US" {
		t.Errorf("region: got %#v, want \"US\"", vals[1])
	}
	if vals[2] != 12.5 {
		t.Errorf("amount: got %#v, want 12.5", vals[2])
	}
	if vals[3] != nil {
		t.Errorf("a column absent from the map should read as NULL, got %#v", vals[3])
	}
}

// TestExtractRowValuesFromMapCaseInsensitive confirms maps fall back to a
// case-insensitive key match, matching how struct fields already resolve.
func TestExtractRowValuesFromMapCaseInsensitive(t *testing.T) {
	fn := &writeFn{columns: []string{"id"}}

	vals := fn.extractRowValues(map[string]any{"ID": int64(3)})
	if vals[0] != int64(3) {
		t.Errorf("expected the differently cased key to resolve, got %#v", vals[0])
	}
}

// TestExtractRowValuesFromNilPointer confirms a nil element yields NULLs
// rather than dereferencing through a nil pointer.
func TestExtractRowValuesFromNilPointer(t *testing.T) {
	fn := &writeFn{columns: []string{"id"}}

	vals := fn.extractRowValues((*TestOrder)(nil))
	if vals[0] != nil {
		t.Errorf("expected NULL for a nil element, got %#v", vals[0])
	}
}

// TestBuildUnnestQueryFromMapElements is the same panic in the other
// reflection path, which builds the parameterized statement.
func TestBuildUnnestQueryFromMapElements(t *testing.T) {
	fn := &writeFn{
		Table:          `"public"."orders"`,
		Options:        validWriteOpts(),
		Type:           beam.EncodedType{T: reflect.TypeOf(map[string]any{})},
		PrimaryKeyCols: []string{"id"},
		columns:        []string{"id", "region"},
		colTypes:       map[string]string{"id": "INT8", "region": "TEXT"},
	}

	batch := []any{
		map[string]any{"id": int64(1), "region": "US"},
		map[string]any{"id": int64(2), "region": "EU"},
	}

	query, args, err := fn.buildUnnestQuery(batch)
	if err != nil {
		t.Fatalf("unexpected error building unnest query from map elements: %v", err)
	}
	if !strings.Contains(query, `"id", "region"`) {
		t.Errorf("query does not name the map-derived columns: %s", query)
	}
	if len(args) != 2 {
		t.Fatalf("expected one array argument per column, got %d", len(args))
	}
}

// TestWriteSetupRejectsLostDialFunc covers a dialer that was configured at
// construction but dropped crossing the serialization boundary. Connecting
// anyway would bypass the proxy the caller installed.
func TestWriteSetupRejectsLostDialFunc(t *testing.T) {
	fn := &writeFn{
		Table: `"public"."orders"`,
		Options: WriteOptions{
			Host:             "localhost",
			Port:             5432,
			Database:         "testdb",
			SSLMode:          SSLModeDisable,
			RequiresDialFunc: true, // survived serialization
			DialFunc:         nil,  // did not
		},
	}

	err := fn.Setup(context.Background())
	if err == nil {
		t.Fatal("Setup connected without the configured dialer, silently bypassing the intended proxy")
	}
	if !strings.Contains(err.Error(), "DialFunc") {
		t.Errorf("error does not identify the lost dialer: %v", err)
	}
}

// TestDialFuncMarkerSurvivesSerialization demonstrates the premise the marker
// rests on: Beam encodes DoFn fields as JSON, which drops the closure and
// keeps the bool. Without the bool, a worker cannot distinguish a pipeline
// that never wanted a dialer from one whose dialer was lost.
func TestDialFuncMarkerSurvivesSerialization(t *testing.T) {
	original := WriteOptions{
		Host:     "localhost",
		Database: "testdb",
		DialFunc: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return nil, fmt.Errorf("unused")
		},
		RequiresDialFunc: true,
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("options must be JSON-encodable to cross the worker boundary: %v", err)
	}

	var decoded WriteOptions
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.DialFunc != nil {
		t.Error("a closure is not serializable; DialFunc should not have survived")
	}
	if !decoded.RequiresDialFunc {
		t.Error("RequiresDialFunc did not survive, so a worker cannot detect the lost dialer")
	}
}

// TestWriteAcceptsDialFunc confirms the construction-time checks do not reject
// a pipeline that legitimately supplies a dialer.
func TestWriteAcceptsDialFunc(t *testing.T) {
	opts := validWriteOpts()
	opts.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, fmt.Errorf("unused")
	}

	if msg := writePanic(t, opts); msg != "" {
		t.Fatalf("Write rejected a pipeline with a custom dialer: %s", msg)
	}
}
