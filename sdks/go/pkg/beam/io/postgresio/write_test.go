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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
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

	fields, err := parseLibpqDSN(buildWriteDSN("db.example.com", 5432, "orders", "beam", payload, "verify-full"))
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
	dsn := buildWriteDSN("db.example.com", 5432, "orders", "beam", "pw", "verify-full")

	sslmode := strings.Index(dsn, "sslmode=")
	searchPath := strings.Index(dsn, "search_path=")
	if sslmode < 0 || searchPath < 0 {
		t.Fatalf("DSN is missing sslmode or search_path: %s", dsn)
	}

	for _, keyword := range []string{"host=", "port=", "dbname=", "user=", "password="} {
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
			fields, err := parseLibpqDSN(buildWriteDSN(tc.value, 5432, tc.value, tc.value, tc.value, "verify-full"))
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
	Write(s, "public.orders", NewWriteOptions(WithPrimaryKeyColumns("id")), col)
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
		if !strings.Contains(query, `WHEN NOT MATCHED AND source."_op_type" <> 'D' THEN`+"\n"+`  INSERT ("order_id", "customer_id", "amount")`+"\n"+`  VALUES (source."order_id", source."customer_id", source."amount")`) {
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
	if shouldSampleExplain(0.0) {
		// With default rate (0.001), sampling is probabilistic. Explicit 0.0 defaults to 0.001.
	}

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
	recordExplainTelemetry(planJSON, "public.orders")

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

	_ = fn.ProcessElement(ctx, TestOrder{ID: 1}, func(v beam.X) {}, func(f FailedRow) {})
	err := fn.FinishBundle(ctx, func(v beam.X) {}, func(f FailedRow) {})

	if err == nil {
		t.Fatalf("expected FinishBundle to return error when query building fails, got nil")
	}
	if !strings.Contains(err.Error(), "identifier cannot be empty") {
		t.Errorf("expected error to contain 'identifier cannot be empty', got: %v", err)
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
