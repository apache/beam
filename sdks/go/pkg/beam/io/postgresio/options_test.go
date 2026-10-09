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
	"testing"
	"time"
)

func TestNewWriteOptionsDefaults(t *testing.T) {
	opts := NewWriteOptions()
	if opts.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", opts.Port)
	}
	if opts.BatchSize != 5000 {
		t.Errorf("expected default batch size 5000, got %d", opts.BatchSize)
	}
	if opts.WriteMode != WriteModeUpsert {
		t.Errorf("expected default WriteModeUpsert, got %v", opts.WriteMode)
	}
	if opts.FlushInterval != 1*time.Second {
		t.Errorf("expected default flush interval 1s, got %v", opts.FlushInterval)
	}
}

func TestWriteOptionsFunctionalBuilders(t *testing.T) {
	opts := NewWriteOptions(
		WithHost("db.example.net"),
		WithPort(5433),
		WithDatabase("prod_db"),
		WithUsername("beam_writer"),
		WithPassword("secret123"),
		WithBatchSize(1000),
		WithMaxBatchBytes(4*1024*1024),
		WithFlushInterval(500*time.Millisecond),
		WithPrimaryKeyColumns("id", "region"),
		WithWriteMode(WriteModeInsert),
		WithMaxConnections(8),
		WithPgBouncer(true),
	)

	if opts.Host != "db.example.net" {
		t.Errorf("expected host db.example.net, got %s", opts.Host)
	}
	if opts.Port != 5433 {
		t.Errorf("expected port 5433, got %d", opts.Port)
	}
	if opts.Database != "prod_db" {
		t.Errorf("expected database prod_db, got %s", opts.Database)
	}
	if opts.Username != "beam_writer" {
		t.Errorf("expected user beam_writer, got %s", opts.Username)
	}
	if opts.BatchSize != 1000 {
		t.Errorf("expected batch size 1000, got %d", opts.BatchSize)
	}
	if opts.MaxBatchBytes != 4*1024*1024 {
		t.Errorf("expected max batch bytes 4MB, got %d", opts.MaxBatchBytes)
	}
	if len(opts.PrimaryKeyCols) != 2 || opts.PrimaryKeyCols[0] != "id" || opts.PrimaryKeyCols[1] != "region" {
		t.Errorf("unexpected primary key columns: %v", opts.PrimaryKeyCols)
	}
	if opts.WriteMode != WriteModeInsert {
		t.Errorf("expected WriteModeInsert, got %v", opts.WriteMode)
	}
	if opts.MaxConnections != 8 {
		t.Errorf("expected max conns 8, got %d", opts.MaxConnections)
	}
	if !opts.UsePgBouncer {
		t.Errorf("expected UsePgBouncer to be true")
	}
}

func TestSanitizeIdentifierValid(t *testing.T) {
	valid := []string{"orders", "user_id", "_internal", "col$1", "Table123"}
	for _, ident := range valid {
		sanitized, err := SanitizeIdentifier(ident)
		if err != nil {
			t.Errorf("unexpected error sanitizing valid identifier %q: %v", ident, err)
		}
		expected := `"` + ident + `"`
		if sanitized != expected {
			t.Errorf("expected %q, got %q", expected, sanitized)
		}
	}
}

func TestSanitizeIdentifierRejectsSqlInjection(t *testing.T) {
	injections := []string{
		`orders"; DROP TABLE users; --`,
		`col' OR '1'='1`,
		`table; SELECT 1`,
		`col"name`,
		`table space`,
		`table-dash`,
	}
	for _, ident := range injections {
		_, err := SanitizeIdentifier(ident)
		if err == nil {
			t.Errorf("expected error for malicious/invalid identifier %q, got nil", ident)
		}
	}
}

func TestSanitizeIdentifierRejectsNullByte(t *testing.T) {
	_, err := SanitizeIdentifier("orders\x00table")
	if err == nil {
		t.Errorf("expected error for identifier containing null byte, got nil")
	}
}

func TestSanitizeIdentifierRejectsEmpty(t *testing.T) {
	_, err := SanitizeIdentifier("   ")
	if err == nil {
		t.Errorf("expected error for empty identifier, got nil")
	}
}

func TestSanitizeTableIdentifierQualified(t *testing.T) {
	sanitized, err := SanitizeTableIdentifier("public.orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `"public"."orders"`
	if sanitized != expected {
		t.Errorf("expected %q, got %q", expected, sanitized)
	}

	sanitizedSingle, err := SanitizeTableIdentifier("orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedSingle := `"orders"`
	if sanitizedSingle != expectedSingle {
		t.Errorf("expected %q, got %q", expectedSingle, sanitizedSingle)
	}
}

func TestSanitizeTableIdentifierRejectsMultiPart(t *testing.T) {
	_, err := SanitizeTableIdentifier("db.schema.table")
	if err == nil {
		t.Errorf("expected error for 3-part identifier, got nil")
	}
}

func TestWriteOptionsReplicationOriginValidation(t *testing.T) {
	opts := NewWriteOptions(WithReplicationOriginName("beam_node_1"))
	if opts.ReplicationOriginName != "beam_node_1" {
		t.Errorf("expected origin beam_node_1, got %q", opts.ReplicationOriginName)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for invalid origin with special characters")
		}
	}()
	_ = NewWriteOptions(WithReplicationOriginName("invalid; DROP TABLE users;"))
}

func TestWriteOptionsMergeAndExplain(t *testing.T) {
	opts := NewWriteOptions(
		WithWriteMode(WriteModeMerge),
		WithOpColumn("_op_type"),
		WithDeleteOpValue("DELETE"),
		WithExplainAnalyze(true),
		WithExplainSampleRate(0.05),
	)

	if opts.WriteMode != WriteModeMerge {
		t.Errorf("expected WriteModeMerge, got %v", opts.WriteMode)
	}
	if opts.OpColumn != "_op_type" {
		t.Errorf("expected OpColumn _op_type, got %q", opts.OpColumn)
	}
	if opts.DeleteOpValue != "DELETE" {
		t.Errorf("expected DeleteOpValue DELETE, got %q", opts.DeleteOpValue)
	}
	if !opts.ExplainAnalyze {
		t.Errorf("expected ExplainAnalyze to be true")
	}
	if opts.ExplainSampleRate != 0.05 {
		t.Errorf("expected ExplainSampleRate 0.05, got %f", opts.ExplainSampleRate)
	}
}
