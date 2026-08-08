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

package spannerio

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/spannerio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func init() {
	register.DoFn2x0[spannerio.DataChangeRecord, func(spannerio.DataChangeRecord)](&filterModTypeFn{})
	register.Emitter1[spannerio.DataChangeRecord]()
}

// filterModTypeFn is a DoFn that passes through only DataChangeRecords whose
// ModType matches the configured value. It is used in integration tests to
// count records by mutation kind (INSERT, UPDATE, DELETE).
type filterModTypeFn struct {
	ModType int32
}

func (fn *filterModTypeFn) ProcessElement(rec spannerio.DataChangeRecord, emit func(spannerio.DataChangeRecord)) {
	if int32(rec.ModType) == fn.ModType {
		emit(rec)
	}
}

func filterByModType(s beam.Scope, records beam.PCollection, modType spannerio.ModType) beam.PCollection {
	return beam.ParDo(s, &filterModTypeFn{ModType: int32(modType)}, records)
}

// TestReadChangeStream_BoundedRead verifies that ReadChangeStream emits
// DataChangeRecords for mutations committed within the read time window.
func TestReadChangeStream_BoundedRead(t *testing.T) {
	integration.CheckFilters(t)

	const db = "projects/test-project/instances/test-instance/databases/test-db-changestream"
	ctx := context.Background()

	endpoint := setUpTestContainer(ctx, t)
	os.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	client := NewClient(ctx, t, endpoint, db)
	instanceAdminClient := NewInstanceAdminClient(ctx, t, endpoint)
	adminClient := NewAdminClient(ctx, t, endpoint)

	CreateInstance(ctx, t, instanceAdminClient, db)
	t.Cleanup(func() { DeleteInstance(ctx, t, instanceAdminClient, db) })

	CreateDatabase(ctx, t, adminClient, db)
	t.Cleanup(func() { DropDatabase(ctx, t, adminClient, db) })

	CreateTable(ctx, t, adminClient, db, []string{
		`CREATE TABLE Items (
			Id INT64 NOT NULL,
			Name STRING(100),
		) PRIMARY KEY (Id)`,
		`CREATE CHANGE STREAM ItemStream FOR Items`,
	})

	// startTime must be after the change stream was created; use the current
	// time (which is after CreateTable returns) with a small buffer.
	startTime := time.Now().UTC()

	// Write two rows in separate transactions so Spanner generates two distinct
	// DataChangeRecords (one per transaction).  A single Apply with both rows
	// produces only one DataChangeRecord because they share the same transaction.
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Items", []string{"Id", "Name"}, []interface{}{int64(1), "alpha"}),
	}); err != nil {
		t.Fatalf("Apply first mutation: %v", err)
	}
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Items", []string{"Id", "Name"}, []interface{}{int64(2), "beta"}),
	}); err != nil {
		t.Fatalf("Apply second mutation: %v", err)
	}

	// Give the emulator a moment to commit, then set the end time far enough
	// ahead that the change stream query completes naturally.
	endTime := time.Now().UTC().Add(10 * time.Second)

	p := beam.NewPipeline()
	s := p.Root()

	records := spannerio.ReadChangeStream(
		s, db, "ItemStream",
		startTime, endTime,
		1000, // heartbeat every 1 second
		spannerio.WithChangeStreamTestEndpoint(endpoint),
	)

	// We expect exactly 2 DataChangeRecords (one per mutation).
	passert.Count(s, records, "change stream records", 2)

	ptest.RunAndValidate(t, p)
}

// TestReadChangeStream_FiltersByTable verifies that mutations to a table not
// covered by the change stream are not emitted.
func TestReadChangeStream_FiltersByTable(t *testing.T) {
	integration.CheckFilters(t)

	const db = "projects/test-project/instances/test-instance/databases/test-db-cs-filter"
	ctx := context.Background()

	endpoint := setUpTestContainer(ctx, t)
	os.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	client := NewClient(ctx, t, endpoint, db)
	instanceAdminClient := NewInstanceAdminClient(ctx, t, endpoint)
	adminClient := NewAdminClient(ctx, t, endpoint)

	CreateInstance(ctx, t, instanceAdminClient, db)
	t.Cleanup(func() { DeleteInstance(ctx, t, instanceAdminClient, db) })

	CreateDatabase(ctx, t, adminClient, db)
	t.Cleanup(func() { DropDatabase(ctx, t, adminClient, db) })

	CreateTable(ctx, t, adminClient, db, []string{
		`CREATE TABLE Watched (
			Id INT64 NOT NULL,
			Val STRING(50),
		) PRIMARY KEY (Id)`,
		`CREATE TABLE Unwatched (
			Id INT64 NOT NULL,
			Val STRING(50),
		) PRIMARY KEY (Id)`,
		// Stream covers only Watched.
		`CREATE CHANGE STREAM WatchedStream FOR Watched`,
	})

	// startTime must be after the change stream was created.
	startTime := time.Now().UTC()

	// Write to both tables.
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Watched", []string{"Id", "Val"}, []interface{}{int64(1), "watched"}),
		spanner.Insert("Unwatched", []string{"Id", "Val"}, []interface{}{int64(1), "ignored"}),
	}); err != nil {
		t.Fatalf("Apply mutations: %v", err)
	}

	endTime := time.Now().UTC().Add(10 * time.Second)

	p := beam.NewPipeline()
	s := p.Root()

	records := spannerio.ReadChangeStream(
		s, db, "WatchedStream",
		startTime, endTime,
		1000,
		spannerio.WithChangeStreamTestEndpoint(endpoint),
	)

	// Only the Watched mutation should appear.
	passert.Count(s, records, "watched records only", 1)

	ptest.RunAndValidate(t, p)
}

// TestReadChangeStream_Empty verifies that a bounded read with no mutations
// in the window produces an empty PCollection.
func TestReadChangeStream_Empty(t *testing.T) {
	integration.CheckFilters(t)

	const db = "projects/test-project/instances/test-instance/databases/test-db-cs-empty"
	ctx := context.Background()

	endpoint := setUpTestContainer(ctx, t)
	os.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	instanceAdminClient := NewInstanceAdminClient(ctx, t, endpoint)
	adminClient := NewAdminClient(ctx, t, endpoint)

	CreateInstance(ctx, t, instanceAdminClient, db)
	t.Cleanup(func() { DeleteInstance(ctx, t, instanceAdminClient, db) })

	CreateDatabase(ctx, t, adminClient, db)
	t.Cleanup(func() { DropDatabase(ctx, t, adminClient, db) })

	CreateTable(ctx, t, adminClient, db, []string{
		`CREATE TABLE EmptyTable (
			Id INT64 NOT NULL,
		) PRIMARY KEY (Id)`,
		`CREATE CHANGE STREAM EmptyStream FOR EmptyTable`,
	})

	// Use a short time window starting just after the change stream is created.
	// No mutations are written, so the pipeline should produce 0 records.
	startTime := time.Now().UTC()
	endTime := startTime.Add(3 * time.Second)

	p := beam.NewPipeline()
	s := p.Root()

	records := spannerio.ReadChangeStream(
		s, db, "EmptyStream",
		startTime, endTime,
		1000,
		spannerio.WithChangeStreamTestEndpoint(endpoint),
	)

	passert.Count(s, records, "empty stream", 0)

	ptest.RunAndValidate(t, p)
}

// TestReadChangeStream_UpdateRecord verifies that UPDATE mutations produce
// DataChangeRecords with ModTypeUpdate, and that both the INSERT and UPDATE
// records for the same row are captured.
func TestReadChangeStream_UpdateRecord(t *testing.T) {
	integration.CheckFilters(t)

	const db = "projects/test-project/instances/test-instance/databases/test-db-cs-update"
	ctx := context.Background()

	endpoint := setUpTestContainer(ctx, t)
	os.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	client := NewClient(ctx, t, endpoint, db)
	instanceAdminClient := NewInstanceAdminClient(ctx, t, endpoint)
	adminClient := NewAdminClient(ctx, t, endpoint)

	CreateInstance(ctx, t, instanceAdminClient, db)
	t.Cleanup(func() { DeleteInstance(ctx, t, instanceAdminClient, db) })

	CreateDatabase(ctx, t, adminClient, db)
	t.Cleanup(func() { DropDatabase(ctx, t, adminClient, db) })

	CreateTable(ctx, t, adminClient, db, []string{
		`CREATE TABLE Items (
			Id INT64 NOT NULL,
			Name STRING(100),
		) PRIMARY KEY (Id)`,
		`CREATE CHANGE STREAM ItemStream FOR Items`,
	})

	startTime := time.Now().UTC()

	// Insert a row, then update it in a separate transaction so Spanner
	// generates two distinct DataChangeRecords.
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Items", []string{"Id", "Name"}, []interface{}{int64(1), "original"}),
	}); err != nil {
		t.Fatalf("Apply insert: %v", err)
	}
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Update("Items", []string{"Id", "Name"}, []interface{}{int64(1), "updated"}),
	}); err != nil {
		t.Fatalf("Apply update: %v", err)
	}

	endTime := time.Now().UTC().Add(10 * time.Second)

	p := beam.NewPipeline()
	s := p.Root()

	records := spannerio.ReadChangeStream(
		s, db, "ItemStream",
		startTime, endTime,
		1000,
		spannerio.WithChangeStreamTestEndpoint(endpoint),
	)

	// 1 INSERT + 1 UPDATE = 2 total records.
	passert.Count(s, records, "total records", 2)

	// Exactly one record should carry ModTypeUpdate.
	updates := filterByModType(s, records, spannerio.ModTypeUpdate)
	passert.Count(s, updates, "update records", 1)

	ptest.RunAndValidate(t, p)
}

// TestReadChangeStream_DeleteRecord verifies that DELETE mutations produce
// DataChangeRecords with ModTypeDelete, and that both the INSERT and DELETE
// records for the same row are captured.
func TestReadChangeStream_DeleteRecord(t *testing.T) {
	integration.CheckFilters(t)

	const db = "projects/test-project/instances/test-instance/databases/test-db-cs-delete"
	ctx := context.Background()

	endpoint := setUpTestContainer(ctx, t)
	os.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	client := NewClient(ctx, t, endpoint, db)
	instanceAdminClient := NewInstanceAdminClient(ctx, t, endpoint)
	adminClient := NewAdminClient(ctx, t, endpoint)

	CreateInstance(ctx, t, instanceAdminClient, db)
	t.Cleanup(func() { DeleteInstance(ctx, t, instanceAdminClient, db) })

	CreateDatabase(ctx, t, adminClient, db)
	t.Cleanup(func() { DropDatabase(ctx, t, adminClient, db) })

	CreateTable(ctx, t, adminClient, db, []string{
		`CREATE TABLE Items (
			Id INT64 NOT NULL,
			Name STRING(100),
		) PRIMARY KEY (Id)`,
		`CREATE CHANGE STREAM ItemStream FOR Items`,
	})

	startTime := time.Now().UTC()

	// Insert a row, then delete it in a separate transaction.
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Items", []string{"Id", "Name"}, []interface{}{int64(1), "to-be-deleted"}),
	}); err != nil {
		t.Fatalf("Apply insert: %v", err)
	}
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("Items", spanner.Key{int64(1)}),
	}); err != nil {
		t.Fatalf("Apply delete: %v", err)
	}

	endTime := time.Now().UTC().Add(10 * time.Second)

	p := beam.NewPipeline()
	s := p.Root()

	records := spannerio.ReadChangeStream(
		s, db, "ItemStream",
		startTime, endTime,
		1000,
		spannerio.WithChangeStreamTestEndpoint(endpoint),
	)

	// 1 INSERT + 1 DELETE = 2 total records.
	passert.Count(s, records, "total records", 2)

	// Exactly one record should carry ModTypeDelete.
	deletes := filterByModType(s, records, spannerio.ModTypeDelete)
	passert.Count(s, deletes, "delete records", 1)

	ptest.RunAndValidate(t, p)
}

// TestReadChangeStream_MultipleTableStream verifies that a change stream
// covering more than one table captures mutations from all watched tables.
func TestReadChangeStream_MultipleTableStream(t *testing.T) {
	integration.CheckFilters(t)

	const db = "projects/test-project/instances/test-instance/databases/test-db-cs-multi"
	ctx := context.Background()

	endpoint := setUpTestContainer(ctx, t)
	os.Setenv("SPANNER_EMULATOR_HOST", endpoint)

	client := NewClient(ctx, t, endpoint, db)
	instanceAdminClient := NewInstanceAdminClient(ctx, t, endpoint)
	adminClient := NewAdminClient(ctx, t, endpoint)

	CreateInstance(ctx, t, instanceAdminClient, db)
	t.Cleanup(func() { DeleteInstance(ctx, t, instanceAdminClient, db) })

	CreateDatabase(ctx, t, adminClient, db)
	t.Cleanup(func() { DropDatabase(ctx, t, adminClient, db) })

	CreateTable(ctx, t, adminClient, db, []string{
		`CREATE TABLE TableA (
			Id INT64 NOT NULL,
			Val STRING(50),
		) PRIMARY KEY (Id)`,
		`CREATE TABLE TableB (
			Id INT64 NOT NULL,
			Val STRING(50),
		) PRIMARY KEY (Id)`,
		// Stream covers both tables.
		`CREATE CHANGE STREAM MultiStream FOR TableA, TableB`,
	})

	startTime := time.Now().UTC()

	// Write one row to each table in the same transaction so we get two
	// DataChangeRecords (one per table) from the stream.
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("TableA", []string{"Id", "Val"}, []interface{}{int64(1), "a"}),
		spanner.Insert("TableB", []string{"Id", "Val"}, []interface{}{int64(1), "b"}),
	}); err != nil {
		t.Fatalf("Apply mutations: %v", err)
	}

	endTime := time.Now().UTC().Add(10 * time.Second)

	p := beam.NewPipeline()
	s := p.Root()

	records := spannerio.ReadChangeStream(
		s, db, "MultiStream",
		startTime, endTime,
		1000,
		spannerio.WithChangeStreamTestEndpoint(endpoint),
	)

	// One DataChangeRecord per table = 2 total.
	passert.Count(s, records, "multi-table records", 2)

	ptest.RunAndValidate(t, p)
}
