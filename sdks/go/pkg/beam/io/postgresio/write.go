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
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/lib/pq"
)

var (
	sinkWrittenRows     = beam.NewCounter("postgresio", "sink_written_rows")
	sinkFailedRows      = beam.NewCounter("postgresio", "sink_failed_rows")
	sinkDeadlockRetries = beam.NewCounter("postgresio", "sink_deadlock_retries")
)

func init() {
	beam.RegisterDoFn(&writeFn{})
	beam.RegisterCoder(
		reflect.TypeOf((*FailedRow)(nil)).Elem(),
		encodeFailedRow,
		decodeFailedRow,
	)
}

func encodeFailedRow(in FailedRow) ([]byte, error) {
	return json.Marshal(in)
}

func decodeFailedRow(in []byte) (FailedRow, error) {
	var out FailedRow
	err := json.Unmarshal(in, &out)
	return out, err
}

// FailedRow captures a rejected record with sanitized diagnostic error information
// for routing to a Dead-Letter Queue (DLQ).
type FailedRow struct {
	Row          any    `json:"row"`
	ErrorMessage string `json:"error_message"`
	SqlState     string `json:"sql_state"`
}

// WriteResult encapsulates the output PCollections produced by the Write transform:
// SuccessfulRows contains all committed records; FailedRows contains dead-letter records.
type WriteResult struct {
	SuccessfulRows beam.PCollection
	FailedRows     beam.PCollection
}

// Write writes elements from an input PCollection to a target PostgreSQL table using
// parameterized array upserts, in-memory deduplication, and deadlock mitigation sorting.
func Write(s beam.Scope, table string, opts WriteOptions, col beam.PCollection) WriteResult {
	s = s.Scope("postgresio.Write")

	// The write path pins search_path to pg_catalog,pg_temp on every pooled
	// connection to close CVE-2018-1058, so an unqualified name cannot resolve
	// to a user table. Rejecting it here turns a runtime "relation does not
	// exist" surfacing from a distributed worker into a pipeline construction
	// error that names the fix.
	if !strings.Contains(table, ".") {
		panic(fmt.Sprintf("postgresio.Write: table name %q must be schema-qualified, for example %q. "+
			"Every connection in the write pool pins search_path to pg_catalog,pg_temp to close "+
			"CVE-2018-1058, so an unqualified name does not resolve to a user table.",
			table, "public."+table))
	}

	sanitizedTable, err := SanitizeTableIdentifier(table)
	if err != nil {
		panic(fmt.Sprintf("postgresio.Write: invalid table name %q: %v", table, err))
	}

	// Upsert and MERGE both resolve conflicts through the primary key. With no
	// key, the upsert degrades to ON CONFLICT DO NOTHING and MERGE has no join
	// condition, so conflicting rows are dropped -- the exact opposite of what
	// both modes promise. Nothing downstream can detect this: the pipeline
	// reports success and the rows are simply absent.
	if (opts.WriteMode == WriteModeUpsert || opts.WriteMode == WriteModeMerge) && len(opts.PrimaryKeyCols) == 0 {
		panic(fmt.Sprintf("postgresio.Write: write mode %v requires primary key columns (PrimaryKeyCols); "+
			"without them conflicting rows are silently discarded rather than merged. "+
			"Set PrimaryKeyCols, or use WriteModeInsert if plain inserts are intended.", opts.WriteMode))
	}

	if opts.WriteMode == WriteModeUpdate && len(opts.PrimaryKeyCols) == 0 {
		panic("postgresio.Write: WriteModeUpdate requires primary key columns (PrimaryKeyCols)")
	}

	// Connection identity is checked here rather than left to the worker. A
	// missing host or database otherwise fails inside Setup on a distributed
	// runner, long after submission, as a libpq connection error that names
	// neither the option that was left empty nor the transform that needed it.
	if opts.Host == "" {
		panic("postgresio.Write: Host must be set. For a Unix domain socket, set Host to the " +
			"socket directory, for example \"/var/run/postgresql\"; libpq's compiled-in default is " +
			"not assumed because it differs between distributions and container images")
	}
	if opts.Database == "" {
		panic("postgresio.Write: Database must be set. libpq would otherwise default it to the " +
			"username, which is rarely the intended target")
	}

	// validateSSLMode already guards the CDC and option-builder paths. Applying
	// it to the effective value here closes the gap for callers that build
	// WriteOptions as a struct literal and bypass NewWriteOptions, so a typo
	// such as "verify_full" is rejected at construction instead of at connect
	// time on every worker.
	effectiveSSLMode := opts.SSLMode
	if effectiveSSLMode == "" {
		effectiveSSLMode = DefaultSSLMode
	}
	if err := validateSSLMode(effectiveSSLMode); err != nil {
		panic(fmt.Sprintf("postgresio.Write: %v", err))
	}

	// MERGE is only expressible through the staged COPY path, and PgBouncer
	// compatibility forces the parameterized path because transaction pooling
	// cannot keep a session-scoped staging table alive between flushes. The
	// two cannot both be satisfied, so the conflict is reported here rather
	// than as a first-flush failure on every worker.
	if opts.WriteMode == WriteModeMerge && opts.UsePgBouncer {
		panic("postgresio.Write: WriteModeMerge cannot be combined with PgBouncer compatibility. " +
			"MERGE is issued through the staged COPY path, which reuses a session-scoped staging table, " +
			"and transaction pooling cannot guarantee that table is present on the connection a later " +
			"flush lands on. Connect directly to PostgreSQL, or use a session-pooling PgBouncer mode.")
	}

	// Checked here as well as in WithReplicationOriginName, because the
	// cross-language SchemaTransform assigns the field directly and never
	// passes through the option.
	if opts.ReplicationOriginName != "" && !originNameRegex.MatchString(opts.ReplicationOriginName) {
		panic(fmt.Sprintf("postgresio.Write: invalid replication origin name %q (must match %s)",
			opts.ReplicationOriginName, originNameRegex.String()))
	}

	// A replication origin is selected on the session. Under transaction
	// pooling a later statement can be routed to a server connection that
	// never selected it, so some writes would go out unstamped and a
	// bidirectional peer would replay exactly those back -- intermittently,
	// which is worse than not working at all.
	if opts.ReplicationOriginName != "" && opts.UsePgBouncer {
		panic("postgresio.Write: WithReplicationOriginName cannot be combined with PgBouncer " +
			"compatibility. Selecting a replication origin is session state, and transaction pooling " +
			"cannot guarantee a later statement runs on a connection that selected it, so writes would " +
			"be stamped intermittently. Connect directly to PostgreSQL, or use a session-pooling " +
			"PgBouncer mode.")
	}

	// Recorded here, in the submitting process, because this is the last point
	// at which the closure is observable. Setup runs on the worker, where a
	// dropped DialFunc and an unconfigured one are otherwise indistinguishable.
	opts.RequiresDialFunc = opts.DialFunc != nil

	// UpdateFields only reaches SQL through the two SET clauses: the upsert's
	// ON CONFLICT DO UPDATE and the update's UPDATE ... SET. Insert has no SET
	// clause, and MERGE builds its own from the primary key and the operation
	// column, so under either mode the option is read by nothing. Accepting it
	// silently would let a pipeline that intends to restrict which columns are
	// overwritten run as though it had said nothing at all.
	if len(opts.UpdateFields) > 0 &&
		opts.WriteMode != WriteModeUpsert && opts.WriteMode != WriteModeUpdate {
		panic(fmt.Sprintf("postgresio.Write: UpdateFields is only honored by WriteModeUpsert and "+
			"WriteModeUpdate, but write mode is %v, which would ignore it. Remove WithUpdateFields, "+
			"or select a write mode that has a SET clause to restrict.", opts.WriteMode))
	}

	elemType := col.Type().Type()

	// Checked against the element type rather than the live table because the
	// table is not reachable from the submitting process. Every column the
	// writer can emit is derived from this type, so a name that is not in it
	// cannot be written whatever the table looks like.
	if err := validateUpdateFields(opts.UpdateFields, structColumns(elemType)); err != nil {
		panic(fmt.Sprintf("postgresio.Write: %v", err))
	}

	fn := &writeFn{
		Table:          sanitizedTable,
		Options:        opts,
		Type:           beam.EncodedType{T: elemType},
		PrimaryKeyCols: opts.PrimaryKeyCols,
	}

	success, failed := beam.ParDo2(s, fn, col)

	return WriteResult{
		SuccessfulRows: success,
		FailedRows:     failed,
	}
}

// buildWriteDSN renders the libpq keyword/value connection string for the sink.
//
// search_path is pinned in the DSN rather than issued as a later SET so that
// every connection the pool opens is isolated from CVE-2018-1058, including
// connections created when a pool member is recycled mid-bundle. Its value is
// a compile-time literal rather than configuration, so it is left unquoted.
//
// Every value that can originate from configuration is quoted, because an
// unquoted value ends at the first space and libpq reads the remainder as
// further keywords. Quoting keeps a credential that legitimately contains a
// space intact, and stops any configured value from introducing a keyword the
// caller never set.
//
// Keyword order is load-bearing as a second layer of defence. libpq lets a
// later duplicate keyword win, so the two settings that carry the security
// properties, sslmode and the pinned search_path, are emitted last and cannot
// be displaced by anything injected from an earlier value. If a custom
// sslrootcert is supplied, it is emitted before sslmode so sslmode still
// governs transport security.
func buildWriteDSN(host string, port int, database, username, password, sslMode, sslRootCert string) string {
	var rootCertClause string
	if strings.TrimSpace(sslRootCert) != "" {
		rootCertClause = fmt.Sprintf(" sslrootcert=%s", quoteDSNValue(sslRootCert))
	}
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s%s sslmode=%s search_path=pg_catalog,pg_temp",
		quoteDSNValue(host), port, quoteDSNValue(database),
		quoteDSNValue(username), quoteDSNValue(password), rootCertClause, quoteDSNValue(sslMode))
}

type writeFn struct {
	Table          string           `json:"table"`
	Options        WriteOptions     `json:"options"`
	Type           beam.EncodedType `json:"type"`
	PrimaryKeyCols []string         `json:"primary_key_cols"`

	db        *sql.DB
	compactor *BatchCompactor
	columns   []string
	colTypes  map[string]string
	workerID  string
}

func (fn *writeFn) Setup(ctx context.Context) error {
	fn.workerID = fmt.Sprintf("%016x", rand.Uint64())

	if fn.Options.WriteMode == WriteModeUpdate && len(fn.PrimaryKeyCols) == 0 {
		return fmt.Errorf("postgresio: WriteModeUpdate requires primary key columns (PrimaryKeyCols)")
	}

	// A configured dialer that arrives nil was dropped crossing the
	// serialization boundary.
	if fn.Options.RequiresDialFunc && fn.Options.DialFunc == nil {
		return errDialFuncLost()
	}

	sslMode := fn.Options.SSLMode
	if sslMode == "" {
		sslMode = DefaultSSLMode
	}
	if err := validateSSLMode(sslMode); err != nil {
		return fmt.Errorf("postgresio: invalid sslmode: %w", err)
	}
	password := fn.Options.ResolvePassword()
	dsn := buildWriteDSN(fn.Options.Host, fn.Options.Port, fn.Options.Database,
		fn.Options.Username, password, sslMode, fn.Options.SSLRootCert)

	connector := &pqConnector{
		dialFunc:          fn.Options.DialFunc,
		dsn:               dsn,
		initSQL:           fn.Options.ConnectionInitSQL,
		replicationOrigin: fn.Options.ReplicationOriginName,
	}
	db := sql.OpenDB(connector)

	// Clamp worker pool connections to prevent connection storms across distributed workers
	maxConns := fn.Options.MaxConnections
	if maxConns <= 0 {
		maxConns = 2
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(30 * time.Minute)

	fn.applyPgBouncerCompatibility(ctx)

	fn.db = db
	fn.compactor = NewBatchCompactor(fn.Options.BatchSize, fn.Options.MaxBatchBytes, fn.Options.FlushInterval)
	fn.inspectColumns(fn.Type.T)

	// Write already rejects this at construction. Repeating it here covers a
	// writeFn assembled directly, and costs one pass over a slice that is
	// almost always empty. Failing in Setup still beats failing at flush: the
	// batch that would carry the bad identifier has not been accumulated yet.
	if err := validateUpdateFields(fn.Options.UpdateFields, fn.columns); err != nil {
		return fmt.Errorf("postgresio: %w", err)
	}

	if err := fn.inspectColumnTypes(ctx); err != nil {
		return err
	}

	return nil
}

// applyPgBouncerCompatibility adjusts the write path so it holds no session
// state, which is the only way it can survive transaction pooling.
//
// PgBouncer in transaction pooling mode hands each transaction whichever
// server connection is free, so nothing that lives in a session survives
// between them.
//
// The staged-COPY path depends on exactly that: it creates a temporary staging
// table once and reuses it across flushes, deliberately, to keep per-batch DDL
// out of the catalogs. Under transaction pooling a later flush can arrive on a
// connection where that table was never created, so the method is downgraded
// to the parameterized path. This is what the option is for; it previously set
// a field nothing read.
func (fn *writeFn) applyPgBouncerCompatibility(ctx context.Context) {
	if !fn.Options.UsePgBouncer {
		return
	}
	if fn.Options.WriteMethod == WriteMethodStagedCopy {
		log.Warnf(ctx, "postgresio: PgBouncer compatibility is enabled, so the staged COPY write method "+
			"has been downgraded to the parameterized UNNEST path. Staged COPY reuses a session-scoped "+
			"staging table, which transaction pooling cannot guarantee is present on the connection a "+
			"later flush lands on.")
		fn.Options.WriteMethod = WriteMethodUnnest
	}
	if fn.Options.ConnectionInitSQL != "" {
		log.Warnf(ctx, "postgresio: ConnectionInitSQL is set alongside PgBouncer compatibility. It runs "+
			"when a pooled connection is opened, but transaction pooling may route a later statement to "+
			"a different server connection that never ran it, so session-level settings apply "+
			"inconsistently. Prefer values that can be set as connection parameters.")
	}
}

// columnTagKeys are the struct tags consulted for a column name, in priority
// order. db is checked first because it names a database column specifically,
// where json also governs unrelated HTTP encoding.
var columnTagKeys = []string{"db", "beam", "json"}

// columnNameFromTag returns the column name a struct tag declares, whether the
// field is explicitly excluded, and whether the tag named anything at all.
//
// Tag values carry options after a comma -- `json:"customer_id,omitempty"` is
// the common one. The whole value used to be taken as the column name, which
// produced a column literally named `customer_id,omitempty`; SanitizeIdentifier
// then rejected it with an error that pointed at the sanitizer rather than at
// the tag.
//
// A name of "-" means the field is not a column. That is the standard
// convention for these tags and previously produced a column named "-".
func columnNameFromTag(tag string) (name string, excluded bool, ok bool) {
	if tag == "" {
		return "", false, false
	}
	name, _, _ = strings.Cut(tag, ",")
	switch name {
	case "":
		// An options-only tag such as `json:",omitempty"` names nothing; fall
		// through to the next tag or to the field name.
		return "", false, false
	case "-":
		return "", true, true
	}
	return name, false, true
}

// fieldTagNames returns the set of column names a field's tags declare.
//
// resolveColumn matches a column against a field by tag, and has to read those
// tags exactly as inspectColumns did when it derived the column name. Comparing
// the raw tag value instead meant a field tagged `json:"customer_id,omitempty"`
// never matched the column `customer_id` that was derived from it, so the
// value read as absent and the column was written NULL.
func fieldTagNames(fld reflect.StructField) map[string]bool {
	names := make(map[string]bool, len(columnTagKeys))
	for _, key := range columnTagKeys {
		if name, excluded, ok := columnNameFromTag(fld.Tag.Get(key)); ok && !excluded {
			names[name] = true
		}
	}
	return names
}

// toSnakeCase converts a Go field name to the snake_case spelling PostgreSQL
// columns conventionally use.
//
// Lowercasing alone mapped CustomerID to "customerid", which matches no real
// column, so every untagged multi-word field silently produced a name the
// server would reject. Consecutive capitals are treated as one acronym, so
// CustomerID becomes customer_id rather than customer_i_d, and HTTPServer
// becomes http_server.
func toSnakeCase(name string) string {
	runes := []rune(name)

	var b strings.Builder
	b.Grow(len(runes) + 4)

	for i, r := range runes {
		if !unicode.IsUpper(r) {
			b.WriteRune(r)
			continue
		}
		// A boundary exists where a lowercase run ends (userID -> user_id) or
		// where an acronym run ends and a new word begins (HTTPServer ->
		// http_server).
		endsLowerRun := i > 0 && (unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1]))
		startsNewWord := i > 0 && i+1 < len(runes) && unicode.IsLower(runes[i+1])
		if endsLowerRun || startsNewWord {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

func (fn *writeFn) inspectColumns(t reflect.Type) {
	fn.columns = structColumns(t)
}

// structColumns derives the ordered list of PostgreSQL column names an element
// type maps to, using the same tag precedence and snake_case fallback the
// writer uses when it builds SQL.
//
// It is package level rather than a writeFn method so that Write can resolve
// the columns in the submitting process, before any writeFn exists, and reject
// a configuration that names a column the element type does not carry.
// A type that is not a struct yields no columns.
func structColumns(t reflect.Type) []string {
	if t == nil {
		return nil
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	columns := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		var colName string
		var excluded, tagged bool
		for _, key := range columnTagKeys {
			colName, excluded, tagged = columnNameFromTag(field.Tag.Get(key))
			if tagged {
				break
			}
		}
		if excluded {
			continue
		}
		if !tagged {
			colName = toSnakeCase(field.Name)
		}
		columns = append(columns, colName)
	}
	return columns
}

// validateUpdateFields reports a configuration error when UpdateFields names a
// column the element type does not carry.
//
// UpdateFields is matched against resolved column names exactly and
// case-sensitively, unlike the primary key lookup in ExtractPrimaryKeys, which
// falls back to a case-insensitive match. A name that resolves to nothing is
// not inert: on the upsert path it is emitted verbatim into
// ON CONFLICT DO UPDATE SET, so PostgreSQL rejects the whole batch at flush
// time with `column "emial" of relation "orders" does not exist`, and on the
// update path it is silently dropped from the SET clause, so the column the
// caller meant to write is never written at all. Both surface long after
// submission, on a worker, which is why this runs at construction.
//
// An empty columns slice means the element type is not a struct, so there is
// nothing to check against.
func validateUpdateFields(updateFields, columns []string) error {
	if len(updateFields) == 0 || len(columns) == 0 {
		return nil
	}

	known := make(map[string]string, len(columns)) // lowercase -> canonical
	exact := make(map[string]bool, len(columns))
	for _, col := range columns {
		exact[col] = true
		lower := strings.ToLower(col)
		if _, seen := known[lower]; !seen {
			known[lower] = col
		}
	}

	for _, f := range updateFields {
		if exact[f] {
			continue
		}
		if canonical, ok := known[strings.ToLower(f)]; ok {
			return fmt.Errorf("update field %q does not match any column of the input type; "+
				"matching is exact and case-sensitive, and the type declares %q. "+
				"Known columns: %s", f, canonical, strings.Join(columns, ", "))
		}
		return fmt.Errorf("update field %q does not match any column of the input type. "+
			"Column names come from the db, beam, or json struct tag, or from the snake_case "+
			"form of the field name. Known columns: %s", f, strings.Join(columns, ", "))
	}
	return nil
}

func (fn *writeFn) inspectColumnTypes(ctx context.Context) error {
	rows, err := fn.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", fn.Table))
	if err != nil {
		return fmt.Errorf("postgresio: failed to query target table types: %w", err)
	}
	defer rows.Close()

	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("postgresio: failed to read column types: %w", err)
	}

	fn.colTypes = make(map[string]string)
	for _, ct := range ctypes {
		fn.colTypes[ct.Name()] = ct.DatabaseTypeName()
	}
	return nil
}

func (fn *writeFn) StartBundle(ctx context.Context, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	fn.compactor = NewBatchCompactor(fn.Options.BatchSize, fn.Options.MaxBatchBytes, fn.Options.FlushInterval)
	return nil
}

func (fn *writeFn) ProcessElement(ctx context.Context, elem beam.X, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	entityKey, sortKey := ExtractPrimaryKeys(elem, fn.PrimaryKeyCols)
	fn.compactor.Add(entityKey, sortKey, elem, estimateElementSize(elem))

	if fn.compactor.ShouldFlush() {
		return fn.flushBatch(ctx, emitSuccess, emitFailed)
	}
	return nil
}

func (fn *writeFn) FinishBundle(ctx context.Context, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	if fn.compactor.Len() > 0 {
		return fn.flushBatch(ctx, emitSuccess, emitFailed)
	}
	return nil
}

func (fn *writeFn) Teardown() {
	if fn.db != nil {
		_ = fn.db.Close()
	}
}

func (fn *writeFn) flushBatch(ctx context.Context, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	batch := fn.compactor.CompactAndSort()
	if len(batch) == 0 {
		return nil
	}

	// Rows in one batch may carry different column sets: a CDC UPDATE omits
	// columns whose TOASTed values the server did not retransmit. A single
	// statement can only name one column list, so the batch is grouped by the
	// set of columns present and each group written separately.
	parts, err := partitionBatchByUnchangedColumns(batch, fn.columns)
	if err != nil {
		// A row type naming a column that does not exist is a defect in that
		// type, not a transient fault, so the bundle fails rather than
		// draining every batch to the dead-letter output under a
		// misconfiguration that will repeat.
		fn.failBatch(ctx, batch, err)
		return err
	}

	for _, part := range parts {
		if err := fn.writePartition(ctx, part, emitSuccess, emitFailed); err != nil {
			return err
		}
	}
	return nil
}

// failBatch logs an error and records metrics for a batch that cannot produce
// a correct write. The caller returns an error to fail the bundle. It does not
// emit to the dead-letter output because runner semantics discard bundle outputs
// on error.
//
// This is deliberately distinct from the execution-failure path in
// writePartition. A batch rejected here is rejected because the pipeline is
// configured in a way that cannot ever produce a correct write, so retrying the
// bundle against the same configuration would fail identically. Continuing
// would drain every row of every batch into the dead-letter output while
// reporting success, which hides a misconfiguration behind an apparently
// healthy pipeline.
func (fn *writeFn) failBatch(ctx context.Context, batch []any, err error) {
	sinkFailedRows.Inc(ctx, int64(len(batch)))
	msg := SanitizeErrorMessage(err)
	log.Errorf(ctx, "postgresio: %s", msg)
}

func (fn *writeFn) writePartition(ctx context.Context, part writePartition, emitSuccess func(beam.X), emitFailed func(FailedRow)) error {
	batch := part.Rows
	if len(batch) == 0 {
		return nil
	}

	// MERGE is expressible only by the staged-COPY path. The parameterized
	// UNNEST path emits INSERT ... ON CONFLICT against the target, a statement
	// with no DELETE arm, so a MERGE batch routed through it would apply the
	// inserts and updates and silently discard every delete. A silently wrong
	// result is worse than a failed bundle, so these cases fail loudly.
	mergeMode := fn.Options.WriteMode == WriteModeMerge
	if mergeMode {
		var err error
		switch {
		case fn.Options.WriteMethod != WriteMethodStagedCopy:
			err = fmt.Errorf("postgresio: write mode MERGE requires the staged COPY write method; the parameterized UNNEST path cannot express deletes")
		case !part.IsComplete():
			err = fmt.Errorf("postgresio: write mode MERGE cannot write partial rows (columns omitted: %s); the staging table is cloned from the target and carries every column",
				strings.Join(part.UnchangedColumns, ", "))
		}
		if err != nil {
			fn.failBatch(ctx, batch, err)
			return err
		}
	}

	// High-Throughput Fast Path: Staged COPY Upsert (>100,000 rows/sec).
	//
	// Only a complete partition is eligible. The staging table is created LIKE
	// the target, so it carries every column; streaming a partial row into it
	// would materialize a zero value for the omitted columns and the merge
	// would then write that over the stored value. Partial partitions fall
	// through to the parameterized path, which can name an arbitrary column
	// subset. This is a rare case, so the hot path is unaffected.
	var copyErr error
	if fn.Options.WriteMethod == WriteMethodStagedCopy && part.IsComplete() {
		// Retried on its own rather than relying on the fallback. A deadlock
		// here says nothing about the batch, and dropping to the slower
		// parameterized path -- or, under MERGE, to the dead-letter queue,
		// which has no fallback at all -- discards a fast-path write that a
		// replay would almost certainly land.
		copyErr = retryOnDeadlock(ctx, func() { sinkDeadlockRetries.Inc(ctx, 1) },
			func(ctx context.Context) error { return fn.executeStagedCopy(ctx, batch) })
		if copyErr == nil {
			sinkWrittenRows.Inc(ctx, int64(len(batch)))
			for _, item := range batch {
				emitSuccess(item)
			}
			return nil
		}
		if mergeMode {
			err := fmt.Errorf("postgresio: MERGE staged COPY failed: %w", copyErr)
			fn.failBatch(ctx, batch, err)
			return err
		}
		log.Warnf(ctx, "postgresio: staged COPY failed (%v), falling back to parameterized UNNEST", copyErr)
	}

	query, args, err := fn.buildUnnestQueryForColumns(batch, part.Columns)
	if err != nil {
		if copyErr != nil {
			err = fmt.Errorf("postgresio: parameterized UNNEST fallback build failed: %w (original COPY error: %v)", err, copyErr)
		} else {
			err = fmt.Errorf("postgresio: parameterized UNNEST build failed: %w", err)
		}
		fn.failBatch(ctx, batch, err)
		return err
	}

	// Autocommit. A replication origin, when configured, is selected once per
	// connection by pqConnector, so every statement this path issues is
	// already stamped with it and no explicit transaction is needed to carry
	// the tag. That applies equally when this path is entered directly and
	// when a staged COPY fails and falls through to it, so the fallback
	// cannot write untagged rows that a bidirectional peer would replay back.
	execBatch := func(ctx context.Context) error {
		_, err := fn.db.ExecContext(ctx, query, args...)
		return err
	}

	execErr := retryOnDeadlock(ctx, func() { sinkDeadlockRetries.Inc(ctx, 1) }, execBatch)
	if execErr == nil {
		sinkWrittenRows.Inc(ctx, int64(len(batch)))
		for _, item := range batch {
			emitSuccess(item)
		}
		return nil
	}

	sqlState := extractSqlState(execErr)
	if copyErr != nil {
		execErr = fmt.Errorf("postgresio: write batch fallback failed with sqlstate %s: %w (original COPY error: %v)", sqlState, execErr, copyErr)
	}

	// Permanent failure: route batch to Dead-Letter Queue (DLQ)
	sinkFailedRows.Inc(ctx, int64(len(batch)))
	sanitizedMsg := SanitizeErrorMessage(execErr)
	log.Errorf(ctx, "postgresio: %s", sanitizedMsg)
	for _, item := range batch {
		emitFailed(FailedRow{
			Row:          item,
			ErrorMessage: sanitizedMsg,
			SqlState:     sqlState,
		})
	}
	return nil
}

// deadlockMaxRetries bounds how many times a batch is replayed after
// PostgreSQL breaks a deadlock cycle involving it.
const deadlockMaxRetries = 5

// deadlockBaseBackoff is the first wait in the full-jitter ladder. It is a
// variable so tests can walk the whole ladder without waiting seconds for it;
// nothing outside tests reassigns it.
var deadlockBaseBackoff = 50 * time.Millisecond

// retryOnDeadlock runs op, replaying it while PostgreSQL reports a detected
// deadlock, with full-jitter exponential backoff.
//
// A deadlock is not a property of the data. PostgreSQL resolves a cycle by
// aborting one participant, and the aborted statement normally succeeds once
// the other side has committed, so this is the one SQLSTATE where an immediate
// replay is both safe and likely to work. Every other error returns
// immediately: retrying a constraint violation only delays the dead-letter
// routing that is already correct for it.
//
// Jitter is full rather than fixed because the workers that deadlock with each
// other are running identical code. A deterministic backoff would reschedule
// them at the same instant and reproduce the same cycle.
//
// The wait honours ctx. A sleep that ignored cancellation would keep a worker
// that is draining for shutdown alive for the remainder of the ladder, and
// would outlive the bundle the work belongs to.
func retryOnDeadlock(ctx context.Context, onRetry func(), op func(context.Context) error) error {
	backoff := deadlockBaseBackoff

	var err error
	for attempt := 0; attempt <= deadlockMaxRetries; attempt++ {
		if err = op(ctx); err == nil {
			return nil
		}
		if extractSqlState(err) != sqlStateDeadlockDetected || attempt == deadlockMaxRetries {
			return err
		}
		if onRetry != nil {
			onRetry()
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("postgresio: abandoned deadlock retry: %w (last database error: %v)", ctx.Err(), err)
		case <-time.After(backoff + time.Duration(rand.Int63n(int64(backoff)))):
		}
		backoff *= 2
	}
	return err
}

// buildCopyStatement returns a COPY ... FROM STDIN statement.
//
// This replaces the deprecated lib/pq copy-statement helper, which quoted the
// table name itself. Because the caller has already quoted the identifier via
// SanitizeTableIdentifier, that helper produced a doubly-quoted name such as
// COPY """public"".""orders""", which PostgreSQL reads as a single table
// literally named `"public"."orders"`.
//
// table must already be a quoted identifier; columns must already be quoted.
func buildCopyStatement(table string, columns []string) (string, error) {
	if table == "" {
		return "", fmt.Errorf("postgresio: COPY target table must not be empty")
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("postgresio: COPY requires at least one column")
	}
	return fmt.Sprintf("COPY %s (%s) FROM STDIN", table, strings.Join(columns, ", ")), nil
}

// stagingTableName derives a stable per-target staging table name.
//
// The name must be deterministic so the table can be reused across batches on
// the same session, and distinct per target table so that a worker writing to
// several tables does not reuse a staging table with the wrong column layout.
// Including the workerID ensures that a new worker (which may have a newer
// schema for the target table) does not reuse an old worker's staging table
// on a pooled connection.
func stagingTableName(qualifiedTable string, workerID string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(qualifiedTable))
	return fmt.Sprintf("beam_stage_%016x_%s", h.Sum64(), workerID)
}

// executeStagedCopy executes a two-phase bulk upsert: a PostgreSQL COPY into a
// session-scoped staging table followed by an atomic set-based ON CONFLICT merge.
//
// A configured replication origin needs nothing here: pqConnector selects it on
// the connection, so this transaction inherits it along with every other.
func (fn *writeFn) executeStagedCopy(ctx context.Context, batch []any) error {
	if len(fn.columns) == 0 {
		return fmt.Errorf("postgresio: no columns discovered for type %v", fn.Type.T)
	}

	sanitizedCols := make([]string, len(fn.columns))
	for i, col := range fn.columns {
		san, err := SanitizeIdentifier(col)
		if err != nil {
			return err
		}
		sanitizedCols[i] = san
	}

	txn, err := fn.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	// Append-only fast path
	if fn.Options.WriteMode == WriteModeInsert && len(fn.PrimaryKeyCols) == 0 {
		copyStmt, err := buildCopyStatement(fn.Table, sanitizedCols)
		if err != nil {
			return err
		}
		stmt, err := txn.PrepareContext(ctx, copyStmt)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, item := range batch {
			rowVals := fn.extractRowValues(item)
			if _, err := stmt.ExecContext(ctx, rowVals...); err != nil {
				return err
			}
		}
		if _, err := stmt.ExecContext(ctx); err != nil {
			return err
		}
		if err := stmt.Close(); err != nil {
			return err
		}
		return txn.Commit()
	}

	// Upsert path via a session-scoped staging table.
	//
	// The staging table is created once per session under a stable name and
	// reused. Creating and dropping a temporary table on every micro-batch
	// inserts and deletes rows in pg_class, pg_attribute, pg_type and
	// pg_depend at the flush rate, which autovacuum on the catalogs cannot
	// keep up with; catalog bloat then slows query planning for every session
	// on the instance, not just this pipeline.
	//
	// ON COMMIT DELETE ROWS empties the table at each commit while keeping the
	// definition, so steady-state flushes perform no catalog DDL at all.
	tempTable := stagingTableName(fn.Table, fn.workerID)
	createSQL := fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DELETE ROWS", tempTable, fn.Table)
	if _, err := txn.ExecContext(ctx, createSQL); err != nil {
		return err
	}

	// MERGE mode carries an operation column classifying each source row as an
	// insert, update or delete. It describes the change, not the stored entity,
	// so it is deliberately not a target column: buildMergeQuery leaves it out
	// of the INSERT column list and the UPDATE SET clauses and reads it only in
	// the WHEN predicates. Because the staging table is cloned LIKE the target,
	// it does not have the column either, and the COPY below would fail with
	// 42703. Add it to the clone.
	alterSQL, err := stagingOpColumnDDL(tempTable, fn.Options.WriteMode, fn.Options.OpColumn)
	if err != nil {
		return err
	}
	if alterSQL != "" {
		if _, err := txn.ExecContext(ctx, alterSQL); err != nil {
			return err
		}
	}

	// Defensive: a prior transaction on this connection may have left rows
	// behind if it did not reach commit.
	if _, err := txn.ExecContext(ctx, fmt.Sprintf("TRUNCATE %s", tempTable)); err != nil {
		return err
	}

	copyStmt, err := buildCopyStatement(tempTable, sanitizedCols)
	if err != nil {
		return err
	}
	stmt, err := txn.PrepareContext(ctx, copyStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, item := range batch {
		rowVals := fn.extractRowValues(item)
		if _, err := stmt.ExecContext(ctx, rowVals...); err != nil {
			return err
		}
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		return err
	}
	if err := stmt.Close(); err != nil {
		return err
	}

	if fn.Options.WriteMode == WriteModeMerge && len(batch) >= 1000 {
		// Pre-analyze staging table to prevent optimizer from selecting sequential scans on large targets
		if _, err := txn.ExecContext(ctx, fmt.Sprintf("ANALYZE %s", tempTable)); err != nil {
			return fmt.Errorf("failed to analyze temp table %s: %w", tempTable, err)
		}
	}
	mergeSql, err := fn.buildStagedCopyMergeQuery(tempTable)
	if err != nil {
		return err
	}

	if fn.Options.ExplainAnalyze && shouldSampleExplain(fn.Options.ExplainSampleRate) {
		explainSql := fmt.Sprintf("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) %s", mergeSql)
		var planJSON []byte
		if err := txn.QueryRowContext(ctx, explainSql).Scan(&planJSON); err != nil {
			return err
		}
		recordExplainTelemetry(ctx, planJSON, fn.Table)
	} else {
		if _, err := txn.ExecContext(ctx, mergeSql); err != nil {
			return err
		}
	}

	return txn.Commit()
}

func (fn *writeFn) buildStagedCopyMergeQuery(tempTable string) (string, error) {
	sanitizedCols := make([]string, len(fn.columns))
	for i, col := range fn.columns {
		san, err := SanitizeIdentifier(col)
		if err != nil {
			return "", err
		}
		sanitizedCols[i] = san
	}

	sanitizedPks := make([]string, len(fn.PrimaryKeyCols))
	pkSet := make(map[string]bool)
	for i, pk := range fn.PrimaryKeyCols {
		san, err := SanitizeIdentifier(pk)
		if err != nil {
			return "", err
		}
		sanitizedPks[i] = san
		pkSet[pk] = true
	}

	colsToUpdate := fn.columns
	if len(fn.Options.UpdateFields) > 0 {
		colsToUpdate = fn.Options.UpdateFields
	}
	updateClauses := make([]string, 0, len(colsToUpdate))
	for _, col := range colsToUpdate {
		if !pkSet[col] {
			san, err := SanitizeIdentifier(col)
			if err != nil {
				return "", err
			}
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", san, san))
		}
	}

	colList := strings.Join(sanitizedCols, ", ")
	if fn.Options.WriteMode == WriteModeMerge {
		sanitizedTemp, err := SanitizeTableIdentifier(tempTable)
		if err != nil {
			return "", err
		}
		return buildMergeQueryForSanitizedTables(fn.Table, sanitizedTemp, fn.columns, fn.PrimaryKeyCols, fn.Options.OpColumn, fn.Options.DeleteOpValue)
	}
	if fn.Options.WriteMode == WriteModeUpdate {
		if len(sanitizedPks) == 0 {
			return "", fmt.Errorf("postgresio: WriteModeUpdate requires primary key columns")
		}
		setClauses := make([]string, 0, len(colsToUpdate))
		for _, col := range colsToUpdate {
			if !pkSet[col] {
				san, err := SanitizeIdentifier(col)
				if err != nil {
					return "", err
				}
				setClauses = append(setClauses, fmt.Sprintf("%s = %s.%s", san, tempTable, san))
			}
		}
		if len(setClauses) == 0 {
			setClauses = append(setClauses, fmt.Sprintf("%s = %s.%s", sanitizedPks[0], tempTable, sanitizedPks[0]))
		}
		whereClauses := make([]string, len(sanitizedPks))
		for i, pk := range sanitizedPks {
			whereClauses[i] = fmt.Sprintf("%s.%s = %s.%s", fn.Table, pk, tempTable, pk)
		}
		return fmt.Sprintf("UPDATE %s SET %s FROM %s WHERE %s",
			fn.Table, strings.Join(setClauses, ", "), tempTable, strings.Join(whereClauses, " AND ")), nil
	}
	if fn.Options.WriteMode == WriteModeInsert {
		return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
			fn.Table, colList, colList, tempTable), nil
	}
	if len(sanitizedPks) == 0 {
		return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT DO NOTHING",
			fn.Table, colList, colList, tempTable), nil
	}
	if len(updateClauses) > 0 {
		return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
			fn.Table, colList, colList, tempTable, strings.Join(sanitizedPks, ", "), strings.Join(updateClauses, ", ")), nil
	}
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO NOTHING",
		fn.Table, colList, colList, tempTable, strings.Join(sanitizedPks, ", ")), nil
}

// derefElement unwraps an input element down to the value that carries its
// columns, returning the zero Value when there is nothing to read.
//
// Elements arrive as structs, pointers to structs, maps, or an interface
// holding any of those. A nil pointer anywhere in that chain yields an invalid
// Value rather than a panic, and resolveColumn reports every column of such an
// element as absent.
func derefElement(item any) reflect.Value {
	v := reflect.ValueOf(item)
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

// resolveColumn returns the value stored under a column name within a single
// input element, along with its declared type where one exists.
//
// The sink accepts both structs and maps: ExtractPrimaryKeys has always
// handled each, and the CDC decoders emit map[string]any for tables whose
// shape is only known at runtime. Resolution has to branch on the element kind
// because reflect panics when the accessor does not match the kind --
// FieldByName and NumField on a map, MapIndex on a struct. Calling them
// unconditionally turned a map-shaped element into a panic that took down the
// whole bundle.
//
// Structs match on the exact field name first, then case-insensitively, then
// on the db, beam and json struct tags. Maps match on the exact key first and
// then case-insensitively, which keeps the map[string]any rows produced by CDC
// behaving like their struct equivalents. Unexported struct fields are skipped
// because reading one through reflection panics.
//
// The declared type is returned only for structs; a map carries no static
// per-key type, so callers fall back to the column type reported by the
// server.
func resolveColumn(v reflect.Value, colName string) (any, reflect.Type, bool) {
	if !v.IsValid() {
		return nil, nil, false
	}

	switch v.Kind() {
	case reflect.Struct:
		if f := v.FieldByName(colName); f.IsValid() && f.CanInterface() {
			return f.Interface(), f.Type(), true
		}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			fld := t.Field(i)
			if !fld.IsExported() {
				continue
			}
			if strings.EqualFold(fld.Name, colName) || fieldTagNames(fld)[colName] {
				return v.Field(i).Interface(), fld.Type, true
			}
		}

	case reflect.Map:
		kt := v.Type().Key()
		if kt.Kind() != reflect.String {
			return nil, nil, false
		}
		key := reflect.ValueOf(colName)
		if key.Type() != kt {
			key = key.Convert(kt)
		}
		if mv := v.MapIndex(key); mv.IsValid() {
			return mv.Interface(), nil, true
		}
		for _, mk := range v.MapKeys() {
			if strings.EqualFold(mk.String(), colName) {
				return v.MapIndex(mk).Interface(), nil, true
			}
		}
	}

	return nil, nil, false
}

func (fn *writeFn) extractRowValues(item any) []any {
	v := derefElement(item)
	vals := make([]any, len(fn.columns))
	for i, colName := range fn.columns {
		val, _, ok := resolveColumn(v, colName)
		if !ok {
			vals[i] = nil
			continue
		}
		switch vec := val.(type) {
		case Vector:
			vals[i] = FormatVectorLiteral(vec)
		case []float32:
			vals[i] = FormatVectorLiteral(vec)
		default:
			vals[i] = val
		}
	}
	return vals
}

const (
	// minElementSizeEstimate floors the per-element estimate so that a batch
	// of tiny rows still counts against MaxBatchBytes at a realistic rate;
	// every row carries per-tuple overhead the Go value does not show.
	minElementSizeEstimate = 64
	// maxSizeInspectionDepth bounds recursion through nested values. Beyond
	// it the static type size is used, which keeps a cyclic or deeply nested
	// graph from making the estimate more expensive than the write.
	maxSizeInspectionDepth = 4

	// Go header sizes, added so that a field contributes its own footprint in
	// addition to the bytes it points at.
	stringHeaderSize = 16
	sliceHeaderSize  = 24
)

// estimateElementSize approximates the number of bytes one element contributes
// to a batch, which is what MaxBatchBytes budgets.
//
// The previous implementation added f.Len() for slices. That is the element
// count, not a byte count, so a []string holding a hundred one-kilobyte values
// counted as 100 rather than ~100,000. Wide rows therefore under-reported by
// orders of magnitude and a flush could carry far more memory than configured.
func estimateElementSize(elem any) int {
	if elem == nil {
		return minElementSizeEstimate
	}
	if sz := estimateValueSize(reflect.ValueOf(elem), 0); sz > minElementSizeEstimate {
		return sz
	}
	return minElementSizeEstimate
}

// estimateValueSize returns the approximate byte footprint of a single value.
func estimateValueSize(v reflect.Value, depth int) int {
	if !v.IsValid() {
		return 0
	}
	if depth > maxSizeInspectionDepth {
		return int(v.Type().Size())
	}

	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return 0
		}
		return estimateValueSize(v.Elem(), depth+1)

	case reflect.String:
		return stringHeaderSize + v.Len()

	case reflect.Slice, reflect.Array:
		elemType := v.Type().Elem()
		// A slice of fixed-width elements is measured by arithmetic rather
		// than by walking it, so a large []byte or []int64 costs nothing to
		// size.
		if isFixedWidthKind(elemType.Kind()) {
			return sliceHeaderSize + v.Len()*int(elemType.Size())
		}
		total := sliceHeaderSize
		for i := 0; i < v.Len(); i++ {
			total += estimateValueSize(v.Index(i), depth+1)
		}
		return total

	case reflect.Map:
		if v.IsNil() {
			return 0
		}
		total := sliceHeaderSize
		iter := v.MapRange()
		for iter.Next() {
			total += estimateValueSize(iter.Key(), depth+1) + estimateValueSize(iter.Value(), depth+1)
		}
		return total

	case reflect.Struct:
		total := 0
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			// An unexported field cannot be read through reflection, so it
			// contributes its declared width.
			if !f.CanInterface() {
				total += int(f.Type().Size())
				continue
			}
			total += estimateValueSize(f, depth+1)
		}
		return total

	default:
		return int(v.Type().Size())
	}
}

// isFixedWidthKind reports whether every value of a kind occupies the same
// number of bytes, which is what makes the multiplication above valid.
func isFixedWidthKind(k reflect.Kind) bool {
	switch k {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return true
	}
	return false
}

func (fn *writeFn) buildUnnestQuery(batch []any) (string, []any, error) {
	return fn.buildUnnestQueryForColumns(batch, fn.columns)
}

// buildUnnestQueryForColumns builds the parameterized write for a specific set
// of columns.
//
// The column set is a parameter rather than always being fn.columns because a
// CDC UPDATE may omit columns whose TOASTed values the server did not
// retransmit. Such a column must be absent from both the INSERT column list and
// the DO UPDATE SET clause: including it would write a zero value over the
// stored one, which is the very data loss the structural TOAST representation
// exists to prevent. Omitting it from the INSERT list means a row that does not
// yet exist in the target takes the column's default, which is the best
// available answer when the source did not send a value.
func (fn *writeFn) buildUnnestQueryForColumns(batch []any, columns []string) (string, []any, error) {
	if len(columns) == 0 {
		return "", nil, fmt.Errorf("no columns discovered for type %v", fn.Type.T)
	}

	sanitizedCols := make([]string, len(columns))
	for i, col := range columns {
		san, err := SanitizeIdentifier(col)
		if err != nil {
			return "", nil, err
		}
		sanitizedCols[i] = san
	}

	columnArrays := make([][]any, len(columns))
	for i := range columnArrays {
		columnArrays[i] = make([]any, len(batch))
	}

	for rowIdx, item := range batch {
		v := derefElement(item)
		for colIdx, colName := range columns {
			val, _, ok := resolveColumn(v, colName)
			if !ok {
				columnArrays[colIdx][rowIdx] = nil
				continue
			}
			columnArrays[colIdx][rowIdx] = val
		}
	}

	unnestPlaceholders := make([]string, len(columns))
	args := make([]any, len(columns))
	var selectCols []string

	for i, col := range columns {
		dbType := fn.colTypes[col]
		if dbType == "" {
			dbType = "TEXT"
		}
		isArray := strings.HasPrefix(dbType, "_")

		if isArray {
			baseType := dbType[1:]
			unnestPlaceholders[i] = fmt.Sprintf("$%d::text[]", i+1)
			selectCols = append(selectCols, fmt.Sprintf("t.col%d::%s[]", i, baseType))

			// Elements are collected into []any rather than []string so that a
			// row whose array is absent stays nil and is encoded as NULL. A
			// []string would force the zero value "", which PostgreSQL reads
			// as an empty-string element rather than a NULL array.
			arrVals := make([]any, len(batch))
			for r := 0; r < len(batch); r++ {
				val := columnArrays[i][r]
				if val == nil {
					continue // leave nil: encoded as NULL
				}
				if s, ok := val.(string); ok {
					arrVals[r] = s
					continue
				}
				// Value() returns (nil, nil) for a nil slice, which is exactly
				// the NULL case, so the result is assigned unconditionally.
				// Guarding on valv != nil here would discard that NULL and fall
				// back to the raw Go slice, which pq cannot encode as an element.
				valv, err := pq.Array(val).Value()
				if err != nil {
					return "", nil, fmt.Errorf("postgresio: failed to encode array column %q: %w", col, err)
				}
				arrVals[r] = valv
			}
			args[i] = pq.Array(arrVals)
		} else {
			unnestPlaceholders[i] = fmt.Sprintf("$%d::%s[]", i+1, dbType)
			selectCols = append(selectCols, fmt.Sprintf("t.col%d", i))
			args[i] = pq.Array(columnArrays[i])
		}
	}

	tCols := make([]string, len(columns))
	for i := range columns {
		tCols[i] = fmt.Sprintf("col%d", i)
	}

	if fn.Options.WriteMode == WriteModeUpdate {
		if len(fn.PrimaryKeyCols) == 0 {
			return "", nil, fmt.Errorf("postgresio: WriteModeUpdate requires primary key columns")
		}
		sanitizedPks := make([]string, len(fn.PrimaryKeyCols))
		pkSet := make(map[string]bool)
		for i, pk := range fn.PrimaryKeyCols {
			san, err := SanitizeIdentifier(pk)
			if err != nil {
				return "", nil, err
			}
			sanitizedPks[i] = san
			pkSet[pk] = true
		}

		updateFieldsSet := make(map[string]bool)
		for _, f := range fn.Options.UpdateFields {
			updateFieldsSet[f] = true
		}

		var selectAsCols []string
		var setClauses []string
		var whereClauses []string

		for i, col := range columns {
			san, err := SanitizeIdentifier(col)
			if err != nil {
				return "", nil, err
			}
			selectAsCols = append(selectAsCols, fmt.Sprintf("%s AS %s", selectCols[i], san))
			if pkSet[col] {
				whereClauses = append(whereClauses, fmt.Sprintf("target.%s = source.%s", san, san))
			} else if len(updateFieldsSet) == 0 || updateFieldsSet[col] {
				setClauses = append(setClauses, fmt.Sprintf("%s = source.%s", san, san))
			}
		}

		if len(setClauses) == 0 {
			setClauses = append(setClauses, fmt.Sprintf("%s = source.%s", sanitizedPks[0], sanitizedPks[0]))
		}

		query := fmt.Sprintf("UPDATE %s AS target SET %s FROM (SELECT %s FROM UNNEST(%s) AS t(%s)) AS source WHERE %s",
			fn.Table,
			strings.Join(setClauses, ", "),
			strings.Join(selectAsCols, ", "),
			strings.Join(unnestPlaceholders, ", "),
			strings.Join(tCols, ", "),
			strings.Join(whereClauses, " AND "),
		)
		return query, args, nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM UNNEST(%s) AS t(%s)",
		fn.Table,
		strings.Join(sanitizedCols, ", "),
		strings.Join(selectCols, ", "),
		strings.Join(unnestPlaceholders, ", "),
		strings.Join(tCols, ", ")))

	if fn.Options.WriteMode == WriteModeUpsert {
		if len(fn.PrimaryKeyCols) == 0 {
			sb.WriteString(" ON CONFLICT DO NOTHING")
		} else {
			sanitizedPks := make([]string, len(fn.PrimaryKeyCols))
			pkSet := make(map[string]bool)
			for i, pk := range fn.PrimaryKeyCols {
				san, err := SanitizeIdentifier(pk)
				if err != nil {
					return "", nil, err
				}
				sanitizedPks[i] = san
				pkSet[pk] = true
			}

			colsToUpdate := columns
			if len(fn.Options.UpdateFields) > 0 {
				colsToUpdate = fn.Options.UpdateFields
			}
			updateClauses := make([]string, 0, len(colsToUpdate))
			for _, col := range colsToUpdate {
				if !pkSet[col] {
					san, err := SanitizeIdentifier(col)
					if err != nil {
						return "", nil, err
					}
					updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", san, san))
				}
			}

			if len(updateClauses) > 0 {
				sb.WriteString(fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
					strings.Join(sanitizedPks, ", "),
					strings.Join(updateClauses, ", ")))
			} else {
				sb.WriteString(fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING",
					strings.Join(sanitizedPks, ", ")))
			}
		}
	}

	return sb.String(), args, nil
}

// sqlStateDeadlockDetected is the SQLSTATE PostgreSQL reports when it breaks a
// deadlock cycle by aborting one of the participating transactions.
const sqlStateDeadlockDetected = "40P01"

// extractSqlState returns the SQLSTATE PostgreSQL reported for an error.
//
// The error is unwrapped rather than type-asserted. Every layer between the
// driver and here may wrap: the staged-COPY path annotates failures with
// context, the UNNEST fallback attaches the original COPY error, and
// database/sql itself wraps in places. A direct assertion sees only the
// outermost error, so a wrapped deadlock reported "UNKNOWN" and the batch went
// to the dead-letter queue instead of being retried -- the one state where
// retrying almost always succeeds.
func extractSqlState(err error) string {
	if err == nil {
		return ""
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code)
	}
	return "UNKNOWN"
}

// ExplainPlanResult captures PostgreSQL JSON plan metrics from EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON).
type ExplainPlanResult struct {
	PlanningTime  float64 `json:"Planning Time"`
	ExecutionTime float64 `json:"Execution Time"`
	Plan          struct {
		NodeType         string `json:"Node Type"`
		RelationName     string `json:"Relation Name"`
		SharedHitBlocks  int64  `json:"Shared Hit Blocks"`
		SharedReadBlocks int64  `json:"Shared Read Blocks"`
		ActualRows       int64  `json:"Actual Rows"`
	} `json:"Plan"`
}

// shouldSampleExplain reports whether a batch should trigger EXPLAIN (ANALYZE).
// A rate <= 0 selects default sampling (0.001, or 0.1% of batches). A rate > 1.0
// is clamped to 1.0 (every batch). Sampling is only invoked if ExplainAnalyze is true.
func shouldSampleExplain(rate float64) bool {
	if rate <= 0 {
		rate = 0.001 // 0.1% default (1 in 1000 batches)
	} else if rate > 1.0 {
		rate = 1.0
	}
	return rand.Float64() < rate
}

func recordExplainTelemetry(ctx context.Context, planJSON []byte, targetTable string) {
	var results []ExplainPlanResult
	if err := json.Unmarshal(planJSON, &results); err != nil || len(results) == 0 {
		return
	}
	res := results[0]
	if res.ExecutionTime > 250.0 {
		log.Warnf(ctx,
			"[POSTGRES_SINK_PLAN_ALERT] table=%s execution_time=%.2fms planning_time=%.2fms node=%s hit_blocks=%d read_blocks=%d",
			targetTable, res.ExecutionTime, res.PlanningTime, res.Plan.NodeType,
			res.Plan.SharedHitBlocks, res.Plan.SharedReadBlocks)
	}
}

// stagingOpColumnDDL returns the statement that adds the MERGE operation column
// to the staging table, or the empty string when the write mode does not use
// one.
//
// The staging table is cloned LIKE the target, and buildMergeQuery treats the
// operation column as a property of the change rather than of the stored row:
// it is absent from the INSERT column list and the UPDATE SET clauses and is
// read only by the WHEN predicates. The target therefore has no such column and
// neither does the clone, so it has to be added before rows can be staged.
//
// ADD COLUMN IF NOT EXISTS is correct in both directions. It is a no-op once
// the session's reused staging table already has the column, so steady-state
// flushes still perform no catalog DDL, and it is also a no-op when the target
// legitimately does carry an identically named column.
//
// text is the right type because the predicate buildMergeQuery emits compares
// the column against a single-quoted string literal.
func stagingOpColumnDDL(tempTable string, mode WriteMode, opColumn string) (string, error) {
	if mode != WriteModeMerge || opColumn == "" {
		return "", nil
	}
	sanOp, err := SanitizeIdentifier(opColumn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s text", tempTable, sanOp), nil
}

// buildMergeQuery generates an SQL-standard MERGE statement for PostgreSQL 15+
// from raw, unquoted table names.
//
// Callers that already hold sanitized identifiers must use
// buildMergeQueryForSanitizedTables instead. Sanitizing a second time fails:
// SanitizeIdentifier rejects the quotation marks the first pass added, because
// it cannot distinguish them from an injection attempt.
func buildMergeQuery(targetTable, sourceTable string, columns, pkCols []string, opCol, deleteOpVal string) (string, error) {
	sanitizedTarget, err := SanitizeTableIdentifier(targetTable)
	if err != nil {
		return "", err
	}
	sanitizedSource, err := SanitizeTableIdentifier(sourceTable)
	if err != nil {
		return "", err
	}
	return buildMergeQueryForSanitizedTables(sanitizedTarget, sanitizedSource, columns, pkCols, opCol, deleteOpVal)
}

// buildMergeQueryForSanitizedTables is buildMergeQuery for callers whose table
// identifiers are already quoted. Column names are still sanitized here.
func buildMergeQueryForSanitizedTables(sanitizedTarget, sanitizedSource string, columns, pkCols []string, opCol, deleteOpVal string) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("postgresio: MERGE mode requires at least one primary key column")
	}

	pkSet := make(map[string]bool)
	var onConditions []string
	for _, pk := range pkCols {
		san, err := SanitizeIdentifier(pk)
		if err != nil {
			return "", err
		}
		pkSet[pk] = true
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", san, san))
	}

	var updateClauses []string
	var insertCols []string
	var insertVals []string

	for _, col := range columns {
		san, err := SanitizeIdentifier(col)
		if err != nil {
			return "", err
		}
		if col != opCol {
			insertCols = append(insertCols, san)
			insertVals = append(insertVals, fmt.Sprintf("source.%s", san))
			if !pkSet[col] {
				updateClauses = append(updateClauses, fmt.Sprintf("%s = source.%s", san, san))
			}
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("MERGE INTO %s AS target\n", sanitizedTarget))
	sb.WriteString(fmt.Sprintf("USING %s AS source\n", sanitizedSource))
	sb.WriteString(fmt.Sprintf("ON %s\n", strings.Join(onConditions, " AND ")))

	hasOp := opCol != ""
	var sanOp string
	if hasOp {
		var err error
		sanOp, err = SanitizeIdentifier(opCol)
		if err != nil {
			return "", err
		}
		delValEscaped := strings.ReplaceAll(deleteOpVal, "'", "''")
		sb.WriteString(fmt.Sprintf("WHEN MATCHED AND source.%s = '%s' THEN\n  DELETE\n", sanOp, delValEscaped))
	}

	if len(updateClauses) > 0 {
		sb.WriteString(fmt.Sprintf("WHEN MATCHED THEN\n  UPDATE SET\n    %s\n", strings.Join(updateClauses, ",\n    ")))
	}

	if hasOp {
		delValEscaped := strings.ReplaceAll(deleteOpVal, "'", "''")
		sb.WriteString(fmt.Sprintf("WHEN NOT MATCHED AND (source.%s IS DISTINCT FROM '%s') THEN\n", sanOp, delValEscaped))
	} else {
		sb.WriteString("WHEN NOT MATCHED THEN\n")
	}
	sb.WriteString(fmt.Sprintf("  INSERT (%s)\n  VALUES (%s)", strings.Join(insertCols, ", "), strings.Join(insertVals, ", ")))

	return sb.String(), nil
}
