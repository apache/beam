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
	"database/sql/driver"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
)

// WriteMode defines the write mutation strategy against the target PostgreSQL table.
type WriteMode int

const (
	// WriteModeInsert executes standard parameterized INSERT statements.
	WriteModeInsert WriteMode = iota
	// WriteModeUpsert executes idempotent INSERT ... ON CONFLICT (pks) DO UPDATE statements.
	WriteModeUpsert
	// WriteModeUpdate executes bulk UPDATE statements matched on primary key columns.
	WriteModeUpdate
	// WriteModeMerge executes SQL-standard MERGE INTO statements (PostgreSQL 15+).
	WriteModeMerge
)

// String returns the mode's name, so diagnostics identify it by the constant
// callers wrote rather than by its ordinal.
func (m WriteMode) String() string {
	switch m {
	case WriteModeInsert:
		return "WriteModeInsert"
	case WriteModeUpsert:
		return "WriteModeUpsert"
	case WriteModeUpdate:
		return "WriteModeUpdate"
	case WriteModeMerge:
		return "WriteModeMerge"
	default:
		return fmt.Sprintf("WriteMode(%d)", int(m))
	}
}

var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]*$`)

// WriteMethod determines the bulk loading mechanism used to insert or upsert rows.
type WriteMethod int

const (
	// WriteMethodStagedCopy utilizes PostgreSQL COPY into a session temporary table
	// followed by an atomic set-based INSERT ... SELECT ... ON CONFLICT DO UPDATE.
	// Recommended for maximum throughput (>100,000 rows/sec).
	WriteMethodStagedCopy WriteMethod = iota

	// WriteMethodUnnest utilizes parameterized UNNEST($1, $2, ...) upsert queries.
	WriteMethodUnnest
)

// DialFunc defines a pluggable network dialer interface for connecting to PostgreSQL.
// Used by cloud-specific dialers (e.g., Google Cloud SQL, AlloyDB) to establish
// authenticated mTLS socket connections.
//
// A DialFunc is a Go closure and cannot be serialized, so it reaches a worker
// only on runners that execute in the submitting process. On a distributed
// runner the field arrives nil. The options structs therefore carry a
// RequiresDialFunc marker, which does serialize, so the worker can tell a
// pipeline that never configured a dialer apart from one whose dialer was
// dropped in transit and fail loudly instead of quietly opening a direct
// connection that bypasses the intended proxy.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// errDialFuncLost reports a dialer that was configured at pipeline
// construction but did not reach this worker.
//
// Continuing without it would open a direct TCP connection to Host:Port,
// silently bypassing the Cloud SQL connector, AlloyDB connector or IAM proxy
// tunnel the caller installed. That route is usually the only authorized one
// and often the only encrypted one, so the connection has to fail rather than
// quietly succeed by another path.
func errDialFuncLost() error {
	return fmt.Errorf("postgresio: a custom DialFunc was configured but did not reach this worker. " +
		"A dialer is a Go closure and cannot be serialized, so it is only available on runners that " +
		"execute in the submitting process. On a distributed runner, reach the database through a " +
		"sidecar or proxy that the worker can dial directly (for example the Cloud SQL Auth Proxy) " +
		"and leave DialFunc unset")
}

// WriteOptions configures connection pooling, write mutation modes, batch thresholds,
// and safety guards for writing to PostgreSQL.
type WriteOptions struct {
	Host           string
	Port           int
	Database       string
	Username       string
	Password       string `beam:"password,secret" json:"password,omitempty"`
	PasswordEnvVar string `json:"password_env_var,omitempty"`
	SSLMode        string
	SSLRootCert    string
	WriteMode      WriteMode
	WriteMethod    WriteMethod
	PrimaryKeyCols []string
	// UpdateFields specifies the subset of columns to update when WriteMode is
	// WriteModeUpsert (ON CONFLICT DO UPDATE) or WriteModeUpdate (UPDATE ...
	// SET). If empty, all columns other than PrimaryKeyCols are updated.
	// Primary key columns are always excluded from the SET clause, so naming
	// one here has no effect.
	//
	// Names are matched against resolved column names exactly and
	// case-sensitively; "Email" does not match the column email. This differs
	// from the primary key lookup in ExtractPrimaryKeys, which falls back to a
	// case-insensitive match. Write rejects a name that matches no column of
	// the input type when the pipeline is constructed.
	//
	// The other write modes have no SET clause to restrict, so Write rejects
	// this field when WriteMode is WriteModeInsert or WriteModeMerge rather
	// than ignoring it.
	UpdateFields          []string
	BatchSize             int
	MaxBatchBytes         int
	FlushInterval         time.Duration
	MaxConnections        int
	UsePgBouncer          bool
	ConnectionInitSQL     string
	ReplicationOriginName string
	DialFunc              DialFunc `beam:"-" json:"-"`

	// RequiresDialFunc records that a custom dialer was configured. It is set
	// automatically by Write and should not be assigned directly. Unlike
	// DialFunc it survives serialization, which is what lets a worker detect
	// that the dialer itself did not.
	RequiresDialFunc bool

	OpColumn          string
	DeleteOpValue     string
	ExplainAnalyze    bool
	ExplainSampleRate float64
}

// String returns a redacted representation of the WriteOptions, safe for logging.
func (o WriteOptions) String() string {
	pwd := "<redacted>"
	if o.Password == "" {
		pwd = "<none>"
	}
	return fmt.Sprintf("WriteOptions{Host: %s, Port: %d, Database: %s, Username: %s, Password: %s, WriteMode: %v, BatchSize: %d}",
		o.Host, o.Port, o.Database, o.Username, pwd, o.WriteMode, o.BatchSize)
}

// pqDialerAdapter adapts a postgresio DialFunc into a pq.Dialer.
type pqDialerAdapter struct {
	ctx      context.Context
	dialFunc DialFunc
}

func (a *pqDialerAdapter) Dial(network, address string) (net.Conn, error) {
	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return a.dialFunc(ctx, network, address)
}

func (a *pqDialerAdapter) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx := a.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return a.dialFunc(ctx, network, address)
}

// pqConnector implements driver.Connector using a custom pq.Dialer and optional per-connection init SQL.
type pqConnector struct {
	dialFunc DialFunc
	dialer   pq.Dialer
	dsn      string
	initSQL  string

	// replicationOrigin, when set, is selected on every connection this
	// connector opens. See selectReplicationOrigin.
	replicationOrigin string
}

func (c *pqConnector) Connect(ctx context.Context) (driver.Conn, error) {
	var conn driver.Conn
	var err error
	if c.dialFunc != nil {
		conn, err = pq.DialOpen(&pqDialerAdapter{ctx: ctx, dialFunc: c.dialFunc}, c.dsn)
	} else if c.dialer != nil {
		conn, err = pq.DialOpen(c.dialer, c.dsn)
	} else {
		d := &pq.Driver{}
		conn, err = d.Open(c.dsn)
	}
	if err != nil {
		return nil, err
	}

	if c.initSQL != "" {
		var initErr error
		if execer, ok := conn.(driver.ExecerContext); ok {
			_, initErr = execer.ExecContext(ctx, c.initSQL, nil)
		} else if execerSync, ok := conn.(driver.Execer); ok {
			_, initErr = execerSync.Exec(c.initSQL, nil)
		}
		if initErr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("postgresio: connection init SQL failed: %w", initErr)
		}
	}

	if err := selectReplicationOrigin(ctx, conn, c.replicationOrigin); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// selectReplicationOrigin marks the connection as replaying from origin, so
// that every WAL record it subsequently produces carries that origin id.
//
// This is what breaks a bidirectional replication loop: a subscription
// declared WITH (origin = none) skips changes that carry any origin, so the
// peer does not replay our writes back at us.
//
// It is done once per physical connection, not per transaction, because the
// selection is session state. PostgreSQL rejects a second selection on a
// session that already has one, and a pooled connection outlives the
// transactions that run on it.
//
// pg_replication_origin_xact_setup is deliberately not used. It takes
// (origin_lsn, origin_timestamp) and records replay *progress*; it does not
// name an origin, and it errors unless a session origin has already been
// selected. It is the wrong tool for stamping outgoing writes.
func selectReplicationOrigin(ctx context.Context, conn driver.Conn, origin string) error {
	if origin == "" {
		return nil
	}

	const stmt = "SELECT pg_catalog.pg_replication_origin_session_setup($1)"
	args := []driver.NamedValue{{Ordinal: 1, Value: origin}}

	var err error
	switch execer := conn.(type) {
	case driver.ExecerContext:
		_, err = execer.ExecContext(ctx, stmt, args)
	default:
		// Every pq connection implements ExecerContext. A driver that does
		// not cannot be told to stamp its writes, and continuing would leave
		// bidirectional loop prevention silently off.
		return fmt.Errorf("postgresio: cannot select replication origin %q: the driver connection does not "+
			"support parameterized statements", origin)
	}
	if err != nil {
		return fmt.Errorf("postgresio: failed to select replication origin %q: %w\n"+
			"The origin must already exist (SELECT pg_replication_origin_create('%s')), and selecting it "+
			"is superuser-only unless EXECUTE on pg_replication_origin_session_setup has been granted to "+
			"this role. Continuing without the origin would disable bidirectional loop prevention",
			origin, err, origin)
	}
	return nil
}

func (c *pqConnector) Driver() driver.Driver {
	return &pq.Driver{}
}

// Option represents a functional option for configuring WriteOptions.
type Option func(*WriteOptions)

// NewWriteOptions creates a WriteOptions struct initialized with production defaults.
func NewWriteOptions(opts ...Option) WriteOptions {
	wo := WriteOptions{
		Port:           5432,
		SSLMode:        DefaultSSLMode,
		WriteMode:      WriteModeUpsert,
		WriteMethod:    WriteMethodStagedCopy,
		BatchSize:      5000,
		MaxBatchBytes:  8 * 1024 * 1024, // 8 MB
		FlushInterval:  1 * time.Second,
		MaxConnections: 2,
	}
	for _, opt := range opts {
		opt(&wo)
	}
	// An option that explicitly clears the mode must not be read as "plaintext".
	if wo.SSLMode == "" {
		wo.SSLMode = DefaultSSLMode
	}
	return wo

}

// WithHost sets the target PostgreSQL server hostname or IP address.
func WithHost(host string) Option {
	return func(o *WriteOptions) {
		o.Host = host
	}
}

// WithPort sets the target PostgreSQL server TCP port.
func WithPort(port int) Option {
	return func(o *WriteOptions) {
		o.Port = port
	}
}

// WithDatabase sets the target database name.
func WithDatabase(db string) Option {
	return func(o *WriteOptions) {
		o.Database = db
	}
}

// WithUsername sets the database authentication username.
func WithUsername(user string) Option {
	return func(o *WriteOptions) {
		o.Username = user
	}
}

// WithPassword sets the database authentication password.
func WithPassword(pass string) Option {
	return func(o *WriteOptions) {
		o.Password = pass
	}
}

// WithPasswordEnvVar sets the environment variable name on the worker to resolve the password.
func WithPasswordEnvVar(envVar string) Option {
	return func(o *WriteOptions) {
		o.PasswordEnvVar = envVar
	}
}

// ResolvePassword returns the password to use, prioritizing PasswordEnvVar, then Password, then PGPASSWORD.
func (o WriteOptions) ResolvePassword() string {
	if o.PasswordEnvVar != "" {
		if val := os.Getenv(o.PasswordEnvVar); val != "" {
			return val
		}
	}
	if o.Password != "" {
		return o.Password
	}
	return os.Getenv("PGPASSWORD")
}

// WithSSLMode sets the SSL/TLS connection mode (disable, require, verify-ca, verify-full).
func WithSSLMode(sslMode string) Option {
	return func(o *WriteOptions) {
		o.SSLMode = sslMode
	}
}

// WithSSLRootCert sets the SSL root certificate path or PEM for verify-ca or verify-full.
func WithSSLRootCert(cert string) Option {
	return func(o *WriteOptions) {
		o.SSLRootCert = cert
	}
}

// WithSSLRootCerts is an alias for WithSSLRootCert.
func WithSSLRootCerts(cert string) Option {
	return WithSSLRootCert(cert)
}

// WithWriteMethod sets the bulk write method (WriteMethodStagedCopy or WriteMethodUnnest).
func WithWriteMethod(method WriteMethod) Option {
	return func(o *WriteOptions) {
		o.WriteMethod = method
	}
}

// WithWriteMode configures the write strategy (Insert, Upsert, or Update).
func WithWriteMode(mode WriteMode) Option {
	return func(o *WriteOptions) {
		o.WriteMode = mode
	}
}

// WithPrimaryKeyColumns specifies primary key column names required for upserts and deadlock sorting.
func WithPrimaryKeyColumns(cols ...string) Option {
	return func(o *WriteOptions) {
		o.PrimaryKeyCols = cols
	}
}

// WithUpdateFields specifies the subset of columns to update when WriteMode is
// WriteModeUpsert (ON CONFLICT DO UPDATE) or WriteModeUpdate (UPDATE ... SET).
// If empty or omitted, all non-primary key columns are updated. Primary key
// columns are always excluded from the SET clause.
//
// Names must match the resolved column names exactly, including case:
// WithUpdateFields("Email") does not select the column email. Write rejects a
// name that matches no column of the input type, and rejects the option
// entirely under WriteModeInsert and WriteModeMerge, which have no SET clause
// to restrict.
func WithUpdateFields(fields ...string) Option {
	return func(o *WriteOptions) {
		o.UpdateFields = append([]string(nil), fields...)
	}
}

// WithBatchSize sets the maximum number of rows buffered before issuing a batch write.
func WithBatchSize(size int) Option {
	return func(o *WriteOptions) {
		o.BatchSize = size
	}
}

// WithMaxBatchBytes sets the maximum payload byte volume buffered before flushing.
func WithMaxBatchBytes(bytes int) Option {
	return func(o *WriteOptions) {
		o.MaxBatchBytes = bytes
	}
}

// WithFlushInterval sets the maximum latency before buffering rows are flushed.
func WithFlushInterval(interval time.Duration) Option {
	return func(o *WriteOptions) {
		o.FlushInterval = interval
	}
}

// WithMaxConnections clamps the worker connection pool size.
func WithMaxConnections(maxConns int) Option {
	return func(o *WriteOptions) {
		o.MaxConnections = maxConns
	}
}

// WithPgBouncer declares that writes go through PgBouncer in transaction
// pooling mode.
//
// Transaction pooling hands each transaction whichever server connection is
// free, so nothing that lives in a session survives between them. The sink
// responds by avoiding session state:
//
//   - WriteMethodStagedCopy is downgraded to WriteMethodUnnest. Staged COPY
//     creates a temporary staging table once and reuses it across flushes; a
//     later flush can land on a connection where that table was never created.
//   - ConnectionInitSQL is warned about, because it runs when a pooled
//     connection is opened and a later statement may be routed to a server
//     connection that never ran it.
//
// WriteModeMerge is rejected outright at pipeline construction, since MERGE is
// only expressible through the staged COPY path.
//
// Leave this off when connecting to PostgreSQL directly, or through PgBouncer
// in session pooling mode; the downgrade costs throughput for no benefit.
func WithPgBouncer(usePgBouncer bool) Option {
	return func(o *WriteOptions) {
		o.UsePgBouncer = usePgBouncer
	}
}

// WithDialFunc injects a custom network dialer for Cloud SQL, AlloyDB, or proxy tunnels.
func WithDialFunc(dialFunc DialFunc) Option {
	return func(o *WriteOptions) {
		o.DialFunc = dialFunc
	}
}

var originNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]{1,64}$`)

// WithReplicationOriginName configures a replication origin identifier for the sink.
// When specified, write transactions are tagged with this origin to prevent bidirectional
// replication loops when replicating between active-active PostgreSQL databases.
func WithReplicationOriginName(originName string) Option {
	return func(o *WriteOptions) {
		if originName != "" && !originNameRegex.MatchString(originName) {
			panic(fmt.Sprintf("postgresio: invalid replication origin name %q (must match ^[a-zA-Z0-9_]{1,64}$)", originName))
		}
		o.ReplicationOriginName = originName
	}
}

// WithOpColumn specifies the column indicating the mutation operation type for WriteModeMerge.
func WithOpColumn(col string) Option {
	return func(o *WriteOptions) {
		o.OpColumn = col
	}
}

// WithDeleteOpValue specifies the value in OpColumn that represents a DELETE mutation for WriteModeMerge.
func WithDeleteOpValue(val string) Option {
	return func(o *WriteOptions) {
		o.DeleteOpValue = val
	}
}

// WithExplainAnalyze enables EXPLAIN (ANALYZE, BUFFERS) query plan sampling on sink writes.
func WithExplainAnalyze(enabled bool) Option {
	return func(o *WriteOptions) {
		o.ExplainAnalyze = enabled
	}
}

// WithExplainSampleRate sets the sampling rate for EXPLAIN (ANALYZE, BUFFERS) [0.0 to 1.0].
// If unset or <= 0, defaults to 0.001 (0.1% or 1 in 1000 batches). A rate > 1.0 is
// clamped to 1.0. Note that sampling is only evaluated when WithExplainAnalyze(true) is
// enabled; by default, EXPLAIN (ANALYZE) is disabled.
func WithExplainSampleRate(rate float64) Option {
	return func(o *WriteOptions) {
		o.ExplainSampleRate = rate
	}
}

// quoteDSNValue renders a value for a libpq keyword/value connection string.
//
// libpq requires a value to be single-quoted when it is empty or contains
// whitespace, and requires a backslash before any single quote or backslash
// within it. Quoting unconditionally is simpler and equally valid, because an
// unquoted value and its quoted form parse identically.
//
// Leaving a value unquoted has two consequences. The value is truncated at its
// first space, so a password containing a space authenticates with the wrong
// credential and the operator sees an authentication failure rather than a
// configuration error. The discarded remainder is then parsed as further
// connection keywords, which lets a value introduce settings the caller never
// asked for, such as sslrootcert. Redirecting the trust anchor defeats
// certificate verification even under sslmode=verify-full.
func quoteDSNValue(v string) string {
	var b strings.Builder
	b.Grow(len(v) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(v); i++ {
		if v[i] == '\\' || v[i] == '\'' {
			b.WriteByte('\\')
		}
		b.WriteByte(v[i])
	}
	b.WriteByte('\'')
	return b.String()
}

// SanitizeIdentifier validates that an identifier conforms to PostgreSQL naming rules,
// rejects null bytes (\0) and quotation marks to prevent SQL injection, and wraps the
// identifier in double quotes.
func SanitizeIdentifier(ident string) (string, error) {
	if strings.TrimSpace(ident) == "" {
		return "", fmt.Errorf("postgresio: identifier cannot be empty")
	}
	if strings.Contains(ident, "\x00") {
		return "", fmt.Errorf("postgresio: identifier contains illegal null byte: %q", ident)
	}
	if strings.Contains(ident, "\"") {
		return "", fmt.Errorf("postgresio: identifier contains illegal quotation mark: %q", ident)
	}
	if !identifierRegex.MatchString(ident) {
		return "", fmt.Errorf("postgresio: identifier %q contains invalid characters (must match ^[a-zA-Z_][a-zA-Z0-9_$]*$)", ident)
	}
	return `"` + ident + `"`, nil
}

// SanitizeTableIdentifier validates and escapes a qualified table identifier (e.g. "public.orders"
// or "orders"), ensuring all schema and table components are safely enclosed in double quotes.
func SanitizeTableIdentifier(table string) (string, error) {
	if strings.TrimSpace(table) == "" {
		return "", fmt.Errorf("postgresio: table name cannot be empty")
	}
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("postgresio: invalid table identifier with more than 2 parts: %q", table)
	}
	sanitizedParts := make([]string, len(parts))
	for i, part := range parts {
		sanitized, err := SanitizeIdentifier(part)
		if err != nil {
			return "", err
		}
		sanitizedParts[i] = sanitized
	}
	return strings.Join(sanitizedParts, "."), nil
}
