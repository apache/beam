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
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// WriteOptions configures connection pooling, write mutation modes, batch thresholds,
// and safety guards for writing to PostgreSQL.
type WriteOptions struct {
	Host                  string
	Port                  int
	Database              string
	Username              string
	Password              string `beam:"password,secret" json:"password,omitempty"`
	PasswordEnvVar        string `json:"password_env_var,omitempty"`
	SSLMode               string
	WriteMode             WriteMode
	WriteMethod           WriteMethod
	PrimaryKeyCols        []string
	BatchSize             int
	MaxBatchBytes         int
	FlushInterval         time.Duration
	MaxConnections        int
	UsePgBouncer          bool
	ConnectionInitSQL     string
	ReplicationOriginName string
	DialFunc              DialFunc `beam:"-" json:"-"`
	OpColumn              string
	DeleteOpValue         string
	ExplainAnalyze        bool
	ExplainSampleRate     float64
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
	dialFunc DialFunc
}

func (a *pqDialerAdapter) Dial(network, address string) (net.Conn, error) {
	return a.dialFunc(context.Background(), network, address)
}

func (a *pqDialerAdapter) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return a.dialFunc(ctx, network, address)
}

// pqConnector implements driver.Connector using a custom pq.Dialer.
type pqConnector struct {
	dialer pq.Dialer
	dsn    string
}

func (c *pqConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return pq.DialOpen(c.dialer, c.dsn)
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

// WithPgBouncer enables compatibility flags for PgBouncer in transaction pooling mode.
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
// If unset or <= 0, defaults to 0.001 (0.1% or 1 in 1000 batches).
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
