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
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---- Internal STRUCT types matching the Spanner change stream SQL response ----
//
// Spanner change stream SQL queries return ARRAY<STRUCT<...>> rows in the old
// change stream format.  These internal types decode that format via the
// spanner: struct tag.

type csRow struct {
	DataChangeRecord      []*csDataChangeRecord      `spanner:"data_change_record"`
	HeartbeatRecord       []*csHeartbeatRecord       `spanner:"heartbeat_record"`
	ChildPartitionsRecord []*csChildPartitionsRecord `spanner:"child_partitions_record"`
}

type csDataChangeRecord struct {
	CommitTimestamp     time.Time       `spanner:"commit_timestamp"`
	RecordSequence      string          `spanner:"record_sequence"`
	ServerTransactionID string          `spanner:"server_transaction_id"`
	IsLastRecord        bool            `spanner:"is_last_record_in_transaction_in_partition"`
	TableName           string          `spanner:"table_name"`
	ColumnTypes         []*csColumnType `spanner:"column_types"`
	Mods                []*csMod        `spanner:"mods"`
	ModType             string          `spanner:"mod_type"`
	ValueCaptureType    string          `spanner:"value_capture_type"`
	NumberOfRecords     int64           `spanner:"number_of_records_in_transaction"`
	NumberOfPartitions  int64           `spanner:"number_of_partitions_in_transaction"`
	TransactionTag      string          `spanner:"transaction_tag"`
	IsSystemTransaction bool            `spanner:"is_system_transaction"`
}

type csColumnType struct {
	Name            string           `spanner:"name"`
	Type            spanner.NullJSON `spanner:"type"`
	IsPrimaryKey    bool             `spanner:"is_primary_key"`
	OrdinalPosition int64            `spanner:"ordinal_position"`
}

type csMod struct {
	Keys      spanner.NullJSON `spanner:"keys"`
	NewValues spanner.NullJSON `spanner:"new_values"`
	OldValues spanner.NullJSON `spanner:"old_values"`
}

type csHeartbeatRecord struct {
	Timestamp time.Time `spanner:"timestamp"`
}

type csChildPartitionsRecord struct {
	StartTimestamp  time.Time           `spanner:"start_timestamp"`
	RecordSequence  string              `spanner:"record_sequence"`
	ChildPartitions []*csChildPartition `spanner:"child_partitions"`
}

type csChildPartition struct {
	Token                 string   `spanner:"token"`
	ParentPartitionTokens []string `spanner:"parent_partition_tokens"`
}

// ---- SDF registration ----

func init() {
	register.DoFn5x2[
		context.Context, *changeStreamWatermarkEstimator, *sdf.LockRTracker, []byte,
		func(beam.EventTime, DataChangeRecord), sdf.ProcessContinuation, error,
	](&readChangeStreamFn{})
	register.Emitter2[beam.EventTime, DataChangeRecord]()
	beam.RegisterType(reflect.TypeOf((*DataChangeRecord)(nil)).Elem())
}

// defaultCheckpointInterval is how long a single ProcessElement invocation
// queries Spanner before self-checkpointing and resuming.
const defaultCheckpointInterval = 10 * time.Second

// validStreamName matches Spanner change stream identifiers, which follow
// standard SQL identifier rules. Used to prevent SQL injection in the
// READ_<name>() query built by buildStatement.
var validStreamName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Beam metrics for observability.
var (
	recordsEmitted      = beam.NewCounter("spannerio.changestream", "records_emitted")
	partitionsCompleted = beam.NewCounter("spannerio.changestream", "partitions_completed")
	errorsTransient     = beam.NewCounter("spannerio.changestream", "errors_transient")
)

// ReadChangeStreamOption configures ReadChangeStream.
type ReadChangeStreamOption func(*readChangeStreamFn)

// WithChangeStreamTestEndpoint sets a custom Spanner endpoint, typically used
// for the Spanner emulator in tests.
func WithChangeStreamTestEndpoint(endpoint string) ReadChangeStreamOption {
	return func(fn *readChangeStreamFn) {
		fn.TestEndpoint = endpoint
	}
}

// ReadChangeStream reads DataChangeRecords from a Spanner change stream and
// returns a PCollection<DataChangeRecord>.
//
// db is the Spanner database path
// (projects/{project}/instances/{instance}/databases/{database}).
// changeStreamName is the name of the change stream (e.g. "MyStream").
// startTime is the inclusive start of the time range to read.
// endTime is the exclusive end of the time range. Use time.Time{} for an
// unbounded (continuously running) read.
// heartbeatMillis controls how often Spanner emits heartbeat records when
// there are no data changes (milliseconds).
func ReadChangeStream(
	s beam.Scope,
	db string,
	changeStreamName string,
	startTime time.Time,
	endTime time.Time,
	heartbeatMillis int64,
	opts ...ReadChangeStreamOption,
) beam.PCollection {
	s = s.Scope("spannerio.ReadChangeStream")

	if !validStreamName.MatchString(changeStreamName) {
		panic(fmt.Sprintf("spannerio.ReadChangeStream: invalid change stream name %q: must match %s", changeStreamName, validStreamName.String()))
	}

	fn := &readChangeStreamFn{
		spannerFn:        newSpannerFn(db),
		ChangeStreamName: changeStreamName,
		StartTime:        startTime,
		EndTime:          endTime,
		HeartbeatMillis:  heartbeatMillis,
	}
	for _, opt := range opts {
		opt(fn)
	}

	imp := beam.Impulse(s)
	return beam.ParDo(s, fn, imp)
}

// readChangeStreamFn is the SDF that reads a Spanner change stream.
//
// The entire partition work queue is encoded inside the restriction
// (PartitionQueueRestriction), which the Beam runner serialises on every
// checkpoint. This provides durable, in-memory coordination with no external
// state store.
type readChangeStreamFn struct {
	spannerFn
	ChangeStreamName string
	StartTime        time.Time
	EndTime          time.Time // zero = unbounded
	HeartbeatMillis  int64
}

func (fn *readChangeStreamFn) Setup(ctx context.Context) error {
	return fn.spannerFn.Setup(ctx)
}

func (fn *readChangeStreamFn) Teardown() {
	fn.spannerFn.Teardown()
}

// CreateInitialRestriction returns a restriction with a single root entry
// (empty partition token) that bootstraps the partition discovery query.
func (fn *readChangeStreamFn) CreateInitialRestriction(_ []byte) PartitionQueueRestriction {
	return PartitionQueueRestriction{
		Pending: []PartitionWork{
			{
				Token:          "", // empty token = root discovery query
				StartTimestamp: fn.StartTime,
				EndTimestamp:   fn.EndTime,
			},
		},
		Bounded: !fn.EndTime.IsZero(),
	}
}

// SplitRestriction returns the restriction unsplit. The queue grows
// dynamically as child partitions are discovered.
func (fn *readChangeStreamFn) SplitRestriction(_ []byte, r PartitionQueueRestriction) []PartitionQueueRestriction {
	return []PartitionQueueRestriction{r}
}

// RestrictionSize returns an estimate of remaining work.
func (fn *readChangeStreamFn) RestrictionSize(_ []byte, r PartitionQueueRestriction) float64 {
	_, remaining := newPartitionQueueTracker(r).GetProgress()
	return remaining
}

// CreateTracker wraps a partitionQueueTracker in a thread-safe LockRTracker.
func (fn *readChangeStreamFn) CreateTracker(r PartitionQueueRestriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(newPartitionQueueTracker(r))
}

// InitialWatermarkEstimatorState returns a sentinel state indicating no
// watermark has been observed yet and no pending partitions are known.
func (fn *readChangeStreamFn) InitialWatermarkEstimatorState(
	_ beam.EventTime,
	_ PartitionQueueRestriction,
	_ []byte,
) []byte {
	return encodeWatermarkState(math.MinInt64, math.MaxInt64)
}

// CreateWatermarkEstimator creates the watermark estimator from its serialised state.
func (fn *readChangeStreamFn) CreateWatermarkEstimator(state []byte) *changeStreamWatermarkEstimator {
	maxObserved, minPending := decodeWatermarkState(state)
	return &changeStreamWatermarkEstimator{maxObserved: maxObserved, minPending: minPending}
}

// WatermarkEstimatorState serialises the watermark estimator state for the runner.
func (fn *readChangeStreamFn) WatermarkEstimatorState(we *changeStreamWatermarkEstimator) []byte {
	return encodeWatermarkState(we.maxObserved, we.minPending)
}

// TruncateRestriction returns an empty (immediately done) restriction, used
// when the pipeline is draining.
func (fn *readChangeStreamFn) TruncateRestriction(
	_ *sdf.LockRTracker,
	_ []byte,
) PartitionQueueRestriction {
	return PartitionQueueRestriction{Bounded: true}
}

// ProcessElement reads from the active partition and emits DataChangeRecords.
//
// Each invocation queries Spanner for up to defaultCheckpointInterval, then
// returns ResumeProcessingIn(0) to self-checkpoint. The runner serialises the
// restriction (which holds the full partition queue), so work resumes exactly
// from the last claimed timestamp after a failure or checkpoint.
func (fn *readChangeStreamFn) ProcessElement(
	ctx context.Context,
	we *changeStreamWatermarkEstimator,
	rt *sdf.LockRTracker,
	_ []byte,
	emit func(beam.EventTime, DataChangeRecord),
) (sdf.ProcessContinuation, error) {
	rest := rt.GetRestriction().(PartitionQueueRestriction)
	if len(rest.Pending) == 0 {
		return sdf.StopProcessing(), nil
	}
	active := rest.Pending[0]

	// Update the watermark estimator with the minimum pending start timestamp
	// so the watermark does not advance past unprocessed partitions.
	minStart := active.StartTimestamp
	for _, p := range rest.Pending[1:] {
		if p.StartTimestamp.Before(minStart) {
			minStart = p.StartTimestamp
		}
	}
	we.SetMinPending(minStart)

	log.Debugf(ctx, "changestream: processing partition %q from %v (%d pending)",
		active.Token, active.StartTimestamp, len(rest.Pending))

	// Use a bounded context so we self-checkpoint periodically.
	// For bounded partitions, extend the timeout to ensure the query can
	// finish naturally before the context expires. Without this extension,
	// the context can expire just before iterator.Done is returned (when
	// the partition's time window is close to defaultCheckpointInterval),
	// causing a spurious checkpoint that re-reads already-processed records.
	checkpointInterval := defaultCheckpointInterval
	if active.bounded() {
		if remaining := time.Until(active.EndTimestamp); remaining+5*time.Second > checkpointInterval {
			checkpointInterval = remaining + 5*time.Second
		}
	}
	queryCtx, cancel := context.WithTimeout(ctx, checkpointInterval)
	defer cancel()

	iter := fn.client.Single().Query(queryCtx, fn.buildStatement(active))
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			// If the query context expired, it's time to checkpoint and resume.
			// Otherwise the partition's query completed naturally.
			if queryCtx.Err() != nil {
				return sdf.ResumeProcessingIn(0), nil
			}
			// Natural end (bounded time window elapsed or partition split
			// completed): mark the active partition as done.
			rt.TryClaim(PartitionTimestampClaim{PartitionDone: true})
			partitionsCompleted.Inc(ctx, 1)
			log.Infof(ctx, "changestream: partition %q completed", active.Token)
			return sdf.ResumeProcessingIn(0), nil
		}
		if err != nil {
			// If the query context expired (timeout or cancellation), checkpoint.
			if queryCtx.Err() != nil {
				return sdf.ResumeProcessingIn(0), nil
			}
			// If the parent context was cancelled (pipeline shutdown), stop.
			if ctx.Err() != nil {
				return sdf.StopProcessing(), nil
			}
			// Transient errors (UNAVAILABLE, ABORTED) are retryable: checkpoint
			// and resume from the current position rather than failing the bundle.
			if isTransientError(err) {
				errorsTransient.Inc(ctx, 1)
				log.Warnf(ctx, "changestream: transient error on partition %q, will checkpoint and retry: %v", active.Token, err)
				return sdf.ResumeProcessingIn(time.Second), nil
			}
			return sdf.StopProcessing(), fmt.Errorf("reading change stream %q partition %q: %w", fn.ChangeStreamName, active.Token, err)
		}

		var rows []*csRow
		if err := row.Column(0, &rows); err != nil {
			return sdf.StopProcessing(), fmt.Errorf("decoding ChangeRecord column: %w", err)
		}

		for _, r := range rows {
			stop, err := fn.handleCSRow(ctx, rt, we, emit, active.Token, *r)
			if err != nil {
				return sdf.StopProcessing(), err
			}
			if stop {
				return sdf.ResumeProcessingIn(0), nil
			}
		}
	}
}

// handleCSRow dispatches a single change stream row. Returns (true, nil) if
// processing should pause (e.g., TryClaim rejected).
func (fn *readChangeStreamFn) handleCSRow(
	ctx context.Context,
	rt *sdf.LockRTracker,
	we *changeStreamWatermarkEstimator,
	emit func(beam.EventTime, DataChangeRecord),
	token string,
	r csRow,
) (stop bool, err error) {
	for _, dcr := range r.DataChangeRecord {
		rec := dataChangeRecordFromCS(token, *dcr)
		if !rt.TryClaim(PartitionTimestampClaim{Timestamp: rec.CommitTimestamp}) {
			return true, nil
		}
		we.ObserveTimestamp(rec.CommitTimestamp)
		emit(mtime.FromTime(rec.CommitTimestamp), rec)
		recordsEmitted.Inc(ctx, 1)
	}

	for _, hb := range r.HeartbeatRecord {
		if !rt.TryClaim(PartitionTimestampClaim{Timestamp: hb.Timestamp}) {
			return true, nil
		}
		we.ObserveTimestamp(hb.Timestamp)
	}

	for _, cpr := range r.ChildPartitionsRecord {
		var children []PartitionWork
		for _, cp := range cpr.ChildPartitions {
			children = append(children, PartitionWork{
				Token:          cp.Token,
				StartTimestamp: cpr.StartTimestamp,
				EndTimestamp:   fn.EndTime,
			})
		}
		if !rt.TryClaim(PartitionTimestampClaim{
			Timestamp:       cpr.StartTimestamp,
			ChildPartitions: children,
		}) {
			return true, nil
		}
		log.Infof(ctx, "changestream: partition %q discovered %d child partitions at %v",
			token, len(children), cpr.StartTimestamp)
	}

	return false, nil
}

// isTransientError returns true for gRPC errors that are safe to retry.
// These match the Java implementation's default retryable codes.
func isTransientError(err error) bool {
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Unavailable, codes.Aborted:
			return true
		}
	}
	return false
}

// buildStatement constructs the Spanner SQL statement for a change stream partition.
func (fn *readChangeStreamFn) buildStatement(active PartitionWork) spanner.Statement {
	params := map[string]interface{}{
		"start_timestamp":        active.StartTimestamp,
		"heartbeat_milliseconds": fn.HeartbeatMillis,
	}

	// NULL partition_token triggers the initial partition discovery query.
	if active.Token == "" {
		params["partition_token"] = nil
	} else {
		params["partition_token"] = active.Token
	}

	// NULL end_timestamp means read indefinitely.
	if active.bounded() {
		params["end_timestamp"] = active.EndTimestamp
	} else {
		params["end_timestamp"] = nil
	}

	sql := fmt.Sprintf(
		"SELECT ChangeRecord FROM READ_%s("+
			"  start_timestamp => @start_timestamp,"+
			"  end_timestamp => @end_timestamp,"+
			"  partition_token => @partition_token,"+
			"  heartbeat_milliseconds => @heartbeat_milliseconds"+
			")",
		fn.ChangeStreamName,
	)
	return spanner.Statement{SQL: sql, Params: params}
}

// dataChangeRecordFromCS converts a csDataChangeRecord (old STRUCT format) to
// our public DataChangeRecord type.
func dataChangeRecordFromCS(token string, cs csDataChangeRecord) DataChangeRecord {
	r := DataChangeRecord{
		PartitionToken:                       token,
		CommitTimestamp:                      cs.CommitTimestamp,
		RecordSequence:                       cs.RecordSequence,
		ServerTransactionID:                  cs.ServerTransactionID,
		IsLastRecordInTransactionInPartition: cs.IsLastRecord,
		Table:                                cs.TableName,
		ModType:                              parseModType(cs.ModType),
		ValueCaptureType:                     parseValueCaptureType(cs.ValueCaptureType),
		NumberOfRecordsInTransaction:         int32(cs.NumberOfRecords),
		NumberOfPartitionsInTransaction:      int32(cs.NumberOfPartitions),
		TransactionTag:                       cs.TransactionTag,
		IsSystemTransaction:                  cs.IsSystemTransaction,
	}

	for _, ct := range cs.ColumnTypes {
		col := &ColumnMetadata{
			Name:            ct.Name,
			IsPrimaryKey:    ct.IsPrimaryKey,
			OrdinalPosition: ct.OrdinalPosition,
		}
		if ct.Type.Valid {
			col.TypeCode, col.ArrayElementTypeCode = parseTypeJSON(ct.Type.Value)
		}
		r.ColumnMetadata = append(r.ColumnMetadata, col)
	}

	for _, m := range cs.Mods {
		mod := &Mod{
			Keys:      jsonObjToModValues(m.Keys),
			NewValues: jsonObjToModValues(m.NewValues),
			OldValues: jsonObjToModValues(m.OldValues),
		}
		r.Mods = append(r.Mods, mod)
	}

	return r
}

// jsonObjToModValues converts a NullJSON object (map of column name → value)
// into a slice of ModValue.
func jsonObjToModValues(nj spanner.NullJSON) []*ModValue {
	if !nj.Valid || nj.Value == nil {
		return nil
	}
	m, ok := nj.Value.(map[string]interface{})
	if !ok {
		return nil
	}
	var out []*ModValue
	for colName, v := range m {
		b, _ := json.Marshal(v)
		out = append(out, &ModValue{ColumnName: colName, Value: string(b)})
	}
	return out
}

// parseTypeJSON extracts the TypeCode (and array element type code) from the
// JSON column type object returned by the change stream, e.g.
// {"code":"STRING"} or {"code":"ARRAY","arrayElementType":{"code":"INT64"}}.
func parseTypeJSON(v interface{}) (spannerpb.TypeCode, spannerpb.TypeCode) {
	m, ok := v.(map[string]interface{})
	if !ok {
		return spannerpb.TypeCode_TYPE_CODE_UNSPECIFIED, spannerpb.TypeCode_TYPE_CODE_UNSPECIFIED
	}
	code := parseTypeCodeString(fmt.Sprintf("%v", m["code"]))
	var elemCode spannerpb.TypeCode
	if ae, ok := m["arrayElementType"].(map[string]interface{}); ok {
		elemCode = parseTypeCodeString(fmt.Sprintf("%v", ae["code"]))
	}
	return code, elemCode
}

func parseTypeCodeString(s string) spannerpb.TypeCode {
	switch s {
	case "BOOL":
		return spannerpb.TypeCode_BOOL
	case "INT64":
		return spannerpb.TypeCode_INT64
	case "FLOAT32":
		return spannerpb.TypeCode_FLOAT32
	case "FLOAT64":
		return spannerpb.TypeCode_FLOAT64
	case "TIMESTAMP":
		return spannerpb.TypeCode_TIMESTAMP
	case "DATE":
		return spannerpb.TypeCode_DATE
	case "STRING":
		return spannerpb.TypeCode_STRING
	case "BYTES":
		return spannerpb.TypeCode_BYTES
	case "ARRAY":
		return spannerpb.TypeCode_ARRAY
	case "STRUCT":
		return spannerpb.TypeCode_STRUCT
	case "NUMERIC":
		return spannerpb.TypeCode_NUMERIC
	case "JSON":
		return spannerpb.TypeCode_JSON
	case "PROTO":
		return spannerpb.TypeCode_PROTO
	case "ENUM":
		return spannerpb.TypeCode_ENUM
	default:
		return spannerpb.TypeCode_TYPE_CODE_UNSPECIFIED
	}
}

func parseModType(s string) ModType {
	switch s {
	case "INSERT":
		return ModTypeInsert
	case "UPDATE":
		return ModTypeUpdate
	case "DELETE":
		return ModTypeDelete
	default:
		return ModTypeUnspecified
	}
}

func parseValueCaptureType(s string) ValueCaptureType {
	switch s {
	case "OLD_AND_NEW_VALUES":
		return ValueCaptureTypeOldAndNewValues
	case "NEW_VALUES":
		return ValueCaptureTypeNewValues
	case "NEW_ROW":
		return ValueCaptureTypeNewRow
	case "NEW_ROW_AND_OLD_VALUES":
		return ValueCaptureTypeNewRowAndOldValues
	default:
		return ValueCaptureTypeUnspecified
	}
}
