#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Streaming source for BigQuery change history (APPENDS/CHANGES functions).

This module provides ``ReadBigQueryChangeHistory``, a streaming PTransform
that continuously polls BigQuery APPENDS() or CHANGES() functions and emits
changed rows as an unbounded PCollection.

**Status: Experimental**: API may change without notice.

Usage::

    import apache_beam as beam
    from apache_beam.io.gcp.bigquery_change_history import ReadBigQueryChangeHistory

    with beam.Pipeline(options=pipeline_options) as p:
        changes = (
            p
            | ReadBigQueryChangeHistory(
                table='my-project:my_dataset.my_table',
                change_function='APPENDS',
                poll_interval_sec=60))

Architecture:
  Poll:    Polling SDF emits lightweight _QueryRange instructions.
  Query:   _ExecuteQueryFn runs the BQ query, writes to a temp table.
  Read:    SDF reads temp table via Storage Read API with dynamic splitting.
  Cleanup: Stateful DoFn tracks stream completion, deletes temp tables.
"""

import dataclasses
import datetime
import logging
import sys
import time
import uuid
from typing import Any
from typing import Iterable
from typing import Optional

import apache_beam as beam
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.iobase import WatermarkEstimator
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.metrics import Metrics
from apache_beam.transforms.core import WatermarkEstimatorProvider
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import retry
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

try:
  from google.cloud import bigquery_storage_v1 as bq_storage
except ImportError:
  bq_storage = None  # type: ignore

try:
  import pyarrow
except ImportError:
  pyarrow = None  # type: ignore

_LOGGER = logging.getLogger(__name__)

__all__ = ['ReadBigQueryChangeHistory']

# Max time range for CHANGES() queries: 1 day.
_MAX_CHANGES_RANGE = Duration(seconds=86400)

# Side output tag for cleanup signals between the Read SDF and Cleanup DoFn.
_CLEANUP_TAG = 'cleanup'

# Default number of Storage Read API streams to request.
# Matches ReadFromBigQuery's MIN_SPLIT_COUNT to enable parallelism.
# The server may return fewer streams if the table is small.
_DEFAULT_MAX_STREAMS = 10

# Default table expiration for auto-created temp datasets: 24 hours in ms.
# Tables created in the dataset auto-expire after this duration if not
# explicitly deleted, acting as a safety net for orphaned temp tables
# (e.g. pipeline crash before cleanup runs).
_DEFAULT_TABLE_EXPIRATION_MS = 24 * 60 * 60 * 1000


@dataclasses.dataclass
class _QueryResult:
  """Bridges the Query step (query execution) to the Read SDF.

  After _ExecuteQueryFn runs a CHANGES/APPENDS query, it emits a _QueryResult
  pointing to the temp table containing query results. The Read SDF reads
  rows from that temp table via the Storage Read API.

  range_start/range_end define the time window this query covers as Beam
  Timestamps (int microseconds internally). The Read SDF uses range_start
  to set an initial watermark hold so the runner doesn't advance the
  watermark past the data's timestamps.
  """
  temp_table_ref: 'bigquery.TableReference'
  range_start: Timestamp
  range_end: Timestamp


@dataclasses.dataclass
class _PollConfig:
  """Input element for the polling SDF.

  Only contains start_time (Beam Timestamp), which
  _PollWatermarkEstimatorProvider uses to initialize the watermark hold.
  All other config is passed via _PollChangeHistoryFn.__init__.
  """
  start_time: Timestamp


@dataclasses.dataclass
class _QueryRange:
  """Lightweight instruction emitted by the polling SDF.

  Contains only the time range to query as Beam Timestamps (int microseconds
  internally). Static config (table, project, etc.) is held by
  _ExecuteQueryFn which receives these after a Reshuffle commit boundary,
  preventing duplicate queries on SDF re-dispatch.
  """
  chunk_start: Timestamp
  chunk_end: Timestamp


class _StreamRestriction:
  """Restriction carrying BQ Storage stream names for cross-worker safety.

  Unlike a plain OffsetRange(0, N), this restriction is self-contained:
  each split carries the actual stream name strings so it can be processed
  on any worker. Composes an OffsetRange for offset logic.
  """
  __slots__ = ('stream_names', 'range')

  def __init__(
      self, stream_names: tuple[str, ...], start: int, stop: int) -> None:
    self.stream_names = stream_names  # tuple of BQ stream name strings
    self.range = OffsetRange(start, stop)

  @property
  def start(self) -> int:
    return self.range.start

  @property
  def stop(self) -> int:
    return self.range.stop

  def __eq__(self, other: object) -> bool:
    if not isinstance(other, _StreamRestriction):
      return False
    return (
        self.stream_names == other.stream_names and self.range == other.range)

  def __hash__(self) -> int:
    return hash((type(self), self.stream_names, self.range))

  def __repr__(self) -> str:
    return (
        '_StreamRestriction(streams=%d, start=%d, stop=%d)' %
        (len(self.stream_names), self.start, self.stop))

  def size(self) -> int:
    return self.range.size()


class _StreamRestrictionTracker(beam.io.iobase.RestrictionTracker):
  """Tracker for _StreamRestriction, delegating offset logic to
  OffsetRestrictionTracker."""
  def __init__(self, restriction: _StreamRestriction) -> None:
    self._stream_names = restriction.stream_names
    self._offset_tracker = OffsetRestrictionTracker(restriction.range)

  def current_restriction(self) -> _StreamRestriction:
    r = self._offset_tracker.current_restriction()
    return _StreamRestriction(self._stream_names, r.start, r.stop)

  def try_claim(self, position: int) -> bool:
    return self._offset_tracker.try_claim(position)

  def try_split(
      self, fraction_of_remainder: float
  ) -> Optional[tuple[_StreamRestriction, _StreamRestriction]]:
    result = self._offset_tracker.try_split(fraction_of_remainder)
    if result is not None:
      primary, residual = result
      return (
          _StreamRestriction(self._stream_names, primary.start, primary.stop),
          _StreamRestriction(self._stream_names, residual.start, residual.stop))
    return None

  def check_done(self) -> None:
    self._offset_tracker.check_done()

  def current_progress(self):
    return self._offset_tracker.current_progress()

  def is_bounded(self) -> bool:
    return True


class _NonSplittableOffsetTracker(OffsetRestrictionTracker):
  """OffsetRestrictionTracker that allows checkpointing but prevents splitting.

  Checkpointing (fraction=0) is required for defer_remainder(). All other
  split fractions are refused, ensuring the polling SDF runs as a singleton.
  """
  def try_split(
      self, fraction_of_remainder: float
  ) -> Optional[tuple[OffsetRange, OffsetRange]]:
    if fraction_of_remainder == 0:
      return super().try_split(fraction_of_remainder)
    return None


class _PollWatermarkEstimator(WatermarkEstimator):
  """Watermark estimator that tracks both a watermark hold and poll cursor.

  The watermark hold (reported via current_watermark) is set to start_ts:
  the earliest data timestamp emitted by the current poll. This prevents
  downstream stages from seeing data as late.

  The poll cursor (last_end) tracks where the next poll should start.
  This is separate from the watermark so we can hold the watermark back
  at start_ts while still advancing the poll cursor to end_ts.

  All timestamps are Beam Timestamps (int microseconds internally).

  State is checkpointed as (watermark_hold, last_end) so
  both values survive SDF re-dispatch.
  """
  def __init__(self, state: tuple[Timestamp, Timestamp]) -> None:
    self._watermark_hold, self._last_end = state

  def observe_timestamp(self, timestamp: Timestamp) -> None:
    pass

  def current_watermark(self) -> Timestamp:
    return self._watermark_hold

  def get_estimator_state(self) -> tuple[Timestamp, Timestamp]:
    return (self._watermark_hold, self._last_end)

  def set_watermark(self, timestamp: Timestamp) -> None:
    if not isinstance(timestamp, Timestamp):
      raise ValueError('set_watermark expects a Timestamp as input')
    if self._watermark_hold and self._watermark_hold > timestamp:
      raise ValueError(
          'Watermark must be monotonically increasing. '
          'Provided %s < current %s' % (timestamp, self._watermark_hold))
    self._watermark_hold = timestamp

  def advance_poll_cursor(self, end: Timestamp) -> None:
    """Record end so the next poll starts from here.

    Only advances forward: if end is earlier than the current cursor
    (e.g. BQ clock regression), the cursor stays put so the next poll
    doesn't re-query an already-covered range.
    """
    self._last_end = max(self._last_end, end)

  def poll_cursor(self) -> Timestamp:
    """Return the start Timestamp for the next poll."""
    return self._last_end


class _PollWatermarkEstimatorProvider(WatermarkEstimatorProvider):
  """Provider for _PollWatermarkEstimator.

  Initializes with watermark hold at start_time and poll cursor at
  start_time (first poll will query from start_time).
  """
  def initial_estimator_state(
      self, element: _PollConfig,
      restriction: OffsetRange) -> tuple[Timestamp, Timestamp]:
    return (element.start_time, element.start_time)

  def create_watermark_estimator(
      self, estimator_state: tuple[Timestamp,
                                   Timestamp]) -> _PollWatermarkEstimator:
    return _PollWatermarkEstimator(estimator_state)


def build_changes_query(
    table: str,
    start: Timestamp,
    end: Timestamp,
    change_function: str,
    change_type_column: str = 'change_type',
    change_timestamp_column: str = 'change_timestamp',
    columns: Optional[list[str]] = None,
    row_filter: Optional[str] = None) -> str:
  """Build a CHANGES() or APPENDS() SQL query.

  Args:
    table: Table name as 'project.dataset.table' or 'project:dataset.table'.
    start: Start timestamp (Beam Timestamp). Inclusive.
    end: End timestamp (Beam Timestamp). Exclusive.
    change_function: 'CHANGES' or 'APPENDS'.
    change_type_column: Output column name for _CHANGE_TYPE pseudo-column.
    change_timestamp_column: Output column name for _CHANGE_TIMESTAMP
        pseudo-column.
    columns: Optional list of column names to select. If None, selects all
        columns. Pseudo-columns are always appended regardless.
    row_filter: Optional SQL WHERE clause (without the WHERE keyword).
        Applied after the CHANGES/APPENDS function.

  Returns:
    SQL string.
  """
  # Normalize 'project:dataset.table' to 'project.dataset.table'
  table = table.replace(':', '.')
  start_iso = start.to_rfc3339()
  end_iso = end.to_rfc3339()
  # Pseudo-columns (_CHANGE_TYPE, _CHANGE_TIMESTAMP) can't be written to
  # destination tables with their original names. Rename them so they can
  # be persisted to the temp table for Storage Read API reading.
  pseudo = (
      f"_CHANGE_TYPE AS {change_type_column}, "
      f"_CHANGE_TIMESTAMP AS {change_timestamp_column}")
  if columns is None:
    select = f"SELECT * EXCEPT(_CHANGE_TYPE, _CHANGE_TIMESTAMP), {pseudo}"
  else:
    select = f"SELECT {', '.join(columns)}, {pseudo}"
  from_clause = (
      f"FROM {change_function}"
      f"(TABLE `{table}`, "
      f"TIMESTAMP '{start_iso}', "
      f"TIMESTAMP '{end_iso}')")
  where = f" WHERE {row_filter}" if row_filter else ""
  return f"{select} {from_clause}{where}"


def compute_ranges(start: Timestamp, end: Timestamp,
                   change_function: str) -> list[tuple[Timestamp, Timestamp]]:
  """Split [start, end) into query-safe chunks.

  CHANGES() has a max 1-day range. APPENDS() has no limit.

  Args:
    start: Start Timestamp. Inclusive.
    end: End Timestamp. Exclusive.
    change_function: 'CHANGES' or 'APPENDS'.

  Returns:
    List of (start, end) Timestamp tuples. Empty if end <= start.
  """
  if end <= start:
    return []

  if change_function != 'CHANGES':
    return [(start, end)]

  # CHANGES: chunk into <=1-day ranges
  ranges = []
  current = start
  while current < end:
    chunk_end = min(current + _MAX_CHANGES_RANGE, end)
    ranges.append((current, chunk_end))
    current = chunk_end
  return ranges


def _utc(ts: Timestamp) -> str:
  """Format a Beam Timestamp as a concise UTC string for logging."""
  return ts.to_utc_datetime(has_tz=True).strftime('%Y-%m-%dT%H:%M:%S.%f')


# =============================================================================
# Poll: _PollChangeHistoryFn (Polling SDF)
# =============================================================================


class _PollChangeHistoryFn(beam.DoFn, beam.transforms.core.RestrictionProvider):
  """SDF that periodically emits _QueryRange instructions.

  Uses defer_remainder() for poll timing and _PollWatermarkEstimator to
  control the watermark. The watermark is initially held at start_time, then
  advanced to start_ts of each poll.

  All timestamps are Beam Timestamps (int microseconds internally).
  Durations (buffer, poll_interval) are Beam Durations.

  Derives start_ts from the poll cursor. On each poll:
  1. start_ts = poll cursor (last end_ts, or start_time on first poll)
  2. end_ts = bq_now - buffer
  3. Computes query chunks, yields _QueryRange per chunk
  4. Advances poll cursor to end_ts (for next poll's start)
  5. Advances watermark to start_ts (earliest data in this poll)
  6. Defers to next poll interval
  """
  def __init__(
      self,
      table: str,
      project: str,
      change_function: str,
      buffer: Duration,
      start_time: Timestamp,
      stop_time: Timestamp,
      poll_interval: Duration,
      location: Optional[str] = None) -> None:
    self._table = table
    self._project = project
    self._change_function = change_function
    self._buffer = buffer
    self._start_time = start_time
    self._stop_time = stop_time
    self._poll_interval = poll_interval
    self._location = location

  def setup(self) -> None:
    self._bq_wrapper = bigquery_tools.BigQueryWrapper()
    if self._location is None:
      table_ref = bigquery_tools.parse_table_reference(
          self._table, project=self._project)
      self._location = self._bq_wrapper.get_table_location(
          table_ref.projectId, table_ref.datasetId, table_ref.tableId)
      _LOGGER.info(
          '[Poll] Inferred location=%s from source table %s',
          self._location,
          self._table)

  @retry.with_exponential_backoff(
      num_retries=3,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _get_bq_timestamp(self) -> Timestamp:
    """Query BigQuery for the current server timestamp.

    Returns a Beam Timestamp created from integer microseconds.
    Uses BQ's CURRENT_TIMESTAMP instead of the local clock to avoid
    data loss from clock skew between the worker VM and BigQuery.
    """
    request = bigquery.BigqueryJobsQueryRequest(
        projectId=self._project,
        queryRequest=bigquery.QueryRequest(
            query='SELECT UNIX_MICROS(CURRENT_TIMESTAMP()) AS ts',
            useLegacySql=False,
            location=self._location))
    response = self._bq_wrapper.client.jobs.Query(request)
    return Timestamp(micros=int(response.rows[0].f[0].v.string_value))

  def initial_restriction(self, element: _PollConfig) -> OffsetRange:
    return OffsetRange(0, sys.maxsize)

  def create_tracker(
      self, restriction: OffsetRange) -> _NonSplittableOffsetTracker:
    # Guarantee at least one poll cycle: restriction.start == 0 on the first
    # invocation (from initial_restriction).  After the first try_claim(0) +
    # defer_remainder, subsequent invocations arrive with start >= 1.
    if restriction.start > 0 and time.time() >= float(self._stop_time):
      _LOGGER.info(
          '[Poll] create_tracker: stop_time reached, '
          'returning empty range to terminate SDF')
      return _NonSplittableOffsetTracker(
          OffsetRange(restriction.start, restriction.start))
    return _NonSplittableOffsetTracker(restriction)

  def restriction_size(
      self, element: _PollConfig, restriction: OffsetRange) -> int:
    return 1

  def split(self, element: _PollConfig,
            restriction: OffsetRange) -> Iterable[OffsetRange]:
    yield restriction

  def truncate(self, element: _PollConfig, restriction: OffsetRange) -> None:
    return None

  def _next_poll_time(self, start_ts: Timestamp,
                      now: float) -> Optional[Timestamp]:
    """Return a Timestamp to defer to, or None if we should poll now."""
    earliest = start_ts + self._buffer + self._poll_interval
    if now < float(earliest):
      return earliest
    return None

  def _emit_query_ranges(
      self,
      start_ts: Timestamp,
      end_ts: Timestamp,
      watermark_estimator: _PollWatermarkEstimator) -> Iterable[_QueryRange]:
    """Compute and yield _QueryRange elements, advancing estimator state."""
    ranges = compute_ranges(start_ts, end_ts, self._change_function)
    _LOGGER.info(
        '[Poll] %d chunks for [%s, %s)',
        len(ranges),
        _utc(start_ts),
        _utc(end_ts))
    Metrics.counter('BigQueryChangeHistory', 'polls').inc()

    watermark_estimator.advance_poll_cursor(end_ts)
    watermark_estimator.set_watermark(start_ts)
    _LOGGER.info(
        '[Poll] Watermark=%s (start_ts), cursor=%s (end_ts)',
        _utc(start_ts),
        _utc(end_ts))

    for chunk_start, chunk_end in ranges:
      yield TimestampedValue(
          _QueryRange(chunk_start=chunk_start, chunk_end=chunk_end), start_ts)

  @beam.DoFn.unbounded_per_element()
  def process(
      self,
      _: _PollConfig,
      restriction_tracker=beam.DoFn.RestrictionParam(),
      watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
          _PollWatermarkEstimatorProvider())
  ) -> Iterable[_QueryRange]:

    now = time.time()
    start_ts = watermark_estimator.poll_cursor()

    defer_to = self._next_poll_time(start_ts, now)
    if defer_to is not None:
      restriction_tracker.defer_remainder(defer_to)
      return

    # Use BQ server time instead of local clock to avoid data loss
    # from clock skew between the worker VM and BigQuery.
    bq_now = self._get_bq_timestamp()
    end_ts = min(bq_now - self._buffer, self._stop_time)

    _LOGGER.info(
        '[Poll] Polling: start=%s, end=%s, watermark=%s, '
        'clock_skew=%.3fs',
        _utc(start_ts),
        _utc(end_ts),
        _utc(watermark_estimator.current_watermark()),
        float(bq_now) - now)

    current_index = restriction_tracker.current_restriction().start

    if not restriction_tracker.try_claim(current_index):
      return
    restriction_tracker.defer_remainder(Timestamp.of(now) + self._poll_interval)

    yield from self._emit_query_ranges(start_ts, end_ts, watermark_estimator)


class _ExecuteQueryFn(beam.DoFn):
  """Executes a BQ CHANGES/APPENDS query from a _QueryRange instruction.
  """
  def __init__(
      self,
      table: str,
      project: str,
      change_function: str,
      temp_dataset: str,
      location: Optional[str],
      change_type_column: str = 'change_type',
      change_timestamp_column: str = 'change_timestamp',
      columns: Optional[list[str]] = None,
      row_filter: Optional[str] = None) -> None:
    self._table = table
    self._project = project
    self._change_function = change_function
    self._temp_dataset = temp_dataset
    self._location = location
    self._change_type_column = change_type_column
    self._change_timestamp_column = change_timestamp_column
    self._columns = columns
    self._row_filter = row_filter

  def setup(self) -> None:
    self._bq_wrapper = bigquery_tools.BigQueryWrapper()
    if self._location is None:
      table_ref = bigquery_tools.parse_table_reference(
          self._table, project=self._project)
      self._location = self._bq_wrapper.get_table_location(
          table_ref.projectId, table_ref.datasetId, table_ref.tableId)
      _LOGGER.info(
          '[Query] Inferred location=%s from source table %s',
          self._location,
          self._table)
    self._bq_wrapper.get_or_create_dataset(
        self._project,
        self._temp_dataset,
        location=self._location,
        default_table_expiration_ms=_DEFAULT_TABLE_EXPIRATION_MS)

  def process(self, qr: _QueryRange) -> Iterable[_QueryResult]:
    """Execute the BQ query described by a _QueryRange and yield _QueryResult.
    """

    sql = build_changes_query(
        self._table,
        qr.chunk_start,
        qr.chunk_end,
        self._change_function,
        self._change_type_column,
        self._change_timestamp_column,
        self._columns,
        self._row_filter)
    temp_table_id = f'beam_ch_temp_{uuid.uuid4().hex[:8]}'
    job_id = f'beam_ch_{uuid.uuid4().hex[:12]}'

    _LOGGER.info(
        '[Query] job_id=%s, temp_table=%s.%s, range=[%s, %s)',
        job_id,
        self._temp_dataset,
        temp_table_id,
        _utc(qr.chunk_start),
        _utc(qr.chunk_end))

    temp_table_ref = bigquery.TableReference(
        projectId=self._project,
        datasetId=self._temp_dataset,
        tableId=temp_table_id)

    reference = bigquery.JobReference(
        jobId=job_id, projectId=self._project, location=self._location)

    request = bigquery.BigqueryJobsInsertRequest(
        projectId=self._project,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                query=bigquery.JobConfigurationQuery(
                    query=sql,
                    useLegacySql=False,
                    destinationTable=temp_table_ref,
                    writeDisposition='WRITE_TRUNCATE',
                ),
            ),
            jobReference=reference))

    _LOGGER.info('[Query] Submitting BQ job %s...', job_id)
    response = self._bq_wrapper._start_job(request)
    _LOGGER.info('[Query] BQ job %s submitted, waiting...', job_id)
    self._bq_wrapper.wait_for_bq_job(
        response.jobReference, sleep_duration_sec=2)
    _LOGGER.info(
        '[Query] BQ job %s DONE. Results in %s.%s',
        job_id,
        self._temp_dataset,
        temp_table_id)
    Metrics.counter('BigQueryChangeHistory', 'queries').inc()

    yield _QueryResult(
        temp_table_ref=temp_table_ref,
        range_start=qr.chunk_start,
        range_end=qr.chunk_end)


class _CDCWatermarkEstimatorProvider(WatermarkEstimatorProvider):
  """WatermarkEstimatorProvider that initializes the hold from _QueryResult.

  Uses range_start from the element to set the initial watermark hold.
  This prevents the runner from advancing the watermark past the data's
  timestamps before any rows are emitted.
  """
  def initial_estimator_state(
      self, element: _QueryResult,
      restriction: _StreamRestriction) -> Timestamp:
    return element.range_start

  def create_watermark_estimator(
      self, estimator_state: Timestamp) -> ManualWatermarkEstimator:
    return ManualWatermarkEstimator(estimator_state)


# =============================================================================
# Read: _ReadStorageStreamsSDF
# =============================================================================


class _ReadStorageStreamsSDF(beam.DoFn,
                             beam.transforms.core.RestrictionProvider):
  """SDF that reads a temp table via BigQuery Storage Read API.

  Note on SDF lifecycle: the runner decomposes this SDF into three internal
  wrapper DoFns, each a separately deserialized copy:
    - Stage A (PairWithRestriction): calls initial_restriction(): no setup()
    - Stage B (SplitAndSizeRestrictions): calls split(), restriction_size()
    - Stage C (ProcessSizedElements): calls setup(), then process()
  Because initial_restriction() runs on a different copy than process(),
  _ensure_client() lazily creates a gRPC client on whichever copy needs one.
  The _StreamRestriction carries stream names directly so no shared state
  is needed between copies.

  Each element is a _QueryResult pointing to a temp table.

  Watermark: Uses ManualWatermarkEstimator so the watermark only advances
  as fast as the change-timestamp values we emit.

  Emits:
    Main output: TimestampedValue(row_dict, event_timestamp)
    Side output (_CLEANUP_TAG): (table_key, (streams_read, total_streams))
  """
  def __init__(
      self,
      batch_arrow_read: bool = True,
      change_timestamp_column: str = 'change_timestamp',
      max_split_rounds: int = 1,
      emit_raw_batches: bool = False) -> None:
    self._batch_arrow_read = batch_arrow_read
    self._change_timestamp_column = change_timestamp_column
    self._max_split_rounds = max_split_rounds
    self._emit_raw_batches = emit_raw_batches
    self._storage_client = None

  def _ensure_client(self) -> None:
    """Lazily initialize the Storage client.

    Called from both setup() and initial_restriction() because the runner
    may invoke initial_restriction on the RestrictionProvider instance
    before setup() runs (or on a separately deserialized copy).
    """
    if self._storage_client is None:
      _LOGGER.info('[Read] creating BigQueryReadClient')
      self._storage_client = bq_storage.BigQueryReadClient()

  def setup(self) -> None:
    self._ensure_client()

  def _split_all_streams(
      self, stream_names: tuple[str, ...],
      max_split_rounds: int) -> tuple[str, ...]:
    """Split each stream at fraction=0.5 for up to max_split_rounds rounds.

    Each round attempts to split every stream in the current list. A
    successful split replaces the original stream with primary + remainder.
    A refused split (both fields empty) keeps the original stream intact.
    Stops when max_split_rounds is reached or a full round produces zero
    new splits.

    BQ's server-side granularity controls how many splits are possible.
    Small tables may not split at all; large tables may allow multiple
    rounds of doubling.
    """
    result = list(stream_names)
    no_split = set()
    for round_num in range(1, max_split_rounds + 1):
      new_result = []
      made_progress = False
      for name in result:
        if name in no_split:
          new_result.append(name)
          continue
        response = self._storage_client.split_read_stream(
            request=bq_storage.types.SplitReadStreamRequest(
                name=name, fraction=0.5))
        primary = response.primary_stream.name
        remainder = response.remainder_stream.name
        if primary and remainder:
          new_result.extend([primary, remainder])
          made_progress = True
        else:
          new_result.append(name)
          no_split.add(name)
      result = new_result
      _LOGGER.info(
          '[Read] _split_all_streams round %d/%d: %d streams '
          '(progress=%s)',
          round_num,
          max_split_rounds,
          len(result),
          made_progress)
      if not made_progress:
        break
    return tuple(result)

  def initial_restriction(self, element: _QueryResult) -> _StreamRestriction:
    """Create ReadSession and return _StreamRestriction with stream names.

    When max_split_rounds > 0, uses SplitReadStream to subdivide each
    stream at fraction=0.5 for up to max_split_rounds rounds, maximizing
    parallelism beyond what CreateReadSession provides.
    """
    self._ensure_client()
    table_key = bigquery_tools.get_hashable_destination(element.temp_table_ref)
    session = self._create_read_session(element.temp_table_ref)
    stream_names = tuple(s.name for s in session.streams)
    original_count = len(stream_names)
    _LOGGER.info(
        '[Read] initial_restriction for %s: %d streams from CreateReadSession',
        table_key,
        original_count)

    if self._max_split_rounds > 0:
      stream_names = self._split_all_streams(
          stream_names, self._max_split_rounds)
      _LOGGER.info(
          '[Read] initial_restriction for %s: %d -> %d streams '
          'after SplitReadStream',
          table_key,
          original_count,
          len(stream_names))

    return _StreamRestriction(stream_names, 0, len(stream_names))

  def create_tracker(
      self, restriction: _StreamRestriction) -> _StreamRestrictionTracker:
    return _StreamRestrictionTracker(restriction)

  def restriction_size(
      self, element: _QueryResult, restriction: _StreamRestriction) -> int:
    return restriction.size()

  def split(self, element: _QueryResult,
            restriction: _StreamRestriction) -> Iterable[_StreamRestriction]:
    """Yield one _StreamRestriction per stream for parallel distribution."""
    if restriction.size() <= 1:
      yield restriction
    else:
      for i in range(restriction.start, restriction.stop):
        yield _StreamRestriction(restriction.stream_names, i, i + 1)

  def is_bounded(self) -> bool:
    return True

  def process(
      self,
      element: _QueryResult,
      restriction_tracker=beam.DoFn.RestrictionParam(),
      watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
          _CDCWatermarkEstimatorProvider())):
    self._ensure_client()
    table_key = bigquery_tools.get_hashable_destination(element.temp_table_ref)

    _LOGGER.info(
        '[Read] Processing %s, range=[%s, %s), '
        'initial watermark=%s',
        table_key,
        _utc(element.range_start),
        _utc(element.range_end),
        _utc(watermark_estimator.current_watermark()))

    restriction = restriction_tracker.current_restriction()
    stream_names = restriction.stream_names
    total_streams = len(stream_names)

    streams_read = 0

    _LOGGER.info(
        '[Read] Reading streams [%d, %d) of %d total for %s',
        restriction.start,
        restriction.stop,
        total_streams,
        table_key)

    for i in range(restriction.start, restriction.stop):
      if not restriction_tracker.try_claim(i):
        _LOGGER.info(
            '[Read] try_claim(%d) FAILED for %s: '
            'runner split or checkpoint, breaking',
            i,
            table_key)
        break

      stream_name = stream_names[i]
      _LOGGER.info(
          '[Read] try_claim(%d) succeeded: reading stream %s', i, stream_name)

      stream_rows = 0
      if self._emit_raw_batches:
        stream_batches = 0
        for raw_batch in self._read_stream_raw(stream_name):
          yield TimestampedValue(raw_batch, element.range_start)
          stream_batches += 1
        Metrics.counter('BigQueryChangeHistory',
                        'batches_emitted').inc(stream_batches)
      else:
        for row in self._read_stream(stream_name):
          ts = row.get(self._change_timestamp_column)
          if ts is None:
            raise ValueError(
                'Row missing %r column. Row keys: %s' %
                (self._change_timestamp_column, list(row.keys())))
          if isinstance(ts, datetime.datetime):
            ts = Timestamp.from_utc_datetime(ts)

          yield TimestampedValue(row, ts)
          stream_rows += 1
        Metrics.counter('BigQueryChangeHistory',
                        'rows_emitted').inc(stream_rows)

      streams_read += 1
      _LOGGER.info(
          '[Read] Finished reading stream %d for %s: %d rows',
          i,
          table_key,
          stream_rows)
      Metrics.counter('BigQueryChangeHistory', 'streams_read').inc()

    # Advance watermark to range_end after reading all streams. The
    # initial hold was set to range_start by _CDCWatermarkEstimatorProvider.
    watermark_estimator.set_watermark(element.range_end)
    _LOGGER.info(
        '[Read] Watermark advanced to %s (range_end) for %s',
        _utc(element.range_end),
        table_key)

    # Release the storage client so the gRPC channel doesn't go stale
    # between process() calls. _ensure_client() will create a fresh one.
    self._storage_client = None

    # Emit cleanup signal. Every split that reads at least one stream
    # reports how many it read.
    if streams_read > 0:
      _LOGGER.info(
          '[Read] Emitting cleanup signal for %s: '
          'streams_read=%d, total_streams=%d',
          table_key,
          streams_read,
          total_streams)
      yield beam.pvalue.TaggedOutput(
          _CLEANUP_TAG, (table_key, (streams_read, total_streams)))

  def _create_read_session(self, table_ref: 'bigquery.TableReference') -> Any:
    """Create a BigQuery Storage ReadSession for the given table."""
    table_path = (
        f'projects/{table_ref.projectId}/'
        f'datasets/{table_ref.datasetId}/'
        f'tables/{table_ref.tableId}')

    requested_session = bq_storage.types.ReadSession()
    requested_session.table = table_path
    requested_session.data_format = bq_storage.types.DataFormat.ARROW
    read_options = requested_session.read_options
    read_options.arrow_serialization_options.buffer_compression = (
        bq_storage.types.ArrowSerializationOptions.CompressionCodec.ZSTD)

    session = self._storage_client.create_read_session(
        parent=f'projects/{table_ref.projectId}',
        read_session=requested_session,
        max_stream_count=_DEFAULT_MAX_STREAMS)
    _LOGGER.info(
        '[Read] _create_read_session: table=%s, %d streams',
        table_path,
        len(session.streams))
    return session

  def _read_stream(self, stream_name: str) -> Iterable[dict[str, Any]]:
    """Read all rows from a single Storage API stream as dicts.

    When batch_arrow_read is enabled, converts entire Arrow RecordBatches
    at once using to_pylist() instead of calling .as_py() on each cell
    individually. This is ~1.5x faster for large tables at the cost of ~2x
    peak memory per batch.
    """
    if self._batch_arrow_read:
      yield from self._read_stream_batch(stream_name)
    else:
      yield from self._read_stream_row_by_row(stream_name)

  def _read_stream_row_by_row(self,
                              stream_name: str) -> Iterable[dict[str, Any]]:
    """Row-by-row Arrow conversion (lower memory than batch mode)."""
    t0 = time.time()
    row_count = 0
    for row in self._storage_client.read_rows(stream_name).rows():
      yield dict((item[0], item[1].as_py()) for item in row.items())
      row_count += 1
    elapsed = time.time() - t0
    _LOGGER.info(
        '[Read] row_by_row: %d rows in %.2fs (%.0f rows/s)',
        row_count,
        elapsed,
        row_count / elapsed if elapsed > 0 else 0)

  def _read_stream_batch(self, stream_name: str) -> Iterable[dict[str, Any]]:
    """Batch-convert Arrow RecordBatches for high throughput."""
    schema = None
    row_count = 0
    t0 = time.time()
    for response in self._storage_client.read_rows(stream_name):
      if schema is None and response.arrow_schema.serialized_schema:
        schema = pyarrow.ipc.read_schema(
            pyarrow.py_buffer(response.arrow_schema.serialized_schema))
      batch_bytes = response.arrow_record_batch.serialized_record_batch
      if batch_bytes and schema is not None:
        batch = pyarrow.ipc.read_record_batch(
            pyarrow.py_buffer(batch_bytes), schema)
        yield from batch.to_pylist()
        row_count += batch.num_rows
    elapsed = time.time() - t0
    _LOGGER.info(
        '[Read] batch_read: %d rows in %.2fs (%.0f rows/s)',
        row_count,
        elapsed,
        row_count / elapsed if elapsed > 0 else 0)

  def _read_stream_raw(self, stream_name: str) -> Iterable[tuple[bytes, bytes]]:
    """Yield raw (schema_bytes, batch_bytes) without decompression.

    Used when emit_raw_batches is enabled to defer decompression and
    Arrow-to-Python conversion to a downstream DoFn after reshuffling.
    Schema bytes are included in each tuple so each batch is
    self-contained and can be decoded independently.
    """
    schema_bytes = b''
    batch_count = 0
    t0 = time.time()
    for response in self._storage_client.read_rows(stream_name):
      if not schema_bytes and response.arrow_schema.serialized_schema:
        schema_bytes = bytes(response.arrow_schema.serialized_schema)
      batch_bytes = response.arrow_record_batch.serialized_record_batch
      if batch_bytes and schema_bytes:
        yield (schema_bytes, bytes(batch_bytes))
        batch_count += 1
    elapsed = time.time() - t0
    _LOGGER.info('[Read] raw_read: %d batches in %.2fs', batch_count, elapsed)


class _DecompressArrowBatchesFn(beam.DoFn):
  """Decompress and convert raw Arrow batches to timestamped row dicts.

  Receives individual (schema_bytes, batch_bytes) tuples after Reshuffle
  and converts each batch to individual row dicts with event timestamps
  extracted from the change_timestamp column.
  """
  def __init__(self, change_timestamp_column: str = 'change_timestamp') -> None:
    self._change_timestamp_column = change_timestamp_column

  def process(self, element: tuple[bytes, bytes]) -> Iterable[dict[str, Any]]:
    schema_bytes, batch_bytes = element
    schema = pyarrow.ipc.read_schema(pyarrow.py_buffer(schema_bytes))
    batch = pyarrow.ipc.read_record_batch(
        pyarrow.py_buffer(batch_bytes), schema)

    rows = batch.to_pylist()
    for row in rows:
      ts = row.get(self._change_timestamp_column)
      if ts is None:
        raise ValueError(
            'Row missing %r column. Row keys: %s' %
            (self._change_timestamp_column, list(row.keys())))
      if isinstance(ts, datetime.datetime):
        ts = Timestamp.from_utc_datetime(ts)
      yield TimestampedValue(row, ts)
    Metrics.counter('BigQueryChangeHistory', 'rows_emitted').inc(len(rows))


# =============================================================================
# Cleanup: _CleanupTempTablesFn
# =============================================================================


class _CleanupTempTablesFn(beam.DoFn):
  """Stateful DoFn that deletes temp tables after all streams are read.

  Receives cleanup signals from the Read SDF as:
    (table_key, (streams_read_count, total_streams))

  Accumulates streams_read across all signals for the same table_key.
  When streams_read >= total_streams, deletes the temp table. The >=
  (rather than ==) guards against duplicate delivery in at-least-once runners.
  """
  STREAMS_READ = beam.transforms.userstate.CombiningValueStateSpec(
      'streams_read', sum)

  def setup(self) -> None:
    _LOGGER.info('[Cleanup] setup: creating BigQueryWrapper')
    self._bq_wrapper = bigquery_tools.BigQueryWrapper()

  def process(
      self,
      element: tuple[str, tuple[int, int]],
      streams_read=beam.DoFn.StateParam(STREAMS_READ)
  ) -> None:
    table_key = element[0]
    split_count = element[1][0]
    total_streams = element[1][1]

    _LOGGER.info(
        '[Cleanup] Received cleanup signal for %s: '
        'split_count=%d, total_streams=%d',
        table_key,
        split_count,
        total_streams)

    streams_read.add(split_count)
    current_read = streams_read.read()

    _LOGGER.info(
        '[Cleanup] State for %s: streams_read=%d/%d',
        table_key,
        current_read,
        total_streams)

    if current_read >= total_streams:
      parsed = bigquery_tools.parse_table_reference(table_key)
      _LOGGER.info(
          '[Cleanup] All streams read: DELETING temp table %s', table_key)
      self._bq_wrapper._delete_table(
          parsed.projectId, parsed.datasetId, parsed.tableId)
      _LOGGER.info('[Cleanup] Deleted temp table %s', table_key)
      Metrics.counter('BigQueryChangeHistory', 'temp_tables_deleted').inc()
      streams_read.clear()
    else:
      _LOGGER.info(
          '[Cleanup] Not yet complete for %s (%d/%d), '
          'waiting for more signals',
          table_key,
          current_read,
          total_streams)


# =============================================================================
# Public API: ReadBigQueryChangeHistory
# =============================================================================


class ReadBigQueryChangeHistory(beam.PTransform):
  """Streaming source for BigQuery change history.

  Continuously polls BigQuery APPENDS() or CHANGES() functions and emits
  changed rows as an unbounded PCollection of dicts.

  Args:
    table: BigQuery table to read changes from.
        Format: 'project:dataset.table' or 'project.dataset.table'.
    poll_interval_sec: Seconds between polls. Default 60.
    start_time: Start reading from this timestamp (float, epoch seconds).
        Default: current time when pipeline starts.
    stop_time: Stop polling at this timestamp. Default: run forever.
    change_function: 'CHANGES' or 'APPENDS'. Default 'APPENDS'.
    buffer_sec: Safety buffer in seconds behind now(). Default 10. BQ does not
        fail or wait if the query end_ts is less than BQ's CURRENT_TIMESTAMP.
        This is an extra guardrail to protect against silent data.
    project: GCP project ID. Default: from pipeline options.
    temp_dataset: Dataset for temp tables. If None (default), a
        per-pipeline dataset is auto-created with a 24-hour table
        expiration as a safety net for orphaned tables. Set this to
        use an existing dataset (e.g. if your service account lacks
        bigquery.datasets.create permission).
    location: BigQuery geographic location for query jobs and temp
        dataset (e.g. 'US', 'us-central1'). If None (default), inferred
        from the source table.
    change_type_column: Output column name for the _CHANGE_TYPE
        pseudo-column. Default 'change_type'. Change this if your source
        table already has a column named 'change_type'.
    change_timestamp_column: Output column name for the
        _CHANGE_TIMESTAMP pseudo-column. Default 'change_timestamp'.
        Change this if your source table already has a column named
        'change_timestamp'. This column is also used internally to
        extract event timestamps for watermark tracking.
    columns: Optional list of column names to select from the source
        table. If None (default), all columns are selected. The
        pseudo-columns (change_type, change_timestamp) are always
        included regardless of this setting.
    row_filter: Optional SQL boolean expression used as a WHERE clause
        on the CHANGES/APPENDS query. Do not include the WHERE keyword.
        Example: ``'status = "active" AND region = "US"'``.
    batch_arrow_read: If True (default), convert Arrow RecordBatches in
        bulk using to_pylist() instead of per-cell .as_py() calls.
        This is 1.5x faster for large tables at the cost of ~2x peak
        memory per RecordBatch. Set to False for minimal memory usage.
    max_split_rounds: Maximum number of recursive SplitReadStream
        rounds. Each round splits every stream at fraction=0.5,
        potentially doubling the stream count (if BQ allows). Default
        1 (one round of splitting). Set 0 to disable splitting
        entirely. Set higher for very large tables where more
        parallelism is needed.
    reshuffle_decompress: If True (default), the Read SDF emits raw
        compressed Arrow batches instead of decoded rows. The batches
        are reshuffled for fan-out and then decoded in a separate DoFn.
        This spreads decompression and Arrow-to-Python conversion CPU
        across more workers. Set to False to decode rows inline within
        the Read SDF.
  """
  def __init__(
      self,
      table: str,
      poll_interval_sec: float = 60,
      start_time: Optional[float] = None,
      stop_time: Optional[float] = None,
      change_function: str = 'APPENDS',
      buffer_sec: float = 10,
      project: Optional[str] = None,
      temp_dataset: Optional[str] = None,
      location: Optional[str] = None,
      change_type_column: str = 'change_type',
      change_timestamp_column: str = 'change_timestamp',
      columns: Optional[list[str]] = None,
      row_filter: Optional[str] = None,
      batch_arrow_read: bool = True,
      max_split_rounds: int = 1,
      reshuffle_decompress: bool = True) -> None:
    super().__init__()
    if bq_storage is None:
      raise ImportError(
          'google-cloud-bigquery-storage is required for '
          'ReadBigQueryChangeHistory. Install it with: '
          'pip install google-cloud-bigquery-storage')
    if pyarrow is None:
      raise ImportError(
          'pyarrow is required for ReadBigQueryChangeHistory. '
          'Install it with: pip install pyarrow')
    if change_function not in ('CHANGES', 'APPENDS'):
      raise ValueError(
          f"change_function must be 'CHANGES' or 'APPENDS', "
          f"got '{change_function}'")
    if poll_interval_sec < 15:
      raise ValueError(
          f'poll_interval_sec must be >= 15, got {poll_interval_sec}')
    if buffer_sec < 0:
      raise ValueError(f'buffer_sec must be >= 0, got {buffer_sec}')
    self._table = table
    self._poll_interval_sec = poll_interval_sec
    self._start_time = start_time
    self._stop_time = stop_time
    self._change_function = change_function
    self._buffer_sec = buffer_sec
    self._project = project
    self._temp_dataset = temp_dataset
    self._location = location
    self._change_type_column = change_type_column
    self._change_timestamp_column = change_timestamp_column
    self._columns = columns
    self._row_filter = row_filter
    self._batch_arrow_read = batch_arrow_read
    self._max_split_rounds = max_split_rounds
    self._reshuffle_decompress = reshuffle_decompress

  def expand(self, pbegin: beam.pvalue.PBegin) -> beam.PCollection:
    project = self._project
    if project is None:
      project = pbegin.pipeline.options.view_as(
          beam.options.pipeline_options.GoogleCloudOptions).project

    if project is None:
      raise ValueError(
          'project must be specified either in ReadBigQueryChangeHistory '
          'or in pipeline options (--project)')

    start_time = Timestamp(self._start_time or time.time())
    stop_time = (
        Timestamp(self._stop_time)
        if self._stop_time is not None else MAX_TIMESTAMP)
    buffer = Duration(seconds=self._buffer_sec)
    poll_interval = Duration(seconds=self._poll_interval_sec)

    temp_dataset = self._temp_dataset
    if temp_dataset is None:
      temp_dataset = f'beam_ch_temp_{uuid.uuid4().hex[:12]}'

    _LOGGER.info(
        '[ReadBigQueryChangeHistory] expand: table=%s, project=%s, '
        'change_function=%s, poll_interval=%d sec, buffer=%d sec, '
        'temp_dataset=%s, start_time=%s, stop_time=%s',
        self._table,
        project,
        self._change_function,
        self._poll_interval_sec,
        self._buffer_sec,
        temp_dataset,
        _utc(start_time),
        _utc(stop_time) if stop_time != MAX_TIMESTAMP else 'INF')

    # Custom polling SDF emits lightweight _QueryRange instructions.
    # The SDF uses defer_remainder() for poll timing and
    # _PollWatermarkEstimator to hold the watermark at data timestamps.
    # On the first invocation it handles the full historical range
    # [start_time, now - buffer) in a single poll.
    config = _PollConfig(start_time=start_time)

    query_ranges = (
        pbegin
        | 'CreatePollConfig' >> beam.Create([config])
        | 'PollChangeHistory' >> beam.ParDo(
            _PollChangeHistoryFn(
                table=self._table,
                project=project,
                change_function=self._change_function,
                buffer=buffer,
                start_time=start_time,
                stop_time=stop_time,
                poll_interval=poll_interval,
                location=self._location)))

    # CommitQueryResults: Reshuffle commits _QueryResult (temp table ref)
    # so that if the Read SDF retries, it re-reads the existing temp table
    # instead of re-running the BQ query.
    # Possible edge-case is that if ReadStorageStreams doesn't read the temp
    # table within 24 hours (table expiration) it can end up in a bad state by
    # trying to query a non-existing table.
    query_results = (
        query_ranges
        | 'CommitQueryRanges' >> beam.Reshuffle()
        | 'ExecuteQueries' >> beam.ParDo(
            _ExecuteQueryFn(
                table=self._table,
                project=project,
                change_function=self._change_function,
                temp_dataset=temp_dataset,
                location=self._location,
                change_type_column=self._change_type_column,
                change_timestamp_column=self._change_timestamp_column,
                columns=self._columns,
                row_filter=self._row_filter))
        | 'CommitQueryResults' >> beam.Reshuffle())

    emit_raw = self._reshuffle_decompress

    read_sdf = beam.ParDo(
        _ReadStorageStreamsSDF(
            batch_arrow_read=self._batch_arrow_read,
            change_timestamp_column=self._change_timestamp_column,
            max_split_rounds=self._max_split_rounds,
            emit_raw_batches=emit_raw))
    if emit_raw:
      read_sdf = read_sdf.with_output_types(tuple[bytes, bytes])
    else:
      read_sdf = read_sdf.with_output_types(dict[str, Any])

    read_outputs = (
        query_results
        | 'ReadStorageStreams' >> read_sdf.with_outputs(
            _CLEANUP_TAG, main='rows'))

    _ = (
        read_outputs[_CLEANUP_TAG]
        | 'CleanupTempTables' >> beam.ParDo(_CleanupTempTablesFn()))

    if emit_raw:
      # Reshuffle raw Arrow batches for fan-out, then decompress and
      # convert to timestamped row dicts in a separate DoFn.
      rows = (
          read_outputs['rows']
          | 'ReshuffleForFanout' >> beam.Reshuffle()
          | 'DecompressBatches' >> beam.ParDo(
              _DecompressArrowBatchesFn(
                  change_timestamp_column=(self._change_timestamp_column))))
      return rows
    else:
      return read_outputs['rows']
