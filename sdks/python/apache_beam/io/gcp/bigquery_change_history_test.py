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

"""Unit tests for BigQuery change history streaming source (no GCP required).

Tests:
  - build_changes_query format
  - compute_ranges chunking
  - ReadBigQueryChangeHistory validation
"""

import contextlib
import datetime
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam import typehints
from apache_beam.io.gcp import bigquery_change_history as bqch
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.bigquery_change_history import ReadBigQueryChangeHistory
from apache_beam.io.gcp.bigquery_change_history import build_changes_query
from apache_beam.io.gcp.bigquery_change_history import compute_ranges
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

# Protect against environments where apitools is not available.
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None  # type: ignore

_DAY = Duration(seconds=86400)


def _ts(*args, **kwargs) -> Timestamp:
  """Create a UTC datetime and return a Beam Timestamp."""
  # pyrefly: ignore[bad-keyword-argument]
  dt = datetime.datetime(*args, tzinfo=datetime.timezone.utc, **kwargs)
  return Timestamp(dt.timestamp())


class BuildChangesQueryTest(unittest.TestCase):
  """Tests for build_changes_query()."""
  def test_appends_query_format(self):
    start = _ts(2025, 1, 1, 0, 0, 0)
    end = _ts(2025, 1, 1, 1, 0, 0)
    sql = build_changes_query(
        'myproject.mydataset.mytable', start, end, 'APPENDS')
    self.assertIn('APPENDS', sql)
    self.assertIn('TABLE `myproject.mydataset.mytable`', sql)
    self.assertIn('2025-01-01T00:00:00', sql)
    self.assertIn('2025-01-01T01:00:00', sql)

  def test_changes_query_format(self):
    start = _ts(2025, 6, 15, 12, 0, 0)
    end = _ts(2025, 6, 15, 18, 0, 0)
    sql = build_changes_query('proj.ds.tbl', start, end, 'CHANGES')
    self.assertIn('CHANGES', sql)
    self.assertIn('TABLE `proj.ds.tbl`', sql)

  def test_changes_hides_is_for_update(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query('proj.ds.tbl', start, end, 'CHANGES')
    self.assertIn(
        'EXCEPT(_CHANGE_TYPE, _CHANGE_TIMESTAMP, _CHANGE_IS_FOR_UPDATE)', sql)

  def test_appends_has_no_is_for_update(self):
    """APPENDS() has no _CHANGE_IS_FOR_UPDATE; EXCEPT on it would fail."""
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query('proj.ds.tbl', start, end, 'APPENDS')
    self.assertIn('EXCEPT(_CHANGE_TYPE, _CHANGE_TIMESTAMP)', sql)
    self.assertNotIn('_CHANGE_IS_FOR_UPDATE', sql)

  def test_columns_select(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query(
        'proj.ds.tbl', start, end, 'APPENDS', columns=['col_a', 'col_b'])
    self.assertIn('SELECT `col_a`, `col_b`, _CHANGE_TYPE AS', sql)
    self.assertNotIn('EXCEPT', sql)

  def test_columns_empty_selects_only_pseudo_columns(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query('proj.ds.tbl', start, end, 'APPENDS', columns=[])
    self.assertIn('SELECT _CHANGE_TYPE AS', sql)

  def test_pseudo_column_names_quoted(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query(
        'proj.ds.tbl',
        start,
        end,
        'APPENDS',
        change_type_column='op type',
        change_timestamp_column='from')
    self.assertIn('_CHANGE_TYPE AS `op type`', sql)
    self.assertIn('_CHANGE_TIMESTAMP AS `from`', sql)

  def test_column_with_backtick_rejected(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    with self.assertRaises(ValueError):
      build_changes_query(
          'proj.ds.tbl', start, end, 'APPENDS', columns=['a` FROM x --'])

  def test_columns_none_selects_all(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query(
        'proj.ds.tbl', start, end, 'APPENDS', columns=None)
    self.assertIn('SELECT * EXCEPT', sql)

  def test_row_filter(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query(
        'proj.ds.tbl', start, end, 'APPENDS', row_filter='status = "active"')
    self.assertIn('WHERE status = "active"', sql)

  def test_no_row_filter(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query(
        'proj.ds.tbl', start, end, 'APPENDS', row_filter=None)
    self.assertNotIn('WHERE', sql)

  def test_columns_and_row_filter(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query(
        'proj.ds.tbl',
        start,
        end,
        'CHANGES',
        columns=['id', 'name'],
        row_filter='id > 100')
    self.assertIn('SELECT `id`, `name`, _CHANGE_TYPE AS', sql)
    self.assertNotIn('EXCEPT', sql)
    self.assertIn('WHERE id > 100', sql)

  def test_colon_normalized_to_dot(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query(
        'myproject:mydataset.mytable', start, end, 'APPENDS')
    self.assertIn('TABLE `myproject.mydataset.mytable`', sql)
    # Verify colon in table ref is normalized (timestamps contain colons)
    table_part = sql.split('TABLE')[1].split(',')[0]
    self.assertNotIn(':', table_part)

  def test_microsecond_precision(self):
    """Verify sub-second precision is preserved in ISO output."""
    # 2025-01-01T00:00:00.123456Z
    start = Timestamp(micros=_ts(2025, 1, 1).micros + 123456)
    end = start + Duration(seconds=1)
    sql = build_changes_query('proj.ds.tbl', start, end, 'APPENDS')
    self.assertIn('2025-01-01T00:00:00.123456Z', sql)


class ComputeRangesTest(unittest.TestCase):
  """Tests for compute_ranges()."""
  def test_appends_single_range(self):
    """APPENDS has no chunking — returns single range even for multi-day."""
    start = Timestamp(0)
    end = Timestamp(0) + _DAY * Duration(seconds=5)
    ranges = compute_ranges(start, end, 'APPENDS')
    self.assertEqual(len(ranges), 1)
    self.assertEqual(ranges[0], (start, end))

  def test_changes_single_day(self):
    """CHANGES within 1 day: single range."""
    start = Timestamp(0)
    end = Timestamp(0) + _DAY
    ranges = compute_ranges(start, end, 'CHANGES')
    self.assertEqual(len(ranges), 1)
    self.assertEqual(ranges[0], (start, end))

  def test_changes_multi_day(self):
    """CHANGES spanning 3 days: should chunk into 3 ranges."""
    start = Timestamp(0)
    end = Timestamp(0) + _DAY * Duration(seconds=3)
    ranges = compute_ranges(start, end, 'CHANGES')
    self.assertEqual(len(ranges), 3)
    # Verify no gaps
    for i in range(len(ranges) - 1):
      self.assertEqual(ranges[i][1], ranges[i + 1][0])
    self.assertEqual(ranges[0][0], start)
    self.assertEqual(ranges[-1][1], end)

  def test_changes_partial_day(self):
    """CHANGES spanning 1.5 days: should chunk into 2 ranges."""
    start = Timestamp(0)
    one_day = Timestamp(0) + _DAY
    end = Timestamp(micros=_DAY.micros + _DAY.micros // 2)
    ranges = compute_ranges(start, end, 'CHANGES')
    self.assertEqual(len(ranges), 2)
    self.assertEqual(ranges[0], (start, one_day))
    self.assertEqual(ranges[1], (one_day, end))

  def test_zero_range(self):
    """end <= start: empty list."""
    t100 = Timestamp(micros=100)
    t50 = Timestamp(micros=50)
    self.assertEqual(compute_ranges(t100, t100, 'CHANGES'), [])
    self.assertEqual(compute_ranges(t100, t50, 'CHANGES'), [])
    self.assertEqual(compute_ranges(t100, t100, 'APPENDS'), [])

  def test_exact_day_boundary(self):
    """Exactly 2 days: should produce 2 chunks."""
    start = Timestamp(0)
    end = Timestamp(0) + _DAY * Duration(seconds=2)
    ranges = compute_ranges(start, end, 'CHANGES')
    self.assertEqual(len(ranges), 2)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class ValidationTest(unittest.TestCase):
  """Tests for ReadBigQueryChangeHistory validation."""
  def test_invalid_change_function(self):
    with self.assertRaises(ValueError):
      ReadBigQueryChangeHistory(table='p:d.t', change_function='INVALID')

  def test_invalid_poll_interval(self):
    with self.assertRaises(ValueError):
      ReadBigQueryChangeHistory(table='p:d.t', poll_interval_sec=0)
    with self.assertRaises(ValueError):
      ReadBigQueryChangeHistory(table='p:d.t', poll_interval_sec=-1)

  def test_default_buffer(self):
    t = ReadBigQueryChangeHistory(table='p:d.t', change_function='CHANGES')
    self.assertEqual(t._buffer_sec, 10)
    t = ReadBigQueryChangeHistory(table='p:d.t', change_function='APPENDS')
    self.assertEqual(t._buffer_sec, 10)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class QuotaProjectTest(unittest.TestCase):
  """Tests that the quota project reaches every BigQuery client."""
  _QUOTA = 'my-billing-project'

  def _assert_wrapper_quota(self, fn):
    with mock.patch.object(bigquery_tools, 'BigQueryWrapper') as mock_wrapper:
      fn.setup()
    self.assertEqual(
        mock_wrapper.call_args.kwargs['quota_project_id'], self._QUOTA)

  def test_poll_fn_setup_passes_quota_project(self):
    fn = bqch._PollChangeHistoryFn(
        table='proj:dataset.table',
        project='p',
        change_function='APPENDS',
        buffer=Duration(seconds=10),
        start_time=Timestamp(0),
        stop_time=Timestamp(100),
        poll_interval=Duration(seconds=60),
        quota_project_id=self._QUOTA)
    self._assert_wrapper_quota(fn)

  def test_execute_query_fn_setup_passes_quota_project(self):
    fn = bqch._ExecuteQueryFn(
        table='proj:dataset.table',
        project='p',
        change_function='APPENDS',
        temp_dataset='tmp',
        location='US',
        quota_project_id=self._QUOTA)
    self._assert_wrapper_quota(fn)

  def test_cleanup_fn_setup_passes_quota_project(self):
    self._assert_wrapper_quota(bqch._CleanupTempTablesFn(self._QUOTA))

  def test_read_sdf_creates_storage_client_with_quota_project(self):
    fn = bqch._ReadStorageStreamsSDF(quota_project_id=self._QUOTA)
    with mock.patch.object(bigquery_tools,
                           'create_bq_storage_client') as mock_create:
      fn.setup()
    mock_create.assert_called_once_with(self._QUOTA)

  def test_expand_resolves_quota_project_from_options(self):
    options = PipelineOptions([
        '--bigquery_quota_project_id=' + self._QUOTA,
        '--no_pipeline_type_check'
    ])
    classes = [
        '_PollChangeHistoryFn',
        '_ExecuteQueryFn',
        '_ReadStorageStreamsSDF',
        '_CleanupTempTablesFn'
    ]
    p = TestPipeline(options=options)
    with contextlib.ExitStack() as stack:
      mocks = [
          stack.enter_context(
              mock.patch.object(
                  getattr(bqch, name),
                  '__init__',
                  autospec=True,
                  side_effect=getattr(bqch, name).__init__)) for name in classes
      ]
      _ = p | ReadBigQueryChangeHistory(
          table='p:d.t', project='p', start_time=0, temp_dataset='tmp')
    for name, m in zip(classes, mocks):
      self.assertEqual(
          m.call_args.kwargs['quota_project_id'], self._QUOTA, name)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class PipelineConstructionTest(unittest.TestCase):
  """Applies the transform with pipeline type checking on (the default)."""
  def _apply(self, reshuffle_decompress):
    p = beam.Pipeline()
    return p | ReadBigQueryChangeHistory(
        table='p:d.t',
        project='p',
        temp_dataset='tmp',
        location='US',
        start_time=1700000000,
        stop_time=1700000060,
        reshuffle_decompress=reshuffle_decompress)

  def test_reshuffle_decompress_output_type(self):
    rows = self._apply(reshuffle_decompress=True)
    self.assertEqual(rows.element_type, typehints.Dict[str, typehints.Any])

  def test_inline_decompress_output_type(self):
    rows = self._apply(reshuffle_decompress=False)
    self.assertEqual(rows.element_type, typehints.Dict[str, typehints.Any])


if __name__ == '__main__':
  unittest.main()
