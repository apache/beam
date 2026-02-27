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
  - _table_key conversion
  - ReadBigQueryChangeHistory validation
"""

import datetime
import unittest

from apache_beam.io.gcp.bigquery_change_history import ReadBigQueryChangeHistory
from apache_beam.io.gcp.bigquery_change_history import _table_key
from apache_beam.io.gcp.bigquery_change_history import build_changes_query
from apache_beam.io.gcp.bigquery_change_history import compute_ranges
from apache_beam.io.gcp.internal.clients import bigquery


class BuildChangesQueryTest(unittest.TestCase):
  """Tests for build_changes_query()."""
  def test_appends_query_format(self):
    # Use UTC-aware datetimes to avoid timezone offset issues
    ts_start = datetime.datetime(
        2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    ts_end = datetime.datetime(
        2025, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    sql = build_changes_query(
        'myproject.mydataset.mytable', ts_start, ts_end, 'APPENDS')
    self.assertIn('APPENDS', sql)
    self.assertIn('TABLE `myproject.mydataset.mytable`', sql)
    self.assertIn('2025-01-01T00:00:00', sql)
    self.assertIn('2025-01-01T01:00:00', sql)

  def test_changes_query_format(self):
    ts_start = datetime.datetime(
        2025, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    ts_end = datetime.datetime(
        2025, 6, 15, 18, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    sql = build_changes_query('proj.ds.tbl', ts_start, ts_end, 'CHANGES')
    self.assertIn('CHANGES', sql)
    self.assertIn('TABLE `proj.ds.tbl`', sql)

  def test_columns_select(self):
    ts_start = datetime.datetime(
        2025, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
    ts_end = datetime.datetime(
        2025, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
    sql = build_changes_query(
        'proj.ds.tbl', ts_start, ts_end, 'APPENDS', columns=['col_a', 'col_b'])
    self.assertIn('SELECT col_a, col_b, _CHANGE_TYPE AS', sql)
    self.assertNotIn('EXCEPT', sql)

  def test_columns_none_selects_all(self):
    ts_start = datetime.datetime(
        2025, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
    ts_end = datetime.datetime(
        2025, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
    sql = build_changes_query(
        'proj.ds.tbl', ts_start, ts_end, 'APPENDS', columns=None)
    self.assertIn('SELECT * EXCEPT', sql)

  def test_row_filter(self):
    ts_start = datetime.datetime(
        2025, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
    ts_end = datetime.datetime(
        2025, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
    sql = build_changes_query(
        'proj.ds.tbl',
        ts_start,
        ts_end,
        'APPENDS',
        row_filter='status = "active"')
    self.assertIn('WHERE status = "active"', sql)

  def test_no_row_filter(self):
    ts_start = datetime.datetime(
        2025, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
    ts_end = datetime.datetime(
        2025, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
    sql = build_changes_query(
        'proj.ds.tbl', ts_start, ts_end, 'APPENDS', row_filter=None)
    self.assertNotIn('WHERE', sql)

  def test_columns_and_row_filter(self):
    ts_start = datetime.datetime(
        2025, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
    ts_end = datetime.datetime(
        2025, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
    sql = build_changes_query(
        'proj.ds.tbl',
        ts_start,
        ts_end,
        'CHANGES',
        columns=['id', 'name'],
        row_filter='id > 100')
    self.assertIn('SELECT id, name, _CHANGE_TYPE AS', sql)
    self.assertNotIn('EXCEPT', sql)
    self.assertIn('WHERE id > 100', sql)

  def test_colon_normalized_to_dot(self):
    ts_start = datetime.datetime(
        2025, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
    ts_end = datetime.datetime(
        2025, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
    sql = build_changes_query(
        'myproject:mydataset.mytable', ts_start, ts_end, 'APPENDS')
    self.assertIn('TABLE `myproject.mydataset.mytable`', sql)
    # Verify colon in table ref is normalized (timestamps contain colons)
    table_part = sql.split('TABLE')[1].split(',')[0]
    self.assertNotIn(':', table_part)


class ComputeRangesTest(unittest.TestCase):
  """Tests for compute_ranges()."""
  def test_appends_single_range(self):
    """APPENDS has no chunking — returns single range even for multi-day."""
    start = 0.0
    end = 86400.0 * 5  # 5 days
    ranges = compute_ranges(start, end, 'APPENDS')
    self.assertEqual(len(ranges), 1)
    self.assertEqual(ranges[0], (start, end))

  def test_changes_single_day(self):
    """CHANGES within 1 day: single range."""
    start = 0.0
    end = 86400.0  # exactly 1 day
    ranges = compute_ranges(start, end, 'CHANGES')
    self.assertEqual(len(ranges), 1)
    self.assertEqual(ranges[0], (start, end))

  def test_changes_multi_day(self):
    """CHANGES spanning 3 days: should chunk into 3 ranges."""
    start = 0.0
    end = 86400.0 * 3  # 3 days
    ranges = compute_ranges(start, end, 'CHANGES')
    self.assertEqual(len(ranges), 3)
    # Verify no gaps
    for i in range(len(ranges) - 1):
      self.assertEqual(ranges[i][1], ranges[i + 1][0])
    self.assertEqual(ranges[0][0], start)
    self.assertEqual(ranges[-1][1], end)

  def test_changes_partial_day(self):
    """CHANGES spanning 1.5 days: should chunk into 2 ranges."""
    start = 0.0
    end = 86400.0 * 1.5
    ranges = compute_ranges(start, end, 'CHANGES')
    self.assertEqual(len(ranges), 2)
    self.assertEqual(ranges[0], (0.0, 86400.0))
    self.assertEqual(ranges[1], (86400.0, end))

  def test_zero_range(self):
    """end <= start: empty list."""
    self.assertEqual(compute_ranges(100.0, 100.0, 'CHANGES'), [])
    self.assertEqual(compute_ranges(100.0, 50.0, 'CHANGES'), [])
    self.assertEqual(compute_ranges(100.0, 100.0, 'APPENDS'), [])

  def test_exact_day_boundary(self):
    """Exactly 2 days: should produce 2 chunks."""
    start = 0.0
    end = 86400.0 * 2
    ranges = compute_ranges(start, end, 'CHANGES')
    self.assertEqual(len(ranges), 2)


class TableKeyTest(unittest.TestCase):
  """Tests for _table_key()."""
  def test_conversion(self):
    ref = bigquery.TableReference(
        projectId='proj', datasetId='ds', tableId='tbl')
    self.assertEqual(_table_key(ref), 'proj.ds.tbl')


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
    self.assertEqual(t._buffer_sec, 15)
    t = ReadBigQueryChangeHistory(table='p:d.t', change_function='APPENDS')
    self.assertEqual(t._buffer_sec, 15)


if __name__ == '__main__':
  unittest.main()
