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

import datetime
import unittest

from apache_beam.io.gcp.bigquery_change_history import ReadBigQueryChangeHistory
from apache_beam.io.gcp.bigquery_change_history import build_changes_query
from apache_beam.io.gcp.bigquery_change_history import compute_ranges
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

  def test_columns_select(self):
    start = _ts(2025, 1, 1)
    end = _ts(2025, 1, 2)
    sql = build_changes_query(
        'proj.ds.tbl', start, end, 'APPENDS', columns=['col_a', 'col_b'])
    self.assertIn('SELECT col_a, col_b, _CHANGE_TYPE AS', sql)
    self.assertNotIn('EXCEPT', sql)

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
    self.assertIn('SELECT id, name, _CHANGE_TYPE AS', sql)
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


if __name__ == '__main__':
  unittest.main()
