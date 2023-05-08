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

"""Unit tests for BigTable service."""

# pytype: skip-file
import os
import pytest
import string
import unittest
import uuid
from datetime import datetime, timezone
from random import choice

from mock import MagicMock
from mock import patch

from apache_beam.internal.metrics.metric import ServiceCallMetric
from apache_beam.io.gcp import bigtableio
from apache_beam.io.gcp import resource_identifiers
from apache_beam.io.gcp.bigtableio import BigtableRow
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.testing.test_pipeline import TestPipeline

# Protect against environments where bigtable library is not available.
try:
  from google.cloud.bigtable import client
  from google.cloud.bigtable.instance import Instance
  from google.cloud.bigtable.row import Cell, DirectRow, PartialRowData
  from google.cloud.bigtable.row_filters import TimestampRange
  from google.cloud.bigtable.table import Table
  from google.rpc.code_pb2 import OK, ALREADY_EXISTS
  from google.rpc.status_pb2 import Status
except ImportError as e:
  client = None


@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
class TestBigtableRow(unittest.TestCase):
  def test_access_attributes_from_parent_class(self):
    row = BigtableRow("test_key", "test_table")
    self.assertEqual(row.row_key, "test_key")
    self.assertEqual(row.table, "test_table")

  def test_set_cell(self):
    row = BigtableRow("key")

    timestamp_seconds = 123456
    dt = datetime.fromtimestamp(timestamp_seconds, timezone.utc)
    row.set_cell("fam_id", "col_id", "test_value", dt)

    cell: Cell = row.find_cells("fam_id", "col_id")[0]
    self.assertEqual(cell.timestamp, dt)
    self.assertEqual(cell.timestamp_micros, timestamp_seconds * 1e6)
    self.assertEqual(cell.value, "test_value")

  def test_set_many_cells(self):
    row = BigtableRow("key")
    num_cells = 100

    timestamps = [
        datetime.fromtimestamp(t, timezone.utc)
        for t in range(1, num_cells + 1)
    ]
    for i in range(num_cells):
      dt = timestamps[i]
      val = i
      row.set_cell("fam_id", "col_id", val, dt)

    cells = row.find_cells("fam_id", "col_id")
    self.assertEqual(len(cells), num_cells)
    for i in range(num_cells):
      self.assertEqual(cells[i].timestamp, timestamps[i])
      self.assertEqual(cells[i].value, i)

  def test_multiple_columns(self):
    row = BigtableRow("key")

    data = [{
        'col': 'col_1',
        'val': 'val1',
        'dt': datetime.fromtimestamp(123, timezone.utc)
    },
            {
                'col': 'col_2',
                'val': 'val2',
                'dt': datetime.fromtimestamp(456, timezone.utc)
            },
            {
                'col': 'col_3',
                'val': 'val3',
                'dt': datetime.fromtimestamp(789, timezone.utc)
            }]

    for d in data:
      row.set_cell('fam_id', d['col'], d['val'], d['dt'])
    self.assertEqual(
        set(row.cells['fam_id'].keys()), set(['col_1', 'col_2', 'col_3']))

    for d in data:
      cells = row.find_cells('fam_id', d['col'])

      self.assertEqual(len(cells), 1)
      self.assertEqual(cells[0].value, d['val'])
      self.assertEqual(cells[0].timestamp, d['dt'])

  def test_multiple_families(self):
    row = BigtableRow("key")

    data = [{
        'fam': 'fam_1',
        'val': 'val1',
        'dt': datetime.fromtimestamp(123, timezone.utc)
    },
            {
                'fam': 'fam_2',
                'val': 'val2',
                'dt': datetime.fromtimestamp(456, timezone.utc)
            },
            {
                'fam': 'fam_3',
                'val': 'val3',
                'dt': datetime.fromtimestamp(789, timezone.utc)
            }]

    for d in data:
      row.set_cell(d['fam'], 'col', d['val'], d['dt'])
    self.assertEqual(set(row.cells.keys()), set(['fam_1', 'fam_2', 'fam_3']))

    for d in data:
      cells = row.find_cells(d['fam'], 'col')

      self.assertEqual(len(cells), 1)
      self.assertEqual(cells[0].value, d['val'])
      self.assertEqual(cells[0].timestamp, d['dt'])

  def test_delete_all_cells(self):
    row = BigtableRow("key")

    for i in range(100):
      row.set_cell('fam_id', 'col_id', 'val')

    self.assertEqual(len(row.find_cells('fam_id', 'col_id')), 100)

    row.delete_cell('fam_id', 'col_id')
    # deleting all cells will delete the column, so we get an error at access
    with self.assertRaises(KeyError):
      row.find_cells('fam_id', 'col_id')
    self.assertEqual(row.cells, {})

  def test_delete_cells_in_timestamp_range(self):
    row = BigtableRow("key")
    num_cells = 10

    timestamps = [
        datetime.fromtimestamp(t, timezone.utc)
        for t in range(1, num_cells + 1)
    ]
    for i in range(num_cells):
      dt = timestamps[i]
      val = i
      row.set_cell('fam_id', 'col_id', val, dt)

    cells = row.find_cells('fam_id', 'col_id')
    self.assertEqual(len(cells), num_cells)

    # delete all but the extremities
    start = datetime.fromtimestamp(2, timezone.utc)
    end = datetime.fromtimestamp(10, timezone.utc)

    # range: start is inclusive, end is exclusive
    row.delete_cell('fam_id', 'col_id', TimestampRange(start, end))
    cells = row.find_cells('fam_id', 'col_id')
    self.assertEqual(len(cells), 2)
    self.assertEqual(cells[0].timestamp, timestamps[0])
    self.assertEqual(cells[-1].timestamp, timestamps[-1])

  def test_delete_all_columns(self):
    row = BigtableRow("key")

    row.set_cell('fam', 'col_1', 'val1')
    row.set_cell('fam', 'col_2', 'val2')
    row.set_cell('fam', 'col_3', 'val3')
    row.set_cell('other_fam', 'col', 'val')

    self.assertEqual(len(row.cells['fam'].keys()), 3)

    row.delete_cells('fam', row.ALL_COLUMNS)

    # deleting all columns will delete the column family, so it is no longer
    # in our cells dictionary
    self.assertTrue('fam' not in row.cells)
    # other column families still exists
    self.assertTrue('other_fam' in row.cells)


@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
class TestWriteBigTable(unittest.TestCase):
  TABLE_PREFIX = "python-test"
  _PROJECT_ID = TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  _INSTANCE_ID = TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  _TABLE_ID = TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]

  def setUp(self):
    client = MagicMock()
    instance = Instance(self._INSTANCE_ID, client)
    self.table = Table(self._TABLE_ID, instance)

  def test_write_metrics(self):
    MetricsEnvironment.process_wide_container().reset()
    write_fn = bigtableio._BigTableWriteFn(
        self._PROJECT_ID, self._INSTANCE_ID, self._TABLE_ID)
    write_fn.table = self.table
    write_fn.start_bundle()
    number_of_rows = 2
    error = Status()
    error.message = 'Entity already exists.'
    error.code = ALREADY_EXISTS
    success = Status()
    success.message = 'Success'
    success.code = OK
    rows_response = [error, success] * number_of_rows
    with patch.object(Table, 'mutate_rows', return_value=rows_response):
      direct_rows = [self.generate_row(i) for i in range(number_of_rows * 2)]
      for direct_row in direct_rows:
        write_fn.process(direct_row)
      try:
        write_fn.finish_bundle()
      except:  # pylint: disable=bare-except
        # Currently we fail the bundle when there are any failures.
        # TODO(https://github.com/apache/beam/issues/21396): remove after
        # bigtableio can selectively retry.
        pass
      self.verify_write_call_metric(
          self._PROJECT_ID,
          self._INSTANCE_ID,
          self._TABLE_ID,
          ServiceCallMetric.bigtable_error_code_to_grpc_status_string(
              ALREADY_EXISTS),
          2)
      self.verify_write_call_metric(
          self._PROJECT_ID,
          self._INSTANCE_ID,
          self._TABLE_ID,
          ServiceCallMetric.bigtable_error_code_to_grpc_status_string(OK),
          2)

  def generate_row(self, index=0):
    rand = choice(string.ascii_letters + string.digits)
    value = ''.join(rand for i in range(100))
    column_family_id = 'cf1'
    key = "beam_key%s" % ('{0:07}'.format(index))
    direct_row = DirectRow(row_key=key)
    for column_id in range(10):
      direct_row.set_cell(
          column_family_id, ('field%s' % column_id).encode('utf-8'),
          value,
          datetime.now())
    return direct_row

  def verify_write_call_metric(
      self, project_id, instance_id, table_id, status, count):
    """Check if a metric was recorded for the Datastore IO write API call."""
    process_wide_monitoring_infos = list(
        MetricsEnvironment.process_wide_container().
        to_runner_api_monitoring_infos(None).values())
    resource = resource_identifiers.BigtableTable(
        project_id, instance_id, table_id)
    labels = {
        monitoring_infos.SERVICE_LABEL: 'BigTable',
        monitoring_infos.METHOD_LABEL: 'google.bigtable.v2.MutateRows',
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.BIGTABLE_PROJECT_ID_LABEL: project_id,
        monitoring_infos.INSTANCE_ID_LABEL: instance_id,
        monitoring_infos.TABLE_ID_LABEL: table_id,
        monitoring_infos.STATUS_LABEL: status
    }
    expected_mi = monitoring_infos.int64_counter(
        monitoring_infos.API_REQUEST_COUNT_URN, count, labels=labels)
    expected_mi.ClearField("start_time")

    found = False
    for actual_mi in process_wide_monitoring_infos:
      actual_mi.ClearField("start_time")
      if expected_mi == actual_mi:
        found = True
        break
    self.assertTrue(
        found, "Did not find write call metric with status: %s" % status)


if __name__ == '__main__':
  unittest.main()
