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

"""Integration tests for BigTable service."""

import logging
import os
import secrets
import time
import unittest
# pytype: skip-file
from datetime import datetime
from datetime import timezone

import pytest

import apache_beam as beam
from apache_beam.io.gcp import bigtableio
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)

# Protect against environments where bigtable library is not available.
try:
  from apitools.base.py.exceptions import HttpError
  from google.cloud.bigtable import client
  from google.cloud.bigtable.row_filters import TimestampRange
  from google.cloud.bigtable.row import DirectRow, PartialRowData, Cell
  from google.cloud.bigtable.table import Table
  from google.cloud.bigtable_admin_v2.types import instance
except ImportError as e:
  client = None
  HttpError = None


def instance_prefix(instance):
  datestr = "".join(filter(str.isdigit, str(datetime.utcnow().date())))
  instance_id = '%s-%s-%s' % (instance, datestr, secrets.token_hex(4))
  assert len(instance_id) < 34, "instance id length needs to be within [6, 33]"
  return instance_id


@pytest.mark.uses_gcp_java_expansion_service
@pytest.mark.uses_transform_service
@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS') or
    os.environ.get('TRANSFORM_SERVICE_PORT'),
    "A valid expansion service is not available for executing the "
    "cross-language test.")
class TestReadFromBigTableIT(unittest.TestCase):
  INSTANCE = "bt-read-tests"
  TABLE_ID = "test-table"

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.args = self.test_pipeline.get_full_options_as_args()
    self.project = self.test_pipeline.get_option('project')

    instance_id = instance_prefix(self.INSTANCE)

    self.client = client.Client(admin=True, project=self.project)
    # create cluster and instance
    self.instance = self.client.instance(
        instance_id,
        display_name=self.INSTANCE,
        instance_type=instance.Instance.Type.DEVELOPMENT)
    cluster = self.instance.cluster("test-cluster", "us-central1-a")
    operation = self.instance.create(clusters=[cluster])
    operation.result(timeout=500)
    _LOGGER.info(
        "Created instance [%s] in project [%s]",
        self.instance.instance_id,
        self.project)

    # create table inside instance
    self.table = self.instance.table(self.TABLE_ID)
    self.table.create()
    _LOGGER.info("Created table [%s]", self.table.table_id)
    if (os.environ.get('TRANSFORM_SERVICE_PORT')):
      self._transform_service_address = (
          'localhost:' + os.environ.get('TRANSFORM_SERVICE_PORT'))
    else:
      self._transform_service_address = None

  def tearDown(self):
    try:
      _LOGGER.info(
          "Deleting table [%s] and instance [%s]",
          self.table.table_id,
          self.instance.instance_id)
      self.table.delete()
      self.instance.delete()
    except HttpError:
      _LOGGER.warning(
          "Failed to clean up table [%s] and instance [%s]",
          self.table.table_id,
          self.instance.instance_id)

  def add_rows(self, num_rows, num_families, num_columns_per_family):
    cells = []
    for i in range(1, num_rows + 1):
      key = 'key-' + str(i)
      row = DirectRow(key, self.table)
      for j in range(num_families):
        fam_name = 'test_col_fam_' + str(j)
        # create the table's column families one time only
        if i == 1:
          col_fam = self.table.column_family(fam_name)
          col_fam.create()
        for k in range(1, num_columns_per_family + 1):
          row.set_cell(fam_name, f'col-{j}-{k}', f'value-{i}-{j}-{k}')

      # after all mutations on the row are done, commit to Bigtable
      row.commit()
      # read the same row back from Bigtable to get the expected data
      # accumulate rows in `cells`
      read_row: PartialRowData = self.table.read_row(key)
      cells.append(read_row.cells)

    return cells

  def test_read_xlang(self):
    # create rows and retrieve expected cells
    expected_cells = self.add_rows(
        num_rows=5, num_families=3, num_columns_per_family=4)

    with beam.Pipeline(argv=self.args) as p:
      cells = (
          p
          | bigtableio.ReadFromBigtable(
              project_id=self.project,
              instance_id=self.instance.instance_id,
              table_id=self.table.table_id,
              expansion_service=self._transform_service_address)
          | "Extract cells" >> beam.Map(lambda row: row._cells))

      assert_that(cells, equal_to(expected_cells))


@pytest.mark.uses_gcp_java_expansion_service
@pytest.mark.uses_transform_service
@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS') or
    os.environ.get('TRANSFORM_SERVICE_PORT'),
    "A valid expansion service is not available for executing the "
    "cross-language test.")
class TestWriteToBigtableXlangIT(unittest.TestCase):
  # These are integration tests for the cross-language write transform.
  INSTANCE = "bt-write-xlang"
  TABLE_ID = "test-table"

  @classmethod
  def setUpClass(cls):
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.project = cls.test_pipeline.get_option('project')
    cls.args = cls.test_pipeline.get_full_options_as_args()

    instance_id = instance_prefix(cls.INSTANCE)

    cls.client = client.Client(admin=True, project=cls.project)
    # create cluster and instance
    cls.instance = cls.client.instance(
        instance_id,
        display_name=cls.INSTANCE,
        instance_type=instance.Instance.Type.DEVELOPMENT)
    cluster = cls.instance.cluster("test-cluster", "us-central1-a")
    operation = cls.instance.create(clusters=[cluster])
    operation.result(timeout=500)
    _LOGGER.warning(
        "Created instance [%s] in project [%s]",
        cls.instance.instance_id,
        cls.project)

  def setUp(self):
    # create table inside instance
    self.table: Table = self.instance.table(
        '%s-%s-%s' %
        (self.TABLE_ID, str(int(time.time())), secrets.token_hex(3)))
    self.table.create()
    _LOGGER.info("Created table [%s]", self.table.table_id)
    if (os.environ.get('TRANSFORM_SERVICE_PORT')):
      self._transform_service_address = (
          'localhost:' + os.environ.get('TRANSFORM_SERVICE_PORT'))
    else:
      self._transform_service_address = None

  def tearDown(self):
    try:
      _LOGGER.info("Deleting table [%s]", self.table.table_id)
      self.table.delete()
    except HttpError:
      _LOGGER.warning("Failed to clean up table [%s]", self.table.table_id)

  @classmethod
  def tearDownClass(cls):
    try:
      _LOGGER.info("Deleting instance [%s]", cls.instance.instance_id)
      cls.instance.delete()
    except HttpError:
      _LOGGER.warning(
          "Failed to clean up instance [%s]", cls.instance.instance_id)

  def run_pipeline(self, rows):
    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create(rows)
          | bigtableio.WriteToBigTable(
              project_id=self.project,
              instance_id=self.instance.instance_id,
              table_id=self.table.table_id,
              use_cross_language=True,
              expansion_service=self._transform_service_address))

  def test_set_mutation(self):
    row1: DirectRow = DirectRow('key-1')
    row2: DirectRow = DirectRow('key-2')
    col_fam = self.table.column_family('col_fam')
    col_fam.create()
    # expected cells
    row1_col1_cell = Cell(b'val1-1', 100_000_000)
    row1_col2_cell = Cell(b'val1-2', 200_000_000)
    row2_col1_cell = Cell(b'val2-1', 100_000_000)
    row2_col2_cell = Cell(b'val2-2', 200_000_000)
    # When setting this cell, we won't set a timestamp. We expect the timestamp
    # to default to -1, and Bigtable will set it to system time at insertion.
    row2_col1_no_timestamp = Cell(b'val2-2-notimestamp', time.time())
    # rows sent to write transform
    row1.set_cell(
        'col_fam', b'col-1', row1_col1_cell.value, row1_col1_cell.timestamp)
    row1.set_cell(
        'col_fam', b'col-2', row1_col2_cell.value, row1_col2_cell.timestamp)
    row2.set_cell(
        'col_fam', b'col-1', row2_col1_cell.value, row2_col1_cell.timestamp)
    row2.set_cell(
        'col_fam', b'col-2', row2_col2_cell.value, row2_col2_cell.timestamp)
    # don't set a timestamp here. it should default to -1
    row2.set_cell('col_fam', b'col-no-timestamp', row2_col1_no_timestamp.value)

    self.run_pipeline([row1, row2])

    # after write transform executes, get actual rows from table
    actual_row1: PartialRowData = self.table.read_row('key-1')
    actual_row2: PartialRowData = self.table.read_row('key-2')

    # check actual rows match with expected rows (value and timestamp)
    self.assertEqual(
        row1_col1_cell, actual_row1.find_cells('col_fam', b'col-1')[0])
    self.assertEqual(
        row1_col2_cell, actual_row1.find_cells('col_fam', b'col-2')[0])
    self.assertEqual(
        row2_col1_cell, actual_row2.find_cells('col_fam', b'col-1')[0])
    self.assertEqual(
        row2_col2_cell, actual_row2.find_cells('col_fam', b'col-2')[0])

    # check mutation that doesn't have a timestamp set is handled properly:
    self.assertEqual(
        row2_col1_no_timestamp.value,
        actual_row2.find_cells('col_fam', b'col-no-timestamp')[0].value)
    # Bigtable sets timestamp as insertion time, which is later than the
    # time.time() we set when creating this test case
    cell_timestamp = actual_row2.find_cells('col_fam',
                                            b'col-no-timestamp')[0].timestamp
    self.assertTrue(
        row2_col1_no_timestamp.timestamp < cell_timestamp,
        msg="Expected cell with unset timestamp to have ingestion time "
        f"attached, but was {cell_timestamp}")

  def test_delete_cells_mutation(self):
    col_fam = self.table.column_family('col_fam')
    col_fam.create()
    # write a row with two columns to the table beforehand.
    write_row: DirectRow = DirectRow('key-1', self.table)
    write_row.set_cell('col_fam', b'col-1', b'val-1')
    write_row.set_cell('col_fam', b'col-2', b'val-2')
    write_row.commit()

    # prepare a row that will delete cells in one of the columns.
    delete_row: DirectRow = DirectRow('key-1')
    delete_row.delete_cell('col_fam', b'col-1')

    self.run_pipeline([delete_row])

    # after transform executes, get actual row from table
    actual_row: PartialRowData = self.table.read_row('key-1')

    # we deleted all the cells in 'col-1', so this check should throw an error
    with self.assertRaises(KeyError):
      actual_row.find_cells('col_fam', b'col-1')

    # check the cell in col-2 is still there
    col2_cells = actual_row.find_cells('col_fam', b'col-2')
    self.assertEqual(1, len(col2_cells))
    self.assertEqual(b'val-2', col2_cells[0].value)

  def test_delete_cells_with_timerange_mutation(self):
    col_fam = self.table.column_family('col_fam')
    col_fam.create()
    # write two cells in a column to the table beforehand.
    write_row: DirectRow = DirectRow('key-1', self.table)
    write_row.set_cell(
        'col_fam',
        b'col',
        b'val',
        datetime.fromtimestamp(100_000_000, tz=timezone.utc))
    write_row.commit()
    write_row.set_cell(
        'col_fam',
        b'col',
        b'new-val',
        datetime.fromtimestamp(200_000_000, tz=timezone.utc))
    write_row.commit()

    # prepare a row that will delete cells within a timestamp range.
    delete_row: DirectRow = DirectRow('key-1')
    delete_row.delete_cell(
        'col_fam',
        b'col',
        time_range=TimestampRange(
            start=datetime.fromtimestamp(99_999_999, tz=timezone.utc),
            end=datetime.fromtimestamp(100_000_001, tz=timezone.utc)))

    self.run_pipeline([delete_row])

    # after transform executes, get actual row from table
    actual_row: PartialRowData = self.table.read_row('key-1')

    # we deleted one cell within the timestamp range.
    # check the other (newer) cell still exists.
    cells = actual_row.find_cells('col_fam', b'col')
    self.assertEqual(1, len(cells))
    self.assertEqual(b'new-val', cells[0].value)
    self.assertEqual(
        datetime.fromtimestamp(200_000_000, tz=timezone.utc),
        cells[0].timestamp)

  def test_delete_column_family_mutation(self):
    # create two column families
    col_fam = self.table.column_family('col_fam-1')
    col_fam.create()
    col_fam = self.table.column_family('col_fam-2')
    col_fam.create()
    # write a row with values in both column families to the table beforehand.
    write_row: DirectRow = DirectRow('key-1', self.table)
    write_row.set_cell('col_fam-1', b'col', b'val')
    write_row.set_cell('col_fam-2', b'col', b'val')
    write_row.commit()

    # prepare a row that will delete a column family from the row
    delete_row: DirectRow = DirectRow('key-1')
    delete_row.delete_cells('col_fam-1', delete_row.ALL_COLUMNS)

    self.run_pipeline([delete_row])

    # after transform executes, get actual row from table
    actual_row: PartialRowData = self.table.read_row('key-1')

    # we deleted column family 'col_fam-1', so this check should throw an error
    with self.assertRaises(KeyError):
      actual_row.find_cells('col_fam-1', b'col')

    # check we have one column family left with the correct cell value
    self.assertEqual(1, len(actual_row.cells))
    self.assertEqual(b'val', actual_row.cell_value('col_fam-2', b'col'))

  def test_delete_row_mutation(self):
    write_row1: DirectRow = DirectRow('key-1', self.table)
    write_row2: DirectRow = DirectRow('key-2', self.table)
    col_fam = self.table.column_family('col_fam')
    col_fam.create()
    # write a couple of rows to the table beforehand
    write_row1.set_cell('col_fam', b'col', b'val-1')
    write_row1.commit()
    write_row2.set_cell('col_fam', b'col', b'val-2')
    write_row2.commit()

    # prepare a row that will delete itself
    delete_row: DirectRow = DirectRow('key-1')
    delete_row.delete()

    self.run_pipeline([delete_row])

    # after write transform executes, get actual rows from table
    actual_row1: PartialRowData = self.table.read_row('key-1')
    actual_row2: PartialRowData = self.table.read_row('key-2')

    # we deleted row with key 'key-1', check it doesn't exist anymore
    # the Bigtable API doesn't throw an error here, just returns a None value
    self.assertEqual(None, actual_row1)
    # check row 2 exists with the correct cell value in col
    self.assertEqual(b'val-2', actual_row2.cell_value('col_fam', b'col'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
