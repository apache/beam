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

import logging
import string
import unittest
import uuid
# pytype: skip-file
from datetime import datetime
from datetime import timezone
from random import choice

from mock import MagicMock
from mock import patch

import apache_beam as beam
from apache_beam.internal.metrics.metric import ServiceCallMetric
from apache_beam.io.gcp import bigtableio
from apache_beam.io.gcp import resource_identifiers
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metric import Lineage
from apache_beam.testing.test_pipeline import TestPipeline

_LOGGER = logging.getLogger(__name__)

# Protect against environments where bigtable library is not available.
try:
  from google.cloud.bigtable import client
  from google.cloud.bigtable.batcher import MutationsBatcher
  from google.cloud.bigtable.row_filters import TimestampRange
  from google.cloud.bigtable.instance import Instance
  from google.cloud.bigtable.row import DirectRow, PartialRowData, Cell
  from google.cloud.bigtable.table import Table
  from google.rpc.code_pb2 import OK, ALREADY_EXISTS
  from google.rpc.status_pb2 import Status
except ImportError as e:
  client = None


@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
class TestBeamRowToPartialRowData(unittest.TestCase):
  # Beam Row schema:
  # - key: bytes
  # - column_families: dict[str, dict[str, list[beam.Row(cell_schema)]]]
  # cell_schema:
  # - value: bytes
  # - timestamp_micros: int

  def test_beam_row_to_bigtable_row(self):
    # create test beam row
    families = {
        'family_1': {
            'col_1': [
                beam.Row(value=b'a-1', timestamp_micros=100_000_000),
                beam.Row(value=b'b-1', timestamp_micros=200_000_000),
                beam.Row(value=b'c-1', timestamp_micros=300_000_000)
            ],
            'col_2': [
                beam.Row(value=b'a-2', timestamp_micros=400_000_000),
                beam.Row(value=b'b-2', timestamp_micros=500_000_000),
                beam.Row(value=b'c-2', timestamp_micros=600_000_000)
            ],
        },
        'family_2': {
            'column_qualifier': [
                beam.Row(value=b'val-1', timestamp_micros=700_000_000),
                beam.Row(value=b'val-2', timestamp_micros=800_000_000),
                beam.Row(value=b'val-3', timestamp_micros=900_000_000)
            ]
        }
    }
    beam_row = beam.Row(key=b'key', column_families=families)

    # get equivalent bigtable row
    doFn = bigtableio.ReadFromBigtable._BeamRowToPartialRowData()
    bigtable_row: PartialRowData = next(doFn.process(beam_row))

    # using bigtable utils (PartialRowData methods), check that beam row data
    # landed in the right cells
    self.assertEqual(beam_row.key, bigtable_row.row_key)
    self.assertEqual([
        Cell(c.value, c.timestamp_micros)
        for c in beam_row.column_families['family_1']['col_1']
    ],
                     bigtable_row.find_cells('family_1', b'col_1'))
    self.assertEqual([
        Cell(c.value, c.timestamp_micros)
        for c in beam_row.column_families['family_1']['col_2']
    ],
                     bigtable_row.find_cells('family_1', b'col_2'))
    self.assertEqual([
        Cell(c.value, c.timestamp_micros)
        for c in beam_row.column_families['family_2']['column_qualifier']
    ],
                     bigtable_row.find_cells('family_2', b'column_qualifier'))


@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
class TestBigtableDirectRowToBeamRow(unittest.TestCase):
  doFn = bigtableio.WriteToBigTable._DirectRowMutationsToBeamRow()

  def test_set_cell(self):
    # create some set cell mutations
    direct_row: DirectRow = DirectRow('key-1')
    direct_row.set_cell(
        'col_fam',
        b'col',
        b'a',
        datetime.fromtimestamp(100_000).replace(tzinfo=timezone.utc))
    direct_row.set_cell(
        'col_fam',
        b'other-col',
        b'b',
        datetime.fromtimestamp(200_000).replace(tzinfo=timezone.utc))
    direct_row.set_cell(
        'other_col_fam',
        b'col',
        b'c',
        datetime.fromtimestamp(300_000).replace(tzinfo=timezone.utc))

    # get equivalent beam row
    beam_row = next(self.doFn.process(direct_row))

    # sort both lists of mutations for convenience
    beam_row_mutations = sorted(beam_row.mutations, key=lambda m: m['value'])
    bt_row_mutations = sorted(
        direct_row._get_mutations(), key=lambda m: m.set_cell.value)
    self.assertEqual(beam_row.key, direct_row.row_key)
    self.assertEqual(len(beam_row_mutations), len(bt_row_mutations))

    # check that the values in each beam mutation is equal to the original
    # Bigtable direct row mutations
    for i in range(len(beam_row_mutations)):
      beam_mutation = beam_row_mutations[i]
      bt_mutation = bt_row_mutations[i].set_cell

      self.assertEqual(beam_mutation['type'], b'SetCell')
      self.assertEqual(
          beam_mutation['family_name'].decode(), bt_mutation.family_name)
      self.assertEqual(
          beam_mutation['column_qualifier'], bt_mutation.column_qualifier)
      self.assertEqual(beam_mutation['value'], bt_mutation.value)
      self.assertEqual(
          int.from_bytes(beam_mutation['timestamp_micros'], 'big'),
          bt_mutation.timestamp_micros)

  def test_delete_cells(self):
    # create some delete cell mutations. one with a timestamp range
    direct_row: DirectRow = DirectRow('key-1')
    direct_row.delete_cell('col_fam', b'col-1')
    direct_row.delete_cell(
        'other_col_fam',
        b'col-2',
        time_range=TimestampRange(
            start=datetime.fromtimestamp(10_000_000, tz=timezone.utc)))
    direct_row.delete_cells(
        'another_col_fam', [b'col-3', b'col-4', b'col-5'],
        time_range=TimestampRange(
            start=datetime.fromtimestamp(50_000_000, tz=timezone.utc),
            end=datetime.fromtimestamp(100_000_000, tz=timezone.utc)))

    # get equivalent beam row
    beam_row = next(self.doFn.process(direct_row))

    # sort both lists of mutations for convenience
    beam_row_mutations = sorted(
        beam_row.mutations, key=lambda m: m['column_qualifier'])
    bt_row_mutations = sorted(
        direct_row._get_mutations(),
        key=lambda m: m.delete_from_column.column_qualifier)
    self.assertEqual(beam_row.key, direct_row.row_key)
    self.assertEqual(len(beam_row_mutations), len(bt_row_mutations))

    # check that the values in each beam mutation is equal to the original
    # Bigtable direct row mutations
    for i in range(len(beam_row_mutations)):
      beam_mutation = beam_row_mutations[i]
      bt_mutation = bt_row_mutations[i].delete_from_column
      print(bt_mutation)

      self.assertEqual(beam_mutation['type'], b'DeleteFromColumn')
      self.assertEqual(
          beam_mutation['family_name'].decode(), bt_mutation.family_name)
      self.assertEqual(
          beam_mutation['column_qualifier'], bt_mutation.column_qualifier)

      # check we set a timestamp range only when appropriate
      if bt_mutation.time_range.start_timestamp_micros:
        self.assertEqual(
            int.from_bytes(beam_mutation['start_timestamp_micros'], 'big'),
            bt_mutation.time_range.start_timestamp_micros)
      else:
        self.assertTrue('start_timestamp_micros' not in beam_mutation)

      if bt_mutation.time_range.end_timestamp_micros:
        self.assertEqual(
            int.from_bytes(beam_mutation['end_timestamp_micros'], 'big'),
            bt_mutation.time_range.end_timestamp_micros)
      else:
        self.assertTrue('end_timestamp_micros' not in beam_mutation)

  def test_delete_column_family(self):
    # create mutation to delete column family
    direct_row: DirectRow = DirectRow('key-1')
    direct_row.delete_cells('col_fam-1', direct_row.ALL_COLUMNS)
    direct_row.delete_cells('col_fam-2', direct_row.ALL_COLUMNS)

    # get equivalent beam row
    beam_row = next(self.doFn.process(direct_row))

    # sort both lists of mutations for convenience
    beam_row_mutations = sorted(
        beam_row.mutations, key=lambda m: m['family_name'])
    bt_row_mutations = sorted(
        direct_row._get_mutations(),
        key=lambda m: m.delete_from_column.family_name)
    self.assertEqual(beam_row.key, direct_row.row_key)
    self.assertEqual(len(beam_row_mutations), len(bt_row_mutations))

    # check that the values in each beam mutation is equal to the original
    # Bigtable direct row mutations
    for i in range(len(beam_row_mutations)):
      beam_mutation = beam_row_mutations[i]
      bt_mutation = bt_row_mutations[i].delete_from_family

      self.assertEqual(beam_mutation['type'], b'DeleteFromFamily')
      self.assertEqual(
          beam_mutation['family_name'].decode(), bt_mutation.family_name)

  def test_delete_row(self):
    # create mutation to delete the Bigtable row
    direct_row: DirectRow = DirectRow('key-1')
    direct_row.delete()

    # get equivalent beam row
    beam_row = next(self.doFn.process(direct_row))
    self.assertEqual(beam_row.key, direct_row.row_key)

    beam_mutation = beam_row.mutations[0]
    self.assertEqual(beam_mutation['type'], b'DeleteFromRow')


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

  def test_write(self):
    direct_rows = [self.generate_row(i) for i in range(5)]
    with patch.object(MutationsBatcher, 'mutate'), \
      patch.object(MutationsBatcher, 'close'), TestPipeline() as p:
      _ = p | beam.Create(direct_rows) | bigtableio.WriteToBigTable(
          self._PROJECT_ID, self._INSTANCE_ID, self._TABLE_ID)
    self.assertSetEqual(
        Lineage.query(p.result.metrics(), Lineage.SINK),
        set([
            f"bigtable:{self._PROJECT_ID}.{self._INSTANCE_ID}.{self._TABLE_ID}"
        ]))

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
