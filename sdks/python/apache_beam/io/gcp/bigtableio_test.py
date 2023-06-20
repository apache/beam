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
import logging
import os
import secrets
import string
import time
import unittest
import uuid
from datetime import datetime
from random import choice

import pytest
from mock import MagicMock
from mock import patch

import apache_beam as beam
from apache_beam.internal.metrics.metric import ServiceCallMetric
from apache_beam.io.gcp import bigtableio
from apache_beam.io.gcp import resource_identifiers
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)

# Protect against environments where bigtable library is not available.
try:
  from apitools.base.py.exceptions import HttpError
  from google.cloud.bigtable import client
  from google.cloud.bigtable.instance import Instance
  from google.cloud.bigtable.row import DirectRow, PartialRowData, Cell
  from google.cloud.bigtable.table import Table
  from google.cloud.bigtable_admin_v2.types import instance
  from google.rpc.code_pb2 import OK, ALREADY_EXISTS
  from google.rpc.status_pb2 import Status
except ImportError as e:
  client = None
  HttpError = None


@pytest.mark.uses_gcp_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_PORT'),
    "EXPANSION_PORT environment var is not provided.")
@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
class TestReadFromBigTable(unittest.TestCase):
  INSTANCE = "bt-read-tests"
  TABLE_ID = "test-table"

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.args = self.test_pipeline.get_full_options_as_args()
    self.project = self.test_pipeline.get_option('project')

    instance_id = '%s-%s-%s' % (
        self.INSTANCE, str(int(time.time())), secrets.token_hex(3))

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

  def tearDown(self):
    try:
      _LOGGER.info(
          "Deleting table [%s] and instance [%s]",
          self.table.table_id,
          self.instance.instance_id)
      self.table.delete()
      self.instance.delete()
    except HttpError:
      _LOGGER.debug(
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
              self.table.table_id, self.instance.instance_id, self.project)
          | "Extract cells" >> beam.Map(lambda row: row._cells))

      assert_that(cells, equal_to(expected_cells))


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
