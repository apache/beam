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

"""Integration tests for BigQuery change history streaming source.
"""

import logging
import secrets
import time
import unittest
import uuid

import apache_beam as beam
from apache_beam.io.gcp.bigquery_change_history import ReadBigQueryChangeHistory
from apache_beam.io.gcp.bigquery_change_history import _CleanupTempTablesFn
from apache_beam.io.gcp.bigquery_change_history import _ExecuteQueryFn
from apache_beam.io.gcp.bigquery_change_history import _PollChangeHistoryFn
from apache_beam.io.gcp.bigquery_change_history import _PollConfig
from apache_beam.io.gcp.bigquery_change_history import _QueryRange
from apache_beam.io.gcp.bigquery_change_history import _QueryResult
from apache_beam.io.gcp.bigquery_change_history import _ReadStorageStreamsSDF
from apache_beam.io.gcp.bigquery_change_history import _table_key
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)


class BigQueryChangeHistoryIntegrationBase(unittest.TestCase):
  """Base class for integration tests against real BigQuery.

  Gets project from pipeline options (--project).
  Creates two unique temp datasets per test class:
    - dataset: for change-history-enabled source tables
    - temp_dataset: for pipeline temp tables (query results, etc.)
  Both are deleted with all contents in tearDownClass.
  """
  @classmethod
  def setUpClass(cls):
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.project = cls.test_pipeline.get_option('project')
    cls.args = cls.test_pipeline.get_full_options_as_args()
    cls.bq_wrapper = BigQueryWrapper()
    suffix = secrets.token_hex(4)
    cls.dataset = f'beam_ch_src_{suffix}'
    cls.temp_dataset = f'beam_ch_tmp_{suffix}'
    cls.bq_wrapper.get_or_create_dataset(cls.project, cls.dataset)
    ds = cls.bq_wrapper.client.datasets.Get(
        bigquery.BigqueryDatasetsGetRequest(
            projectId=cls.project, datasetId=cls.dataset))
    cls.location = ds.location
    cls.bq_wrapper.get_or_create_dataset(
        cls.project, cls.temp_dataset, location=cls.location)
    _LOGGER.info(
        'Created datasets: source=%s, temp=%s (location=%s)',
        cls.dataset,
        cls.temp_dataset,
        cls.location)

  @classmethod
  def tearDownClass(cls):
    for dataset in (cls.dataset, cls.temp_dataset):
      try:
        cls.bq_wrapper.client.datasets.Delete(
            bigquery.BigqueryDatasetsDeleteRequest(
                projectId=cls.project, datasetId=dataset, deleteContents=True))
        _LOGGER.info('Deleted dataset %s', dataset)
      except Exception as e:
        _LOGGER.warning('Failed to clean up dataset %s: %s', dataset, e)

  @classmethod
  def _create_temp_table_with_data(cls, table_id, rows, schema=None):
    """Create a table in the temp dataset and insert rows via streaming."""
    if schema is None:
      schema = [('id', 'INTEGER'), ('name', 'STRING'), ('value', 'FLOAT')]
    table_schema = bigquery.TableSchema()
    for field_name, field_type in schema:
      field = bigquery.TableFieldSchema()
      field.name = field_name
      field.type = field_type
      table_schema.fields.append(field)

    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=cls.project, datasetId=cls.temp_dataset,
            tableId=table_id),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=cls.project, datasetId=cls.temp_dataset, table=table)
    cls.bq_wrapper.client.tables.Insert(request)

    # Wait for table to be visible
    cls.bq_wrapper.get_table(cls.project, cls.temp_dataset, table_id)

    if rows:
      cls.bq_wrapper.insert_rows(cls.project, cls.temp_dataset, table_id, rows)
      # Give streaming buffer time to flush
      time.sleep(5)

    return bigquery.TableReference(
        projectId=cls.project, datasetId=cls.temp_dataset, tableId=table_id)

  @classmethod
  def _create_change_history_table(cls, table_id, rows=None):
    """Create a table with enable_change_history via DDL."""
    ddl = (
        f'CREATE TABLE IF NOT EXISTS '
        f'`{cls.project}.{cls.dataset}.{table_id}` '
        f'(id INT64, name STRING, value FLOAT64) '
        f'OPTIONS (enable_change_history = true)')

    job_id = f'beam_ch_ddl_{uuid.uuid4().hex[:8]}'
    reference = bigquery.JobReference(jobId=job_id, projectId=cls.project)
    request = bigquery.BigqueryJobsInsertRequest(
        projectId=cls.project,
        job=bigquery.Job(
            configuration=bigquery.JobConfiguration(
                query=bigquery.JobConfigurationQuery(
                    query=ddl, useLegacySql=False)),
            jobReference=reference))
    response = cls.bq_wrapper._start_job(request)
    cls.bq_wrapper.wait_for_bq_job(response.jobReference, sleep_duration_sec=2)

    # Wait for table to be visible
    cls.bq_wrapper.get_table(cls.project, cls.dataset, table_id)

    if rows:
      cls.bq_wrapper.insert_rows(cls.project, cls.dataset, table_id, rows)
      time.sleep(5)

    return bigquery.TableReference(
        projectId=cls.project, datasetId=cls.dataset, tableId=table_id)


class CleanupTempTablesFnTest(BigQueryChangeHistoryIntegrationBase):
  """Integration tests for _CleanupTempTablesFn against real BigQuery."""
  def test_single_complete_signal_deletes_table(self):
    """A single signal with streams_read == total deletes the temp table."""
    table_id = f'cleanup_test_{secrets.token_hex(4)}'
    table_ref = self._create_temp_table_with_data(
        table_id, [{
            'id': 1, 'name': 'a', 'value': 1.0
        }])
    table_key = _table_key(table_ref)

    # Feed cleanup signal: all 5 streams read out of 5
    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create([(table_key, (5, 5))])
          | beam.ParDo(_CleanupTempTablesFn()))

    # Verify table was deleted
    time.sleep(2)
    with self.assertRaises(Exception):
      self.bq_wrapper.get_table(self.project, self.temp_dataset, table_id)

  def test_partial_signals_then_complete(self):
    """Partial signals don't delete; final signal triggers cleanup."""
    table_id = f'cleanup_partial_{secrets.token_hex(4)}'
    table_ref = self._create_temp_table_with_data(
        table_id, [{
            'id': 1, 'name': 'a', 'value': 1.0
        }])
    table_key = _table_key(table_ref)

    # Feed two partial signals: 3/10 + 7/10 = 10/10
    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create([
              (table_key, (3, 10)),
              (table_key, (7, 10)),
          ])
          | beam.ParDo(_CleanupTempTablesFn()))

    time.sleep(2)
    with self.assertRaises(Exception):
      self.bq_wrapper.get_table(self.project, self.temp_dataset, table_id)


class ReadStorageStreamsSDFTest(BigQueryChangeHistoryIntegrationBase):
  """Integration tests for _ReadStorageStreamsSDF against real BigQuery.

  Tables must include change_timestamp (TIMESTAMP) and change_type (STRING)
  columns to match the schema that _ExecuteQueryFn produces in the real
  pipeline. The Read SDF extracts event timestamps from change_timestamp.
  """
  _READ_SCHEMA = [
      ('id', 'INTEGER'),
      ('name', 'STRING'),
      ('value', 'FLOAT'),
      ('change_timestamp', 'TIMESTAMP'),
      ('change_type', 'STRING'),
  ]

  def test_reads_rows_from_temp_table(self):
    """SDF reads rows from a real temp table via Storage Read API."""
    table_id = f'sdf_test_{secrets.token_hex(4)}'
    now = time.time()
    rows = [
        {
            'id': 1,
            'name': 'alice',
            'value': 10.0,
            'change_timestamp': now,
            'change_type': 'INSERT'
        },
        {
            'id': 2,
            'name': 'bob',
            'value': 20.0,
            'change_timestamp': now,
            'change_type': 'INSERT'
        },
        {
            'id': 3,
            'name': 'charlie',
            'value': 30.0,
            'change_timestamp': now,
            'change_type': 'INSERT'
        },
    ]
    table_ref = self._create_temp_table_with_data(
        table_id, rows, schema=self._READ_SCHEMA)

    query_result = _QueryResult(
        temp_table_ref=table_ref, range_start=now - 60, range_end=now + 60)

    with beam.Pipeline(argv=self.args) as p:
      outputs = (
          p
          | beam.Create([query_result])
          | beam.ParDo(_ReadStorageStreamsSDF()).with_outputs(
              'cleanup', main='rows'))

      # Check that we get 3 rows
      row_count = (
          outputs['rows']
          | 'CountRows' >> beam.combiners.Count.Globally())
      assert_that(row_count, equal_to([3]), label='CheckRowCount')

  def test_cleanup_signal_emitted(self):
    """SDF emits cleanup signal with correct counts."""
    table_id = f'sdf_cleanup_{secrets.token_hex(4)}'
    now = time.time()
    rows = [{
        'id': 1,
        'name': 'a',
        'value': 1.0,
        'change_timestamp': now,
        'change_type': 'INSERT'
    }]
    table_ref = self._create_temp_table_with_data(
        table_id, rows, schema=self._READ_SCHEMA)

    query_result = _QueryResult(
        temp_table_ref=table_ref, range_start=now - 60, range_end=now + 60)

    with beam.Pipeline(argv=self.args) as p:
      outputs = (
          p
          | beam.Create([query_result])
          | beam.ParDo(_ReadStorageStreamsSDF()).with_outputs(
              'cleanup', main='rows'))

      # Verify cleanup signal
      cleanup_table_keys = (
          outputs['cleanup']
          | 'ExtractKey' >> beam.Map(lambda x: x[0]))
      assert_that(
          cleanup_table_keys,
          equal_to([_table_key(table_ref)]),
          label='CheckCleanupKey')

  def test_empty_table(self):
    """Empty table produces 0 rows and cleanup signal."""
    table_id = f'sdf_empty_{secrets.token_hex(4)}'
    now = time.time()
    table_ref = self._create_temp_table_with_data(
        table_id, [], schema=self._READ_SCHEMA)

    query_result = _QueryResult(
        temp_table_ref=table_ref, range_start=now - 60, range_end=now + 60)

    with beam.Pipeline(argv=self.args) as p:
      outputs = (
          p
          | beam.Create([query_result])
          | beam.ParDo(_ReadStorageStreamsSDF()).with_outputs(
              'cleanup', main='rows'))

      row_count = (
          outputs['rows']
          | 'CountRows' >> beam.combiners.Count.Globally())
      assert_that(row_count, equal_to([0]), label='CheckZeroRows')


class PollChangeHistoryFnTest(BigQueryChangeHistoryIntegrationBase):
  """Integration test for _PollChangeHistoryFn in isolation."""
  def test_poll_emits_query_ranges(self):
    """Poll SDF emits _QueryRange elements with valid time ranges."""
    table_str = f'{self.project}:{self.dataset}.nonexistent'
    start_time = time.time() - 120

    config = _PollConfig(start_time=start_time)

    poll_sdf = _PollChangeHistoryFn(
        table=table_str,
        project=self.project,
        change_function='APPENDS',
        buffer_sec=0,
        start_time=start_time,
        stop_time=time.time() + 5,
        poll_interval_sec=60)

    with beam.Pipeline(argv=self.args) as p:
      ranges = (p | beam.Create([config]) | beam.ParDo(poll_sdf))

      # assert_that works directly on unbounded PCollections (no GBK).
      def check_ranges(actual):
        assert len(actual) >= 1, f'Expected >= 1 range, got {len(actual)}'
        for r in actual:
          assert r.chunk_start < r.chunk_end, (
              f'Invalid range: {r.chunk_start} >= {r.chunk_end}')

      assert_that(ranges, check_ranges)


class ExecuteQueryFnTest(BigQueryChangeHistoryIntegrationBase):
  """Integration test for _ExecuteQueryFn in isolation."""
  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls.test_table_id = f'exec_test_{secrets.token_hex(4)}'
    cls.test_table_ref = cls._create_change_history_table(
        cls.test_table_id,
        rows=[
            {
                'id': 1, 'name': 'row1', 'value': 1.0
            },
            {
                'id': 2, 'name': 'row2', 'value': 2.0
            },
        ])
    cls.insert_time = time.time()
    time.sleep(10)

  def test_execute_query_produces_query_result(self):
    """ExecuteQueryFn creates a temp table from a _QueryRange."""
    table_str = f'{self.project}:{self.dataset}.{self.test_table_id}'
    start_time = self.insert_time - 120

    query_range = _QueryRange(chunk_start=start_time, chunk_end=time.time())

    with beam.Pipeline(argv=self.args) as p:
      results = (
          p
          | beam.Create([query_range])
          | beam.ParDo(
              _ExecuteQueryFn(
                  table=table_str,
                  project=self.project,
                  change_function='APPENDS',
                  temp_dataset=self.temp_dataset,
                  location=self.location)))

      result_count = results | beam.combiners.Count.Globally()
      assert_that(result_count, equal_to([1]), label='CheckOneResult')


class EndToEndTest(BigQueryChangeHistoryIntegrationBase):
  """End-to-end test using the public ReadBigQueryChangeHistory API.

  Creates a change-history-enabled table, inserts rows, then runs the
  full pipeline via the public PTransform and verifies rows come through.
  """
  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls.test_table_id = f'e2e_test_{secrets.token_hex(4)}'
    cls.test_table_ref = cls._create_change_history_table(
        cls.test_table_id,
        rows=[
            {
                'id': 1, 'name': 'alice', 'value': 10.0
            },
            {
                'id': 2, 'name': 'bob', 'value': 20.0
            },
            {
                'id': 3, 'name': 'charlie', 'value': 30.0
            },
        ])
    cls.insert_time = time.time()
    # Wait for streaming buffer + change history propagation
    _LOGGER.info('Waiting for streaming buffer to flush...')
    time.sleep(15)

  def test_public_api_reads_inserted_rows(self):
    """ReadBigQueryChangeHistory PTransform with polling SDF."""
    table_str = f'{self.project}:{self.dataset}.{self.test_table_id}'
    start_time = self.insert_time - 120  # 2 min before insert
    stop_time = time.time()

    with beam.Pipeline(argv=self.args) as p:
      rows = (
          p
          | ReadBigQueryChangeHistory(
              table=table_str,
              poll_interval_sec=10,
              start_time=start_time,
              stop_time=stop_time,
              change_function='APPENDS',
              buffer_sec=0,
              project=self.project,
              temp_dataset=self.temp_dataset))

      def check_rows(actual):
        assert len(actual) == 3, f'Expected 3 rows, got {len(actual)}'
        got = sorted([{
            k: v
            for k, v in row.items() if k != 'change_timestamp'
        } for row in actual],
                     key=lambda r: r['id'])
        expected = [
            {
                'id': 1,
                'name': 'alice',
                'value': 10.0,
                'change_type': 'INSERT'
            },
            {
                'id': 2, 'name': 'bob', 'value': 20.0, 'change_type': 'INSERT'
            },
            {
                'id': 3,
                'name': 'charlie',
                'value': 30.0,
                'change_type': 'INSERT'
            },
        ]
        assert got == expected, (
            f'Row mismatch:\n  got:      {got}\n  expected: {expected}')

      assert_that(rows, check_rows)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
