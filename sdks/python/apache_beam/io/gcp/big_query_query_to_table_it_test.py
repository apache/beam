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

"""
Integration test for Google Cloud BigQuery.
"""

# pytype: skip-file

import base64
import datetime
import logging
import secrets
import time
import unittest

import pytest
from hamcrest.core.core.allof import all_of
from tenacity import retry
from tenacity import stop_after_attempt

from apache_beam.io.gcp import big_query_query_to_table_pipeline
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  pass

_LOGGER = logging.getLogger(__name__)

WAIT_UNTIL_FINISH_DURATION_MS = 15 * 60 * 1000

BIG_QUERY_DATASET_ID = 'python_query_to_table_'
NEW_TYPES_INPUT_TABLE = 'python_new_types_table'
NEW_TYPES_OUTPUT_SCHEMA = (
    '{"fields": [{"name": "bytes","type": "BYTES"},'
    '{"name": "date","type": "DATE"},{"name": "time","type": "TIME"}]}')
NEW_TYPES_OUTPUT_VERIFY_QUERY = ('SELECT bytes, date, time FROM `%s`;')
NEW_TYPES_OUTPUT_EXPECTED = [(
    b'xyw',
    datetime.date(2011, 1, 1),
    datetime.time(23, 59, 59, 999999),
),
                             (
                                 b'abc',
                                 datetime.date(2000, 1, 1),
                                 datetime.time(0, 0),
                             ),
                             (
                                 b'\xe4\xbd\xa0\xe5\xa5\xbd',
                                 datetime.date(3000, 12, 31),
                                 datetime.time(23, 59, 59, 990000),
                             ),
                             (
                                 b'\xab\xac\xad',
                                 datetime.date(2000, 1, 1),
                                 datetime.time(0, 0),
                             )]
LEGACY_QUERY = (
    'SELECT * FROM (SELECT "apple" as fruit), (SELECT "orange" as fruit),')
STANDARD_QUERY = (
    'SELECT * FROM (SELECT "apple" as fruit) '
    'UNION ALL (SELECT "orange" as fruit)')
NEW_TYPES_QUERY = ('SELECT bytes, date, time FROM [%s.%s]')
DIALECT_OUTPUT_SCHEMA = ('{"fields": [{"name": "fruit","type": "STRING"}]}')
DIALECT_OUTPUT_VERIFY_QUERY = ('SELECT fruit from `%s`;')
DIALECT_OUTPUT_EXPECTED = [('apple', ), ('orange', )]


class BigQueryQueryToTableIT(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%d%s' % (
        BIG_QUERY_DATASET_ID, int(time.time()), secrets.token_hex(3))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.output_table" % (self.dataset_id)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id, deleteContents=True)
    try:
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      _LOGGER.debug('Failed to clean up dataset %s' % self.dataset_id)

  def _setup_new_types_env(self):
    table_schema = bigquery.TableSchema()
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'bytes'
    table_field.type = 'BYTES'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'date'
    table_field.type = 'DATE'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'time'
    table_field.type = 'TIME'
    table_schema.fields.append(table_field)
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=self.project,
            datasetId=self.dataset_id,
            tableId=NEW_TYPES_INPUT_TABLE),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)
    table_data = [{
        'bytes': b'xyw', 'date': '2011-01-01', 'time': '23:59:59.999999'
    }, {
        'bytes': b'abc', 'date': '2000-01-01', 'time': '00:00:00'
    },
                  {
                      'bytes': b'\xe4\xbd\xa0\xe5\xa5\xbd',
                      'date': '3000-12-31',
                      'time': '23:59:59.990000'
                  },
                  {
                      'bytes': b'\xab\xac\xad',
                      'date': '2000-01-01',
                      'time': '00:00:00'
                  }]
    # the API Tools bigquery client expects byte values to be base-64 encoded
    # TODO https://github.com/apache/beam/issues/19073: upgrade to
    # google-cloud-bigquery which does not require handling the encoding in
    # beam
    for row in table_data:
      row['bytes'] = base64.b64encode(row['bytes']).decode('utf-8')
    passed, errors = self.bigquery_client.insert_rows(
        self.project, self.dataset_id, NEW_TYPES_INPUT_TABLE, table_data)
    self.assertTrue(passed, 'Error in BQ setup: %s' % errors)

  @pytest.mark.it_postcommit
  @retry(reraise=True, stop=stop_after_attempt(3))
  def test_big_query_legacy_sql(self):
    verify_query = DIALECT_OUTPUT_VERIFY_QUERY % self.output_table
    expected_checksum = test_utils.compute_hash(DIALECT_OUTPUT_EXPECTED)
    pipeline_verifiers = [
        PipelineStateMatcher(),
        BigqueryMatcher(
            project=self.project,
            query=verify_query,
            checksum=expected_checksum)
    ]

    extra_opts = {
        'query': LEGACY_QUERY,
        'output': self.output_table,
        'output_schema': DIALECT_OUTPUT_SCHEMA,
        'use_standard_sql': False,
        'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION_MS,
        'on_success_matcher': all_of(*pipeline_verifiers),
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)

  @pytest.mark.it_postcommit
  def test_big_query_standard_sql(self):
    verify_query = DIALECT_OUTPUT_VERIFY_QUERY % self.output_table
    expected_checksum = test_utils.compute_hash(DIALECT_OUTPUT_EXPECTED)
    pipeline_verifiers = [
        PipelineStateMatcher(),
        BigqueryMatcher(
            project=self.project,
            query=verify_query,
            checksum=expected_checksum)
    ]

    extra_opts = {
        'query': STANDARD_QUERY,
        'output': self.output_table,
        'output_schema': DIALECT_OUTPUT_SCHEMA,
        'use_standard_sql': True,
        'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION_MS,
        'on_success_matcher': all_of(*pipeline_verifiers),
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)

  @pytest.mark.it_postcommit
  def test_big_query_new_types(self):
    expected_checksum = test_utils.compute_hash(NEW_TYPES_OUTPUT_EXPECTED)
    verify_query = NEW_TYPES_OUTPUT_VERIFY_QUERY % self.output_table
    pipeline_verifiers = [
        PipelineStateMatcher(),
        BigqueryMatcher(
            project=self.project,
            query=verify_query,
            checksum=expected_checksum)
    ]
    self._setup_new_types_env()
    extra_opts = {
        'query': NEW_TYPES_QUERY % (self.dataset_id, NEW_TYPES_INPUT_TABLE),
        'output': self.output_table,
        'output_schema': NEW_TYPES_OUTPUT_SCHEMA,
        'use_standard_sql': False,
        'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION_MS,
        'use_json_exports': True,
        'on_success_matcher': all_of(*pipeline_verifiers)
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)

  @pytest.mark.it_postcommit
  def test_big_query_new_types_avro(self):
    expected_checksum = test_utils.compute_hash(NEW_TYPES_OUTPUT_EXPECTED)
    verify_query = NEW_TYPES_OUTPUT_VERIFY_QUERY % self.output_table
    pipeline_verifiers = [
        PipelineStateMatcher(),
        BigqueryMatcher(
            project=self.project,
            query=verify_query,
            checksum=expected_checksum)
    ]
    self._setup_new_types_env()
    extra_opts = {
        'query': NEW_TYPES_QUERY % (self.dataset_id, NEW_TYPES_INPUT_TABLE),
        'output': self.output_table,
        'output_schema': NEW_TYPES_OUTPUT_SCHEMA,
        'use_standard_sql': False,
        'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION_MS,
        'on_success_matcher': all_of(*pipeline_verifiers),
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
