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

from __future__ import absolute_import

import datetime
import logging
import os
import random
import sys
import time
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

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

BIG_QUERY_DATASET_ID = 'python_query_to_table_'
NEW_TYPES_INPUT_TABLE = 'python_new_types_table'
NEW_TYPES_OUTPUT_SCHEMA = (
    '{"fields": [{"name": "bytes","type": "BYTES"},'
    '{"name": "date","type": "DATE"},{"name": "time","type": "TIME"}]}')
NEW_TYPES_OUTPUT_VERIFY_QUERY = ('SELECT date FROM `%s`;')
# There are problems with query time and bytes with current version of bigquery.
NEW_TYPES_OUTPUT_EXPECTED = [
    (datetime.date(2000, 1, 1),),
    (datetime.date(2011, 1, 1),),
    (datetime.date(3000, 12, 31),)]
LEGACY_QUERY = (
    'SELECT * FROM (SELECT "apple" as fruit), (SELECT "orange" as fruit),')
STANDARD_QUERY = (
    'SELECT * FROM (SELECT "apple" as fruit) '
    'UNION ALL (SELECT "orange" as fruit)')
NEW_TYPES_QUERY = (
    'SELECT bytes, date, time FROM [%s.%s]')
DIALECT_OUTPUT_SCHEMA = ('{"fields": [{"name": "fruit","type": "STRING"}]}')
DIALECT_OUTPUT_VERIFY_QUERY = ('SELECT fruit from `%s`;')
DIALECT_OUTPUT_EXPECTED = [(u'apple',), (u'orange',)]
KMS_KEY = 'projects/apache-beam-testing/locations/global/keyRings/beam-it/' \
          'cryptoKeys/test'


class BigQueryQueryToTableIT(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%s%d' % (BIG_QUERY_DATASET_ID, str(int(time.time())),
                                  random.randint(0, 10000))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.output_table" % (self.dataset_id)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id,
        deleteContents=True)
    try:
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      logging.debug('Failed to clean up dataset %s' % self.dataset_id)

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
    table_data = [
        {'bytes':b'xyw=', 'date':'2011-01-01', 'time':'23:59:59.999999'},
        {'bytes':b'abc=', 'date':'2000-01-01', 'time':'00:00:00'},
        {'bytes':b'dec=', 'date':'3000-12-31', 'time':'23:59:59.990000'}
    ]
    self.bigquery_client.insert_rows(
        self.project, self.dataset_id, NEW_TYPES_INPUT_TABLE, table_data)

  @attr('IT')
  def test_big_query_legacy_sql(self):
    verify_query = DIALECT_OUTPUT_VERIFY_QUERY % self.output_table
    expected_checksum = test_utils.compute_hash(DIALECT_OUTPUT_EXPECTED)
    pipeline_verifiers = [PipelineStateMatcher(), BigqueryMatcher(
        project=self.project,
        query=verify_query,
        checksum=expected_checksum)]

    extra_opts = {'query': LEGACY_QUERY,
                  'output': self.output_table,
                  'output_schema': DIALECT_OUTPUT_SCHEMA,
                  'use_standard_sql': False,
                  'on_success_matcher': all_of(*pipeline_verifiers)}
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)

  @attr('IT')
  def test_big_query_standard_sql(self):
    verify_query = DIALECT_OUTPUT_VERIFY_QUERY % self.output_table
    expected_checksum = test_utils.compute_hash(DIALECT_OUTPUT_EXPECTED)
    pipeline_verifiers = [PipelineStateMatcher(), BigqueryMatcher(
        project=self.project,
        query=verify_query,
        checksum=expected_checksum)]

    extra_opts = {'query': STANDARD_QUERY,
                  'output': self.output_table,
                  'output_schema': DIALECT_OUTPUT_SCHEMA,
                  'use_standard_sql': True,
                  'on_success_matcher': all_of(*pipeline_verifiers)}
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)

  # TODO(BEAM-6660): Enable this test when ready.
  @unittest.skip('This test requires BQ Dataflow native source support for ' +
                 'KMS, which is not available yet.')
  @attr('IT')
  def test_big_query_standard_sql_kms_key(self):
    verify_query = DIALECT_OUTPUT_VERIFY_QUERY % self.output_table
    expected_checksum = test_utils.compute_hash(DIALECT_OUTPUT_EXPECTED)
    pipeline_verifiers = [PipelineStateMatcher(), BigqueryMatcher(
        project=self.project,
        query=verify_query,
        checksum=expected_checksum)]
    extra_opts = {'query': STANDARD_QUERY,
                  'output': self.output_table,
                  'output_schema': DIALECT_OUTPUT_SCHEMA,
                  'use_standard_sql': True,
                  'on_success_matcher': all_of(*pipeline_verifiers),
                  'kms_key': KMS_KEY
                 }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)

    table = self.bigquery_client.get_table(
        self.project, self.dataset_id, 'output_table')
    self.assertEqual(KMS_KEY, table.encryptionConfiguration.kmsKeyName)

  @unittest.skipIf(sys.version_info[0] == 3 and
                   os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                   'This test still needs to be fixed on Python 3'
                   'TODO: BEAM-6769')
  @attr('IT')
  def test_big_query_new_types(self):
    expected_checksum = test_utils.compute_hash(NEW_TYPES_OUTPUT_EXPECTED)
    verify_query = NEW_TYPES_OUTPUT_VERIFY_QUERY % self.output_table
    pipeline_verifiers = [PipelineStateMatcher(), BigqueryMatcher(
        project=self.project,
        query=verify_query,
        checksum=expected_checksum)]
    self._setup_new_types_env()
    extra_opts = {
        'query': NEW_TYPES_QUERY % (self.dataset_id, NEW_TYPES_INPUT_TABLE),
        'output': self.output_table,
        'output_schema': NEW_TYPES_OUTPUT_SCHEMA,
        'use_standard_sql': False,
        'on_success_matcher': all_of(*pipeline_verifiers)}
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
