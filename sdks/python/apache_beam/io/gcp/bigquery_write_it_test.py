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

"""Unit tests for BigQuery sources and sinks."""
from __future__ import absolute_import

import base64
import datetime
import logging
import random
import time
import unittest

import hamcrest as hc
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.testing.test_pipeline import TestPipeline

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None
# pylint: enable=wrong-import-order, wrong-import-position


class BigQueryWriteIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_write_to_table_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%s%d' % (self.BIG_QUERY_DATASET_ID,
                                  str(int(time.time())),
                                  random.randint(0, 10000))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.{}" % (self.dataset_id)
    logging.info("Created dataset %s in project %s",
                 self.dataset_id, self.project)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id,
        deleteContents=True)
    try:
      logging.info("Deleting dataset %s in project %s",
                   self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      logging.debug('Failed to clean up dataset %s in project %s',
                    self.dataset_id, self.project)

  def create_table(self, tablename):
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
            tableId=tablename),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)

  @attr('IT')
  def test_big_query_write(self):
    output_table = 'python_write_table'
    output_table = self.output_table.format(output_table)

    input_data = [
        {'number': 1, 'str': 'abc'},
        {'number': 2, 'str': 'def'},
    ]
    table_schema = {"fields": [
        {"name": "number", "type": "INTEGER"},
        {"name": "str", "type": "STRING"}]}

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT number, str FROM %s" % output_table,
            data=[(1, 'abc',), (2, 'def',)])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (p | 'create' >> beam.Create(input_data)
       | 'write' >> beam.io.WriteToBigQuery(
           output_table,
           schema=table_schema,
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

  @attr('IT')
  def test_big_query_write_schema_autodetect(self):
    if self.runner_name == 'TestDataflowRunner':
      self.skipTest('DataflowRunner does not support schema autodetection')

    output_table = 'python_write_table'
    output_table = self.output_table.format(output_table)

    input_data = [
        {'number': 1, 'str': 'abc'},
        {'number': 2, 'str': 'def'},
    ]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT number, str FROM %s" % output_table,
            data=[(1, 'abc',), (2, 'def',)])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers),
        experiments='use_beam_bq_sink')

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (p | 'create' >> beam.Create(input_data)
       | 'write' >> beam.io.WriteToBigQuery(
           output_table,
           method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
           schema=beam.io.gcp.bigquery.SCHEMA_AUTODETECT,
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

  @attr('IT')
  def test_big_query_write_new_types(self):
    output_table = 'python_new_types_table'
    output_table = self.output_table.format(output_table)

    input_data = [
        {'bytes': b'xyw', 'date': '2011-01-01', 'time': '23:59:59.999999'},
        {'bytes': b'abc', 'date': '2000-01-01', 'time': '00:00:00'},
        {'bytes': b'\xe4\xbd\xa0\xe5\xa5\xbd', 'date': '3000-12-31',
         'time': '23:59:59'},
        {'bytes': b'\xab\xac\xad', 'date': '2000-01-01', 'time': '00:00:00'}
    ]
    # bigquery io expects bytes to be base64 encoded values
    for row in input_data:
      row['bytes'] = base64.b64encode(row['bytes'])

    table_schema = {"fields": [
        {"name": "bytes", "type": "BYTES"},
        {"name": "date", "type": "DATE"},
        {"name": "time", "type": "TIME"}]}

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT bytes, date, time FROM %s" % output_table,
            data=[(b'xyw', datetime.date(2011, 1, 1),
                   datetime.time(23, 59, 59, 999999), ),
                  (b'abc', datetime.date(2000, 1, 1),
                   datetime.time(0, 0, 0), ),
                  (b'\xe4\xbd\xa0\xe5\xa5\xbd', datetime.date(3000, 12, 31),
                   datetime.time(23, 59, 59), ),
                  (b'\xab\xac\xad', datetime.date(2000, 1, 1),
                   datetime.time(0, 0, 0), )])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (p | 'create' >> beam.Create(input_data)
       | 'write' >> beam.io.WriteToBigQuery(
           output_table,
           schema=table_schema,
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

  @attr('IT')
  def test_big_query_write_without_schema(self):
    output_table = 'python_no_schema_table'
    self.create_table(output_table)
    output_table = self.output_table.format(output_table)

    input_data = [
        {'bytes': b'xyw', 'date': '2011-01-01', 'time': '23:59:59.999999'},
        {'bytes': b'abc', 'date': '2000-01-01', 'time': '00:00:00'},
        {'bytes': b'\xe4\xbd\xa0\xe5\xa5\xbd', 'date': '3000-12-31',
         'time': '23:59:59'},
        {'bytes': b'\xab\xac\xad', 'date': '2000-01-01', 'time': '00:00:00'}
    ]
    # bigquery io expects bytes to be base64 encoded values
    for row in input_data:
      row['bytes'] = base64.b64encode(row['bytes'])

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT bytes, date, time FROM %s" % output_table,
            data=[(b'xyw', datetime.date(2011, 1, 1),
                   datetime.time(23, 59, 59, 999999), ),
                  (b'abc', datetime.date(2000, 1, 1),
                   datetime.time(0, 0, 0), ),
                  (b'\xe4\xbd\xa0\xe5\xa5\xbd', datetime.date(3000, 12, 31),
                   datetime.time(23, 59, 59), ),
                  (b'\xab\xac\xad', datetime.date(2000, 1, 1),
                   datetime.time(0, 0, 0), )])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (p | 'create' >> beam.Create(input_data)
       | 'write' >> beam.io.WriteToBigQuery(
           output_table,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
