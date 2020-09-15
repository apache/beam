#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
# pytype: skip-file

from __future__ import absolute_import

import base64
import datetime
import logging
import random
import time
import unittest
from decimal import Decimal

import hamcrest as hc
import pytz
from future.utils import iteritems
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.bigquery_tools import FileFormat
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

_LOGGER = logging.getLogger(__name__)


class BigQueryWriteIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_write_to_table_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%s%d' % (
        self.BIG_QUERY_DATASET_ID,
        str(int(time.time())),
        random.randint(0, 10000))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", self.dataset_id, self.project)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id, deleteContents=True)
    try:
      _LOGGER.info(
          "Deleting dataset %s in project %s", self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      _LOGGER.debug(
          'Failed to clean up dataset %s in project %s',
          self.dataset_id,
          self.project)

  def create_table(self, table_name):
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
            tableId=table_name),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)

  @attr('IT')
  def test_big_query_write(self):
    table_name = 'python_write_table'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    input_data = [
        {
            'number': 1, 'str': 'abc'
        },
        {
            'number': 2, 'str': 'def'
        },
        {
            'number': 3, 'str': u'你好'
        },
        {
            'number': 4, 'str': u'привет'
        },
    ]
    table_schema = {
        "fields": [{
            "name": "number", "type": "INTEGER"
        }, {
            "name": "str", "type": "STRING"
        }]
    }

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT number, str FROM %s" % table_id,
            data=[(
                1,
                'abc',
            ), (
                2,
                'def',
            ), (
                3,
                u'你好',
            ), (
                4,
                u'привет',
            )])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (
          p | 'create' >> beam.Create(input_data)
          | 'write' >> beam.io.WriteToBigQuery(
              table_id,
              schema=table_schema,
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

  @attr('IT')
  def test_big_query_write_schema_autodetect(self):
    if self.runner_name == 'TestDataflowRunner':
      self.skipTest('DataflowRunner does not support schema autodetection')

    table_name = 'python_write_table'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    input_data = [
        {
            'number': 1, 'str': 'abc'
        },
        {
            'number': 2, 'str': 'def'
        },
    ]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT number, str FROM %s" % table_id,
            data=[(
                1,
                'abc',
            ), (
                2,
                'def',
            )])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (
          p | 'create' >> beam.Create(input_data)
          | 'write' >> beam.io.WriteToBigQuery(
              table_id,
              method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
              schema=beam.io.gcp.bigquery.SCHEMA_AUTODETECT,
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
              temp_file_format=FileFormat.JSON))

  @attr('IT')
  def test_big_query_write_new_types(self):
    table_name = 'python_new_types_table'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    row_data = {
        'float': 0.33,
        'numeric': Decimal('10'),
        'bytes': base64.b64encode(b'\xab\xac').decode('utf-8'),
        'date': '3000-12-31',
        'time': '23:59:59',
        'datetime': '2018-12-31T12:44:31',
        'timestamp': '2018-12-31 12:44:31.744957 UTC',
        'geo': 'POINT(30 10)'
    }

    input_data = [row_data]
    # add rows with only one key value pair and None values for all other keys
    for key, value in iteritems(row_data):
      input_data.append({key: value})

    table_schema = {
        "fields": [{
            "name": "float", "type": "FLOAT"
        }, {
            "name": "numeric", "type": "NUMERIC"
        }, {
            "name": "bytes", "type": "BYTES"
        }, {
            "name": "date", "type": "DATE"
        }, {
            "name": "time", "type": "TIME"
        }, {
            "name": "datetime", "type": "DATETIME"
        }, {
            "name": "timestamp", "type": "TIMESTAMP"
        }, {
            "name": "geo", "type": "GEOGRAPHY"
        }]
    }

    expected_row = (
        0.33,
        Decimal('10'),
        b'\xab\xac',
        datetime.date(3000, 12, 31),
        datetime.time(23, 59, 59),
        datetime.datetime(2018, 12, 31, 12, 44, 31),
        datetime.datetime(2018, 12, 31, 12, 44, 31, 744957, tzinfo=pytz.utc),
        'POINT(30 10)',
    )

    expected_data = [expected_row]

    # add rows with only one key value pair and None values for all other keys
    for i, value in enumerate(expected_row):
      row = [None] * len(expected_row)
      row[i] = value
      expected_data.append(tuple(row))

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query='SELECT float, numeric, bytes, date, time, datetime,'
            'timestamp, geo FROM %s' % table_id,
            data=expected_data)
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (
          p | 'create' >> beam.Create(input_data)
          | 'write' >> beam.io.WriteToBigQuery(
              table_id,
              schema=table_schema,
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

  @attr('IT')
  def test_big_query_write_without_schema(self):
    table_name = 'python_no_schema_table'
    self.create_table(table_name)
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    input_data = [{
        'bytes': b'xyw', 'date': '2011-01-01', 'time': '23:59:59.999999'
    }, {
        'bytes': b'abc', 'date': '2000-01-01', 'time': '00:00:00'
    },
                  {
                      'bytes': b'\xe4\xbd\xa0\xe5\xa5\xbd',
                      'date': '3000-12-31',
                      'time': '23:59:59'
                  },
                  {
                      'bytes': b'\xab\xac\xad',
                      'date': '2000-01-01',
                      'time': '00:00:00'
                  }]
    # bigquery io expects bytes to be base64 encoded values
    for row in input_data:
      row['bytes'] = base64.b64encode(row['bytes'])

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT bytes, date, time FROM %s" % table_id,
            data=[(
                b'xyw',
                datetime.date(2011, 1, 1),
                datetime.time(23, 59, 59, 999999),
            ), (
                b'abc',
                datetime.date(2000, 1, 1),
                datetime.time(0, 0, 0),
            ),
                  (
                      b'\xe4\xbd\xa0\xe5\xa5\xbd',
                      datetime.date(3000, 12, 31),
                      datetime.time(23, 59, 59),
                  ),
                  (
                      b'\xab\xac\xad',
                      datetime.date(2000, 1, 1),
                      datetime.time(0, 0, 0),
                  )])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (
          p | 'create' >> beam.Create(input_data)
          | 'write' >> beam.io.WriteToBigQuery(
              table_id,
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
              temp_file_format=FileFormat.JSON))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
