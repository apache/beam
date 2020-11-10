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
from functools import wraps

from future.utils import iteritems
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None
# pylint: enable=wrong-import-order, wrong-import-position

_LOGGER = logging.getLogger(__name__)


def skip(runners):
  if not isinstance(runners, list):
    runners = [runners]

  def inner(fn):
    @wraps(fn)
    def wrapped(self):
      if self.runner_name in runners:
        self.skipTest(
            'This test doesn\'t work on these runners: {}'.format(runners))
      else:
        return fn(self)

    return wrapped

  return inner


def datetime_to_utc(element):
  for k, v in element.items():
    if isinstance(v, (datetime.time, datetime.date)):
      element[k] = str(v)
    if isinstance(v, datetime.datetime) and v.tzinfo:
      # For datetime objects, we'll
      offset = v.utcoffset()
      utc_dt = (v - offset).strftime('%Y-%m-%d %H:%M:%S.%f UTC')
      element[k] = utc_dt
  return element


class BigQueryReadIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_read_table_'

  @classmethod
  def setUpClass(cls):
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.args = cls.test_pipeline.get_full_options_as_args()
    cls.runner_name = type(cls.test_pipeline.runner).__name__
    cls.project = cls.test_pipeline.get_option('project')

    cls.bigquery_client = BigQueryWrapper()
    cls.dataset_id = '%s%s%d' % (
        cls.BIG_QUERY_DATASET_ID,
        str(int(time.time())),
        random.randint(0, 10000))
    cls.bigquery_client.get_or_create_dataset(cls.project, cls.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", cls.dataset_id, cls.project)

  @classmethod
  def tearDownClass(cls):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=cls.project, datasetId=cls.dataset_id, deleteContents=True)
    try:
      _LOGGER.info(
          "Deleting dataset %s in project %s", cls.dataset_id, cls.project)
      cls.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      _LOGGER.debug(
          'Failed to clean up dataset %s in project %s',
          cls.dataset_id,
          cls.project)


class ReadTests(BigQueryReadIntegrationTests):
  TABLE_DATA = [{
      'number': 1, 'str': 'abc'
  }, {
      'number': 2, 'str': 'def'
  }, {
      'number': 3, 'str': u'你好'
  }, {
      'number': 4, 'str': u'привет'
  }]

  @classmethod
  def setUpClass(cls):
    super(ReadTests, cls).setUpClass()
    cls.table_name = 'python_write_table'
    cls.create_table(cls.table_name)

    table_id = '{}.{}'.format(cls.dataset_id, cls.table_name)
    cls.query = 'SELECT number, str FROM `%s`' % table_id

  @classmethod
  def create_table(cls, table_name):
    table_schema = bigquery.TableSchema()
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'number'
    table_field.type = 'INTEGER'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'str'
    table_field.type = 'STRING'
    table_schema.fields.append(table_field)
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=cls.project, datasetId=cls.dataset_id,
            tableId=table_name),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=cls.project, datasetId=cls.dataset_id, table=table)
    cls.bigquery_client.client.tables.Insert(request)
    cls.bigquery_client.insert_rows(
        cls.project, cls.dataset_id, table_name, cls.TABLE_DATA)

  @skip(['PortableRunner', 'FlinkRunner'])
  @attr('IT')
  def test_native_source(self):
    with beam.Pipeline(argv=self.args) as p:
      result = (
          p | 'read' >> beam.io.Read(
              beam.io.BigQuerySource(query=self.query, use_standard_sql=True)))
      assert_that(result, equal_to(self.TABLE_DATA))

  @attr('IT')
  def test_iobase_source(self):
    query = StaticValueProvider(str, self.query)
    with beam.Pipeline(argv=self.args) as p:
      result = (
          p | 'read with value provider query' >> beam.io.ReadFromBigQuery(
              query=query, use_standard_sql=True, project=self.project))
      assert_that(result, equal_to(self.TABLE_DATA))


class ReadNewTypesTests(BigQueryReadIntegrationTests):
  @classmethod
  def setUpClass(cls):
    super(ReadNewTypesTests, cls).setUpClass()
    cls.table_name = 'python_new_types'
    cls.create_table(cls.table_name)

    table_id = '{}.{}'.format(cls.dataset_id, cls.table_name)
    cls.query = 'SELECT float, numeric, bytes, date, time, datetime,' \
                'timestamp, geo FROM `%s`' % table_id

  @classmethod
  def create_table(cls, table_name):
    table_schema = bigquery.TableSchema()
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'float'
    table_field.type = 'FLOAT'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'numeric'
    table_field.type = 'NUMERIC'
    table_schema.fields.append(table_field)
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
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'datetime'
    table_field.type = 'DATETIME'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'timestamp'
    table_field.type = 'TIMESTAMP'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'geo'
    table_field.type = 'GEOGRAPHY'
    table_schema.fields.append(table_field)
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=cls.project, datasetId=cls.dataset_id,
            tableId=table_name),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=cls.project, datasetId=cls.dataset_id, table=table)
    cls.bigquery_client.client.tables.Insert(request)
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

    table_data = [row_data]
    # add rows with only one key value pair and None values for all other keys
    for key, value in iteritems(row_data):
      table_data.append({key: value})

    cls.bigquery_client.insert_rows(
        cls.project, cls.dataset_id, table_name, table_data)

  def get_expected_data(self, native=True):
    byts = b'\xab\xac'
    expected_row = {
        'float': 0.33,
        'numeric': Decimal('10'),
        'bytes': base64.b64encode(byts) if native else byts,
        'date': '3000-12-31',
        'time': '23:59:59',
        'datetime': '2018-12-31T12:44:31',
        'timestamp': '2018-12-31 12:44:31.744957 UTC',
        'geo': 'POINT(30 10)'
    }

    expected_data = [expected_row]

    # add rows with only one key value pair and None values for all other keys
    for key, value in iteritems(expected_row):
      row = {k: None for k in expected_row}
      row[key] = value
      expected_data.append(row)

    return expected_data

  @skip(['PortableRunner', 'FlinkRunner'])
  @attr('IT')
  def test_native_source(self):
    with beam.Pipeline(argv=self.args) as p:
      result = (
          p
          | 'read' >> beam.io.Read(
              beam.io.BigQuerySource(query=self.query, use_standard_sql=True)))
      assert_that(result, equal_to(self.get_expected_data()))

  @attr('IT')
  def test_iobase_source(self):
    with beam.Pipeline(argv=self.args) as p:
      result = (
          p
          | 'read' >> beam.io.ReadFromBigQuery(
              query=self.query,
              use_standard_sql=True,
              project=self.project,
              bigquery_job_labels={'launcher': 'apache_beam_tests'})
          | beam.Map(datetime_to_utc))
      assert_that(result, equal_to(self.get_expected_data(native=False)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
