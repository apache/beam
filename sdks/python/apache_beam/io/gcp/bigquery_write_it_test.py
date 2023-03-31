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

import base64
import datetime
import logging
import os
import secrets
import time
import unittest
from decimal import Decimal

import hamcrest as hc
import mock
import pytest
import pytz
from hamcrest.core import assert_that as hamcrest_assert
from parameterized import param
from parameterized import parameterized

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryWriteFn
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.bigquery_tools import FileFormat
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp

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
    self.dataset_id = '%s%d%s' % (
        self.BIG_QUERY_DATASET_ID, int(time.time()), secrets.token_hex(3))
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
    table_field.name = 'int64'
    table_field.type = 'INT64'
    table_field.mode = 'REQUIRED'
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
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=self.project,
            datasetId=self.dataset_id,
            tableId=table_name),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)

  @pytest.mark.it_postcommit
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

  @pytest.mark.it_postcommit
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

  @pytest.mark.it_postcommit
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
    for key, value in row_data.items():
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

  @pytest.mark.it_postcommit
  def test_big_query_write_without_schema(self):
    table_name = 'python_no_schema_table'
    self.create_table(table_name)
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    input_data = [{
        'int64': 1,
        'bytes': b'xyw',
        'date': '2011-01-01',
        'time': '23:59:59.999999'
    },
                  {
                      'int64': 2,
                      'bytes': b'abc',
                      'date': '2000-01-01',
                      'time': '00:00:00'
                  },
                  {
                      'int64': 3,
                      'bytes': b'\xe4\xbd\xa0\xe5\xa5\xbd',
                      'date': '3000-12-31',
                      'time': '23:59:59'
                  },
                  {
                      'int64': 4,
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
            query="SELECT int64, bytes, date, time FROM %s" % table_id,
            data=[(
                1,
                b'xyw',
                datetime.date(2011, 1, 1),
                datetime.time(23, 59, 59, 999999),
            ),
                  (
                      2,
                      b'abc',
                      datetime.date(2000, 1, 1),
                      datetime.time(0, 0, 0),
                  ),
                  (
                      3,
                      b'\xe4\xbd\xa0\xe5\xa5\xbd',
                      datetime.date(3000, 12, 31),
                      datetime.time(23, 59, 59),
                  ),
                  (
                      4,
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

  @pytest.mark.it_postcommit
  def test_big_query_write_insert_errors_reporting(self):
    """
    Test that errors returned by beam.io.WriteToBigQuery
    contain both the failed rows amd the reason for it failing.
    """
    table_name = 'python_write_table'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    input_data = [{
        'number': 1,
        'str': 'some_string',
    }, {
        'number': 2
    },
                  {
                      'number': 3,
                      'str': 'some_string',
                      'additional_field_str': 'some_string',
                  }]

    table_schema = {
        "fields": [{
            "name": "number", "type": "INTEGER", 'mode': 'REQUIRED'
        }, {
            "name": "str", "type": "STRING", 'mode': 'REQUIRED'
        }]
    }

    bq_result_errors = [(
        {
            "number": 2
        },
        [{
            "reason": "invalid",
            "location": "",
            "debugInfo": "",
            "message": "Missing required field: Msg_0_CLOUD_QUERY_TABLE.str."
        }],
    ),
                        ({
                            "number": 3,
                            "str": "some_string",
                            "additional_field_str": "some_string"
                        },
                         [{
                             "reason": "invalid",
                             "location": "additional_field_str",
                             "debugInfo": "",
                             "message": "no such field: additional_field_str."
                         }])]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT number, str FROM %s" % table_id,
            data=[(1, 'some_string')]),
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      errors = (
          p | 'create' >> beam.Create(input_data)
          | 'write' >> beam.io.WriteToBigQuery(
              table_id,
              schema=table_schema,
              method='STREAMING_INSERTS',
              insert_retry_strategy='RETRY_NEVER',
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

      assert_that(
          errors[BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS]
          | 'ParseErrors' >> beam.Map(lambda err: (err[1], err[2])),
          equal_to(bq_result_errors))

  @pytest.mark.it_postcommit
  @parameterized.expand([
      param(file_format=FileFormat.AVRO),
      param(file_format=FileFormat.JSON),
      param(file_format=None),
  ])
  @mock.patch(
      "apache_beam.io.gcp.bigquery_file_loads._MAXIMUM_SOURCE_URIS", new=1)
  def test_big_query_write_temp_table_append_schema_update(self, file_format):
    """
    Test that nested schema update options and schema relaxation
    are respected when appending to an existing table via temporary tables.

    _MAXIMUM_SOURCE_URIS and max_file_size are both set to 1 to force multiple
    load jobs and usage of temporary tables.
    """
    table_name = 'python_append_schema_update'
    self.create_table(table_name)
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    # bytes, date, time fields are optional and omitted in the test
    # only required and new columns are specified
    table_schema = {
        "fields": [{
            "name": "int64",
            "type": "INT64",
            "mode": "NULLABLE",
        }, {
            "name": "bool",
            "type": "BOOL",
        },
                   {
                       "name": "nested_field",
                       "type": "RECORD",
                       "mode": "REPEATED",
                       "fields": [
                           {
                               "name": "fruit",
                               "type": "STRING",
                               "mode": "NULLABLE"
                           },
                       ]
                   }]
    }
    input_data = [{
        "int64": 1, "bool": True, "nested_field": [{
            "fruit": "Apple"
        }]
    }, {
        "bool": False, "nested_field": [{
            "fruit": "Mango"
        }]
    },
                  {
                      "int64": None,
                      "bool": True,
                      "nested_field": [{
                          "fruit": "Banana"
                      }]
                  }]
    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=BigqueryFullResultMatcher(
            project=self.project,
            query="""
            SELECT bytes, date, time, int64, bool, fruit
            FROM {},
            UNNEST(nested_field) as nested_field
            ORDER BY fruit
            """.format(table_id),
            data=[(None, None, None, 1, True,
                   "Apple"), (None, None, None, None, True, "Banana"), (
                       None, None, None, None, False, "Mango")]))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (
          p | 'create' >> beam.Create(input_data)
          | 'write' >> beam.io.WriteToBigQuery(
              table_id,
              schema=table_schema,
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
              max_file_size=1,  # bytes
              method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
              additional_bq_parameters={
                  'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION',
                                          'ALLOW_FIELD_RELAXATION']},
              temp_file_format=file_format))


class BigQueryXlangStorageWriteIT(unittest.TestCase):
  BIGQUERY_DATASET = 'python_xlang_storage_write'

  ELEMENTS = [
      # (int, float, numeric, string, bool, bytes, timestamp)
      {
          "int": 1,
          "float": 0.1,
          "numeric": Decimal("1.11"),
          "str": "a",
          "bool": True,
          "bytes": b'a',
          "timestamp": Timestamp(1000, 100)
      },
      {
          "int": 2,
          "float": 0.2,
          "numeric": Decimal("2.22"),
          "str": "b",
          "bool": False,
          "bytes": b'b',
          "timestamp": Timestamp(2000, 200)
      },
      {
          "int": 3,
          "float": 0.3,
          "numeric": Decimal("3.33"),
          "str": "c",
          "bool": True,
          "bytes": b'd',
          "timestamp": Timestamp(3000, 300)
      },
      {
          "int": 4,
          "float": 0.4,
          "numeric": Decimal("4.44"),
          "str": "d",
          "bool": False,
          "bytes": b'd',
          "timestamp": Timestamp(4000, 400)
      }
  ]

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.args = self.test_pipeline.get_full_options_as_args()
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%s%s' % (
        self.BIGQUERY_DATASET, str(int(time.time())), secrets.token_hex(3))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", self.dataset_id, self.project)
    if not os.environ.get('EXPANSION_PORT'):
      raise ValueError("NO EXPANSION PORT")
    else:
      _LOGGER.info("expansion port: %s", os.environ.get('EXPANSION_PORT'))
    self.expansion_service = ('localhost:%s' % os.environ.get('EXPANSION_PORT'))

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

  def parse_expected_data(self, expected_elements):
    data = []
    for row in expected_elements:
      values = list(row.values())
      for i, val in enumerate(values):
        if isinstance(val, Timestamp):
          # BigQuery matcher query returns a datetime.datetime object
          values[i] = val.to_utc_datetime().replace(
              tzinfo=datetime.timezone.utc)
      data.append(tuple(values))

    return data

  def storage_write_test(self, table_name, items, schema):
    table_id = '{}:{}.{}'.format(self.project, self.dataset_id, table_name)

    bq_matcher = BigqueryFullResultMatcher(
        project=self.project,
        query="SELECT * FROM %s" % '{}.{}'.format(self.dataset_id, table_name),
        data=self.parse_expected_data(items))

    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create(items)
          | beam.io.WriteToBigQuery(
              table=table_id,
              method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
              schema=schema,
              expansion_service=self.expansion_service))
    hamcrest_assert(p, bq_matcher)

  @pytest.mark.uses_gcp_java_expansion_service
  def test_storage_write_all_types(self):
    table_name = "python_storage_write_all_types"
    schema = (
        "int:INTEGER,float:FLOAT,numeric:NUMERIC,str:STRING,"
        "bool:BOOLEAN,bytes:BYTES,timestamp:TIMESTAMP")
    self.storage_write_test(table_name, self.ELEMENTS, schema)

  @pytest.mark.uses_gcp_java_expansion_service
  def test_storage_write_nested_records_and_lists(self):
    table_name = "python_storage_write_nested_records_and_lists"
    schema = {
        "fields": [{
            "name": "repeated_int", "type": "INTEGER", "mode": "REPEATED"
        },
                   {
                       "name": "struct",
                       "type": "STRUCT",
                       "fields": [{
                           "name": "nested_int", "type": "INTEGER"
                       }, {
                           "name": "nested_str", "type": "STRING"
                       }]
                   },
                   {
                       "name": "repeated_struct",
                       "type": "STRUCT",
                       "mode": "REPEATED",
                       "fields": [{
                           "name": "nested_numeric", "type": "NUMERIC"
                       }, {
                           "name": "nested_bytes", "type": "BYTES"
                       }]
                   }]
    }
    items = [{
        "repeated_int": [1, 2, 3],
        "struct": {
            "nested_int": 1, "nested_str": "a"
        },
        "repeated_struct": [{
            "nested_numeric": Decimal("1.23"), "nested_bytes": b'a'
        },
                            {
                                "nested_numeric": Decimal("3.21"),
                                "nested_bytes": b'aa'
                            }]
    }]

    self.storage_write_test(table_name, items, schema)

  @pytest.mark.uses_gcp_java_expansion_service
  def test_storage_write_beam_rows(self):
    table_id = '{}:{}.python_xlang_storage_write_beam_rows'.format(
        self.project, self.dataset_id)

    row_elements = [
        beam.Row(
            my_int=e['int'],
            my_float=e['float'],
            my_numeric=e['numeric'],
            my_string=e['str'],
            my_bool=e['bool'],
            my_bytes=e['bytes'],
            my_timestamp=e['timestamp']) for e in self.ELEMENTS
    ]

    bq_matcher = BigqueryFullResultMatcher(
        project=self.project,
        query="SELECT * FROM %s" %
        '{}.python_xlang_storage_write_beam_rows'.format(self.dataset_id),
        data=self.parse_expected_data(self.ELEMENTS))

    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create(row_elements)
          | beam.io.StorageWriteToBigQuery(
              table=table_id, expansion_service=self.expansion_service))
    hamcrest_assert(p, bq_matcher)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
