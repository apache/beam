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

import datetime
import decimal
import gc
import json
import logging
import os
import pickle
import re
import secrets
import time
import unittest
import uuid

import hamcrest as hc
import mock
import pytest
import pytz
import requests
from parameterized import param
from parameterized import parameterized

import apache_beam as beam
from apache_beam.internal import pickler
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.io.filebasedsink_test import _TestCaseWithTempDirCleanUp
from apache_beam.io.gcp import bigquery as beam_bq
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.bigquery import TableRowJsonCoder
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import _StreamToBigQuery
from apache_beam.io.gcp.bigquery_file_loads_test import _ELEMENTS
from apache_beam.io.gcp.bigquery_read_internal import _JsonToDictCoder
from apache_beam.io.gcp.bigquery_read_internal import bigquery_export_destination_uri
from apache_beam.io.gcp.bigquery_tools import JSON_COMPLIANCE_ERROR
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.internal.clients.bigquery import bigquery_v2_client
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.tests import utils
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultStreamingMatcher
from apache_beam.io.gcp.tests.bigquery_matcher import BigQueryTableMatcher
from apache_beam.options import value_provider
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.runners.dataflow.test_dataflow_runner import TestDataflowRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position

try:
  from apitools.base.py.exceptions import HttpError
  from google.cloud import bigquery as gcp_bigquery
  from google.api_core import exceptions
except ImportError:
  gcp_bigquery = None
  HttpError = None
  exceptions = None
# pylint: enable=wrong-import-order, wrong-import-position

_LOGGER = logging.getLogger(__name__)


def _load_or_default(filename):
  try:
    with open(filename) as f:
      return json.load(f)
  except:  # pylint: disable=bare-except
    return {}


@unittest.skipIf(
    HttpError is None or gcp_bigquery is None,
    'GCP dependencies are not installed')
class TestTableRowJsonCoder(unittest.TestCase):
  def test_row_as_table_row(self):
    schema_definition = [('s', 'STRING'), ('i', 'INTEGER'), ('f', 'FLOAT'),
                         ('b', 'BOOLEAN'), ('n', 'NUMERIC'), ('r', 'RECORD'),
                         ('g', 'GEOGRAPHY')]
    data_definition = [
        'abc',
        123,
        123.456,
        True,
        decimal.Decimal('987654321.987654321'), {
            'a': 'b'
        },
        'LINESTRING(1 2, 3 4, 5 6, 7 8)'
    ]
    str_def = (
        '{"s": "abc", '
        '"i": 123, '
        '"f": 123.456, '
        '"b": true, '
        '"n": "987654321.987654321", '
        '"r": {"a": "b"}, '
        '"g": "LINESTRING(1 2, 3 4, 5 6, 7 8)"}')
    schema = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(name=k, type=v) for k,
            v in schema_definition
        ])
    coder = TableRowJsonCoder(table_schema=schema)

    def value_or_decimal_to_json(val):
      if isinstance(val, decimal.Decimal):
        return to_json_value(str(val))
      else:
        return to_json_value(val)

    test_row = bigquery.TableRow(
        f=[
            bigquery.TableCell(v=value_or_decimal_to_json(e))
            for e in data_definition
        ])

    self.assertEqual(str_def, coder.encode(test_row))
    self.assertEqual(test_row, coder.decode(coder.encode(test_row)))
    # A coder without schema can still decode.
    self.assertEqual(
        test_row, TableRowJsonCoder().decode(coder.encode(test_row)))

  def test_row_and_no_schema(self):
    coder = TableRowJsonCoder()
    test_row = bigquery.TableRow(
        f=[
            bigquery.TableCell(v=to_json_value(e))
            for e in ['abc', 123, 123.456, True]
        ])
    with self.assertRaisesRegex(AttributeError,
                                r'^The TableRowJsonCoder requires'):
      coder.encode(test_row)

  def json_compliance_exception(self, value):
    with self.assertRaisesRegex(ValueError, re.escape(JSON_COMPLIANCE_ERROR)):
      schema_definition = [('f', 'FLOAT')]
      schema = bigquery.TableSchema(
          fields=[
              bigquery.TableFieldSchema(name=k, type=v) for k,
              v in schema_definition
          ])
      coder = TableRowJsonCoder(table_schema=schema)
      test_row = bigquery.TableRow(
          f=[bigquery.TableCell(v=to_json_value(value))])
      coder.encode(test_row)

  def test_invalid_json_nan(self):
    self.json_compliance_exception(float('nan'))

  def test_invalid_json_inf(self):
    self.json_compliance_exception(float('inf'))

  def test_invalid_json_neg_inf(self):
    self.json_compliance_exception(float('-inf'))


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestJsonToDictCoder(unittest.TestCase):
  @staticmethod
  def _make_schema(fields):
    def _fill_schema(fields):
      for field in fields:
        table_field = bigquery.TableFieldSchema()
        table_field.name, table_field.type, table_field.mode, nested_fields, \
          = field
        if nested_fields:
          table_field.fields = list(_fill_schema(nested_fields))
        yield table_field

    schema = bigquery.TableSchema()
    schema.fields = list(_fill_schema(fields))
    return schema

  def test_coder_is_pickable(self):
    try:
      schema = self._make_schema([
          (
              'record',
              'RECORD',
              'NULLABLE', [
                  ('float', 'FLOAT', 'NULLABLE', []),
              ]),
          ('integer', 'INTEGER', 'NULLABLE', []),
      ])
      coder = _JsonToDictCoder(schema)
      pickler.loads(pickler.dumps(coder))
    except pickle.PicklingError:
      self.fail('{} is not pickable'.format(coder.__class__.__name__))

  def test_values_are_converted(self):
    input_row = b'{"float": "10.5", "string": "abc"}'
    expected_row = {'float': 10.5, 'string': 'abc'}
    schema = self._make_schema([('float', 'FLOAT', 'NULLABLE', []),
                                ('string', 'STRING', 'NULLABLE', [])])
    coder = _JsonToDictCoder(schema)

    actual = coder.decode(input_row)
    self.assertEqual(expected_row, actual)

  def test_null_fields_are_preserved(self):
    input_row = b'{"float": "10.5"}'
    expected_row = {'float': 10.5, 'string': None}
    schema = self._make_schema([('float', 'FLOAT', 'NULLABLE', []),
                                ('string', 'STRING', 'NULLABLE', [])])
    coder = _JsonToDictCoder(schema)

    actual = coder.decode(input_row)
    self.assertEqual(expected_row, actual)

  def test_record_field_is_properly_converted(self):
    input_row = b'{"record": {"float": "55.5"}, "integer": 10}'
    expected_row = {'record': {'float': 55.5}, 'integer': 10}
    schema = self._make_schema([
        (
            'record',
            'RECORD',
            'NULLABLE', [
                ('float', 'FLOAT', 'NULLABLE', []),
            ]),
        ('integer', 'INTEGER', 'NULLABLE', []),
    ])
    coder = _JsonToDictCoder(schema)

    actual = coder.decode(input_row)
    self.assertEqual(expected_row, actual)

  def test_record_and_repeatable_field_is_properly_converted(self):
    input_row = b'{"record": [{"float": "55.5"}, {"float": "65.5"}], ' \
                b'"integer": 10}'
    expected_row = {'record': [{'float': 55.5}, {'float': 65.5}], 'integer': 10}
    schema = self._make_schema([
        (
            'record',
            'RECORD',
            'REPEATED', [
                ('float', 'FLOAT', 'NULLABLE', []),
            ]),
        ('integer', 'INTEGER', 'NULLABLE', []),
    ])
    coder = _JsonToDictCoder(schema)

    actual = coder.decode(input_row)
    self.assertEqual(expected_row, actual)

  def test_repeatable_field_is_properly_converted(self):
    input_row = b'{"repeated": ["55.5", "65.5"], "integer": "10"}'
    expected_row = {'repeated': [55.5, 65.5], 'integer': 10}
    schema = self._make_schema([
        ('repeated', 'FLOAT', 'REPEATED', []),
        ('integer', 'INTEGER', 'NULLABLE', []),
    ])
    coder = _JsonToDictCoder(schema)

    actual = coder.decode(input_row)
    self.assertEqual(expected_row, actual)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestReadFromBigQuery(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--gcs_location')

    cls.UserDefinedOptions = UserDefinedOptions

  def tearDown(self):
    # Reset runtime options to avoid side-effects caused by other tests.
    RuntimeValueProvider.set_runtime_options(None)

  @classmethod
  def tearDownClass(cls):
    # Unset the option added in setupClass to avoid interfere with other tests.
    # Force a gc so PipelineOptions.__subclass__() no longer contains it.
    del cls.UserDefinedOptions
    gc.collect()

  def test_get_destination_uri_empty_runtime_vp(self):
    with self.assertRaisesRegex(ValueError,
                                '^ReadFromBigQuery requires a GCS '
                                'location to be provided'):
      # Don't provide any runtime values.
      RuntimeValueProvider.set_runtime_options({})
      options = self.UserDefinedOptions()

      bigquery_export_destination_uri(
          options.gcs_location, None, uuid.uuid4().hex)

  def test_get_destination_uri_none(self):
    with self.assertRaisesRegex(ValueError,
                                '^ReadFromBigQuery requires a GCS '
                                'location to be provided'):
      bigquery_export_destination_uri(None, None, uuid.uuid4().hex)

  def test_get_destination_uri_runtime_vp(self):
    # Provide values at job-execution time.
    RuntimeValueProvider.set_runtime_options({'gcs_location': 'gs://bucket'})
    options = self.UserDefinedOptions()
    unique_id = uuid.uuid4().hex

    uri = bigquery_export_destination_uri(options.gcs_location, None, unique_id)
    self.assertEqual(
        uri, 'gs://bucket/' + unique_id + '/bigquery-table-dump-*.json')

  def test_get_destination_uri_static_vp(self):
    unique_id = uuid.uuid4().hex
    uri = bigquery_export_destination_uri(
        StaticValueProvider(str, 'gs://bucket'), None, unique_id)
    self.assertEqual(
        uri, 'gs://bucket/' + unique_id + '/bigquery-table-dump-*.json')

  def test_get_destination_uri_fallback_temp_location(self):
    # Don't provide any runtime values.
    RuntimeValueProvider.set_runtime_options({})
    options = self.UserDefinedOptions()

    with self.assertLogs('apache_beam.io.gcp.bigquery_read_internal',
                         level='DEBUG') as context:
      bigquery_export_destination_uri(
          options.gcs_location, 'gs://bucket', uuid.uuid4().hex)
    self.assertEqual(
        context.output,
        [
            'DEBUG:apache_beam.io.gcp.bigquery_read_internal:gcs_location is '
            'empty, using temp_location instead'
        ])

  @mock.patch.object(BigQueryWrapper, '_delete_table')
  @mock.patch.object(BigQueryWrapper, '_delete_dataset')
  @mock.patch('apache_beam.io.gcp.internal.clients.bigquery.BigqueryV2')
  def test_temp_dataset_is_configurable(
      self, api, delete_dataset, delete_table):
    temp_dataset = bigquery.DatasetReference(
        projectId='temp-project', datasetId='bq_dataset')
    bq = BigQueryWrapper(client=api, temp_dataset_id=temp_dataset.datasetId)
    gcs_location = 'gs://gcs_location'

    c = beam.io.gcp.bigquery._CustomBigQuerySource(
        query='select * from test_table',
        gcs_location=gcs_location,
        method=beam.io.ReadFromBigQuery.Method.EXPORT,
        validate=True,
        pipeline_options=beam.options.pipeline_options.PipelineOptions(),
        job_name='job_name',
        step_name='step_name',
        project='execution_project',
        **{'temp_dataset': temp_dataset})

    c._setup_temporary_dataset(bq)
    api.datasets.assert_not_called()

    # User provided temporary dataset should not be deleted but the temporary
    # table created by Beam should be deleted.
    bq.clean_up_temporary_dataset(temp_dataset.projectId)
    delete_dataset.assert_not_called()
    delete_table.assert_called_with(
        temp_dataset.projectId, temp_dataset.datasetId, mock.ANY)

  @parameterized.expand([
      param(
          exception_type=exceptions.Forbidden if exceptions else None,
          error_message='accessDenied'),
      param(
          exception_type=exceptions.ServiceUnavailable if exceptions else None,
          error_message='backendError'),
  ])
  def test_create_temp_dataset_exception(self, exception_type, error_message):

    with mock.patch.object(bigquery_v2_client.BigqueryV2.JobsService,
                           'Insert'),\
      mock.patch.object(BigQueryWrapper,
                        'get_or_create_dataset') as mock_insert, \
      mock.patch('time.sleep'), \
      self.assertRaises(Exception) as exc,\
      beam.Pipeline() as p:

      mock_insert.side_effect = exception_type(error_message)

      _ = p | ReadFromBigQuery(
          project='apache-beam-testing',
          query='SELECT * FROM `project.dataset.table`',
          gcs_location='gs://temp_location')

    mock_insert.assert_called()
    self.assertIn(error_message, exc.exception.args[0])

  @parameterized.expand([
      param(
          exception_type=exceptions.BadRequest if exceptions else None,
          error_message='invalidQuery'),
      param(
          exception_type=exceptions.NotFound if exceptions else None,
          error_message='notFound'),
      param(
          exception_type=exceptions.Forbidden if exceptions else None,
          error_message='responseTooLarge')
  ])
  def test_query_job_exception(self, exception_type, error_message):

    with mock.patch.object(beam.io.gcp.bigquery._CustomBigQuerySource,
                           'estimate_size') as mock_estimate,\
      mock.patch.object(BigQueryWrapper,
                        'get_query_location') as mock_query_location,\
      mock.patch.object(bigquery_v2_client.BigqueryV2.JobsService,
                        'Insert') as mock_query_job,\
      mock.patch.object(bigquery_v2_client.BigqueryV2.DatasetsService, 'Get'), \
      mock.patch('time.sleep'), \
      self.assertRaises(Exception) as exc, \
      beam.Pipeline() as p:

      mock_estimate.return_value = None
      mock_query_location.return_value = None
      mock_query_job.side_effect = exception_type(error_message)

      _ = p | ReadFromBigQuery(
          query='SELECT * FROM `project.dataset.table`',
          gcs_location='gs://temp_location')

    mock_query_job.assert_called()
    self.assertIn(error_message, exc.exception.args[0])

  @parameterized.expand([
      param(
          exception_type=exceptions.BadRequest if exceptions else None,
          error_message='invalid'),
      param(
          exception_type=exceptions.Forbidden if exceptions else None,
          error_message='accessDenied')
  ])
  def test_read_export_exception(self, exception_type, error_message):

    with mock.patch.object(beam.io.gcp.bigquery._CustomBigQuerySource,
                           'estimate_size') as mock_estimate,\
      mock.patch.object(bigquery_v2_client.BigqueryV2.TablesService, 'Get'),\
      mock.patch.object(bigquery_v2_client.BigqueryV2.JobsService,
                        'Insert') as mock_query_job, \
      mock.patch('time.sleep'), \
      self.assertRaises(Exception) as exc,\
      beam.Pipeline() as p:

      mock_estimate.return_value = None
      mock_query_job.side_effect = exception_type(error_message)

      _ = p | ReadFromBigQuery(
          project='apache-beam-testing',
          method=ReadFromBigQuery.Method.EXPORT,
          table='project:dataset.table',
          gcs_location="gs://temp_location")

    mock_query_job.assert_called()
    self.assertIn(error_message, exc.exception.args[0])


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQuerySink(unittest.TestCase):
  def test_table_spec_display_data(self):
    sink = beam.io.BigQuerySink('dataset.table')
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher('table', 'dataset.table'),
        DisplayDataItemMatcher('validation', False)
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_parse_schema_descriptor(self):
    sink = beam.io.BigQuerySink('dataset.table', schema='s:STRING, n:INTEGER')
    self.assertEqual(sink.table_reference.datasetId, 'dataset')
    self.assertEqual(sink.table_reference.tableId, 'table')
    result_schema = {
        field['name']: field['type']
        for field in sink.schema['fields']
    }
    self.assertEqual({'n': 'INTEGER', 's': 'STRING'}, result_schema)

  def test_project_table_display_data(self):
    sinkq = beam.io.BigQuerySink('project:dataset.table')
    dd = DisplayData.create_from(sinkq)
    expected_items = [
        DisplayDataItemMatcher('table', 'project:dataset.table'),
        DisplayDataItemMatcher('validation', False)
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestWriteToBigQuery(unittest.TestCase):
  def _cleanup_files(self):
    if os.path.exists('insert_calls1'):
      os.remove('insert_calls1')

    if os.path.exists('insert_calls2'):
      os.remove('insert_calls2')

  def setUp(self):
    self._cleanup_files()

  def tearDown(self):
    self._cleanup_files()

  def test_noop_schema_parsing(self):
    expected_table_schema = None
    table_schema = beam.io.gcp.bigquery.BigQueryWriteFn.get_table_schema(
        schema=None)
    self.assertEqual(expected_table_schema, table_schema)

  def test_dict_schema_parsing(self):
    schema = {
        'fields': [{
            'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'
        },
                   {
                       'name': 'r',
                       'type': 'RECORD',
                       'mode': 'NULLABLE',
                       'fields': [{
                           'name': 'x', 'type': 'INTEGER', 'mode': 'NULLABLE'
                       }]
                   }]
    }
    table_schema = beam.io.gcp.bigquery.BigQueryWriteFn.get_table_schema(schema)
    string_field = bigquery.TableFieldSchema(
        name='s', type='STRING', mode='NULLABLE')
    nested_field = bigquery.TableFieldSchema(
        name='x', type='INTEGER', mode='NULLABLE')
    number_field = bigquery.TableFieldSchema(
        name='n', type='INTEGER', mode='NULLABLE')
    record_field = bigquery.TableFieldSchema(
        name='r', type='RECORD', mode='NULLABLE', fields=[nested_field])
    expected_table_schema = bigquery.TableSchema(
        fields=[string_field, number_field, record_field])
    self.assertEqual(expected_table_schema, table_schema)

  def test_string_schema_parsing(self):
    schema = 's:STRING, n:INTEGER'
    expected_dict_schema = {
        'fields': [{
            'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'
        }]
    }
    dict_schema = (
        beam.io.gcp.bigquery.WriteToBigQuery.get_dict_table_schema(schema))
    self.assertEqual(expected_dict_schema, dict_schema)

  def test_table_schema_parsing(self):
    string_field = bigquery.TableFieldSchema(
        name='s', type='STRING', mode='NULLABLE')
    nested_field = bigquery.TableFieldSchema(
        name='x', type='INTEGER', mode='NULLABLE')
    number_field = bigquery.TableFieldSchema(
        name='n', type='INTEGER', mode='NULLABLE')
    record_field = bigquery.TableFieldSchema(
        name='r', type='RECORD', mode='NULLABLE', fields=[nested_field])
    schema = bigquery.TableSchema(
        fields=[string_field, number_field, record_field])
    expected_dict_schema = {
        'fields': [{
            'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'
        },
                   {
                       'name': 'r',
                       'type': 'RECORD',
                       'mode': 'NULLABLE',
                       'fields': [{
                           'name': 'x', 'type': 'INTEGER', 'mode': 'NULLABLE'
                       }]
                   }]
    }
    dict_schema = (
        beam.io.gcp.bigquery.WriteToBigQuery.get_dict_table_schema(schema))
    self.assertEqual(expected_dict_schema, dict_schema)

  def test_table_schema_parsing_end_to_end(self):
    string_field = bigquery.TableFieldSchema(
        name='s', type='STRING', mode='NULLABLE')
    nested_field = bigquery.TableFieldSchema(
        name='x', type='INTEGER', mode='NULLABLE')
    number_field = bigquery.TableFieldSchema(
        name='n', type='INTEGER', mode='NULLABLE')
    record_field = bigquery.TableFieldSchema(
        name='r', type='RECORD', mode='NULLABLE', fields=[nested_field])
    schema = bigquery.TableSchema(
        fields=[string_field, number_field, record_field])
    table_schema = beam.io.gcp.bigquery.BigQueryWriteFn.get_table_schema(
        beam.io.gcp.bigquery.WriteToBigQuery.get_dict_table_schema(schema))
    self.assertEqual(table_schema, schema)

  def test_none_schema_parsing(self):
    schema = None
    expected_dict_schema = None
    dict_schema = (
        beam.io.gcp.bigquery.WriteToBigQuery.get_dict_table_schema(schema))
    self.assertEqual(expected_dict_schema, dict_schema)

  def test_noop_dict_schema_parsing(self):
    schema = {
        'fields': [{
            'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'
        }]
    }
    expected_dict_schema = schema
    dict_schema = (
        beam.io.gcp.bigquery.WriteToBigQuery.get_dict_table_schema(schema))
    self.assertEqual(expected_dict_schema, dict_schema)

  def test_schema_autodetect_not_allowed_with_avro_file_loads(self):
    with TestPipeline() as p:
      pc = p | beam.Impulse()

      with self.assertRaisesRegex(ValueError, '^A schema must be provided'):
        _ = (
            pc
            | 'No Schema' >> beam.io.gcp.bigquery.WriteToBigQuery(
                "dataset.table",
                schema=None,
                temp_file_format=bigquery_tools.FileFormat.AVRO))

      with self.assertRaisesRegex(ValueError,
                                  '^Schema auto-detection is not supported'):
        _ = (
            pc
            | 'Schema Autodetected' >> beam.io.gcp.bigquery.WriteToBigQuery(
                "dataset.table",
                schema=beam.io.gcp.bigquery.SCHEMA_AUTODETECT,
                temp_file_format=bigquery_tools.FileFormat.AVRO))

  def test_to_from_runner_api(self):
    """Tests that serialization of WriteToBigQuery is correct.

    This is not intended to be a change-detector test. As such, this only tests
    the more complicated serialization logic of parameters: ValueProviders,
    callables, and side inputs.
    """
    FULL_OUTPUT_TABLE = 'test_project:output_table'

    p = TestPipeline()

    # Used for testing side input parameters.
    table_record_pcv = beam.pvalue.AsDict(
        p | "MakeTable" >> beam.Create([('table', FULL_OUTPUT_TABLE)]))

    # Used for testing value provider parameters.
    schema = value_provider.StaticValueProvider(str, '"a:str"')

    original = WriteToBigQuery(
        table=lambda _,
        side_input: side_input['table'],
        table_side_inputs=(table_record_pcv, ),
        schema=schema)

    # pylint: disable=expression-not-assigned
    p | 'MyWriteToBigQuery' >> original

    # Run the pipeline through to generate a pipeline proto from an empty
    # context. This ensures that the serialization code ran.
    pipeline_proto, context = TestPipeline.from_runner_api(
        p.to_runner_api(), p.runner, p.get_pipeline_options()).to_runner_api(
            return_context=True)

    # Find the transform from the context.
    write_to_bq_id = [
        k for k,
        v in pipeline_proto.components.transforms.items()
        if v.unique_name == 'MyWriteToBigQuery'
    ][0]
    deserialized_node = context.transforms.get_by_id(write_to_bq_id)
    deserialized = deserialized_node.transform
    self.assertIsInstance(deserialized, WriteToBigQuery)

    # Test that the serialization of a value provider is correct.
    self.assertEqual(original.schema, deserialized.schema)

    # Test that the serialization of a callable is correct.
    self.assertEqual(
        deserialized._table(None, {'table': FULL_OUTPUT_TABLE}),
        FULL_OUTPUT_TABLE)

    # Test that the serialization of a side input is correct.
    self.assertEqual(
        len(original.table_side_inputs), len(deserialized.table_side_inputs))
    original_side_input_data = original.table_side_inputs[0]._side_input_data()
    deserialized_side_input_data = deserialized.table_side_inputs[
        0]._side_input_data()
    self.assertEqual(
        original_side_input_data.access_pattern,
        deserialized_side_input_data.access_pattern)
    self.assertEqual(
        original_side_input_data.window_mapping_fn,
        deserialized_side_input_data.window_mapping_fn)
    self.assertEqual(
        original_side_input_data.view_fn, deserialized_side_input_data.view_fn)

  def test_streaming_triggering_frequency_without_auto_sharding(self):
    def noop(table, **kwargs):
      return []

    client = mock.Mock()
    client.insert_rows_json = mock.Mock(side_effect=noop)
    opt = StandardOptions()
    opt.streaming = True
    with self.assertRaises(ValueError,
                           msg="triggering_frequency with STREAMING_INSERTS" +
                           "can only be used with with_auto_sharding=True"):
      with beam.Pipeline(runner='BundleBasedDirectRunner', options=opt) as p:
        _ = (
            p
            | beam.Create([{
                'columnA': 'value1'
            }])
            | WriteToBigQuery(
                table='project:dataset.table',
                schema={
                    'fields': [{
                        'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                    }]
                },
                create_disposition='CREATE_NEVER',
                triggering_frequency=1,
                with_auto_sharding=False,
                test_client=client))

  def test_streaming_triggering_frequency_with_auto_sharding(self):
    def noop(table, **kwargs):
      return []

    client = mock.Mock()
    client.insert_rows_json = mock.Mock(side_effect=noop)
    opt = StandardOptions()
    opt.streaming = True
    with beam.Pipeline(runner='BundleBasedDirectRunner', options=opt) as p:
      _ = (
          p
          | beam.Create([{
              'columnA': 'value1'
          }])
          | WriteToBigQuery(
              table='project:dataset.table',
              schema={
                  'fields': [{
                      'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                  }]
              },
              create_disposition='CREATE_NEVER',
              triggering_frequency=1,
              with_auto_sharding=True,
              test_client=client))

  @mock.patch('google.cloud.bigquery.Client.insert_rows_json')
  def test_streaming_inserts_flush_on_byte_size_limit(self, mock_insert):
    mock_insert.return_value = []
    table = 'project:dataset.table'
    rows = [
        {
            'columnA': 'value1'
        },
        {
            'columnA': 'value2'
        },
        # this very large row exceeds max size, so should be sent to DLQ
        {
            'columnA': "large_string" * 100
        }
    ]
    with beam.Pipeline() as p:
      failed_rows = (
          p
          | beam.Create(rows)
          | WriteToBigQuery(
              table=table,
              method='STREAMING_INSERTS',
              create_disposition='CREATE_NEVER',
              schema='columnA:STRING',
              max_insert_payload_size=500))

      expected_failed_rows = [(table, rows[2])]
      assert_that(failed_rows.failed_rows, equal_to(expected_failed_rows))
    self.assertEqual(2, mock_insert.call_count)

  @parameterized.expand([
      param(
          exception_type=exceptions.Forbidden if exceptions else None,
          error_message='accessDenied'),
      param(
          exception_type=exceptions.ServiceUnavailable if exceptions else None,
          error_message='backendError')
  ])
  def test_load_job_exception(self, exception_type, error_message):

    with mock.patch.object(bigquery_v2_client.BigqueryV2.JobsService,
                     'Insert') as mock_load_job,\
      mock.patch('apache_beam.io.gcp.internal.clients'
                 '.storage.storage_v1_client.StorageV1.ObjectsService'),\
      mock.patch('time.sleep'),\
      self.assertRaises(Exception) as exc,\
      beam.Pipeline() as p:

      mock_load_job.side_effect = exception_type(error_message)

      _ = (
          p
          | beam.Create([{
              'columnA': 'value1'
          }])
          | WriteToBigQuery(
              table='project:dataset.table',
              schema={
                  'fields': [{
                      'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                  }]
              },
              create_disposition='CREATE_NEVER',
              custom_gcs_temp_location="gs://temp_location",
              method='FILE_LOADS'))

    mock_load_job.assert_called()
    self.assertIn(error_message, exc.exception.args[0])

  @parameterized.expand([
      param(
          exception_type=exceptions.ServiceUnavailable if exceptions else None,
          error_message='backendError'),
      param(
          exception_type=exceptions.InternalServerError if exceptions else None,
          error_message='internalError'),
  ])
  def test_copy_load_job_exception(self, exception_type, error_message):

    from apache_beam.io.gcp import bigquery_file_loads

    old_max_file_size = bigquery_file_loads._DEFAULT_MAX_FILE_SIZE
    old_max_partition_size = bigquery_file_loads._MAXIMUM_LOAD_SIZE
    old_max_files_per_partition = bigquery_file_loads._MAXIMUM_SOURCE_URIS
    bigquery_file_loads._DEFAULT_MAX_FILE_SIZE = 15
    bigquery_file_loads._MAXIMUM_LOAD_SIZE = 30
    bigquery_file_loads._MAXIMUM_SOURCE_URIS = 1

    with mock.patch.object(bigquery_v2_client.BigqueryV2.JobsService,
                        'Insert') as mock_insert_copy_job, \
      mock.patch.object(BigQueryWrapper,
                        'perform_load_job') as mock_load_job, \
      mock.patch.object(BigQueryWrapper,
                        'wait_for_bq_job'), \
      mock.patch('apache_beam.io.gcp.internal.clients'
        '.storage.storage_v1_client.StorageV1.ObjectsService'), \
      mock.patch('time.sleep'), \
      self.assertRaises(Exception) as exc, \
      beam.Pipeline() as p:

      mock_insert_copy_job.side_effect = exception_type(error_message)

      dummy_job_reference = beam.io.gcp.internal.clients.bigquery.JobReference()
      dummy_job_reference.jobId = 'job_id'
      dummy_job_reference.location = 'US'
      dummy_job_reference.projectId = 'apache-beam-testing'

      mock_load_job.return_value = dummy_job_reference

      _ = (
          p
          | beam.Create([{
              'columnA': 'value1'
          }, {
              'columnA': 'value2'
          }, {
              'columnA': 'value3'
          }])
          | WriteToBigQuery(
              table='project:dataset.table',
              schema={
                  'fields': [{
                      'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                  }]
              },
              create_disposition='CREATE_NEVER',
              custom_gcs_temp_location="gs://temp_location",
              method='FILE_LOADS'))

    bigquery_file_loads._DEFAULT_MAX_FILE_SIZE = old_max_file_size
    bigquery_file_loads._MAXIMUM_LOAD_SIZE = old_max_partition_size
    bigquery_file_loads._MAXIMUM_SOURCE_URIS = old_max_files_per_partition

    self.assertEqual(4, mock_insert_copy_job.call_count)
    self.assertIn(error_message, exc.exception.args[0])


@unittest.skipIf(
    HttpError is None or exceptions is None,
    'GCP dependencies are not installed')
class BigQueryStreamingInsertsErrorHandling(unittest.TestCase):

  # Running tests with a variety of exceptions from  https://googleapis.dev
  #     /python/google-api-core/latest/_modules/google/api_core/exceptions.html.
  # Choosing some exceptions that produce reasons included in
  # bigquery_tools._NON_TRANSIENT_ERRORS and some that are not
  @parameterized.expand([
      # reason not in _NON_TRANSIENT_ERRORS for row 1 on first attempt
      # transient error retried and succeeds on second attempt, 0 rows sent to
      # failed rows
      param(
          insert_response=[
            exceptions.TooManyRequests if exceptions else None,
            None],
          error_reason='Too Many Requests', # not in _NON_TRANSIENT_ERRORS
          failed_rows=[]),
      # reason not in _NON_TRANSIENT_ERRORS for row 1 on both attempts, sent to
      # failed rows after hitting max_retries
      param(
          insert_response=[
            exceptions.InternalServerError if exceptions else None,
            exceptions.InternalServerError if exceptions else None],
          error_reason='Internal Server Error', # not in _NON_TRANSIENT_ERRORS
          failed_rows=['value1', 'value3', 'value5']),
      # reason in _NON_TRANSIENT_ERRORS for row 1 on both attempts, sent to
      # failed_rows after hitting max_retries
      param(
          insert_response=[
            exceptions.Forbidden if exceptions else None,
            exceptions.Forbidden if exceptions else None],
          error_reason='Forbidden', # in _NON_TRANSIENT_ERRORS
          failed_rows=['value1', 'value3', 'value5']),
  ])
  def test_insert_rows_json_exception_retry_always(
      self, insert_response, error_reason, failed_rows):
    # In this test, a pipeline will always retry all caught exception types
    # since RetryStrategy is not set and defaults to RETRY_ALWAYS
    with mock.patch('time.sleep'):
      call_counter = 0
      mock_response = mock.Mock()
      mock_response.reason = error_reason

      def store_callback(table, **kwargs):
        nonlocal call_counter
        # raise exception if insert_response element is an exception
        if insert_response[call_counter]:
          exception_type = insert_response[call_counter]
          call_counter += 1
          raise exception_type('some exception', response=mock_response)
        # return empty list if not insert_response element, indicating
        # successful call to insert_rows_json
        else:
          call_counter += 1
          return []

      client = mock.Mock()
      client.insert_rows_json.side_effect = store_callback

      # Using the bundle based direct runner to avoid pickling problems
      # with mocks.
      with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
        bq_write_out = (
            p
            | beam.Create([{
                'columnA': 'value1', 'columnB': 'value2'
            }, {
                'columnA': 'value3', 'columnB': 'value4'
            }, {
                'columnA': 'value5', 'columnB': 'value6'
            }])
            # Using _StreamToBigQuery in order to be able to pass max_retries
            # in order to limit run time of test with RETRY_ALWAYS
            | _StreamToBigQuery(
                table_reference='project:dataset.table',
                table_side_inputs=[],
                schema_side_inputs=[],
                schema='anyschema',
                batch_size=None,
                triggering_frequency=None,
                create_disposition='CREATE_NEVER',
                write_disposition=None,
                kms_key=None,
                retry_strategy=RetryStrategy.RETRY_ALWAYS,
                additional_bq_parameters=[],
                ignore_insert_ids=False,
                ignore_unknown_columns=False,
                with_auto_sharding=False,
                test_client=client,
                max_retries=len(insert_response) - 1,
                num_streaming_keys=500))

        failed_values = (
            bq_write_out[beam_bq.BigQueryWriteFn.FAILED_ROWS]
            | beam.Map(lambda x: x[1]['columnA']))

        assert_that(failed_values, equal_to(failed_rows))

  # Running tests with a variety of exceptions from  https://googleapis.dev
  #     /python/google-api-core/latest/_modules/google/api_core/exceptions.html.
  # Choosing some exceptions that produce reasons that are included in
  # bigquery_tools._NON_TRANSIENT_ERRORS and some that are not
  @parameterized.expand([
      param(
          # not in _NON_TRANSIENT_ERRORS
          exception_type=exceptions.BadGateway if exceptions else None,
          error_reason='Bad Gateway'),
      param(
          # in _NON_TRANSIENT_ERRORS
          exception_type=exceptions.Unauthorized if exceptions else None,
          error_reason='Unauthorized'),
  ])
  @mock.patch('time.sleep')
  @mock.patch('google.cloud.bigquery.Client.insert_rows_json')
  def test_insert_rows_json_exception_retry_never(
      self, mock_send, unused_mock_sleep, exception_type, error_reason):
    # In this test, a pipeline will never retry caught exception types
    # since RetryStrategy is set to RETRY_NEVER
    mock_response = mock.Mock()
    mock_response.reason = error_reason
    mock_send.side_effect = [
        exception_type('some exception', response=mock_response)
    ]

    with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
      bq_write_out = (
          p
          | beam.Create([{
              'columnA': 'value1'
          }, {
              'columnA': 'value2'
          }])
          | WriteToBigQuery(
              table='project:dataset.table',
              schema={
                  'fields': [{
                      'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                  }]
              },
              create_disposition='CREATE_NEVER',
              method='STREAMING_INSERTS',
              insert_retry_strategy=RetryStrategy.RETRY_NEVER))
      failed_values = (
          bq_write_out[beam_bq.BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS]
          | beam.Map(lambda x: x[1]['columnA']))

      assert_that(failed_values, equal_to(['value1', 'value2']))

    self.assertEqual(1, mock_send.call_count)

  # Running tests with a variety of exceptions from  https://googleapis.dev
  #     /python/google-api-core/latest/_modules/google/api_core/exceptions.html.
  # Choosing some exceptions that produce reasons that are included in
  # bigquery_tools._NON_TRANSIENT_ERRORS and some that are not
  @parameterized.expand([
      param(
          exception_type=exceptions.DeadlineExceeded if exceptions else None,
          error_reason='Deadline Exceeded', # not in _NON_TRANSIENT_ERRORS
          failed_values=[],
          expected_call_count=2),
      param(
          exception_type=exceptions.Conflict if exceptions else None,
          error_reason='Conflict', # not in _NON_TRANSIENT_ERRORS
          failed_values=[],
          expected_call_count=2),
      param(
          exception_type=exceptions.TooManyRequests if exceptions else None,
          error_reason='Too Many Requests', # not in _NON_TRANSIENT_ERRORS
          failed_values=[],
          expected_call_count=2),
      param(
          exception_type=exceptions.InternalServerError if exceptions else None,
          error_reason='Internal Server Error', # not in _NON_TRANSIENT_ERRORS
          failed_values=[],
          expected_call_count=2),
      param(
          exception_type=exceptions.BadGateway if exceptions else None,
          error_reason='Bad Gateway', # not in _NON_TRANSIENT_ERRORS
          failed_values=[],
          expected_call_count=2),
      param(
          exception_type=exceptions.ServiceUnavailable if exceptions else None,
          error_reason='Service Unavailable', # not in _NON_TRANSIENT_ERRORS
          failed_values=[],
          expected_call_count=2),
      param(
          exception_type=exceptions.GatewayTimeout if exceptions else None,
          error_reason='Gateway Timeout', # not in _NON_TRANSIENT_ERRORS
          failed_values=[],
          expected_call_count=2),
      param(
          exception_type=exceptions.BadRequest if exceptions else None,
          error_reason='Bad Request', # in _NON_TRANSIENT_ERRORS
          failed_values=['value1', 'value2'],
          expected_call_count=1),
      param(
          exception_type=exceptions.Unauthorized if exceptions else None,
          error_reason='Unauthorized', # in _NON_TRANSIENT_ERRORS
          failed_values=['value1', 'value2'],
          expected_call_count=1),
      param(
          exception_type=exceptions.Forbidden if exceptions else None,
          error_reason='Forbidden', # in _NON_TRANSIENT_ERRORS
          failed_values=['value1', 'value2'],
          expected_call_count=1),
      param(
          exception_type=exceptions.NotFound if exceptions else None,
          error_reason='Not Found', # in _NON_TRANSIENT_ERRORS
          failed_values=['value1', 'value2'],
          expected_call_count=1),
      param(
          exception_type=exceptions.MethodNotImplemented
            if exceptions else None,
          error_reason='Not Implemented', # in _NON_TRANSIENT_ERRORS
          failed_values=['value1', 'value2'],
          expected_call_count=1),
  ])
  @mock.patch('time.sleep')
  @mock.patch('google.cloud.bigquery.Client.insert_rows_json')
  def test_insert_rows_json_exception_retry_on_transient_error(
      self,
      mock_send,
      unused_mock_sleep,
      exception_type,
      error_reason,
      failed_values,
      expected_call_count):
    # In this test, a pipeline will only retry caught exception types
    # with reasons that are not in _NON_TRANSIENT_ERRORS since RetryStrategy is
    # set to RETRY_ON_TRANSIENT_ERROR
    mock_response = mock.Mock()
    mock_response.reason = error_reason
    mock_send.side_effect = [
        exception_type('some exception', response=mock_response),
        # Return no exception and no errors on 2nd call, if there is a 2nd call
        []
    ]

    with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
      bq_write_out = (
          p
          | beam.Create([{
              'columnA': 'value1'
          }, {
              'columnA': 'value2'
          }])
          | WriteToBigQuery(
              table='project:dataset.table',
              schema={
                  'fields': [{
                      'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                  }]
              },
              create_disposition='CREATE_NEVER',
              method='STREAMING_INSERTS',
              insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR))
      failed_values_out = (
          bq_write_out[beam_bq.BigQueryWriteFn.FAILED_ROWS]
          | beam.Map(lambda x: x[1]['columnA']))

      assert_that(failed_values_out, equal_to(failed_values))
    self.assertEqual(expected_call_count, mock_send.call_count)

  # Running tests with persistent exceptions with exception types not
  # caught in BigQueryWrapper._insert_all_rows but retriable by
  # retry.with_exponential_backoff
  @parameterized.expand([
      param(
          exception_type=requests.exceptions.ConnectionError,
          error_message='some connection error'),
      param(
          exception_type=requests.exceptions.Timeout,
          error_message='some timeout error'),
  ])
  @mock.patch('time.sleep')
  @mock.patch('google.cloud.bigquery.Client.insert_rows_json')
  def test_insert_rows_json_persistent_retriable_exception(
      self, mock_send, unused_mock_sleep, exception_type, error_message):
    # In this test, each insert_rows_json call will result in an exception
    # and be retried with retry.with_exponential_backoff until MAX_RETRIES is
    # reached
    mock_send.side_effect = exception_type(error_message)

    # Expecting 1 initial call plus maximum number of retries
    expected_call_count = 1 + bigquery_tools.MAX_RETRIES

    with self.assertRaises(Exception) as exc:
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create([{
                'columnA': 'value1'
            }, {
                'columnA': 'value2'
            }])
            | WriteToBigQuery(
                table='project:dataset.table',
                schema={
                    'fields': [{
                        'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                    }]
                },
                create_disposition='CREATE_NEVER',
                method='STREAMING_INSERTS'))

    self.assertEqual(expected_call_count, mock_send.call_count)
    self.assertIn(error_message, exc.exception.args[0])

  # Running tests with intermittent exceptions with exception types not
  # caught in BigQueryWrapper._insert_all_rows but retriable by
  # retry.with_exponential_backoff
  @parameterized.expand([
      param(
          exception_type=requests.exceptions.ConnectionError,
          error_message='some connection error'),
      param(
          exception_type=requests.exceptions.Timeout,
          error_message='some timeout error'),
  ])
  @mock.patch('time.sleep')
  @mock.patch('google.cloud.bigquery.Client.insert_rows_json')
  def test_insert_rows_json_intermittent_retriable_exception(
      self, mock_send, unused_mock_sleep, exception_type, error_message):
    # In this test, the first 2 insert_rows_json calls will result in an
    # exception and be retried with retry.with_exponential_backoff. The last
    # call will not raise an exception and will succeed.
    mock_send.side_effect = [
        exception_type(error_message), exception_type(error_message), []
    ]

    with beam.Pipeline() as p:
      _ = (
          p
          | beam.Create([{
              'columnA': 'value1'
          }, {
              'columnA': 'value2'
          }])
          | WriteToBigQuery(
              table='project:dataset.table',
              schema={
                  'fields': [{
                      'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                  }]
              },
              create_disposition='CREATE_NEVER',
              method='STREAMING_INSERTS'))

    self.assertEqual(3, mock_send.call_count)

  # Running tests with a variety of error reasons from
  # https://cloud.google.com/bigquery/docs/error-messages
  # This covers the scenario when
  # the google.cloud.bigquery.Client.insert_rows_json call returns an error list
  # rather than raising an exception.
  # Choosing some error reasons that are included in
  # bigquery_tools._NON_TRANSIENT_ERRORS and some that are not
  @parameterized.expand([
      # reason in _NON_TRANSIENT_ERRORS for row 1, sent to failed_rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }],
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }],
          ],
          failed_rows=['value1']),
      # reason in _NON_TRANSIENT_ERRORS for row 1
      # reason not in _NON_TRANSIENT_ERRORS for row 2 on 1st run
      # row 1 sent to failed_rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }, {
                  'index': 1, 'errors': [{
                      'reason': 'internalError'
                  }]
              }],
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }],
          ],
          failed_rows=['value1']),
      # reason not in _NON_TRANSIENT_ERRORS for row 1 on first attempt
      # transient error succeeds on second attempt, 0 rows sent to failed rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'internalError'
                  }]
              }],
              [],
          ],
          failed_rows=[]),
  ])
  def test_insert_rows_json_errors_retry_always(
      self, insert_response, failed_rows, unused_sleep_mock=None):
    # In this test, a pipeline will always retry all errors
    # since RetryStrategy is not set and defaults to RETRY_ALWAYS
    with mock.patch('time.sleep'):
      call_counter = 0

      def store_callback(table, **kwargs):
        nonlocal call_counter
        response = insert_response[call_counter]
        call_counter += 1
        return response

      client = mock.Mock()
      client.insert_rows_json = mock.Mock(side_effect=store_callback)

      # Using the bundle based direct runner to avoid pickling problems
      # with mocks.
      with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
        bq_write_out = (
            p
            | beam.Create([{
                'columnA': 'value1', 'columnB': 'value2'
            }, {
                'columnA': 'value3', 'columnB': 'value4'
            }, {
                'columnA': 'value5', 'columnB': 'value6'
            }])
            # Using _StreamToBigQuery in order to be able to pass max_retries
            # in order to limit run time of test with RETRY_ALWAYS
            | _StreamToBigQuery(
                table_reference='project:dataset.table',
                table_side_inputs=[],
                schema_side_inputs=[],
                schema='anyschema',
                batch_size=None,
                triggering_frequency=None,
                create_disposition='CREATE_NEVER',
                write_disposition=None,
                kms_key=None,
                retry_strategy=RetryStrategy.RETRY_ALWAYS,
                additional_bq_parameters=[],
                ignore_insert_ids=False,
                ignore_unknown_columns=False,
                with_auto_sharding=False,
                test_client=client,
                max_retries=len(insert_response) - 1,
                num_streaming_keys=500))

        failed_values = (
            bq_write_out[beam_bq.BigQueryWriteFn.FAILED_ROWS]
            | beam.Map(lambda x: x[1]['columnA']))

        assert_that(failed_values, equal_to(failed_rows))

  # Running tests with a variety of error reasons from
  # https://cloud.google.com/bigquery/docs/error-messages
  # This covers the scenario when
  # the google.cloud.bigquery.Client.insert_rows_json call returns an error list
  # rather than raising an exception.
  # Choosing some error reasons that are included in
  # bigquery_tools._NON_TRANSIENT_ERRORS and some that are not
  @parameterized.expand([
      # reason in _NON_TRANSIENT_ERRORS for row 1, sent to failed_rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalidQuery'
                  }]
              }],
          ],
          streaming=False),
      # reason not in _NON_TRANSIENT_ERRORS for row 1, sent to failed_rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'internalError'
                  }]
              }],
          ],
          streaming=False),
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }],
          ],
          streaming=True),
      # reason not in _NON_TRANSIENT_ERRORS for row 1, sent to failed_rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'internalError'
                  }]
              }],
          ],
          streaming=True),
  ])
  @mock.patch('time.sleep')
  @mock.patch('google.cloud.bigquery.Client.insert_rows_json')
  def test_insert_rows_json_errors_retry_never(
      self, mock_send, unused_mock_sleep, insert_response, streaming):
    # In this test, a pipeline will never retry errors since RetryStrategy is
    # set to RETRY_NEVER
    mock_send.side_effect = insert_response
    opt = StandardOptions()
    opt.streaming = streaming
    with beam.Pipeline(runner='BundleBasedDirectRunner', options=opt) as p:
      bq_write_out = (
          p
          | beam.Create([{
              'columnA': 'value1'
          }, {
              'columnA': 'value2'
          }])
          | WriteToBigQuery(
              table='project:dataset.table',
              schema={
                  'fields': [{
                      'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                  }]
              },
              create_disposition='CREATE_NEVER',
              method='STREAMING_INSERTS',
              insert_retry_strategy=RetryStrategy.RETRY_NEVER))
      failed_values = (
          bq_write_out[beam_bq.BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS]
          | beam.Map(lambda x: x[1]['columnA']))

      assert_that(failed_values, equal_to(['value1']))

    self.assertEqual(1, mock_send.call_count)

  # Running tests with a variety of error reasons from
  # https://cloud.google.com/bigquery/docs/error-messages
  # This covers the scenario when
  # the google.cloud.bigquery.Client.insert_rows_json call returns an error list
  # rather than raising an exception.
  # Choosing some error reasons that are included in
  # bigquery_tools._NON_TRANSIENT_ERRORS and some that are not
  @parameterized.expand([
      # reason in _NON_TRANSIENT_ERRORS for row 1, sent to failed_rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }],
          ],
          failed_rows=['value1'],
          streaming=False),
      # reason not in _NON_TRANSIENT_ERRORS for row 1 on 1st attempt
      # transient error succeeds on 2nd attempt, 0 rows sent to failed rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'internalError'
                  }]
              }],
              [],
          ],
          failed_rows=[],
          streaming=False),
      # reason in _NON_TRANSIENT_ERRORS for row 1
      # reason not in _NON_TRANSIENT_ERRORS for row 2 on 1st and 2nd attempt
      # all rows with errors are retried when any row has a retriable error
      # row 1 sent to failed_rows after final attempt
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }, {
                  'index': 1, 'errors': [{
                      'reason': 'internalError'
                  }]
              }],
              [
                  {
                      'index': 0, 'errors': [{
                          'reason': 'invalid'
                      }]
                  },
              ],
          ],
          failed_rows=['value1'],
          streaming=False),
      # reason in _NON_TRANSIENT_ERRORS for row 1, sent to failed_rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }],
          ],
          failed_rows=['value1'],
          streaming=True),
      # reason not in _NON_TRANSIENT_ERRORS for row 1 on 1st attempt
      # transient error succeeds on 2nd attempt, 0 rows sent to failed rows
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'internalError'
                  }]
              }],
              [],
          ],
          failed_rows=[],
          streaming=True),
      # reason in _NON_TRANSIENT_ERRORS for row 1
      # reason not in _NON_TRANSIENT_ERRORS for row 2 on 1st and 2nd attempt
      # all rows with errors are retried when any row has a retriable error
      # row 1 sent to failed_rows after final attempt
      param(
          insert_response=[
              [{
                  'index': 0, 'errors': [{
                      'reason': 'invalid'
                  }]
              }, {
                  'index': 1, 'errors': [{
                      'reason': 'internalError'
                  }]
              }],
              [
                  {
                      'index': 0, 'errors': [{
                          'reason': 'invalid'
                      }]
                  },
              ],
          ],
          failed_rows=['value1'],
          streaming=True),
  ])
  @mock.patch('time.sleep')
  @mock.patch('google.cloud.bigquery.Client.insert_rows_json')
  def test_insert_rows_json_errors_retry_on_transient_error(
      self,
      mock_send,
      unused_mock_sleep,
      insert_response,
      failed_rows,
      streaming=False):
    # In this test, a pipeline will only retry errors with reasons that are not
    # in _NON_TRANSIENT_ERRORS since RetryStrategy is set to
    # RETRY_ON_TRANSIENT_ERROR
    call_counter = 0

    def store_callback(table, **kwargs):
      nonlocal call_counter
      response = insert_response[call_counter]
      call_counter += 1
      return response

    mock_send.side_effect = store_callback

    opt = StandardOptions()
    opt.streaming = streaming

    # Using the bundle based direct runner to avoid pickling problems
    # with mocks.
    with beam.Pipeline(runner='BundleBasedDirectRunner', options=opt) as p:
      bq_write_out = (
          p
          | beam.Create([{
              'columnA': 'value1'
          }, {
              'columnA': 'value2'
          }, {
              'columnA': 'value3'
          }])
          | WriteToBigQuery(
              table='project:dataset.table',
              schema={
                  'fields': [{
                      'name': 'columnA', 'type': 'STRING', 'mode': 'NULLABLE'
                  }]
              },
              create_disposition='CREATE_NEVER',
              method='STREAMING_INSERTS',
              insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR))

      failed_values = (
          bq_write_out[beam_bq.BigQueryWriteFn.FAILED_ROWS]
          | beam.Map(lambda x: x[1]['columnA']))

      assert_that(failed_values, equal_to(failed_rows))


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class BigQueryStreamingInsertTransformTests(unittest.TestCase):
  def test_dofn_client_process_performs_batching(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project-id', datasetId='dataset_id', tableId='table_id'))
    client.insert_rows_json.return_value = []
    create_disposition = beam.io.BigQueryDisposition.CREATE_NEVER
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND

    fn = beam.io.gcp.bigquery.BigQueryWriteFn(
        batch_size=2,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        kms_key=None,
        test_client=client)

    fn.process(('project-id:dataset_id.table_id', {'month': 1}))

    # InsertRows not called as batch size is not hit yet
    self.assertFalse(client.insert_rows_json.called)

  def test_dofn_client_process_flush_called(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project-id', datasetId='dataset_id', tableId='table_id'))
    client.insert_rows_json.return_value = []
    create_disposition = beam.io.BigQueryDisposition.CREATE_NEVER
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND

    fn = beam.io.gcp.bigquery.BigQueryWriteFn(
        batch_size=2,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        kms_key=None,
        test_client=client)

    fn.start_bundle()
    fn.process(('project-id:dataset_id.table_id', ({'month': 1}, 'insertid1')))
    fn.process(('project-id:dataset_id.table_id', ({'month': 2}, 'insertid2')))
    fn.finish_bundle()
    # InsertRows called as batch size is hit
    self.assertTrue(client.insert_rows_json.called)

  def test_dofn_client_finish_bundle_flush_called(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project-id', datasetId='dataset_id', tableId='table_id'))
    client.insert_rows_json.return_value = []
    create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND

    fn = beam.io.gcp.bigquery.BigQueryWriteFn(
        batch_size=2,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        kms_key=None,
        test_client=client)

    fn.start_bundle()

    # Destination is a tuple of (destination, schema) to ensure the table is
    # created.
    fn.process(('project-id:dataset_id.table_id', ({'month': 1}, 'insertid3')))

    self.assertTrue(client.tables.Get.called)
    # InsertRows not called as batch size is not hit
    self.assertFalse(client.insert_rows_json.called)

    fn.finish_bundle()
    # InsertRows called in finish bundle
    self.assertTrue(client.insert_rows_json.called)

  def test_dofn_client_no_records(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project-id', datasetId='dataset_id', tableId='table_id'))
    client.tabledata.InsertAll.return_value = \
      bigquery.TableDataInsertAllResponse(insertErrors=[])
    create_disposition = beam.io.BigQueryDisposition.CREATE_NEVER
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND

    fn = beam.io.gcp.bigquery.BigQueryWriteFn(
        batch_size=2,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        kms_key=None,
        test_client=client)

    fn.start_bundle()
    # InsertRows not called as batch size is not hit
    self.assertFalse(client.tabledata.InsertAll.called)

    fn.finish_bundle()
    # InsertRows not called in finish bundle as no records
    self.assertFalse(client.tabledata.InsertAll.called)

  def test_with_batched_input(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project-id', datasetId='dataset_id', tableId='table_id'))
    client.insert_rows_json.return_value = []
    create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND

    fn = beam.io.gcp.bigquery.BigQueryWriteFn(
        batch_size=10,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        kms_key=None,
        with_batched_input=True,
        test_client=client)

    fn.start_bundle()

    # Destination is a tuple of (destination, schema) to ensure the table is
    # created.
    fn.process((
        'project-id:dataset_id.table_id',
        [({
            'month': 1
        }, 'insertid3'), ({
            'month': 2
        }, 'insertid2'), ({
            'month': 3
        }, 'insertid1')]))

    # InsertRows called since the input is already batched.
    self.assertTrue(client.insert_rows_json.called)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class PipelineBasedStreamingInsertTest(_TestCaseWithTempDirCleanUp):
  @mock.patch('time.sleep')
  def test_failure_has_same_insert_ids(self, unused_mock_sleep):
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)
    file_name_1 = os.path.join(tempdir, 'file1')
    file_name_2 = os.path.join(tempdir, 'file2')

    def store_callback(table, **kwargs):
      insert_ids = [r for r in kwargs['row_ids']]
      colA_values = [r['columnA'] for r in kwargs['json_rows']]
      json_output = {'insertIds': insert_ids, 'colA_values': colA_values}
      # The first time we try to insert, we save those insertions in
      # file insert_calls1.
      if not os.path.exists(file_name_1):
        with open(file_name_1, 'w') as f:
          json.dump(json_output, f)
        raise RuntimeError()
      else:
        with open(file_name_2, 'w') as f:
          json.dump(json_output, f)

      return []

    client = mock.Mock()
    client.insert_rows_json = mock.Mock(side_effect=store_callback)

    # Using the bundle based direct runner to avoid pickling problems
    # with mocks.
    with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
      _ = (
          p
          | beam.Create([{
              'columnA': 'value1', 'columnB': 'value2'
          }, {
              'columnA': 'value3', 'columnB': 'value4'
          }, {
              'columnA': 'value5', 'columnB': 'value6'
          }])
          | _StreamToBigQuery(
              table_reference='project:dataset.table',
              table_side_inputs=[],
              schema_side_inputs=[],
              schema='anyschema',
              batch_size=None,
              triggering_frequency=None,
              create_disposition='CREATE_NEVER',
              write_disposition=None,
              kms_key=None,
              retry_strategy=None,
              additional_bq_parameters=[],
              ignore_insert_ids=False,
              ignore_unknown_columns=False,
              with_auto_sharding=False,
              test_client=client,
              num_streaming_keys=500))

    with open(file_name_1) as f1, open(file_name_2) as f2:
      self.assertEqual(json.load(f1), json.load(f2))

  @parameterized.expand([
      param(retry_strategy=RetryStrategy.RETRY_ALWAYS),
      param(retry_strategy=RetryStrategy.RETRY_NEVER),
      param(retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR),
  ])
  def test_failure_in_some_rows_does_not_duplicate(self, retry_strategy=None):
    with mock.patch('time.sleep'):
      # In this test we simulate a failure to write out two out of three rows.
      # Row 0 and row 2 fail to be written on the first attempt, and then
      # succeed on the next attempt (if there is one).
      tempdir = '%s%s' % (self._new_tempdir(), os.sep)
      file_name_1 = os.path.join(tempdir, 'file1_partial')
      file_name_2 = os.path.join(tempdir, 'file2_partial')

      def store_callback(table, **kwargs):
        insert_ids = [r for r in kwargs['row_ids']]
        colA_values = [r['columnA'] for r in kwargs['json_rows']]

        # The first time this function is called, all rows are included
        # so we need to filter out 'failed' rows.
        json_output_1 = {
            'insertIds': [insert_ids[1]], 'colA_values': [colA_values[1]]
        }
        # The second time this function is called, only rows 0 and 2 are incl
        # so we don't need to filter any of them. We just write them all out.
        json_output_2 = {'insertIds': insert_ids, 'colA_values': colA_values}

        # The first time we try to insert, we save those insertions in
        # file insert_calls1.
        if not os.path.exists(file_name_1):
          with open(file_name_1, 'w') as f:
            json.dump(json_output_1, f)
          return [
              {
                  'index': 0,
                  'errors': [{
                      'reason': 'i dont like this row'
                  }, {
                      'reason': 'its bad'
                  }]
              },
              {
                  'index': 2,
                  'errors': [{
                      'reason': 'i het this row'
                  }, {
                      'reason': 'its no gud'
                  }]
              },
          ]
        else:
          with open(file_name_2, 'w') as f:
            json.dump(json_output_2, f)
            return []

      client = mock.Mock()
      client.insert_rows_json = mock.Mock(side_effect=store_callback)

      # The expected rows to be inserted according to the insert strategy
      if retry_strategy == RetryStrategy.RETRY_NEVER:
        result = ['value3']
      else:  # RETRY_ALWAYS and RETRY_ON_TRANSIENT_ERRORS should insert all rows
        result = ['value1', 'value3', 'value5']

      # Using the bundle based direct runner to avoid pickling problems
      # with mocks.
      with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
        bq_write_out = (
            p
            | beam.Create([{
                'columnA': 'value1', 'columnB': 'value2'
            }, {
                'columnA': 'value3', 'columnB': 'value4'
            }, {
                'columnA': 'value5', 'columnB': 'value6'
            }])
            | _StreamToBigQuery(
                table_reference='project:dataset.table',
                table_side_inputs=[],
                schema_side_inputs=[],
                schema='anyschema',
                batch_size=None,
                triggering_frequency=None,
                create_disposition='CREATE_NEVER',
                write_disposition=None,
                kms_key=None,
                retry_strategy=retry_strategy,
                additional_bq_parameters=[],
                ignore_insert_ids=False,
                ignore_unknown_columns=False,
                with_auto_sharding=False,
                test_client=client,
                num_streaming_keys=500))

        failed_values = (
            bq_write_out[beam_bq.BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS]
            | beam.Map(lambda x: x[1]['columnA']))

        assert_that(
            failed_values,
            equal_to(list({'value1', 'value3', 'value5'}.difference(result))))

      data1 = _load_or_default(file_name_1)
      data2 = _load_or_default(file_name_2)

      self.assertListEqual(
          sorted(data1.get('colA_values', []) + data2.get('colA_values', [])),
          result)
      self.assertEqual(len(data1['colA_values']), 1)

  @parameterized.expand([
      param(with_auto_sharding=False),
      param(with_auto_sharding=True),
  ])
  def test_batch_size_with_auto_sharding(self, with_auto_sharding):
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)
    file_name_1 = os.path.join(tempdir, 'file1')
    file_name_2 = os.path.join(tempdir, 'file2')

    def store_callback(table, **kwargs):
      insert_ids = [r for r in kwargs['row_ids']]
      colA_values = [r['columnA'] for r in kwargs['json_rows']]
      json_output = {'insertIds': insert_ids, 'colA_values': colA_values}
      # Expect two batches of rows will be inserted. Store them separately.
      if not os.path.exists(file_name_1):
        with open(file_name_1, 'w') as f:
          json.dump(json_output, f)
      else:
        with open(file_name_2, 'w') as f:
          json.dump(json_output, f)

      return []

    client = mock.Mock()
    client.insert_rows_json = mock.Mock(side_effect=store_callback)

    # Using the bundle based direct runner to avoid pickling problems
    # with mocks.
    with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
      _ = (
          p
          | beam.Create([{
              'columnA': 'value1', 'columnB': 'value2'
          }, {
              'columnA': 'value3', 'columnB': 'value4'
          }, {
              'columnA': 'value5', 'columnB': 'value6'
          }])
          | _StreamToBigQuery(
              table_reference='project:dataset.table',
              table_side_inputs=[],
              schema_side_inputs=[],
              schema='anyschema',
              # Set a batch size such that the input elements will be inserted
              # in 2 batches.
              batch_size=2,
              triggering_frequency=None,
              create_disposition='CREATE_NEVER',
              write_disposition=None,
              kms_key=None,
              retry_strategy=None,
              additional_bq_parameters=[],
              ignore_insert_ids=False,
              ignore_unknown_columns=False,
              with_auto_sharding=with_auto_sharding,
              test_client=client,
              num_streaming_keys=500))

    with open(file_name_1) as f1, open(file_name_2) as f2:
      out1 = json.load(f1)
      self.assertEqual(out1['colA_values'], ['value1', 'value3'])
      out2 = json.load(f2)
      self.assertEqual(out2['colA_values'], ['value5'])


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class BigQueryStreamingInsertTransformIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_bq_streaming_inserts_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.dataset_id = '%s%d%s' % (
        self.BIG_QUERY_DATASET_ID, int(time.time()), secrets.token_hex(3))
    self.bigquery_client = bigquery_tools.BigQueryWrapper()
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.output_table" % (self.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", self.dataset_id, self.project)

  @pytest.mark.it_postcommit
  def test_value_provider_transform(self):
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)
    schema = {
        'fields': [{
            'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'language', 'type': 'STRING', 'mode': 'NULLABLE'
        }]
    }

    additional_bq_parameters = {
        'timePartitioning': {
            'type': 'DAY'
        },
        'clustering': {
            'fields': ['language']
        }
    }

    table_ref = bigquery_tools.parse_table_reference(output_table_1)
    table_ref2 = bigquery_tools.parse_table_reference(output_table_2)

    pipeline_verifiers = [
        BigQueryTableMatcher(
            project=self.project,
            dataset=table_ref.datasetId,
            table=table_ref.tableId,
            expected_properties=additional_bq_parameters),
        BigQueryTableMatcher(
            project=self.project,
            dataset=table_ref2.datasetId,
            table=table_ref2.tableId,
            expected_properties=additional_bq_parameters),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, language FROM %s" % output_table_1,
            data=[(d['name'], d['language']) for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, language FROM %s" % output_table_2,
            data=[(d['name'], d['language']) for d in _ELEMENTS
                  if 'language' in d])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      input = p | beam.Create([row for row in _ELEMENTS if 'language' in row])

      _ = (
          input
          | "WriteWithMultipleDests" >> beam.io.gcp.bigquery.WriteToBigQuery(
              table=value_provider.StaticValueProvider(
                  str, '%s:%s' % (self.project, output_table_1)),
              schema=value_provider.StaticValueProvider(dict, schema),
              additional_bq_parameters=additional_bq_parameters,
              method='STREAMING_INSERTS'))
      _ = (
          input
          | "WriteWithMultipleDests2" >> beam.io.gcp.bigquery.WriteToBigQuery(
              table=value_provider.StaticValueProvider(
                  str, '%s:%s' % (self.project, output_table_2)),
              schema=beam.io.gcp.bigquery.SCHEMA_AUTODETECT,
              additional_bq_parameters=lambda _: additional_bq_parameters,
              method='FILE_LOADS'))

  @pytest.mark.it_postcommit
  def test_multiple_destinations_transform(self):
    streaming = self.test_pipeline.options.view_as(StandardOptions).streaming
    if streaming and isinstance(self.test_pipeline.runner, TestDataflowRunner):
      self.skipTest("TestStream is not supported on TestDataflowRunner")

    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)

    full_output_table_1 = '%s:%s' % (self.project, output_table_1)
    full_output_table_2 = '%s:%s' % (self.project, output_table_2)

    schema1 = {
        'fields': [{
            'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'language', 'type': 'STRING', 'mode': 'NULLABLE'
        }]
    }
    schema2 = {
        'fields': [{
            'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'foundation', 'type': 'STRING', 'mode': 'NULLABLE'
        }]
    }

    bad_record = {'language': 1, 'manguage': 2}

    if streaming:
      pipeline_verifiers = [
          PipelineStateMatcher(PipelineState.RUNNING),
          BigqueryFullResultStreamingMatcher(
              project=self.project,
              query="SELECT name, language FROM %s" % output_table_1,
              data=[(d['name'], d['language']) for d in _ELEMENTS
                    if 'language' in d]),
          BigqueryFullResultStreamingMatcher(
              project=self.project,
              query="SELECT name, foundation FROM %s" % output_table_2,
              data=[(d['name'], d['foundation']) for d in _ELEMENTS
                    if 'foundation' in d])
      ]
    else:
      pipeline_verifiers = [
          BigqueryFullResultMatcher(
              project=self.project,
              query="SELECT name, language FROM %s" % output_table_1,
              data=[(d['name'], d['language']) for d in _ELEMENTS
                    if 'language' in d]),
          BigqueryFullResultMatcher(
              project=self.project,
              query="SELECT name, foundation FROM %s" % output_table_2,
              data=[(d['name'], d['foundation']) for d in _ELEMENTS
                    if 'foundation' in d])
      ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      if streaming:
        _SIZE = len(_ELEMENTS)
        test_stream = (
            TestStream().advance_watermark_to(0).add_elements(
                _ELEMENTS[:_SIZE // 2]).advance_watermark_to(100).add_elements(
                    _ELEMENTS[_SIZE // 2:]).advance_watermark_to_infinity())
        input = p | test_stream
      else:
        input = p | beam.Create(_ELEMENTS)

      schema_table_pcv = beam.pvalue.AsDict(
          p | "MakeSchemas" >> beam.Create([(full_output_table_1, schema1),
                                            (full_output_table_2, schema2)]))

      table_record_pcv = beam.pvalue.AsDict(
          p | "MakeTables" >> beam.Create([('table1', full_output_table_1),
                                           ('table2', full_output_table_2)]))

      input2 = p | "Broken record" >> beam.Create([bad_record])

      input = (input, input2) | beam.Flatten()

      r = (
          input
          | "WriteWithMultipleDests" >> beam.io.gcp.bigquery.WriteToBigQuery(
              table=lambda x,
              tables:
              (tables['table1'] if 'language' in x else tables['table2']),
              table_side_inputs=(table_record_pcv, ),
              schema=lambda dest,
              table_map: table_map.get(dest, None),
              schema_side_inputs=(schema_table_pcv, ),
              insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
              method='STREAMING_INSERTS'))

      assert_that(
          r[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS_WITH_ERRORS]
          | beam.Map(lambda elm: (elm[0], elm[1])),
          equal_to([(full_output_table_1, bad_record)]))

      assert_that(
          r[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS],
          equal_to([(full_output_table_1, bad_record)]),
          label='FailedRowsMatch')

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


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class PubSubBigQueryIT(unittest.TestCase):

  INPUT_TOPIC = 'psit_topic_output'
  INPUT_SUB = 'psit_subscription_input'

  BIG_QUERY_DATASET_ID = 'python_pubsub_bq_'
  SCHEMA = {
      'fields': [{
          'name': 'number', 'type': 'INTEGER', 'mode': 'NULLABLE'
      }]
  }

  _SIZE = 4

  WAIT_UNTIL_FINISH_DURATION = 15 * 60 * 1000

  def setUp(self):
    # Set up PubSub
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')
    self.uuid = str(uuid.uuid4())
    from google.cloud import pubsub
    self.pub_client = pubsub.PublisherClient()
    self.input_topic = self.pub_client.create_topic(
        name=self.pub_client.topic_path(
            self.project, self.INPUT_TOPIC + self.uuid))
    self.sub_client = pubsub.SubscriberClient()
    self.input_sub = self.sub_client.create_subscription(
        name=self.sub_client.subscription_path(
            self.project, self.INPUT_SUB + self.uuid),
        topic=self.input_topic.name)

    # Set up BQ
    self.dataset_ref = utils.create_bq_dataset(
        self.project, self.BIG_QUERY_DATASET_ID)
    self.output_table = "%s.output_table" % (self.dataset_ref.dataset_id)

  def tearDown(self):
    # Tear down PubSub
    test_utils.cleanup_topics(self.pub_client, [self.input_topic])
    test_utils.cleanup_subscriptions(self.sub_client, [self.input_sub])
    # Tear down BigQuery
    utils.delete_bq_dataset(self.project, self.dataset_ref)

  def _run_pubsub_bq_pipeline(self, method, triggering_frequency=None):
    l = [i for i in range(self._SIZE)]

    matchers = [
        PipelineStateMatcher(PipelineState.RUNNING),
        BigqueryFullResultStreamingMatcher(
            project=self.project,
            query="SELECT number FROM %s" % self.output_table,
            data=[(i, ) for i in l])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*matchers),
        wait_until_finish_duration=self.WAIT_UNTIL_FINISH_DURATION,
        streaming=True,
        allow_unsafe_triggers=True)

    def add_schema_info(element):
      yield {'number': element}

    messages = [str(i).encode('utf-8') for i in l]
    for message in messages:
      self.pub_client.publish(self.input_topic.name, message)

    with beam.Pipeline(argv=args) as p:
      mesages = (
          p
          | ReadFromPubSub(subscription=self.input_sub.name)
          | beam.ParDo(add_schema_info))
      _ = mesages | WriteToBigQuery(
          self.output_table,
          schema=self.SCHEMA,
          method=method,
          triggering_frequency=triggering_frequency)

  @pytest.mark.it_postcommit
  def test_streaming_inserts(self):
    self._run_pubsub_bq_pipeline(WriteToBigQuery.Method.STREAMING_INSERTS)

  @pytest.mark.it_postcommit
  def test_file_loads(self):
    self._run_pubsub_bq_pipeline(
        WriteToBigQuery.Method.FILE_LOADS, triggering_frequency=20)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class BigQueryFileLoadsIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_bq_file_loads_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.dataset_id = '%s%d%s' % (
        self.BIG_QUERY_DATASET_ID, int(time.time()), secrets.token_hex(3))
    self.bigquery_client = bigquery_tools.BigQueryWrapper()
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = '%s.output_table' % (self.dataset_id)
    self.table_ref = bigquery_tools.parse_table_reference(self.output_table)
    _LOGGER.info(
        'Created dataset %s in project %s', self.dataset_id, self.project)

  @pytest.mark.it_postcommit
  def test_avro_file_load(self):
    # Construct elements such that they can be written via Avro but not via
    # JSON. See BEAM-8841.
    from apache_beam.io.gcp import bigquery_file_loads
    old_max_files = bigquery_file_loads._MAXIMUM_SOURCE_URIS
    old_max_file_size = bigquery_file_loads._DEFAULT_MAX_FILE_SIZE
    bigquery_file_loads._MAXIMUM_SOURCE_URIS = 1
    bigquery_file_loads._DEFAULT_MAX_FILE_SIZE = 100
    elements = [
        {
            'name': 'Negative infinity',
            'value': -float('inf'),
            'timestamp': datetime.datetime(1970, 1, 1, tzinfo=pytz.utc),
        },
        {
            'name': 'Not a number',
            'value': float('nan'),
            'timestamp': datetime.datetime(2930, 12, 9, tzinfo=pytz.utc),
        },
    ]

    schema = beam.io.gcp.bigquery.WriteToBigQuery.get_dict_table_schema(
        bigquery.TableSchema(
            fields=[
                bigquery.TableFieldSchema(
                    name='name', type='STRING', mode='REQUIRED'),
                bigquery.TableFieldSchema(
                    name='value', type='FLOAT', mode='REQUIRED'),
                bigquery.TableFieldSchema(
                    name='timestamp', type='TIMESTAMP', mode='REQUIRED'),
            ]))

    pipeline_verifiers = [
        # Some gymnastics here to avoid comparing NaN since NaN is not equal to
        # anything, including itself.
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, value, timestamp FROM {} WHERE value<0".format(
                self.output_table),
            data=[(d['name'], d['value'], d['timestamp'])
                  for d in elements[:1]],
        ),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, timestamp FROM {}".format(self.output_table),
            data=[(d['name'], d['timestamp']) for d in elements],
        ),
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers),
    )

    with beam.Pipeline(argv=args) as p:
      input = p | 'CreateInput' >> beam.Create(elements)
      schema_pc = p | 'CreateSchema' >> beam.Create([schema])

      _ = (
          input
          | 'WriteToBigQuery' >> beam.io.gcp.bigquery.WriteToBigQuery(
              table='%s:%s' % (self.project, self.output_table),
              schema=lambda _,
              schema: schema,
              schema_side_inputs=(beam.pvalue.AsSingleton(schema_pc), ),
              method='FILE_LOADS',
              temp_file_format=bigquery_tools.FileFormat.AVRO,
          ))
    bigquery_file_loads._MAXIMUM_SOURCE_URIS = old_max_files
    bigquery_file_loads._DEFAULT_MAX_FILE_SIZE = old_max_file_size

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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
