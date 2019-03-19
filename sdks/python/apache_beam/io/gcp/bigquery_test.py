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

import decimal
import json
import logging
import random
import re
import time
import unittest

import hamcrest as hc
import mock
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.bigquery import TableRowJsonCoder
from apache_beam.io.gcp.bigquery_file_loads_test import _ELEMENTS
from apache_beam.io.gcp.bigquery_tools import JSON_COMPLIANCE_ERROR
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.options import value_provider
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestTableRowJsonCoder(unittest.TestCase):

  def test_row_as_table_row(self):
    schema_definition = [
        ('s', 'STRING'),
        ('i', 'INTEGER'),
        ('f', 'FLOAT'),
        ('b', 'BOOLEAN'),
        ('n', 'NUMERIC'),
        ('r', 'RECORD'),
        ('g', 'GEOGRAPHY')]
    data_definition = [
        'abc',
        123,
        123.456,
        True,
        decimal.Decimal('987654321.987654321'),
        {'a': 'b'},
        'LINESTRING(1 2, 3 4, 5 6, 7 8)']
    str_def = ('{"s": "abc", '
               '"i": 123, '
               '"f": 123.456, '
               '"b": true, '
               '"n": "987654321.987654321", '
               '"r": {"a": "b"}, '
               '"g": "LINESTRING(1 2, 3 4, 5 6, 7 8)"}')
    schema = bigquery.TableSchema(
        fields=[bigquery.TableFieldSchema(name=k, type=v)
                for k, v in schema_definition])
    coder = TableRowJsonCoder(table_schema=schema)

    def value_or_decimal_to_json(val):
      if isinstance(val, decimal.Decimal):
        return to_json_value(str(val))
      else:
        return to_json_value(val)

    test_row = bigquery.TableRow(
        f=[bigquery.TableCell(v=value_or_decimal_to_json(e))
           for e in data_definition])

    self.assertEqual(str_def, coder.encode(test_row))
    self.assertEqual(test_row, coder.decode(coder.encode(test_row)))
    # A coder without schema can still decode.
    self.assertEqual(
        test_row, TableRowJsonCoder().decode(coder.encode(test_row)))

  def test_row_and_no_schema(self):
    coder = TableRowJsonCoder()
    test_row = bigquery.TableRow(
        f=[bigquery.TableCell(v=to_json_value(e))
           for e in ['abc', 123, 123.456, True]])
    with self.assertRaisesRegexp(AttributeError,
                                 r'^The TableRowJsonCoder requires'):
      coder.encode(test_row)

  def json_compliance_exception(self, value):
    with self.assertRaisesRegexp(ValueError, re.escape(JSON_COMPLIANCE_ERROR)):
      schema_definition = [('f', 'FLOAT')]
      schema = bigquery.TableSchema(
          fields=[bigquery.TableFieldSchema(name=k, type=v)
                  for k, v in schema_definition])
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
class TestBigQuerySource(unittest.TestCase):

  def test_display_data_item_on_validate_true(self):
    source = beam.io.BigQuerySource('dataset.table', validate=True)

    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('validation', True),
        DisplayDataItemMatcher('table', 'dataset.table')]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_table_reference_display_data(self):
    source = beam.io.BigQuerySource('dataset.table')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('validation', False),
        DisplayDataItemMatcher('table', 'dataset.table')]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

    source = beam.io.BigQuerySource('project:dataset.table')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('validation', False),
        DisplayDataItemMatcher('table', 'project:dataset.table')]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

    source = beam.io.BigQuerySource('xyz.com:project:dataset.table')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('validation',
                               False),
        DisplayDataItemMatcher('table',
                               'xyz.com:project:dataset.table')]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_parse_table_reference(self):
    source = beam.io.BigQuerySource('dataset.table')
    self.assertEqual(source.table_reference.datasetId, 'dataset')
    self.assertEqual(source.table_reference.tableId, 'table')

    source = beam.io.BigQuerySource('project:dataset.table')
    self.assertEqual(source.table_reference.projectId, 'project')
    self.assertEqual(source.table_reference.datasetId, 'dataset')
    self.assertEqual(source.table_reference.tableId, 'table')

    source = beam.io.BigQuerySource('xyz.com:project:dataset.table')
    self.assertEqual(source.table_reference.projectId, 'xyz.com:project')
    self.assertEqual(source.table_reference.datasetId, 'dataset')
    self.assertEqual(source.table_reference.tableId, 'table')

    source = beam.io.BigQuerySource(query='my_query')
    self.assertEqual(source.query, 'my_query')
    self.assertIsNone(source.table_reference)
    self.assertTrue(source.use_legacy_sql)

  def test_query_only_display_data(self):
    source = beam.io.BigQuerySource(query='my_query')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('validation', False),
        DisplayDataItemMatcher('query', 'my_query')]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_specify_query_sql_format(self):
    source = beam.io.BigQuerySource(query='my_query', use_standard_sql=True)
    self.assertEqual(source.query, 'my_query')
    self.assertFalse(source.use_legacy_sql)

  def test_specify_query_flattened_records(self):
    source = beam.io.BigQuerySource(query='my_query', flatten_results=False)
    self.assertFalse(source.flatten_results)

  def test_specify_query_unflattened_records(self):
    source = beam.io.BigQuerySource(query='my_query', flatten_results=True)
    self.assertTrue(source.flatten_results)

  def test_specify_query_without_table(self):
    source = beam.io.BigQuerySource(query='my_query')
    self.assertEqual(source.query, 'my_query')
    self.assertIsNone(source.table_reference)

  def test_date_partitioned_table_name(self):
    source = beam.io.BigQuerySource('dataset.table$20030102', validate=True)
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('validation', True),
        DisplayDataItemMatcher('table', 'dataset.table$20030102')]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQuerySink(unittest.TestCase):

  def test_table_spec_display_data(self):
    sink = beam.io.BigQuerySink('dataset.table')
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher('table', 'dataset.table'),
        DisplayDataItemMatcher('validation', False)]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_parse_schema_descriptor(self):
    sink = beam.io.BigQuerySink(
        'dataset.table', schema='s:STRING, n:INTEGER')
    self.assertEqual(sink.table_reference.datasetId, 'dataset')
    self.assertEqual(sink.table_reference.tableId, 'table')
    result_schema = {
        field.name: field.type for field in sink.table_schema.fields}
    self.assertEqual({'n': 'INTEGER', 's': 'STRING'}, result_schema)

  def test_project_table_display_data(self):
    sinkq = beam.io.BigQuerySink('PROJECT:dataset.table')
    dd = DisplayData.create_from(sinkq)
    expected_items = [
        DisplayDataItemMatcher('table', 'PROJECT:dataset.table'),
        DisplayDataItemMatcher('validation', False)]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_simple_schema_as_json(self):
    sink = beam.io.BigQuerySink(
        'PROJECT:dataset.table', schema='s:STRING, n:INTEGER')
    self.assertEqual(
        json.dumps({'fields': [
            {'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'}]}),
        sink.schema_as_json())

  def test_nested_schema_as_json(self):
    string_field = bigquery.TableFieldSchema(
        name='s', type='STRING', mode='NULLABLE', description='s description')
    number_field = bigquery.TableFieldSchema(
        name='n', type='INTEGER', mode='REQUIRED', description='n description')
    record_field = bigquery.TableFieldSchema(
        name='r', type='RECORD', mode='REQUIRED', description='r description',
        fields=[string_field, number_field])
    schema = bigquery.TableSchema(fields=[record_field])
    sink = beam.io.BigQuerySink('dataset.table', schema=schema)
    self.assertEqual(
        {'fields': [
            {'name': 'r', 'type': 'RECORD', 'mode': 'REQUIRED',
             'description': 'r description', 'fields': [
                 {'name': 's', 'type': 'STRING', 'mode': 'NULLABLE',
                  'description': 's description'},
                 {'name': 'n', 'type': 'INTEGER', 'mode': 'REQUIRED',
                  'description': 'n description'}]}]},
        json.loads(sink.schema_as_json()))


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class WriteToBigQuery(unittest.TestCase):

  def test_noop_schema_parsing(self):
    expected_table_schema = None
    table_schema = beam.io.gcp.bigquery.BigQueryWriteFn.get_table_schema(
        schema=None)
    self.assertEqual(expected_table_schema, table_schema)

  def test_dict_schema_parsing(self):
    schema = {'fields': [
        {'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'r', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
            {'name': 'x', 'type': 'INTEGER', 'mode': 'NULLABLE'}]}]}
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
    expected_dict_schema = {'fields': [
        {'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'}]}
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
    expected_dict_schema = {'fields': [
        {'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'r', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
            {'name': 'x', 'type': 'INTEGER', 'mode': 'NULLABLE'}]}]}
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
    schema = {'fields': [
        {'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'n', 'type': 'INTEGER', 'mode': 'NULLABLE'}]}
    expected_dict_schema = schema
    dict_schema = (
        beam.io.gcp.bigquery.WriteToBigQuery.get_dict_table_schema(schema))
    self.assertEqual(expected_dict_schema, dict_schema)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class BigQueryStreamingInsertTransformTests(unittest.TestCase):

  def test_dofn_client_process_performs_batching(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project_id', datasetId='dataset_id', tableId='table_id'))
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

    fn.process(('project_id:dataset_id.table_id', {'month': 1}))

    # InsertRows not called as batch size is not hit yet
    self.assertFalse(client.tabledata.InsertAll.called)

  def test_dofn_client_process_flush_called(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project_id', datasetId='dataset_id', tableId='table_id'))
    client.tabledata.InsertAll.return_value = (
        bigquery.TableDataInsertAllResponse(insertErrors=[]))
    create_disposition = beam.io.BigQueryDisposition.CREATE_NEVER
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND

    fn = beam.io.gcp.bigquery.BigQueryWriteFn(
        batch_size=2,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        kms_key=None,
        test_client=client)

    fn.start_bundle()
    fn.process(('project_id:dataset_id.table_id', {'month': 1}))
    fn.process(('project_id:dataset_id.table_id', {'month': 2}))
    # InsertRows called as batch size is hit
    self.assertTrue(client.tabledata.InsertAll.called)

  def test_dofn_client_finish_bundle_flush_called(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project_id', datasetId='dataset_id', tableId='table_id'))
    client.tabledata.InsertAll.return_value = \
      bigquery.TableDataInsertAllResponse(insertErrors=[])
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
    fn.process(('project_id:dataset_id.table_id', {'month': 1}))

    self.assertTrue(client.tables.Get.called)
    # InsertRows not called as batch size is not hit
    self.assertFalse(client.tabledata.InsertAll.called)

    fn.finish_bundle()
    # InsertRows called in finish bundle
    self.assertTrue(client.tabledata.InsertAll.called)

  def test_dofn_client_no_records(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project_id', datasetId='dataset_id', tableId='table_id'))
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


class BigQueryStreamingInsertTransformIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_bq_streaming_inserts_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.dataset_id = '%s%s%d' % (self.BIG_QUERY_DATASET_ID,
                                  str(int(time.time())),
                                  random.randint(0, 10000))
    self.bigquery_client = bigquery_tools.BigQueryWrapper()
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.output_table" % (self.dataset_id)
    logging.info("Created dataset %s in project %s",
                 self.dataset_id, self.project)

  @attr('IT')
  def test_value_provider_transform(self):
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)
    schema = {'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'language', 'type': 'STRING', 'mode': 'NULLABLE'}]}

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_1,
            data=[(d['name'], d['language'])
                  for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_2,
            data=[(d['name'], d['language'])
                  for d in _ELEMENTS
                  if 'language' in d])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers),
        experiments='use_beam_bq_sink')

    with beam.Pipeline(argv=args) as p:
      input = p | beam.Create([row for row in _ELEMENTS if 'language' in row])

      _ = (input
           | "WriteWithMultipleDests" >> beam.io.gcp.bigquery.WriteToBigQuery(
               table=value_provider.StaticValueProvider(
                   str, '%s:%s' % (self.project, output_table_1)),
               schema=value_provider.StaticValueProvider(dict, schema),
               method='STREAMING_INSERTS'))
      _ = (input
           | "WriteWithMultipleDests2" >> beam.io.gcp.bigquery.WriteToBigQuery(
               table=value_provider.StaticValueProvider(
                   str, '%s:%s' % (self.project, output_table_2)),
               method='FILE_LOADS'))

  @attr('IT')
  def test_multiple_destinations_transform(self):
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)

    full_output_table_1 = '%s:%s' % (self.project, output_table_1)
    full_output_table_2 = '%s:%s' % (self.project, output_table_2)

    schema1 = {'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'language', 'type': 'STRING', 'mode': 'NULLABLE'}]}
    schema2 = {'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'foundation', 'type': 'STRING', 'mode': 'NULLABLE'}]}

    bad_record = {'language': 1, 'manguage': 2}

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_1,
            data=[(d['name'], d['language'])
                  for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_2,
            data=[(d['name'], d['foundation'])
                  for d in _ELEMENTS
                  if 'foundation' in d])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers),
        experiments='use_beam_bq_sink')

    with beam.Pipeline(argv=args) as p:
      input = p | beam.Create(_ELEMENTS)

      input2 = p | "Broken record" >> beam.Create([bad_record])

      input = (input, input2) | beam.Flatten()

      r = (input
           | "WriteWithMultipleDests" >> beam.io.gcp.bigquery.WriteToBigQuery(
               table=lambda x: (full_output_table_1
                                if 'language' in x
                                else full_output_table_2),
               schema=lambda dest: (schema1
                                    if dest == full_output_table_1
                                    else schema2),
               method='STREAMING_INSERTS'))

      assert_that(r[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS],
                  equal_to([(full_output_table_1, bad_record)]))

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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
