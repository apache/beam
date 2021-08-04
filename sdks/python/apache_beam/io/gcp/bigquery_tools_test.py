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

# pytype: skip-file

import datetime
import decimal
import io
import json
import logging
import math
import re
import time
import unittest

import fastavro
import mock
import pytz

import apache_beam as beam
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.io.gcp import resource_identifiers
from apache_beam.io.gcp.bigquery import TableRowJsonCoder
from apache_beam.io.gcp.bigquery_tools import JSON_COMPLIANCE_ERROR
from apache_beam.io.gcp.bigquery_tools import AvroRowWriter
from apache_beam.io.gcp.bigquery_tools import BigQueryJobTypes
from apache_beam.io.gcp.bigquery_tools import JsonRowWriter
from apache_beam.io.gcp.bigquery_tools import RowAsDictJsonCoder
from apache_beam.io.gcp.bigquery_tools import check_schema_equal
from apache_beam.io.gcp.bigquery_tools import generate_bq_job_name
from apache_beam.io.gcp.bigquery_tools import parse_table_reference
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError, HttpForbiddenError
  from google.api_core.exceptions import ClientError, DeadlineExceeded
  from google.api_core.exceptions import InternalServerError
except ImportError:
  ClientError = None
  DeadlineExceeded = None
  HttpError = None
  HttpForbiddenError = None
  InternalServerError = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestTableSchemaParser(unittest.TestCase):
  def test_parse_table_schema_from_json(self):
    string_field = bigquery.TableFieldSchema(
        name='s', type='STRING', mode='NULLABLE', description='s description')
    number_field = bigquery.TableFieldSchema(
        name='n', type='INTEGER', mode='REQUIRED', description='n description')
    record_field = bigquery.TableFieldSchema(
        name='r',
        type='RECORD',
        mode='REQUIRED',
        description='r description',
        fields=[string_field, number_field])
    expected_schema = bigquery.TableSchema(fields=[record_field])
    json_str = json.dumps({
        'fields': [{
            'name': 'r',
            'type': 'RECORD',
            'mode': 'REQUIRED',
            'description': 'r description',
            'fields': [{
                'name': 's',
                'type': 'STRING',
                'mode': 'NULLABLE',
                'description': 's description'
            },
                       {
                           'name': 'n',
                           'type': 'INTEGER',
                           'mode': 'REQUIRED',
                           'description': 'n description'
                       }]
        }]
    })
    self.assertEqual(parse_table_schema_from_json(json_str), expected_schema)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestTableReferenceParser(unittest.TestCase):
  def test_calling_with_table_reference(self):
    table_ref = bigquery.TableReference()
    table_ref.projectId = 'test_project'
    table_ref.datasetId = 'test_dataset'
    table_ref.tableId = 'test_table'
    parsed_ref = parse_table_reference(table_ref)
    self.assertEqual(table_ref, parsed_ref)
    self.assertIsNot(table_ref, parsed_ref)

  def test_calling_with_callable(self):
    callable_ref = lambda: 'foo'
    parsed_ref = parse_table_reference(callable_ref)
    self.assertIs(callable_ref, parsed_ref)

  def test_calling_with_value_provider(self):
    value_provider_ref = StaticValueProvider(str, 'test_dataset.test_table')
    parsed_ref = parse_table_reference(value_provider_ref)
    self.assertIs(value_provider_ref, parsed_ref)

  def test_calling_with_fully_qualified_table_ref(self):
    projectId = 'test_project'
    datasetId = 'test_dataset'
    tableId = 'test_table'
    fully_qualified_table = '{}:{}.{}'.format(projectId, datasetId, tableId)
    parsed_ref = parse_table_reference(fully_qualified_table)
    self.assertIsInstance(parsed_ref, bigquery.TableReference)
    self.assertEqual(parsed_ref.projectId, projectId)
    self.assertEqual(parsed_ref.datasetId, datasetId)
    self.assertEqual(parsed_ref.tableId, tableId)

  def test_calling_with_hyphened_table_ref(self):
    projectId = 'test_project'
    datasetId = 'test_dataset'
    tableId = 'test-table'
    fully_qualified_table = '{}:{}.{}'.format(projectId, datasetId, tableId)
    parsed_ref = parse_table_reference(fully_qualified_table)
    self.assertIsInstance(parsed_ref, bigquery.TableReference)
    self.assertEqual(parsed_ref.projectId, projectId)
    self.assertEqual(parsed_ref.datasetId, datasetId)
    self.assertEqual(parsed_ref.tableId, tableId)

  def test_calling_with_partially_qualified_table_ref(self):
    datasetId = 'test_dataset'
    tableId = 'test_table'
    partially_qualified_table = '{}.{}'.format(datasetId, tableId)
    parsed_ref = parse_table_reference(partially_qualified_table)
    self.assertIsInstance(parsed_ref, bigquery.TableReference)
    self.assertEqual(parsed_ref.datasetId, datasetId)
    self.assertEqual(parsed_ref.tableId, tableId)

  def test_calling_with_insufficient_table_ref(self):
    table = 'test_table'
    self.assertRaises(ValueError, parse_table_reference, table)

  def test_calling_with_all_arguments(self):
    projectId = 'test_project'
    datasetId = 'test_dataset'
    tableId = 'test_table'
    parsed_ref = parse_table_reference(
        tableId, dataset=datasetId, project=projectId)
    self.assertIsInstance(parsed_ref, bigquery.TableReference)
    self.assertEqual(parsed_ref.projectId, projectId)
    self.assertEqual(parsed_ref.datasetId, datasetId)
    self.assertEqual(parsed_ref.tableId, tableId)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQueryWrapper(unittest.TestCase):
  def test_delete_non_existing_dataset(self):
    client = mock.Mock()
    client.datasets.Delete.side_effect = HttpError(
        response={'status': '404'}, url='', content='')
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    wrapper._delete_dataset('', '')
    self.assertTrue(client.datasets.Delete.called)

  @mock.patch('time.sleep', return_value=None)
  def test_delete_dataset_retries_fail(self, patched_time_sleep):
    client = mock.Mock()
    client.datasets.Delete.side_effect = ValueError("Cannot delete")
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    with self.assertRaises(ValueError):
      wrapper._delete_dataset('', '')
    self.assertEqual(
        beam.io.gcp.bigquery_tools.MAX_RETRIES + 1,
        client.datasets.Delete.call_count)
    self.assertTrue(client.datasets.Delete.called)

  def test_delete_non_existing_table(self):
    client = mock.Mock()
    client.tables.Delete.side_effect = HttpError(
        response={'status': '404'}, url='', content='')
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    wrapper._delete_table('', '', '')
    self.assertTrue(client.tables.Delete.called)

  @mock.patch('time.sleep', return_value=None)
  def test_delete_table_retries_fail(self, patched_time_sleep):
    client = mock.Mock()
    client.tables.Delete.side_effect = ValueError("Cannot delete")
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    with self.assertRaises(ValueError):
      wrapper._delete_table('', '', '')
    self.assertTrue(client.tables.Delete.called)

  @mock.patch('time.sleep', return_value=None)
  def test_delete_dataset_retries_for_timeouts(self, patched_time_sleep):
    client = mock.Mock()
    client.datasets.Delete.side_effect = [
        HttpError(response={'status': '408'}, url='', content=''),
        bigquery.BigqueryDatasetsDeleteResponse()
    ]
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    wrapper._delete_dataset('', '')
    self.assertTrue(client.datasets.Delete.called)

  @mock.patch('time.sleep', return_value=None)
  def test_delete_table_retries_for_timeouts(self, patched_time_sleep):
    client = mock.Mock()
    client.tables.Delete.side_effect = [
        HttpError(response={'status': '408'}, url='', content=''),
        bigquery.BigqueryTablesDeleteResponse()
    ]
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    wrapper._delete_table('', '', '')
    self.assertTrue(client.tables.Delete.called)

  @mock.patch('time.sleep', return_value=None)
  def test_temporary_dataset_is_unique(self, patched_time_sleep):
    client = mock.Mock()
    client.datasets.Get.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project_id', datasetId='dataset_id'))
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    with self.assertRaises(RuntimeError):
      wrapper.create_temporary_dataset('project_id', 'location')
    self.assertTrue(client.datasets.Get.called)

  def test_get_or_create_dataset_created(self):
    client = mock.Mock()
    client.datasets.Get.side_effect = HttpError(
        response={'status': '404'}, url='', content='')
    client.datasets.Insert.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project_id', datasetId='dataset_id'))
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    new_dataset = wrapper.get_or_create_dataset('project_id', 'dataset_id')
    self.assertEqual(new_dataset.datasetReference.datasetId, 'dataset_id')

  def test_get_or_create_dataset_fetched(self):
    client = mock.Mock()
    client.datasets.Get.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project_id', datasetId='dataset_id'))
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    new_dataset = wrapper.get_or_create_dataset('project_id', 'dataset_id')
    self.assertEqual(new_dataset.datasetReference.datasetId, 'dataset_id')

  def test_get_or_create_table(self):
    client = mock.Mock()
    client.tables.Insert.return_value = 'table_id'
    client.tables.Get.side_effect = [None, 'table_id']
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    new_table = wrapper.get_or_create_table(
        'project_id',
        'dataset_id',
        'table_id',
        bigquery.TableSchema(
            fields=[
                bigquery.TableFieldSchema(
                    name='b', type='BOOLEAN', mode='REQUIRED')
            ]),
        False,
        False)
    self.assertEqual(new_table, 'table_id')

  def test_get_or_create_table_race_condition(self):
    client = mock.Mock()
    client.tables.Insert.side_effect = HttpError(
        response={'status': '409'}, url='', content='')
    client.tables.Get.side_effect = [None, 'table_id']
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    new_table = wrapper.get_or_create_table(
        'project_id',
        'dataset_id',
        'table_id',
        bigquery.TableSchema(
            fields=[
                bigquery.TableFieldSchema(
                    name='b', type='BOOLEAN', mode='REQUIRED')
            ]),
        False,
        False)
    self.assertEqual(new_table, 'table_id')

  def test_get_or_create_table_intermittent_exception(self):
    client = mock.Mock()
    client.tables.Insert.side_effect = [
        HttpError(response={'status': '408'}, url='', content=''), 'table_id'
    ]
    client.tables.Get.side_effect = [None, 'table_id']
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    new_table = wrapper.get_or_create_table(
        'project_id',
        'dataset_id',
        'table_id',
        bigquery.TableSchema(
            fields=[
                bigquery.TableFieldSchema(
                    name='b', type='BOOLEAN', mode='REQUIRED')
            ]),
        False,
        False)
    self.assertEqual(new_table, 'table_id')

  def test_get_or_create_table_invalid_tablename(self):
    invalid_names = ['big-query', 'table name', 'a' * 1025]
    for table_id in invalid_names:
      client = mock.Mock()
      client.tables.Get.side_effect = [None]
      wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)

      self.assertRaises(
          ValueError,
          wrapper.get_or_create_table,
          'project_id',
          'dataset_id',
          table_id,
          bigquery.TableSchema(
              fields=[
                  bigquery.TableFieldSchema(
                      name='b', type='BOOLEAN', mode='REQUIRED')
              ]),
          False,
          False)

  def test_wait_for_job_returns_true_when_job_is_done(self):
    def make_response(state):
      m = mock.Mock()
      m.status.errorResult = None
      m.status.state = state
      return m

    client, job_ref = mock.Mock(), mock.Mock()
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    # Return 'DONE' the second time get_job is called.
    wrapper.get_job = mock.Mock(
        side_effect=[make_response('RUNNING'), make_response('DONE')])

    result = wrapper.wait_for_bq_job(
        job_ref, sleep_duration_sec=0, max_retries=5)
    self.assertTrue(result)

  def test_wait_for_job_retries_fail(self):
    client, response, job_ref = mock.Mock(), mock.Mock(), mock.Mock()
    response.status.state = 'RUNNING'
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    # Return 'RUNNING' response forever.
    wrapper.get_job = lambda *args: response

    with self.assertRaises(RuntimeError) as context:
      wrapper.wait_for_bq_job(job_ref, sleep_duration_sec=0, max_retries=5)
    self.assertEqual(
        'The maximum number of retries has been reached',
        str(context.exception))

  def test_get_query_location(self):
    client = mock.Mock()
    query = """
        SELECT
            av.column1, table.column1
        FROM `dataset.authorized_view` as av
        JOIN `dataset.table` as table ON av.column2 = table.column2
    """
    job = mock.MagicMock(spec=bigquery.Job)
    job.statistics.query.referencedTables = [
        bigquery.TableReference(
            projectId="first_project_id",
            datasetId="first_dataset",
            tableId="table_used_by_authorized_view"),
        bigquery.TableReference(
            projectId="second_project_id",
            datasetId="second_dataset",
            tableId="table"),
    ]
    client.jobs.Insert.return_value = job

    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    wrapper.get_table_location = mock.Mock(
        side_effect=[
            HttpForbiddenError(response={'status': '404'}, url='', content=''),
            "US"
        ])
    location = wrapper.get_query_location(
        project_id="second_project_id", query=query, use_legacy_sql=False)
    self.assertEqual("US", location)

  def test_perform_load_job_source_mutual_exclusivity(self):
    client = mock.Mock()
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)

    # Both source_uri and source_stream specified.
    with self.assertRaises(ValueError):
      wrapper.perform_load_job(
          destination=parse_table_reference('project:dataset.table'),
          job_id='job_id',
          source_uris=['gs://example.com/*'],
          source_stream=io.BytesIO())

    # Neither source_uri nor source_stream specified.
    with self.assertRaises(ValueError):
      wrapper.perform_load_job(destination='P:D.T', job_id='J')

  def test_perform_load_job_with_source_stream(self):
    client = mock.Mock()
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)

    wrapper.perform_load_job(
        destination=parse_table_reference('project:dataset.table'),
        job_id='job_id',
        source_stream=io.BytesIO(b'some,data'))

    client.jobs.Insert.assert_called_once()
    upload = client.jobs.Insert.call_args[1]["upload"]
    self.assertEqual(b'some,data', upload.stream.read())

  def verify_write_call_metric(
      self, project_id, dataset_id, table_id, status, count):
    """Check if an metric was recorded for the BQ IO write API call."""
    process_wide_monitoring_infos = list(
        MetricsEnvironment.process_wide_container().
        to_runner_api_monitoring_infos(None).values())
    resource = resource_identifiers.BigQueryTable(
        project_id, dataset_id, table_id)
    labels = {
        # TODO(ajamato): Add Ptransform label.
        monitoring_infos.SERVICE_LABEL: 'BigQuery',
        # Refer to any method which writes elements to BigQuery in batches
        # as "BigQueryBatchWrite". I.e. storage API's insertAll, or future
        # APIs introduced.
        monitoring_infos.METHOD_LABEL: 'BigQueryBatchWrite',
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.BIGQUERY_PROJECT_ID_LABEL: project_id,
        monitoring_infos.BIGQUERY_DATASET_LABEL: dataset_id,
        monitoring_infos.BIGQUERY_TABLE_LABEL: table_id,
        monitoring_infos.STATUS_LABEL: status,
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

  @unittest.skipIf(ClientError is None, 'GCP dependencies are not installed')
  def test_insert_rows_sets_metric_on_failure(self):
    MetricsEnvironment.process_wide_container().reset()
    client = mock.Mock()
    client.insert_rows_json = mock.Mock(
        # Fail a few times, then succeed.
        side_effect=[
            DeadlineExceeded("Deadline Exceeded"),
            InternalServerError("Internal Error"),
            [],
        ])
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    wrapper.insert_rows("my_project", "my_dataset", "my_table", [])

    # Expect two failing calls, then a success (i.e. two retries).
    self.verify_write_call_metric(
        "my_project", "my_dataset", "my_table", "deadline_exceeded", 1)
    self.verify_write_call_metric(
        "my_project", "my_dataset", "my_table", "internal", 1)
    self.verify_write_call_metric(
        "my_project", "my_dataset", "my_table", "ok", 1)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQueryReader(unittest.TestCase):
  def get_test_rows(self):
    now = time.time()
    dt = datetime.datetime.utcfromtimestamp(float(now))
    ts = dt.strftime('%Y-%m-%d %H:%M:%S.%f UTC')
    expected_rows = [{
        'i': 1,
        's': 'abc',
        'f': 2.3,
        'b': True,
        't': ts,
        'dt': '2016-10-31',
        'ts': '22:39:12.627498',
        'dt_ts': '2008-12-25T07:30:00',
        'r': {
            's2': 'b'
        },
        'rpr': [{
            's3': 'c', 'rpr2': [{
                'rs': ['d', 'e'], 's4': None
            }]
        }]
    },
                     {
                         'i': 10,
                         's': 'xyz',
                         'f': -3.14,
                         'b': False,
                         'rpr': [],
                         't': None,
                         'dt': None,
                         'ts': None,
                         'dt_ts': None,
                         'r': None,
                     }]

    nested_schema = [
        bigquery.TableFieldSchema(name='s2', type='STRING', mode='NULLABLE')
    ]
    nested_schema_2 = [
        bigquery.TableFieldSchema(name='s3', type='STRING', mode='NULLABLE'),
        bigquery.TableFieldSchema(
            name='rpr2',
            type='RECORD',
            mode='REPEATED',
            fields=[
                bigquery.TableFieldSchema(
                    name='rs', type='STRING', mode='REPEATED'),
                bigquery.TableFieldSchema(
                    name='s4', type='STRING', mode='NULLABLE')
            ])
    ]

    schema = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(
                name='b', type='BOOLEAN', mode='REQUIRED'),
            bigquery.TableFieldSchema(name='f', type='FLOAT', mode='REQUIRED'),
            bigquery.TableFieldSchema(
                name='i', type='INTEGER', mode='REQUIRED'),
            bigquery.TableFieldSchema(name='s', type='STRING', mode='REQUIRED'),
            bigquery.TableFieldSchema(
                name='t', type='TIMESTAMP', mode='NULLABLE'),
            bigquery.TableFieldSchema(name='dt', type='DATE', mode='NULLABLE'),
            bigquery.TableFieldSchema(name='ts', type='TIME', mode='NULLABLE'),
            bigquery.TableFieldSchema(
                name='dt_ts', type='DATETIME', mode='NULLABLE'),
            bigquery.TableFieldSchema(
                name='r', type='RECORD', mode='NULLABLE', fields=nested_schema),
            bigquery.TableFieldSchema(
                name='rpr',
                type='RECORD',
                mode='REPEATED',
                fields=nested_schema_2)
        ])

    table_rows = [
        bigquery.TableRow(
            f=[
                bigquery.TableCell(v=to_json_value('true')),
                bigquery.TableCell(v=to_json_value(str(2.3))),
                bigquery.TableCell(v=to_json_value(str(1))),
                bigquery.TableCell(v=to_json_value('abc')),
                # For timestamps cannot use str() because it will truncate the
                # number representing the timestamp.
                bigquery.TableCell(v=to_json_value('%f' % now)),
                bigquery.TableCell(v=to_json_value('2016-10-31')),
                bigquery.TableCell(v=to_json_value('22:39:12.627498')),
                bigquery.TableCell(v=to_json_value('2008-12-25T07:30:00')),
                # For record we cannot use dict because it doesn't create nested
                # schemas correctly so we have to use this f,v based format
                bigquery.TableCell(v=to_json_value({'f': [{
                    'v': 'b'
                }]})),
                bigquery.TableCell(
                    v=to_json_value([{
                        'v': {
                            'f': [{
                                'v': 'c'
                            },
                                  {
                                      'v': [{
                                          'v': {
                                              'f': [{
                                                  'v': [{
                                                      'v': 'd'
                                                  }, {
                                                      'v': 'e'
                                                  }]
                                              }, {
                                                  'v': None
                                              }]
                                          }
                                      }]
                                  }]
                        }
                    }]))
            ]),
        bigquery.TableRow(
            f=[
                bigquery.TableCell(v=to_json_value('false')),
                bigquery.TableCell(v=to_json_value(str(-3.14))),
                bigquery.TableCell(v=to_json_value(str(10))),
                bigquery.TableCell(v=to_json_value('xyz')),
                bigquery.TableCell(v=None),
                bigquery.TableCell(v=None),
                bigquery.TableCell(v=None),
                bigquery.TableCell(v=None),
                bigquery.TableCell(v=None),
                # REPEATED field without any values.
                bigquery.TableCell(v=None)
            ])
    ]
    return table_rows, schema, expected_rows

  def test_read_from_table(self):
    client = mock.Mock()
    client.jobs.Insert.return_value = bigquery.Job(
        jobReference=bigquery.JobReference(jobId='somejob'))
    table_rows, schema, expected_rows = self.get_test_rows()
    client.jobs.GetQueryResults.return_value = bigquery.GetQueryResultsResponse(
        jobComplete=True, rows=table_rows, schema=schema)
    actual_rows = []
    with beam.io.BigQuerySource(
        'dataset.table',
        use_dataflow_native_source=True).reader(client) as reader:
      for row in reader:
        actual_rows.append(row)
    self.assertEqual(actual_rows, expected_rows)
    self.assertEqual(schema, reader.schema)

  def test_read_from_query(self):
    client = mock.Mock()
    client.jobs.Insert.return_value = bigquery.Job(
        jobReference=bigquery.JobReference(jobId='somejob'))
    table_rows, schema, expected_rows = self.get_test_rows()
    client.jobs.GetQueryResults.return_value = bigquery.GetQueryResultsResponse(
        jobComplete=True, rows=table_rows, schema=schema)
    actual_rows = []
    with beam.io.BigQuerySource(
        query='query',
        use_dataflow_native_source=True).reader(client) as reader:
      for row in reader:
        actual_rows.append(row)
    self.assertEqual(actual_rows, expected_rows)
    self.assertEqual(schema, reader.schema)
    self.assertTrue(reader.use_legacy_sql)
    self.assertTrue(reader.flatten_results)

  def test_read_from_query_sql_format(self):
    client = mock.Mock()
    client.jobs.Insert.return_value = bigquery.Job(
        jobReference=bigquery.JobReference(jobId='somejob'))
    table_rows, schema, expected_rows = self.get_test_rows()
    client.jobs.GetQueryResults.return_value = bigquery.GetQueryResultsResponse(
        jobComplete=True, rows=table_rows, schema=schema)
    actual_rows = []
    with beam.io.BigQuerySource(
        query='query', use_standard_sql=True,
        use_dataflow_native_source=True).reader(client) as reader:
      for row in reader:
        actual_rows.append(row)
    self.assertEqual(actual_rows, expected_rows)
    self.assertEqual(schema, reader.schema)
    self.assertFalse(reader.use_legacy_sql)
    self.assertTrue(reader.flatten_results)

  def test_read_from_query_unflatten_records(self):
    client = mock.Mock()
    client.jobs.Insert.return_value = bigquery.Job(
        jobReference=bigquery.JobReference(jobId='somejob'))
    table_rows, schema, expected_rows = self.get_test_rows()
    client.jobs.GetQueryResults.return_value = bigquery.GetQueryResultsResponse(
        jobComplete=True, rows=table_rows, schema=schema)
    actual_rows = []
    with beam.io.BigQuerySource(
        query='query', flatten_results=False,
        use_dataflow_native_source=True).reader(client) as reader:
      for row in reader:
        actual_rows.append(row)
    self.assertEqual(actual_rows, expected_rows)
    self.assertEqual(schema, reader.schema)
    self.assertTrue(reader.use_legacy_sql)
    self.assertFalse(reader.flatten_results)

  def test_using_both_query_and_table_fails(self):
    with self.assertRaisesRegex(
        ValueError,
        r'Both a BigQuery table and a query were specified\. Please specify '
        r'only one of these'):
      beam.io.BigQuerySource(
          table='dataset.table', query='query', use_dataflow_native_source=True)

  def test_using_neither_query_nor_table_fails(self):
    with self.assertRaisesRegex(
        ValueError, r'A BigQuery table or a query must be specified'):
      beam.io.BigQuerySource(use_dataflow_native_source=True)

  def test_read_from_table_as_tablerows(self):
    client = mock.Mock()
    client.jobs.Insert.return_value = bigquery.Job(
        jobReference=bigquery.JobReference(jobId='somejob'))
    table_rows, schema, _ = self.get_test_rows()
    client.jobs.GetQueryResults.return_value = bigquery.GetQueryResultsResponse(
        jobComplete=True, rows=table_rows, schema=schema)
    actual_rows = []
    # We set the coder to TableRowJsonCoder, which is a signal that
    # the caller wants to see the rows as TableRows.
    with beam.io.BigQuerySource(
        'dataset.table',
        coder=TableRowJsonCoder,
        use_dataflow_native_source=True).reader(client) as reader:
      for row in reader:
        actual_rows.append(row)
    self.assertEqual(actual_rows, table_rows)
    self.assertEqual(schema, reader.schema)

  @mock.patch('time.sleep', return_value=None)
  def test_read_from_table_and_job_complete_retry(self, patched_time_sleep):
    client = mock.Mock()
    client.jobs.Insert.return_value = bigquery.Job(
        jobReference=bigquery.JobReference(jobId='somejob'))
    table_rows, schema, expected_rows = self.get_test_rows()
    # Return jobComplete=False on first call to trigger the code path where
    # query needs to handle waiting a bit.
    client.jobs.GetQueryResults.side_effect = [
        bigquery.GetQueryResultsResponse(jobComplete=False),
        bigquery.GetQueryResultsResponse(
            jobComplete=True, rows=table_rows, schema=schema)
    ]
    actual_rows = []
    with beam.io.BigQuerySource(
        'dataset.table',
        use_dataflow_native_source=True).reader(client) as reader:
      for row in reader:
        actual_rows.append(row)
    self.assertEqual(actual_rows, expected_rows)

  def test_read_from_table_and_multiple_pages(self):
    client = mock.Mock()
    client.jobs.Insert.return_value = bigquery.Job(
        jobReference=bigquery.JobReference(jobId='somejob'))
    table_rows, schema, expected_rows = self.get_test_rows()
    # Return a pageToken on first call to trigger the code path where
    # query needs to handle multiple pages of results.
    client.jobs.GetQueryResults.side_effect = [
        bigquery.GetQueryResultsResponse(
            jobComplete=True, rows=table_rows, schema=schema,
            pageToken='token'),
        bigquery.GetQueryResultsResponse(
            jobComplete=True, rows=table_rows, schema=schema)
    ]
    actual_rows = []
    with beam.io.BigQuerySource(
        'dataset.table',
        use_dataflow_native_source=True).reader(client) as reader:
      for row in reader:
        actual_rows.append(row)
    # We return expected rows for each of the two pages of results so we
    # adjust our expectation below accordingly.
    self.assertEqual(actual_rows, expected_rows * 2)

  def test_table_schema_without_project(self):
    # Reader should pick executing project by default.
    source = beam.io.BigQuerySource(
        table='mydataset.mytable', use_dataflow_native_source=True)
    options = PipelineOptions(flags=['--project', 'myproject'])
    source.pipeline_options = options
    reader = source.reader()
    self.assertEqual(
        'SELECT * FROM [myproject:mydataset.mytable];', reader.query)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQueryWriter(unittest.TestCase):
  @mock.patch('time.sleep', return_value=None)
  def test_no_table_and_create_never(self, patched_time_sleep):
    client = mock.Mock()
    client.tables.Get.side_effect = HttpError(
        response={'status': '404'}, url='', content='')
    create_disposition = beam.io.BigQueryDisposition.CREATE_NEVER
    with self.assertRaisesRegex(
        RuntimeError,
        r'Table project:dataset\.table not found but create '
        r'disposition is CREATE_NEVER'):
      with beam.io.BigQuerySink(
          'project:dataset.table',
          create_disposition=create_disposition).writer(client):
        pass

  def test_no_table_and_create_if_needed(self):
    client = mock.Mock()
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project', datasetId='dataset', tableId='table'),
        schema=bigquery.TableSchema())
    client.tables.Get.side_effect = HttpError(
        response={'status': '404'}, url='', content='')
    client.tables.Insert.return_value = table
    create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    with beam.io.BigQuerySink(
        'project:dataset.table',
        schema='somefield:INTEGER',
        create_disposition=create_disposition).writer(client):
      pass
    self.assertTrue(client.tables.Get.called)
    self.assertTrue(client.tables.Insert.called)

  @mock.patch('time.sleep', return_value=None)
  def test_no_table_and_create_if_needed_and_no_schema(
      self, patched_time_sleep):
    client = mock.Mock()
    client.tables.Get.side_effect = HttpError(
        response={'status': '404'}, url='', content='')
    create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    with self.assertRaisesRegex(
        RuntimeError,
        r'Table project:dataset\.table requires a schema\. None '
        r'can be inferred because the table does not exist'):
      with beam.io.BigQuerySink(
          'project:dataset.table',
          create_disposition=create_disposition).writer(client):
        pass

  @mock.patch('time.sleep', return_value=None)
  def test_table_not_empty_and_write_disposition_empty(
      self, patched_time_sleep):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project', datasetId='dataset', tableId='table'),
        schema=bigquery.TableSchema())
    client.tabledata.List.return_value = bigquery.TableDataList(totalRows=1)
    write_disposition = beam.io.BigQueryDisposition.WRITE_EMPTY
    with self.assertRaisesRegex(
        RuntimeError,
        r'Table project:dataset\.table is not empty but write '
        r'disposition is WRITE_EMPTY'):
      with beam.io.BigQuerySink(
          'project:dataset.table',
          write_disposition=write_disposition).writer(client):
        pass

  def test_table_empty_and_write_disposition_empty(self):
    client = mock.Mock()
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project', datasetId='dataset', tableId='table'),
        schema=bigquery.TableSchema())
    client.tables.Get.return_value = table
    client.tabledata.List.return_value = bigquery.TableDataList(totalRows=0)
    client.tables.Insert.return_value = table
    write_disposition = beam.io.BigQueryDisposition.WRITE_EMPTY
    with beam.io.BigQuerySink(
        'project:dataset.table',
        write_disposition=write_disposition).writer(client):
      pass
    self.assertTrue(client.tables.Get.called)
    self.assertTrue(client.tabledata.List.called)
    self.assertFalse(client.tables.Delete.called)
    self.assertFalse(client.tables.Insert.called)

  @mock.patch('time.sleep', return_value=None)
  def test_table_with_write_disposition_truncate(self, _patched_sleep):
    client = mock.Mock()
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project', datasetId='dataset', tableId='table'),
        schema=bigquery.TableSchema())
    client.tables.Get.return_value = table
    client.tables.Insert.return_value = table
    write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
    with beam.io.BigQuerySink(
        'project:dataset.table',
        write_disposition=write_disposition).writer(client):
      pass
    self.assertTrue(client.tables.Get.called)
    self.assertTrue(client.tables.Delete.called)
    self.assertTrue(client.tables.Insert.called)

  def test_table_with_write_disposition_append(self):
    client = mock.Mock()
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project', datasetId='dataset', tableId='table'),
        schema=bigquery.TableSchema())
    client.tables.Get.return_value = table
    client.tables.Insert.return_value = table
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
    with beam.io.BigQuerySink(
        'project:dataset.table',
        write_disposition=write_disposition).writer(client):
      pass
    self.assertTrue(client.tables.Get.called)
    self.assertFalse(client.tables.Delete.called)
    self.assertFalse(client.tables.Insert.called)

  def test_rows_are_written(self):
    client = mock.Mock()
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project', datasetId='dataset', tableId='table'),
        schema=bigquery.TableSchema())
    client.tables.Get.return_value = table
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND

    client.insert_rows_json.return_value = []

    with beam.io.BigQuerySink(
        'project:dataset.table',
        write_disposition=write_disposition).writer(client) as writer:
      writer.Write({'i': 1, 'b': True, 's': 'abc', 'f': 3.14})

    sample_row = {'i': 1, 'b': True, 's': 'abc', 'f': 3.14}
    client.insert_rows_json.assert_called_with(
        '%s.%s.%s' % ('project', 'dataset', 'table'),
        json_rows=[sample_row],
        row_ids=['_1'],
        skip_invalid_rows=True)

  def test_table_schema_without_project(self):
    # Writer should pick executing project by default.
    sink = beam.io.BigQuerySink(table='mydataset.mytable')
    options = PipelineOptions(flags=['--project', 'myproject'])
    sink.pipeline_options = options
    writer = sink.writer()
    self.assertEqual('myproject', writer.project_id)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestRowAsDictJsonCoder(unittest.TestCase):
  def test_row_as_dict(self):
    coder = RowAsDictJsonCoder()
    test_value = {'s': 'abc', 'i': 123, 'f': 123.456, 'b': True}
    self.assertEqual(test_value, coder.decode(coder.encode(test_value)))

  def test_decimal_in_row_as_dict(self):
    decimal_value = decimal.Decimal('123456789.987654321')
    coder = RowAsDictJsonCoder()
    # Bigquery IO uses decimals to represent NUMERIC types.
    # To export to BQ, it's necessary to convert to strings, due to the
    # lower precision of JSON numbers. This means that we can't recognize
    # a NUMERIC when we decode from JSON, thus we match the string here.
    test_value = {'f': 123.456, 'b': True, 'numerico': decimal_value}
    output_value = {'f': 123.456, 'b': True, 'numerico': str(decimal_value)}
    self.assertEqual(output_value, coder.decode(coder.encode(test_value)))

  def json_compliance_exception(self, value):
    with self.assertRaisesRegex(ValueError, re.escape(JSON_COMPLIANCE_ERROR)):
      coder = RowAsDictJsonCoder()
      test_value = {'s': value}
      coder.decode(coder.encode(test_value))

  def test_invalid_json_nan(self):
    self.json_compliance_exception(float('nan'))

  def test_invalid_json_inf(self):
    self.json_compliance_exception(float('inf'))

  def test_invalid_json_neg_inf(self):
    self.json_compliance_exception(float('-inf'))


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestJsonRowWriter(unittest.TestCase):
  def test_write_row(self):
    rows = [
        {
            'name': 'beam', 'game': 'dream'
        },
        {
            'name': 'team', 'game': 'cream'
        },
    ]

    with io.BytesIO() as buf:
      # Mock close() so we can access the buffer contents
      # after JsonRowWriter is closed.
      with mock.patch.object(buf, 'close') as mock_close:
        writer = JsonRowWriter(buf)
        for row in rows:
          writer.write(row)
        writer.close()

        mock_close.assert_called_once()

      buf.seek(0)
      read_rows = [
          json.loads(row)
          for row in buf.getvalue().strip().decode('utf-8').split('\n')
      ]

    self.assertEqual(read_rows, rows)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestAvroRowWriter(unittest.TestCase):
  def test_write_row(self):
    schema = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(name='stamp', type='TIMESTAMP'),
            bigquery.TableFieldSchema(
                name='number', type='FLOAT', mode='REQUIRED'),
        ])
    stamp = datetime.datetime(2020, 2, 25, 12, 0, 0, tzinfo=pytz.utc)

    with io.BytesIO() as buf:
      # Mock close() so we can access the buffer contents
      # after AvroRowWriter is closed.
      with mock.patch.object(buf, 'close') as mock_close:
        writer = AvroRowWriter(buf, schema)
        writer.write({'stamp': stamp, 'number': float('NaN')})
        writer.close()

        mock_close.assert_called_once()

      buf.seek(0)
      records = [r for r in fastavro.reader(buf)]

    self.assertEqual(len(records), 1)
    self.assertTrue(math.isnan(records[0]['number']))
    self.assertEqual(records[0]['stamp'], stamp)


class TestBQJobNames(unittest.TestCase):
  def test_simple_names(self):
    self.assertEqual(
        "beam_bq_job_EXPORT_beamappjobtest_abcd",
        generate_bq_job_name(
            "beamapp-job-test", "abcd", BigQueryJobTypes.EXPORT))

    self.assertEqual(
        "beam_bq_job_LOAD_beamappjobtest_abcd",
        generate_bq_job_name("beamapp-job-test", "abcd", BigQueryJobTypes.LOAD))

    self.assertEqual(
        "beam_bq_job_QUERY_beamappjobtest_abcd",
        generate_bq_job_name(
            "beamapp-job-test", "abcd", BigQueryJobTypes.QUERY))

    self.assertEqual(
        "beam_bq_job_COPY_beamappjobtest_abcd",
        generate_bq_job_name("beamapp-job-test", "abcd", BigQueryJobTypes.COPY))

  def test_random_in_name(self):
    self.assertEqual(
        "beam_bq_job_COPY_beamappjobtest_abcd_randome",
        generate_bq_job_name(
            "beamapp-job-test", "abcd", BigQueryJobTypes.COPY, "randome"))

  def test_matches_template(self):
    base_pattern = "beam_bq_job_[A-Z]+_[a-z0-9-]+_[a-z0-9-]+(_[a-z0-9-]+)?"
    job_name = generate_bq_job_name(
        "beamapp-job-test", "abcd", BigQueryJobTypes.COPY, "randome")
    self.assertRegex(job_name, base_pattern)

    job_name = generate_bq_job_name(
        "beamapp-job-test", "abcd", BigQueryJobTypes.COPY)
    self.assertRegex(job_name, base_pattern)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestCheckSchemaEqual(unittest.TestCase):
  def test_simple_schemas(self):
    schema1 = bigquery.TableSchema(fields=[])
    self.assertTrue(check_schema_equal(schema1, schema1))

    schema2 = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(name="a", mode="NULLABLE", type="INT64")
        ])
    self.assertTrue(check_schema_equal(schema2, schema2))
    self.assertFalse(check_schema_equal(schema1, schema2))

    schema3 = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(
                name="b",
                mode="REPEATED",
                type="RECORD",
                fields=[
                    bigquery.TableFieldSchema(
                        name="c", mode="REQUIRED", type="BOOL")
                ])
        ])
    self.assertTrue(check_schema_equal(schema3, schema3))
    self.assertFalse(check_schema_equal(schema2, schema3))

  def test_field_order(self):
    """Test that field order is ignored when ignore_field_order=True."""
    schema1 = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(
                name="a", mode="REQUIRED", type="FLOAT64"),
            bigquery.TableFieldSchema(name="b", mode="REQUIRED", type="INT64"),
        ])

    schema2 = bigquery.TableSchema(fields=list(reversed(schema1.fields)))

    self.assertFalse(check_schema_equal(schema1, schema2))
    self.assertTrue(
        check_schema_equal(schema1, schema2, ignore_field_order=True))

  def test_descriptions(self):
    """
        Test that differences in description are ignored
        when ignore_descriptions=True.
        """
    schema1 = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(
                name="a",
                mode="REQUIRED",
                type="FLOAT64",
                description="Field A",
            ),
            bigquery.TableFieldSchema(
                name="b",
                mode="REQUIRED",
                type="INT64",
            ),
        ])

    schema2 = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(
                name="a",
                mode="REQUIRED",
                type="FLOAT64",
                description="Field A is for Apple"),
            bigquery.TableFieldSchema(
                name="b",
                mode="REQUIRED",
                type="INT64",
                description="Field B",
            ),
        ])

    self.assertFalse(check_schema_equal(schema1, schema2))
    self.assertTrue(
        check_schema_equal(schema1, schema2, ignore_descriptions=True))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
