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
import unittest

import fastavro
import mock
import pytz
from parameterized import parameterized

import apache_beam as beam
from apache_beam.io.gcp import resource_identifiers
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
from apache_beam.options.value_provider import StaticValueProvider

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError, HttpForbiddenError
  from google.api_core.exceptions import ClientError, DeadlineExceeded
  from google.api_core.exceptions import InternalServerError
  import google.cloud
except ImportError:
  ClientError = None
  DeadlineExceeded = None
  HttpError = None
  HttpForbiddenError = None
  InternalServerError = None
  google = None
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

  @parameterized.expand([
      ('project:dataset.test_table', 'project', 'dataset', 'test_table'),
      ('project:dataset.test-table', 'project', 'dataset', 'test-table'),
      ('project:dataset.test- table', 'project', 'dataset', 'test- table'),
      ('project.dataset. test_table', 'project', 'dataset', ' test_table'),
      ('project.dataset.test$table', 'project', 'dataset', 'test$table'),
  ])
  def test_calling_with_fully_qualified_table_ref(
      self,
      fully_qualified_table: str,
      project_id: str,
      dataset_id: str,
      table_id: str,
  ):
    parsed_ref = parse_table_reference(fully_qualified_table)
    self.assertIsInstance(parsed_ref, bigquery.TableReference)
    self.assertEqual(parsed_ref.projectId, project_id)
    self.assertEqual(parsed_ref.datasetId, dataset_id)
    self.assertEqual(parsed_ref.tableId, table_id)

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

  @unittest.skipIf(
      google and not hasattr(google.cloud, '_http'),  # pylint: disable=c-extension-no-member
      'Dependencies not installed')
  @mock.patch('time.sleep', return_value=None)
  @mock.patch('google.cloud._http.JSONConnection.http')
  def test_user_agent_insert_all(self, http_mock, patched_sleep):
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper()
    try:
      wrapper._insert_all_rows('p', 'd', 't', [{'name': 'any'}], None)
    except:  # pylint: disable=bare-except
      # Ignore errors. The errors come from the fact that we did not mock
      # the response from the API, so the overall insert_all_rows call fails
      # soon after the BQ API is called.
      pass
    call = http_mock.request.mock_calls[-2]
    self.assertIn('apache-beam-', call[2]['headers']['User-Agent'])

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
            projectId='project-id', datasetId='dataset_id'))
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    with self.assertRaises(RuntimeError):
      wrapper.create_temporary_dataset('project-id', 'location')
    self.assertTrue(client.datasets.Get.called)

  @mock.patch('time.sleep', return_value=None)
  def test_user_agent_passed(self, sleep_mock):
    try:
      wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper()
    except:  # pylint: disable=bare-except
      self.skipTest('Unable to create a BQ Wrapper')
    request_mock = mock.Mock()
    wrapper.client._http.request = request_mock
    try:
      wrapper.create_temporary_dataset('project-id', 'location')
    except:  # pylint: disable=bare-except
      # Ignore errors. The errors come from the fact that we did not mock
      # the response from the API, so the overall create_dataset call fails
      # soon after the BQ API is called.
      pass
    call = request_mock.mock_calls[-1]
    self.assertIn('apache-beam-', call[2]['headers']['user-agent'])

  def test_get_or_create_dataset_created(self):
    client = mock.Mock()
    client.datasets.Get.side_effect = HttpError(
        response={'status': '404'}, url='', content='')
    client.datasets.Insert.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project-id', datasetId='dataset_id'))
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    new_dataset = wrapper.get_or_create_dataset('project-id', 'dataset_id')
    self.assertEqual(new_dataset.datasetReference.datasetId, 'dataset_id')

  def test_get_or_create_dataset_fetched(self):
    client = mock.Mock()
    client.datasets.Get.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project-id', datasetId='dataset_id'))
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    new_dataset = wrapper.get_or_create_dataset('project-id', 'dataset_id')
    self.assertEqual(new_dataset.datasetReference.datasetId, 'dataset_id')

  def test_get_or_create_table(self):
    client = mock.Mock()
    client.tables.Insert.return_value = 'table_id'
    client.tables.Get.side_effect = [None, 'table_id']
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)
    new_table = wrapper.get_or_create_table(
        'project-id',
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
        'project-id',
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
        'project-id',
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

  @parameterized.expand(['', 'a' * 1025])
  def test_get_or_create_table_invalid_tablename(self, table_id):
    client = mock.Mock()
    client.tables.Get.side_effect = [None]
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)

    self.assertRaises(
        ValueError,
        wrapper.get_or_create_table,
        'project-id',
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
    wrapper.perform_load_job(
        destination=parse_table_reference('project:dataset.table'), job_id='J')

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

  def test_perform_load_job_with_load_job_id(self):
    client = mock.Mock()
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)

    wrapper.perform_load_job(
        destination=parse_table_reference('project:dataset.table'),
        job_id='job_id',
        source_uris=['gs://example.com/*'],
        load_job_project_id='loadId')
    call_args = client.jobs.Insert.call_args
    self.assertEqual('loadId', call_args[0][0].projectId)

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

  @unittest.skipIf(ClientError is None, 'GCP dependencies are not installed')
  def test_start_query_job_priority_configuration(self):
    client = mock.Mock()
    wrapper = beam.io.gcp.bigquery_tools.BigQueryWrapper(client)

    query_result = mock.Mock()
    query_result.pageToken = None
    wrapper._get_query_results = mock.Mock(return_value=query_result)

    wrapper._start_query_job(
        "my_project",
        "my_query",
        use_legacy_sql=False,
        flatten_results=False,
        job_id="my_job_id",
        priority=beam.io.BigQueryQueryPriority.BATCH)

    self.assertEqual(
        client.jobs.Insert.call_args[0][0].job.configuration.query.priority,
        'BATCH')

    wrapper._start_query_job(
        "my_project",
        "my_query",
        use_legacy_sql=False,
        flatten_results=False,
        job_id="my_job_id",
        priority=beam.io.BigQueryQueryPriority.INTERACTIVE)

    self.assertEqual(
        client.jobs.Insert.call_args[0][0].job.configuration.query.priority,
        'INTERACTIVE')


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

  def test_ensure_ascii(self):
    coder = RowAsDictJsonCoder()
    test_value = {'s': '🎉'}
    output_value = b'{"s": "\xf0\x9f\x8e\x89"}'

    self.assertEqual(output_value, coder.encode(test_value))


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
