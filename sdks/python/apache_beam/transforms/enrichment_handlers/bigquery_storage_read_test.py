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

import unittest
from unittest import mock

from apache_beam.pvalue import Row as BeamRow

try:
  from apache_beam.transforms.enrichment_handlers import bigquery_storage_read
except ImportError:
  raise unittest.SkipTest(
      "Google Cloud BigQuery Storage dependencies are not installed.")


class TestBigQueryStorageEnrichmentHandler(unittest.TestCase):
  def setUp(self):
    self.project = 'test-project'
    self.table_name = 'test-project.test_dataset.test_table'
    self.fields = ['id']
    self.row_restriction_template = 'id = "{id}"'
    self.column_names = ['id', 'value']

  def make_handler(self, **kwargs):
    handler_kwargs = {
        'project': self.project,
        'table_name': self.table_name,
        'row_restriction_template': self.row_restriction_template,
        'fields': self.fields,
        'column_names': self.column_names,
    }
    handler_kwargs.update(kwargs)  # Override defaults with provided kwargs
    return bigquery_storage_read.BigQueryStorageEnrichmentHandler(
        **handler_kwargs)

  def test_init_invalid_args(self):
    # Both row_restriction_template and row_restriction_template_fn
    with self.assertRaises(ValueError):
      bigquery_storage_read.BigQueryStorageEnrichmentHandler(
          project=self.project,
          table_name=self.table_name,
          row_restriction_template='foo',
          row_restriction_template_fn=lambda d, p, r: 'bar',
          fields=self.fields)
    # Neither row_restriction_template nor row_restriction_template_fn
    with self.assertRaises(ValueError):
      bigquery_storage_read.BigQueryStorageEnrichmentHandler(
          project=self.project, table_name=self.table_name, fields=self.fields)
    # Both fields and condition_value_fn
    with self.assertRaises(ValueError):
      bigquery_storage_read.BigQueryStorageEnrichmentHandler(
          project=self.project,
          table_name=self.table_name,
          row_restriction_template='foo',
          fields=self.fields,
          condition_value_fn=lambda r: {'id': 1})
    # Neither fields nor condition_value_fn
    with self.assertRaises(ValueError):
      bigquery_storage_read.BigQueryStorageEnrichmentHandler(
          project=self.project,
          table_name=self.table_name,
          row_restriction_template='foo')

  def test_get_condition_values_dict_fields(self):
    handler = self.make_handler()
    row = BeamRow(id=1, value='a')
    self.assertEqual(handler._get_condition_values_dict(row), {'id': 1})

  def test_get_condition_values_dict_missing_field(self):
    handler = self.make_handler()
    row = BeamRow(value='a')
    self.assertIsNone(handler._get_condition_values_dict(row))

  def test_get_condition_values_dict_condition_value_fn(self):
    handler = self.make_handler(
        fields=None, condition_value_fn=lambda r: {'id': 2})
    row = BeamRow(id=2, value='b')
    self.assertEqual(handler._get_condition_values_dict(row), {'id': 2})

  def test_build_single_row_filter_template(self):
    handler = self.make_handler()
    row = BeamRow(id=3, value='c')
    cond = {'id': 3}
    self.assertEqual(handler._build_single_row_filter(row, cond), 'id = "3"')

  def test_build_single_row_filter_fn(self):
    fn = lambda d, p, r: f"id = '{d['id']}'"
    handler = self.make_handler(
        row_restriction_template=None, row_restriction_template_fn=fn)
    row = BeamRow(id=4, value='d')
    cond = {'id': 4}
    self.assertEqual(handler._build_single_row_filter(row, cond), "id = '4'")

  def test_apply_renaming(self):
    handler = self.make_handler(column_names=['id as new_id', 'value'])
    bq_row = {'id': 1, 'value': 'foo'}
    self.assertEqual(
        handler._apply_renaming(bq_row), {
            'new_id': 1, 'value': 'foo'
        })

  def test_apply_renaming_all_columns_aliased(self):
    """Test column aliasing when all columns are aliased - expected output
    should have aliased keys."""
    handler = self.make_handler(
        column_names=['id as user_id', 'value as user_value'])
    bq_row = {'id': 42, 'value': 'test_data'}
    # When all columns are aliased, the expected output should only contain
    # aliased keys
    expected_result = {'user_id': 42, 'user_value': 'test_data'}
    actual_result = handler._apply_renaming(bq_row)
    self.assertEqual(actual_result, expected_result)

    # Verify that no original column names remain in the output
    self.assertNotIn('id', actual_result)
    self.assertNotIn('value', actual_result)

    # Verify that all expected aliased keys are present
    self.assertIn('user_id', actual_result)
    self.assertIn('user_value', actual_result)

  def test_create_row_key(self):
    handler = self.make_handler()
    row = BeamRow(id=5, value='e')
    self.assertEqual(handler.create_row_key(row), (('id', 5), ))

  @mock.patch.object(
      bigquery_storage_read.BigQueryStorageEnrichmentHandler,
      '_execute_storage_read')
  def test_call_single_match(self, mock_exec):
    handler = self.make_handler()
    row = BeamRow(id=6, value='f')
    mock_exec.return_value = [{'id': 6, 'value': 'fetched'}]
    req, resp = handler(row)
    self.assertEqual(req, row)
    self.assertEqual(resp.id, 6)
    self.assertEqual(resp.value, 'fetched')

  @mock.patch.object(
      bigquery_storage_read.BigQueryStorageEnrichmentHandler,
      '_execute_storage_read')
  def test_call_single_no_match(self, mock_exec):
    handler = self.make_handler()
    row = BeamRow(id=7, value='g')
    mock_exec.return_value = []
    req, resp = handler(row)
    self.assertEqual(req, row)
    self.assertEqual(resp, BeamRow())

  @mock.patch.object(
      bigquery_storage_read.BigQueryStorageEnrichmentHandler,
      '_execute_storage_read')
  def test_call_batch(self, mock_exec):
    handler = self.make_handler()
    rows = [BeamRow(id=8, value='h'), BeamRow(id=9, value='i')]
    mock_exec.return_value = [{
        'id': 8, 'value': 'h_bq'
    }, {
        'id': 9, 'value': 'i_bq'
    }]
    result = handler(rows)
    self.assertEqual(result[0][0], rows[0])
    self.assertEqual(result[0][1].id, 8)
    self.assertEqual(result[0][1].value, 'h_bq')
    self.assertEqual(result[1][0], rows[1])
    self.assertEqual(result[1][1].id, 9)
    self.assertEqual(result[1][1].value, 'i_bq')

  @mock.patch.object(
      bigquery_storage_read.BigQueryStorageEnrichmentHandler,
      '_execute_storage_read')
  def test_call_batch_no_match(self, mock_exec):
    handler = self.make_handler()
    rows = [BeamRow(id=10, value='j'), BeamRow(id=11, value='k')]
    mock_exec.return_value = []
    result = handler(rows)
    self.assertEqual(result[0][0], rows[0])
    self.assertEqual(result[0][1], BeamRow())
    self.assertEqual(result[1][0], rows[1])
    self.assertEqual(result[1][1], BeamRow())

  def test_get_cache_key(self):
    handler = self.make_handler()
    row = BeamRow(id=12, value='l')
    self.assertEqual(handler.get_cache_key(row), str((('id', 12), )))
    rows = [BeamRow(id=13, value='m'), BeamRow(id=14, value='n')]
    self.assertEqual(
        handler.get_cache_key(rows), [str((('id', 13), )), str((('id', 14), ))])

  def test_batch_elements_kwargs(self):
    handler = self.make_handler(
        min_batch_size=2, max_batch_size=5, max_batch_duration_secs=10)
    self.assertEqual(
        handler.batch_elements_kwargs(), {
            'min_batch_size': 2,
            'max_batch_size': 5,
            'max_batch_duration_secs': 10
        })

  def test_max_stream_count_default(self):
    """Test that max_stream_count defaults to 100."""
    handler = self.make_handler()
    self.assertEqual(handler.max_stream_count, 100)

  def test_max_stream_count_custom(self):
    """Test that max_stream_count can be set to a custom value."""
    handler = self.make_handler(max_stream_count=50)
    self.assertEqual(handler.max_stream_count, 50)

  def test_max_stream_count_zero(self):
    """Test that max_stream_count can be set to 0."""
    handler = self.make_handler(max_stream_count=0)
    self.assertEqual(handler.max_stream_count, 0)

  @mock.patch(
      'apache_beam.transforms.enrichment_handlers.'
      'bigquery_storage_read.BigQueryReadClient')
  def test_max_stream_count_passed_to_bq_api(self, mock_client_class):
    """Test that max_stream_count is passed to BigQuery API request."""
    handler = self.make_handler(max_stream_count=25)

    # Mock the BigQuery client instance and session
    mock_client_instance = mock.MagicMock()
    mock_client_class.return_value = mock_client_instance

    mock_session = mock.MagicMock()
    mock_session.streams = []
    mock_session.arrow_schema = None
    mock_client_instance.create_read_session.return_value = mock_session

    # Initialize the client through __enter__ and call _execute_storage_read
    handler.__enter__()
    handler._execute_storage_read("id = 1")

    # Verify that create_read_session was called with the correct
    # max_stream_count
    mock_client_instance.create_read_session.assert_called_once()
    call_args = mock_client_instance.create_read_session.call_args
    request = call_args.kwargs['request']
    self.assertEqual(request['max_stream_count'], 25)

  @mock.patch.object(
      bigquery_storage_read.BigQueryStorageEnrichmentHandler,
      '_execute_storage_read')
  def test_call_single_match_all_columns_aliased(self, mock_exec):
    """Test end-to-end enrichment flow when all columns are aliased."""
    # Create handler with all columns aliased
    handler = self.make_handler(
        column_names=['id as user_id', 'value as user_value'],
        fields=['id'])  # Note: fields still uses original column name

    row = BeamRow(id=6, value='f')
    # Mock returns data with original column names (as BigQuery would)
    mock_exec.return_value = [{'id': 6, 'value': 'fetched_value'}]

    req, resp = handler(row)

    # Verify request is unchanged
    self.assertEqual(req, row)

    # Verify response has aliased column names
    self.assertEqual(resp.user_id, 6)
    self.assertEqual(resp.user_value, 'fetched_value')

    # Verify original column names are not present in response
    with self.assertRaises(AttributeError):
      _ = resp.id  # Should not exist
    with self.assertRaises(AttributeError):
      _ = resp.value  # Should not exist

  @mock.patch.object(
      bigquery_storage_read.BigQueryStorageEnrichmentHandler,
      '_execute_storage_read')
  def test_call_batch_all_columns_aliased(self, mock_exec):
    """Test batch enrichment flow when all columns are aliased."""
    # Create handler with all columns aliased
    handler = self.make_handler(
        column_names=['id as customer_id', 'value as customer_name'],
        fields=['id'])

    rows = [BeamRow(id=100, value='john'), BeamRow(id=200, value='jane')]
    # Mock returns data with original column names
    mock_exec.return_value = [{
        'id': 100, 'value': 'John Doe'
    }, {
        'id': 200, 'value': 'Jane Smith'
    }]

    result = handler(rows)

    # Verify we get correct number of results
    self.assertEqual(len(result), 2)

    # Verify first result
    self.assertEqual(result[0][0], rows[0])  # Original request unchanged
    self.assertEqual(result[0][1].customer_id, 100)  # Aliased column name
    self.assertEqual(
        result[0][1].customer_name, 'John Doe')  # Aliased column name

    # Verify second result
    self.assertEqual(result[1][0], rows[1])  # Original request unchanged
    self.assertEqual(result[1][1].customer_id, 200)  # Aliased column name
    self.assertEqual(
        result[1][1].customer_name, 'Jane Smith')  # Aliased column name

    # Verify original column names are not present in responses
    for _, response in result:
      with self.assertRaises(AttributeError):
        _ = response.id  # Should not exist
      with self.assertRaises(AttributeError):
        _ = response.value  # Should not exist


if __name__ == '__main__':
  unittest.main()
