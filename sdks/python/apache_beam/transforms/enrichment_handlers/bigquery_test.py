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
import pickle
import unittest
from unittest import mock

from parameterized import parameterized

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  from google.cloud import bigquery

  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery import BigQueryEnrichmentHandler
except ImportError:
  raise unittest.SkipTest(
      'Google Cloud BigQuery dependencies are not installed.')


def _condition_value_fn(row: beam.Row):
  return [row.id]


def _query_fn(row: beam.Row):
  return ('SELECT * FROM project.dataset.table WHERE id = ?', [row.id])


def _tuple_query_fn_for_payload(row: beam.Row):
  return ('SELECT * FROM project.dataset.table WHERE id = ?', [row.id])


class TestBigQueryEnrichment(unittest.TestCase):
  def setUp(self) -> None:
    self.project = 'apache-beam-testing'

  @parameterized.expand([
      ("", "", [], None, None, 1, 2),
      ("table", "", ["id"], _condition_value_fn, None, 2, 10),
      ("table", "id='{}'", ["id"], _condition_value_fn, None, 2, 10),
      ("table", "id='{}'", ["id"], None, _query_fn, 2, 10),
      ("table", "id = ? AND name = ?", ["id"], None, None, 1, 10),
  ])
  def test_invalid_constructor_params(
      self,
      table_name,
      row_restriction_template,
      fields,
      condition_value_fn,
      query_fn,
      min_batch_size,
      max_batch_size):
    """
    TC 1: Only batch sizes are provided. It should raise an error.
    TC 2: Either of `row_restriction_template` or `query_fn` is not provided.
    TC 3: Both `fields` and `condition_value_fn` are provided.
    TC 4: Query construction details are provided along with `query_fn`.
    TC 5: More positional placeholders than `fields` entries.
    """
    with self.assertRaises(ValueError):
      _ = BigQueryEnrichmentHandler(
          project=self.project,
          table_name=table_name,
          row_restriction_template=row_restriction_template,
          fields=fields,
          condition_value_fn=condition_value_fn,
          query_fn=query_fn,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
      )

  def test_batch_mode_fans_out_response_for_duplicate_keys(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id='{}'",
        fields=['id'],
        min_batch_size=2,
        max_batch_size=2,
    )
    requests = [beam.Row(id='1', name='first'), beam.Row(id='1', name='second')]

    with mock.patch.object(handler,
                           '_execute_query',
                           return_value=[{'id': '1', 'value': 'enriched'}]):
      responses = handler(requests)

    self.assertEqual(
        responses,
        [
            (requests[0], beam.Row(id='1', value='enriched')),
            (requests[1], beam.Row(id='1', value='enriched')),
        ],
    )

  def test_batch_mode_emits_empty_rows_for_all_unmatched_duplicate_keys(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id='{}'",
        fields=['id'],
        min_batch_size=2,
        max_batch_size=2,
        throw_exception_on_empty_results=False,
    )
    requests = [beam.Row(id='1', name='first'), beam.Row(id='1', name='second')]

    with mock.patch.object(handler, '_execute_query', return_value=None):
      responses = handler(requests)

    self.assertEqual(
        responses,
        [(requests[0], beam.Row()), (requests[1], beam.Row())],
    )

  def test_batch_elements_kwargs_include_max_batch_duration_secs(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id='{}'",
        fields=['id'],
        min_batch_size=2,
        max_batch_size=10,
        max_batch_duration_secs=0.75,
    )

    self.assertEqual(
        handler.batch_elements_kwargs(),
        {
            'min_batch_size': 2,
            'max_batch_size': 10,
            'max_batch_duration_secs': 0.75,
        })

  def test_batch_elements_kwargs_omit_max_batch_duration_secs_by_default(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id='{}'",
        fields=['id'],
        min_batch_size=2,
        max_batch_size=10,
    )

    self.assertEqual(
        handler.batch_elements_kwargs(), {
            'min_batch_size': 2,
            'max_batch_size': 10,
        })

  def test_unbatched_query_binds_scalar_query_parameters(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id='{}'",
        fields=['id'],
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = [{
        'id': "O'Brien", 'value': 'enriched'
    }]

    request = beam.Row(id="O'Brien", name='first')
    response = handler(request)

    self.assertEqual(
        response, (request, beam.Row(id="O'Brien", value='enriched')))
    handler.client.query.assert_called_once()
    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        call_kwargs['query'], 'SELECT * FROM project.dataset.table WHERE id=?')
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [bigquery.ScalarQueryParameter(None, 'STRING', "O'Brien")])

  def test_batched_query_binds_parameters_and_prevents_sql_injection(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id='{}' AND distribution_center_id={}",
        fields=['id', 'distribution_center_id'],
        min_batch_size=2,
        max_batch_size=10,
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    malicious_id = "1') LIMIT 1; DROP TABLE project.dataset.audit_log; --"
    brace_id = "{malicious_format_key}"
    requests = [
        beam.Row(id=malicious_id, distribution_center_id=3),
        beam.Row(id=brace_id, distribution_center_id=7),
    ]
    responses = handler(requests)

    self.assertEqual(
        responses, [(requests[0], beam.Row()), (requests[1], beam.Row())])
    handler.client.query.assert_called_once()
    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        call_kwargs['query'],
        'SELECT * FROM project.dataset.table WHERE '
        '(id=? AND distribution_center_id=?) or '
        '(id=? AND distribution_center_id=?)')
    self.assertNotIn('DROP TABLE', call_kwargs['query'])
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [
            bigquery.ScalarQueryParameter(None, 'STRING', malicious_id),
            bigquery.ScalarQueryParameter(None, 'INT64', 3),
            bigquery.ScalarQueryParameter(None, 'STRING', brace_id),
            bigquery.ScalarQueryParameter(None, 'INT64', 7),
        ])

  def test_native_question_mark_placeholders(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template='id = ? AND active = ?',
        fields=['id', 'active'],
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    request = beam.Row(id=42, active=True)
    _ = handler(request)

    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        call_kwargs['query'],
        'SELECT * FROM project.dataset.table WHERE id = ? AND active = ?')
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [
            bigquery.ScalarQueryParameter(None, 'INT64', 42),
            bigquery.ScalarQueryParameter(None, 'BOOL', True),
        ])

  def test_query_fn_with_parameterized_tuple(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        query_fn=_tuple_query_fn_for_payload,
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    payload = "1' OR '1'='1"
    _ = handler(beam.Row(id=payload))

    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        call_kwargs['query'],
        'SELECT * FROM project.dataset.table WHERE id = ?')
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [bigquery.ScalarQueryParameter(None, 'STRING', payload)])

  @parameterized.expand([
      ("project.dataset.table; DROP TABLE x; --", ['id']),
      ("project.dataset.table WHERE 1=1", ['id']),
      ("`project.dataset.table", ['id']),
      ("project.dataset.table`", ['id']),
      ("``project.dataset.table``", ['id']),
      ("`project`.`dataset.table", ['id']),
      ("project..table", ['id']),
      ("project.dataset.table--comment", ['id']),
      ("project.dataset.table", ["id; DROP TABLE x"]),
      ("project.dataset.table", ["id, (SELECT 1)"]),
      ("project.dataset.table", ["a`"]),
      ("project.dataset.table", ["`a"]),
      ("project.dataset.table", ["*", "a`"]),
  ])
  def test_invalid_identifiers_raise_value_error(
      self, table_name, column_names):
    with self.assertRaises(ValueError):
      _ = BigQueryEnrichmentHandler(
          project=self.project,
          table_name=table_name,
          row_restriction_template='id = ?',
          fields=['id'],
          column_names=column_names,
      )

  def test_balanced_backtick_identifiers_accepted(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='`my-project`.`my_dataset`.`my_table`',
        row_restriction_template='`id` = ?',
        fields=['id'],
        column_names=['`id`', '`order`', 'details.`nested_field`'],
    )
    self.assertEqual(
        handler.query_template,
        'SELECT `id`,`order`,details.`nested_field` '
        'FROM `my-project`.`my_dataset`.`my_table` WHERE `id` = ?')

  def test_question_mark_in_sql_literals_and_comments_ignored(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template=(
            "/* exists? */ url = 'https://example.com?x=1&y=2' "
            "AND `is_active?` = ?"),
        fields=['active'],
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    _ = handler(beam.Row(active=True))

    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        call_kwargs['query'],
        "SELECT * FROM project.dataset.table WHERE "
        "/* exists? */ url = 'https://example.com?x=1&y=2' "
        "AND `is_active?` = ?")
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [bigquery.ScalarQueryParameter(None, 'BOOL', True)])

  def test_partial_string_placeholder_uses_concat(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="name LIKE '%{}%'",
        fields=['name'],
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    payload = "abc' OR '1'='1"
    _ = handler(beam.Row(name=payload))

    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        call_kwargs['query'],
        "SELECT * FROM project.dataset.table WHERE "
        "name LIKE CONCAT('%', CAST(? AS STRING), '%')")
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [bigquery.ScalarQueryParameter(None, 'STRING', payload)])

  def test_escaped_double_braces_and_raw_regex_quantifiers(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template=(
            r"REGEXP_CONTAINS(zip, r'^[0-9]{{5}}$') "
            r"AND tag = '{{literal}}' AND id = {}"),
        fields=['id'],
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    _ = handler(beam.Row(id=99))

    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        call_kwargs['query'],
        r"SELECT * FROM project.dataset.table WHERE "
        r"REGEXP_CONTAINS(zip, r'^[0-9]{5}$') "
        r"AND tag = '{literal}' AND id = ?")
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [bigquery.ScalarQueryParameter(None, 'INT64', 99)])

  @parameterized.expand([
      ("id = '{id}' AND center_id = {}", ),
      ("id = {0} AND center_id = ?", ),
      ("id = @id AND center_id = {}", ),
  ])
  def test_mixed_auto_and_manual_placeholders_raise_value_error(self, template):
    with self.assertRaises(ValueError):
      _ = BigQueryEnrichmentHandler(
          project=self.project,
          table_name='project.dataset.table',
          row_restriction_template=template,
          fields=['id', 'center_id'],
      )

  def test_named_and_at_placeholders_preserve_system_variables(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template=(
            "proj = @@project_id AND id = @id AND region = '{region}'"),
        fields=['id', 'region'],
        min_batch_size=2,
        max_batch_size=10,
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    requests = [
        beam.Row(id=1, region='us'),
        beam.Row(id=2, region='eu'),
    ]
    _ = handler(requests)

    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        call_kwargs['query'],
        'SELECT * FROM project.dataset.table WHERE '
        '(proj = @@project_id AND id = ? AND region = ?) or '
        '(proj = @@project_id AND id = ? AND region = ?)')
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [
            bigquery.ScalarQueryParameter(None, 'INT64', 1),
            bigquery.ScalarQueryParameter(None, 'STRING', 'us'),
            bigquery.ScalarQueryParameter(None, 'INT64', 2),
            bigquery.ScalarQueryParameter(None, 'STRING', 'eu'),
        ])

  def test_unknown_named_placeholder_raises_key_error(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id = '{unknown_field}'",
        fields=['id'],
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    with self.assertRaises(KeyError):
      _ = handler(beam.Row(id=1))

  def test_missing_field_in_unbatched_mode_raises_key_error(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template='id = ? AND center = ?',
        fields=['id', 'center'],
        min_batch_size=1,
        max_batch_size=1,
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    with self.assertRaises(KeyError):
      _ = handler(beam.Row(id=1))

  def test_static_zero_placeholder_restriction_omits_job_config(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="label='item1' or label='item2'",
        fields=['label'],
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    _ = handler(beam.Row(label='item1'))

    handler.client.query.assert_called_once_with(
        query="SELECT * FROM project.dataset.table "
        "WHERE label='item1' or label='item2'")

  def test_batch_mode_infers_typed_null_parameter_from_peer_rows(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template='id = ?',
        fields=['id'],
        min_batch_size=2,
        max_batch_size=10,
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = [{
        'id': 10, 'val': 'ok'
    }]

    requests = [beam.Row(id=None), beam.Row(id=10)]
    responses = handler(requests)

    self.assertEqual(
        responses, [
            (requests[1], beam.Row(id=10, val='ok')),
            (requests[0], beam.Row()),
        ])
    call_kwargs = handler.client.query.call_args.kwargs
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [
            bigquery.ScalarQueryParameter(None, 'INT64', None),
            bigquery.ScalarQueryParameter(None, 'INT64', 10),
        ])

  def test_empty_batch_returns_empty_list_without_querying(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template='id = ?',
        fields=['id'],
    )
    handler.client = mock.MagicMock()
    self.assertEqual(handler([]), [])
    handler.client.query.assert_not_called()

  def test_batch_exceeding_max_query_parameters_is_chunked(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template='id = ? AND center_id = ?',
        fields=['id', 'center_id'],
        min_batch_size=1,
        max_batch_size=10000,
        throw_exception_on_empty_results=False,
    )
    handler.client = mock.MagicMock()
    handler.client.query.side_effect = [
        mock.MagicMock(
            result=mock.MagicMock(
                return_value=[{
                    'id': 0, 'center_id': 1, 'v': 'a'
                }])),
        mock.MagicMock(
            result=mock.MagicMock(
                return_value=[{
                    'id': 5000, 'center_id': 1, 'v': 'b'
                }])),
    ]

    requests = [beam.Row(id=i, center_id=1) for i in range(5001)]
    responses = handler(requests)

    self.assertEqual(handler.client.query.call_count, 2)
    first_params = (
        handler.client.query.call_args_list[0].kwargs['job_config'].
        query_parameters)
    second_params = (
        handler.client.query.call_args_list[1].kwargs['job_config'].
        query_parameters)
    self.assertEqual(len(first_params), 10000)
    self.assertEqual(len(second_params), 2)
    self.assertEqual(len(responses), 5001)

  def test_handler_is_picklable(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id = '{}'",
        fields=['id'],
    )
    unpickled = pickle.loads(pickle.dumps(handler))
    self.assertEqual(unpickled.query_template, handler.query_template)

  @mock.patch('apache_beam.transforms.enrichment_handlers.bigquery.bigquery')
  def test_pipeline_end_to_end_enrichment_with_mocked_bigquery(self, mock_bq):
    mock_bq.ScalarQueryParameter = bigquery.ScalarQueryParameter
    mock_bq.ArrayQueryParameter = bigquery.ArrayQueryParameter
    mock_bq.QueryJobConfig = bigquery.QueryJobConfig

    mock_client = mock.MagicMock()
    mock_client.query.return_value.result.return_value = [
        {
            'id': "1' OR '1'='1", 'quantity': 5
        },
        {
            'id': '2', 'quantity': 9
        },
    ]
    mock_bq.Client.return_value = mock_client

    inputs = [
        beam.Row(id="1' OR '1'='1", name='A'),
        beam.Row(id='2', name='B'),
    ]
    expected = [
        beam.Row(id="1' OR '1'='1", name='A', quantity=5),
        beam.Row(id='2', name='B', quantity=9),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id = '{}'",
        fields=['id'],
        min_batch_size=2,
        max_batch_size=2,
    )

    with TestPipeline() as p:
      res = p | beam.Create(inputs) | Enrichment(handler)
      assert_that(res, equal_to(expected))

    mock_client.query.assert_called_once()
    call_kwargs = mock_client.query.call_args.kwargs
    self.assertNotIn("1' OR '1'='1", call_kwargs['query'])
    self.assertEqual(
        list(call_kwargs['job_config'].query_parameters),
        [
            bigquery.ScalarQueryParameter(None, 'STRING', "1' OR '1'='1"),
            bigquery.ScalarQueryParameter(None, 'STRING', '2'),
        ])
    mock_client.close.assert_called_once()


if __name__ == '__main__':
  unittest.main()
