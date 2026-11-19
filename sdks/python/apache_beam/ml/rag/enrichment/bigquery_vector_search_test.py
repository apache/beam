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

  from apache_beam.ml.rag.enrichment.bigquery_vector_search import BigQueryVectorSearchEnrichmentHandler
  from apache_beam.ml.rag.enrichment.bigquery_vector_search import BigQueryVectorSearchParameters
  from apache_beam.ml.rag.types import Chunk
  from apache_beam.ml.rag.types import Content
  from apache_beam.ml.rag.types import Embedding
  from apache_beam.transforms.enrichment import Enrichment
except ImportError:
  raise unittest.SkipTest('BigQuery dependencies not installed')


class TestBigQueryVectorSearchEnrichment(unittest.TestCase):
  def setUp(self) -> None:
    self.project = 'apache-beam-testing'
    self.table_name = 'apache-beam-testing.dataset.embeddings'

  def test_vector_search_binds_query_parameters_for_id_embedding_and_metadata(
      self):
    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content', 'metadata'],
        neighbor_count=2,
        metadata_restriction_template=(
            "check_metadata(metadata, 'language', '{language}')"),
    )
    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)
    handler.client = mock.MagicMock()
    handler.client.query.return_value.result.return_value = []

    malicious_id = "q1' as id, [0.1] as embedding)); DROP TABLE x; --"
    malicious_lang = "en'); DROP TABLE x; --"
    chunk = Chunk(
        id=malicious_id,
        index=0,
        embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
        content=Content(text='query'),
        metadata={'language': malicious_lang},
    )

    response = handler([chunk])

    self.assertEqual(response, [(chunk, {})])
    handler.client.query.assert_called_once()
    query_str = handler.client.query.call_args.args[0]
    job_config = handler.client.query.call_args.kwargs['job_config']
    self.assertNotIn('DROP TABLE', query_str)
    self.assertIn(
        "check_metadata(metadata, 'language', @_bq_meta_0)", query_str)
    self.assertIn(
        'SELECT @_bq_item_id_0 as id, @_bq_item_emb_0 as embedding', query_str)
    self.assertEqual(
        job_config.to_api_repr()['query']['parameterMode'], 'NAMED')
    self.assertEqual(
        list(job_config.query_parameters),
        [
            bigquery.ScalarQueryParameter(
                '_bq_meta_0', 'STRING', malicious_lang),
            bigquery.ScalarQueryParameter(
                '_bq_item_id_0', 'STRING', malicious_id),
            bigquery.ArrayQueryParameter(
                '_bq_item_emb_0', 'FLOAT64', [0.1, 0.2, 0.3]),
        ],
    )

  @parameterized.expand([
      ([float('nan'), 0.2], ),
      ([0.1, float('inf')], ),
      ([float('-inf'), 0.2], ),
  ])
  def test_non_finite_embedding_raises_value_error(self, bad_vec):
    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
    )
    chunk = Chunk(
        id='q1',
        index=0,
        embedding=Embedding(dense_embedding=bad_vec),
        content=Content(text='query'),
    )
    with self.assertRaisesRegex(ValueError, 'non-finite'):
      _ = params.build_query([chunk])

  def test_callable_tuple_positional_params_across_groups_invoked_once(self):
    call_count = 0

    def price_filter(item):
      nonlocal call_count
      call_count += 1
      return (
          'price >= ? AND price <= ?',
          [item.metadata['min_p'], item.metadata['max_p']],
      )

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content', 'price'],
        neighbor_count=3,
        metadata_restriction_template=price_filter,
    )
    chunks = [
        Chunk(
            id='c1',
            embedding=Embedding(dense_embedding=[0.1, 0.2]),
            content=Content(text='q1'),
            metadata={
                'min_p': 10.0, 'max_p': 50.0
            },
        ),
        Chunk(
            id='c2',
            embedding=Embedding(dense_embedding=[0.3, 0.4]),
            content=Content(text='q2'),
            metadata={
                'min_p': 10.0, 'max_p': 50.0
            },
        ),
        Chunk(
            id='c3',
            embedding=Embedding(dense_embedding=[0.5, 0.6]),
            content=Content(text='q3'),
            metadata={
                'min_p': 100.0, 'max_p': 200.0
            },
        ),
    ]

    query_str, query_params = params.build_query(chunks)
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    self.assertEqual(call_count, 3)
    self.assertEqual(
        job_config.to_api_repr()['query']['parameterMode'], 'NAMED')
    self.assertNotIn('?', query_str)
    self.assertIn(
        'WHERE price >= @_bq_meta_0 AND price <= @_bq_meta_1', query_str)
    self.assertIn(
        'WHERE price >= @_bq_meta_2 AND price <= @_bq_meta_3', query_str)
    param_names = [p.name for p in query_params]
    self.assertEqual(len(param_names), len(set(param_names)))

  def test_callable_tuple_named_bigquery_params_rewritten_without_mutation(
      self):
    shared_min = bigquery.ScalarQueryParameter('min_p', 'FLOAT64', 5.0)
    other_min = bigquery.ScalarQueryParameter('min_p', 'FLOAT64', 25.0)

    def named_filter(item):
      chosen = shared_min if item.id == 'c1' else other_min
      return ('(@min_p IS NULL OR price >= @min_p)', [chosen])

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
        metadata_restriction_template=named_filter,
    )
    chunks = [
        Chunk(
            id='c1',
            embedding=Embedding(dense_embedding=[0.1, 0.2]),
            content=Content(text='q1'),
        ),
        Chunk(
            id='c2',
            embedding=Embedding(dense_embedding=[0.3, 0.4]),
            content=Content(text='q2'),
        ),
    ]

    query_str, query_params = params.build_query(chunks)
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    self.assertEqual(shared_min.name, 'min_p')
    self.assertEqual(other_min.name, 'min_p')
    self.assertEqual(
        job_config.to_api_repr()['query']['parameterMode'], 'NAMED')
    self.assertIn(
        'WHERE (@_bq_meta_0 IS NULL OR price >= @_bq_meta_0)', query_str)
    self.assertIn(
        'WHERE (@_bq_meta_1 IS NULL OR price >= @_bq_meta_1)', query_str)

  @parameterized.expand([
      ('project.dataset.table`; DROP TABLE x; --', 'embedding', ['content'], 2),
      ('`project.dataset.table', 'embedding', ['content'], 2),
      ('project.dataset.table`', 'embedding', ['content'], 2),
      ('``project.dataset.table``', 'embedding', ['content'], 2),
      ('project.dataset.table', 'embedding; DROP TABLE x', ['content'], 2),
      ('project.dataset.table', '`embedding`', ['content'], 2),
      ('project.dataset.table', 'embedding', ['content`'], 2),
      ('project.dataset.table', 'embedding', [], 2),
      ('project.dataset.table', 'embedding', ['content'], 0),
      ('project.dataset.table', 'embedding', ['content'], -1),
      ('project.dataset.table', 'embedding', ['content'], True),
  ])
  def test_invalid_parameters_raise_value_error(
      self, table_name, embedding_column, columns, neighbor_count):
    with self.assertRaises(ValueError):
      _ = BigQueryVectorSearchParameters(
          project=self.project,
          table_name=table_name,
          embedding_column=embedding_column,
          columns=columns,
          neighbor_count=neighbor_count,
      )

  def test_backticked_table_name_not_double_wrapped(self):
    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name='`apache-beam-testing.dataset.embeddings`',
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
    )
    chunk = Chunk(
        id='q1',
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        content=Content(text='query'),
    )
    query_str, _ = params.build_query([chunk])
    self.assertIn('FROM `apache-beam-testing.dataset.embeddings`', query_str)
    self.assertNotIn('``', query_str)

  def test_options_serialized_as_safe_json_sql_string_literal(self):
    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
        options={
            'use_brute_force': True,
            'note': "\\'; DROP TABLE x; --",
        },
    )
    chunk = Chunk(
        id='q1',
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        content=Content(text='query'),
    )
    query_str, _ = params.build_query([chunk])
    self.assertIn(
        r""", options => '{"note": "\\\\\'"""
        r"""; DROP TABLE x; --", "use_brute_force": true}'""",
        query_str,
    )

    with self.assertRaises(ValueError):
      _ = BigQueryVectorSearchParameters(
          project=self.project,
          table_name=self.table_name,
          embedding_column='embedding',
          columns=['content'],
          neighbor_count=2,
          options="raw_injection') --",  # type: ignore[arg-type]
      )
    with self.assertRaises(ValueError):
      _ = BigQueryVectorSearchParameters(
          project=self.project,
          table_name=self.table_name,
          embedding_column='embedding',
          columns=['content'],
          neighbor_count=2,
          options={'bad_float': float('nan')},
      )

  def test_empty_batch_returns_empty_list_without_querying(self):
    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
    )
    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)
    handler.client = mock.MagicMock()

    self.assertEqual(handler([]), [])
    handler.client.query.assert_not_called()

  def test_handler_and_parameters_picklable(self):
    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
        metadata_restriction_template="language = '{language}'",
        options={'use_brute_force': True},
    )
    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)
    restored = pickle.loads(pickle.dumps(handler))
    chunk = Chunk(
        id='q1',
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        content=Content(text='query'),
        metadata={'language': 'en'},
    )
    q1, p1 = handler.vector_search_parameters.build_query([chunk])
    q2, p2 = restored.vector_search_parameters.build_query([chunk])
    self.assertEqual(q1, q2)
    self.assertEqual(p1, p2)

  @mock.patch(
      'apache_beam.ml.rag.enrichment.bigquery_vector_search.bigquery.Client')
  def test_context_manager_creates_and_closes_client(self, mock_client_cls):
    mock_client = mock.MagicMock()
    mock_client_cls.return_value = mock_client
    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
    )
    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params,
        min_batch_size=5,
        max_batch_size=50,
        location='US',
    )
    self.assertEqual(
        handler.batch_elements_kwargs(), {
            'min_batch_size': 5, 'max_batch_size': 50
        })
    with handler:
      mock_client_cls.assert_called_once_with(
          project=self.project, location='US')
      mock_client.close.assert_not_called()
    mock_client.close.assert_called_once()

  @mock.patch(
      'apache_beam.ml.rag.enrichment.bigquery_vector_search.bigquery.Client')
  def test_pipeline_end_to_end_vector_search_enrichment(self, mock_client_cls):
    class _FakeRow:
      def __init__(self, row_id, data):
        self.id = row_id
        self._data = data

      def items(self):
        return self._data.items()

    malicious_id = "q1'); DROP TABLE x; --"
    mock_client = mock.MagicMock()
    mock_client.query.return_value.result.return_value = [
        _FakeRow(
            malicious_id, {
                'id': malicious_id, 'chunks': [{
                    'content': 'matched doc'
                }]
            }),
    ]
    mock_client_cls.return_value = mock_client

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=1,
        metadata_restriction_template="language = '{language}'",
    )
    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)

    input_chunk = Chunk(
        id=malicious_id,
        index=0,
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        content=Content(text='query'),
        metadata={'language': "en' OR '1'='1"},
    )
    expected_chunk = Chunk(
        id=malicious_id,
        index=0,
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        content=Content(text='query'),
        metadata={
            'language': "en' OR '1'='1",
            'enrichment_data': {
                'id': malicious_id, 'chunks': [{
                    'content': 'matched doc'
                }]
            },
        },
    )

    with TestPipeline() as p:
      res = p | beam.Create([input_chunk],
                            reshuffle=False) | Enrichment(handler)
      assert_that(res, equal_to([expected_chunk]))

    mock_client.query.assert_called_once()
    query_str = mock_client.query.call_args.args[0]
    self.assertNotIn('DROP TABLE', query_str)
    self.assertNotIn("OR '1'='1", query_str)

  def test_callable_validation_format_query_and_subchunking(self):
    with self.assertRaises(ValueError):
      _ = BigQueryVectorSearchParameters(
          project=self.project,
          table_name=self.table_name,
          embedding_column='embedding',
          columns=['content'],
          neighbor_count=2,
          metadata_restriction_template='language = ?',
      )
    with self.assertRaises(ValueError):
      _ = BigQueryVectorSearchParameters(
          project=self.project,
          table_name=self.table_name,
          embedding_column='embedding',
          columns=['content'],
          neighbor_count=2,
          distance_type='COSINE; DROP TABLE x',
      )

    base_chunk = Chunk(
        id='q1',
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        content=Content(text='query'),
        metadata={'p': 10},
    )

    raw_str_params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
        distance_type='COSINE',
        include_distance=True,
        metadata_restriction_template=lambda item: 'price > 0',
    )
    formatted_sql = raw_str_params.format_query([base_chunk])
    self.assertIn("distance_type => 'COSINE'", formatted_sql)
    self.assertIn('WHERE price > 0', formatted_sql)
    with self.assertRaises(ValueError):
      _ = raw_str_params.build_query([])
    with self.assertRaisesRegex(ValueError, 'missing embedding'):
      _ = raw_str_params.build_query(
          [Chunk(id='empty', embedding=None, content=Content(text='q'))])

    def _numbered_fn(item):
      return ('price >= {0} AND price <= {0}', [item.metadata['p']])

    numbered_params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
        metadata_restriction_template=_numbered_fn,
    )
    num_sql, num_qparams = numbered_params.build_query([base_chunk])
    self.assertIn(
        'WHERE price >= @_bq_meta_0 AND price <= @_bq_meta_0', num_sql)
    self.assertEqual(len(num_qparams), 3)

    empty_tuple_params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=2,
        metadata_restriction_template=lambda item: ('price > 0', []),
    )
    empty_sql, _ = empty_tuple_params.build_query([base_chunk])
    self.assertIn('WHERE price > 0', empty_sql)

    named_a = bigquery.ScalarQueryParameter('a', 'INT64', 1)
    bad_results = [
        ('price > ?', []),
        ('a = @a AND b = ?', [named_a, 2]),
        ('a = ?', [named_a]),
        ('a = @missing', [named_a]),
        ('a = {0} AND b = ?', [1, 2]),
        ('a = ? AND b = {0}', [1, 2]),
        ('a = {named}', [1]),
        ('a = {5}', [1]),
        ('a = ?', [1, 2]),
    ]
    for bad_res in bad_results:
      bad_params = BigQueryVectorSearchParameters(
          project=self.project,
          table_name=self.table_name,
          embedding_column='embedding',
          columns=['content'],
          neighbor_count=2,
          metadata_restriction_template=lambda item, r=bad_res: r,
      )
      with self.assertRaises(ValueError):
        _ = bad_params.build_query([base_chunk])

    # Verify _execute_chunk splitting when query_parameters exceed limit
    # and log_query=True path.
    class _RowObj:
      def __init__(self, row_id):
        self.id = row_id

      def items(self):
        return [('id', self.id)]

    split_params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=1,
    )
    split_handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=split_params, log_query=True)
    split_handler.client = mock.MagicMock()
    split_handler.client.query.side_effect = [
        mock.MagicMock(result=mock.MagicMock(return_value=[_RowObj('c1')])),
        mock.MagicMock(result=mock.MagicMock(return_value=[_RowObj('c2')])),
    ]
    three_chunks = [
        Chunk(
            id=f'c{i}',
            embedding=Embedding(dense_embedding=[0.1, 0.2]),
            content=Content(text='q'),
        ) for i in (1, 2, 3)
    ]
    with mock.patch('apache_beam.ml.rag.enrichment.bigquery_vector_search.'
                    'MAX_BQ_QUERY_PARAMETERS',
                    4):
      res = split_handler(three_chunks)
    self.assertEqual(split_handler.client.query.call_count, 2)
    self.assertEqual(
        res,
        [
            (three_chunks[0], {
                'id': 'c1'
            }),
            (three_chunks[1], {
                'id': 'c2'
            }),
            (three_chunks[2], {}),
        ])


if __name__ == '__main__':
  unittest.main()
