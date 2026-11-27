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

import unittest
from unittest.mock import patch

from parameterized import parameterized

try:
  from pg8000.exceptions import DatabaseError
  from sqlalchemy import exc as sqlalchemy_exc

  from apache_beam.io.requestresponse import ExponentialBackOffRepeater
  from apache_beam.io.requestresponse import UserCodeQuotaException
  from apache_beam.io.requestresponse import UserCodeTimeoutException
  from apache_beam.ml.rag.enrichment import postgres_search
  from apache_beam.ml.rag.enrichment.postgres_search import DatabaseTypeAdapter
  from apache_beam.ml.rag.enrichment.postgres_search import ExternalSQLDBConnectionConfig
  from apache_beam.ml.rag.enrichment.postgres_search import PostgresVectorDistance
  from apache_beam.ml.rag.enrichment.postgres_search import PostgresVectorSearchEnrichmentHandler
  from apache_beam.ml.rag.enrichment.postgres_search import PostgresVectorSearchParameters
  from apache_beam.ml.rag.enrichment.postgres_search import _build_query
  from apache_beam.ml.rag.enrichment.postgres_search import join_fn
  from apache_beam.ml.rag.types import Content
  from apache_beam.ml.rag.types import EmbeddableItem
  from apache_beam.ml.rag.types import Embedding
except ImportError as e:
  raise unittest.SkipTest(f'Postgres dependencies not installed: {str(e)}')

_CONNECTION = ExternalSQLDBConnectionConfig(
    db_adapter=DatabaseTypeAdapter.POSTGRESQL,
    host='localhost',
    port=5432,
    user='user',
    password='password',
    db_id='db')


def _item(id, dense=None, **metadata):
  return EmbeddableItem(
      id=id,
      content=Content(text=f'query {id}'),
      metadata=metadata,
      embedding=Embedding(dense_embedding=dense) if dense else None)


def _row(query_index, id, distance, **fields):
  return {
      '__beam_query_index': query_index,
      'id': id,
      '__beam_distance': distance,
      **fields
  }


def _db_error(sqlstate):
  orig = DatabaseError({'S': 'ERROR', 'C': sqlstate, 'M': 'error'})
  return sqlalchemy_exc.OperationalError('SELECT 1', {}, orig)


class _FakeResult:
  def __init__(self, rows):
    self._rows = rows

  def mappings(self):
    return self

  def all(self):
    return self._rows


class _FakeConnection:
  def __init__(self, engine):
    self._engine = engine

  def __enter__(self):
    return self

  def __exit__(self, *args):
    pass

  def execute(self, statement, params):
    self._engine.queries.append((statement.text, params))
    outcome = self._engine.outcomes.pop(0)
    if isinstance(outcome, Exception):
      raise outcome
    return _FakeResult(outcome)


class _FakeEngine:
  """Records queries and returns rows or raises errors, one per query."""
  def __init__(self, *outcomes):
    self.outcomes = list(outcomes)
    self.queries = []
    self.disposed = False

  def connect(self):
    return _FakeConnection(self)

  def dispose(self):
    self.disposed = True


def _handler(engine, **kwargs):
  params = kwargs.pop(
      'search_parameters', PostgresVectorSearchParameters(table_name='docs'))
  handler = PostgresVectorSearchEnrichmentHandler(_CONNECTION, params, **kwargs)
  handler._engine = engine
  return handler


class TestPublicNames(unittest.TestCase):
  def test_all_names_exist(self):
    for name in postgres_search.__all__:
      self.assertTrue(hasattr(postgres_search, name), name)

  def test_exports_what_a_connection_needs(self):
    # Users build the connection from these without importing the CloudSQL
    # enrichment handler module.
    for name in ('CloudSQLConnectionConfig',
                 'DatabaseTypeAdapter',
                 'ExternalSQLDBConnectionConfig'):
      self.assertIn(name, postgres_search.__all__)


class TestPostgresVectorSearchParameters(unittest.TestCase):
  def test_defaults_match_the_postgres_writer(self):
    params = PostgresVectorSearchParameters(table_name='embeddings')
    self.assertEqual(params.embedding_column, 'embedding')
    self.assertEqual(params.id_column, 'id')
    self.assertEqual(params.columns, ['content', 'metadata'])
    self.assertEqual(params.limit, 3)
    self.assertEqual(params.distance, PostgresVectorDistance.COSINE)

  @parameterized.expand([
      ('empty', ''),
      ('statement', 'docs; DROP TABLE docs'),
      ('quoted', '"docs"'),
      ('three_parts', 'db.public.docs'),
      ('starts_with_digit', '1docs'),
      ('space', 'my docs'),
  ])
  def test_invalid_table_name(self, _, table_name):
    with self.assertRaisesRegex(ValueError, 'table_name must be'):
      PostgresVectorSearchParameters(table_name=table_name)

  def test_schema_qualified_table_name(self):
    params = PostgresVectorSearchParameters(table_name='public.docs')
    self.assertEqual(params.table_name, 'public.docs')

  @parameterized.expand([
      ('embedding_column', {
          'embedding_column': 'embedding) OR (1=1'
      }),
      ('id_column', {
          'id_column': ''
      }),
      ('columns', {
          'columns': ['content', 'metadata->>x']
      }),
      ('columns', {
          'columns': ['public.content']
      }),
  ])
  def test_invalid_column_name(self, arg_name, kwargs):
    with self.assertRaisesRegex(ValueError, f'{arg_name} must contain'):
      PostgresVectorSearchParameters(table_name='docs', **kwargs)

  @parameterized.expand([
      (['content', 'content'], ),
      (['ID'], ),
      (['Content', 'content'], ),
  ])
  def test_duplicate_columns(self, columns):
    with self.assertRaisesRegex(ValueError, 'must not repeat a column'):
      PostgresVectorSearchParameters(table_name='docs', columns=columns)

  @parameterized.expand([(0, ), (-1, ), (True, ), (2.5, ), ('3', )])
  def test_invalid_limit(self, limit):
    with self.assertRaisesRegex(ValueError, 'limit must be a positive'):
      PostgresVectorSearchParameters(table_name='docs', limit=limit)

  def test_invalid_distance(self):
    with self.assertRaisesRegex(ValueError, 'distance must be a'):
      PostgresVectorSearchParameters(table_name='docs', distance='<=>')

  def test_filter_params_fn_requires_filter_template(self):
    with self.assertRaisesRegex(ValueError, 'requires filter_template'):
      PostgresVectorSearchParameters(
          table_name='docs', filter_params_fn=lambda item: {})

  def test_empty_filter_template(self):
    with self.assertRaisesRegex(ValueError, 'must not be empty'):
      PostgresVectorSearchParameters(table_name='docs', filter_template=' ')

  def test_filter_parameters_require_filter_params_fn(self):
    with self.assertRaisesRegex(ValueError,
                                r"parameters \['lang'\], so filter_params_fn"):
      PostgresVectorSearchParameters(
          table_name='docs', filter_template="metadata->>'lang' = :lang")

  def test_filter_without_parameters(self):
    params = PostgresVectorSearchParameters(
        table_name='docs', filter_template="metadata->>'lang' = 'en'")
    self.assertEqual(params.filter_param_names, set())

  def test_casts_are_not_filter_parameters(self):
    params = PostgresVectorSearchParameters(
        table_name='docs',
        filter_template="(metadata->>'n')::int > :n AND metadata ? 'tag'",
        filter_params_fn=lambda item: {'n': 1})
    self.assertEqual(params.filter_param_names, {'n'})


class TestBuildQuery(unittest.TestCase):
  def test_query_per_item(self):
    query, params = _build_query(
        PostgresVectorSearchParameters(table_name='docs', limit=2),
        [_item('a', [1.0, 0.0]), _item('b', [0.0, 1.0])])

    self.assertEqual(
        query,
        'SELECT * FROM ('
        '(SELECT 0 AS __beam_query_index, id, content, metadata, '
        'embedding <=> CAST(:query_vector_0 AS vector) AS __beam_distance '
        'FROM docs ORDER BY embedding <=> CAST(:query_vector_0 AS vector) '
        'LIMIT 2) UNION ALL '
        '(SELECT 1 AS __beam_query_index, id, content, metadata, '
        'embedding <=> CAST(:query_vector_1 AS vector) AS __beam_distance '
        'FROM docs ORDER BY embedding <=> CAST(:query_vector_1 AS vector) '
        'LIMIT 2)) AS search_results '
        'ORDER BY __beam_query_index, __beam_distance')
    self.assertEqual(
        params, {
            'query_vector_0': '[1.0,0.0]', 'query_vector_1': '[0.0,1.0]'
        })

  @parameterized.expand([
      (PostgresVectorDistance.COSINE, '<=>'),
      (PostgresVectorDistance.L2, '<->'),
      (PostgresVectorDistance.INNER_PRODUCT, '<#>'),
  ])
  def test_distance_operator(self, distance, operator):
    query, _ = _build_query(
        PostgresVectorSearchParameters(table_name='docs', distance=distance),
        [_item('a', [1.0, 0.0])])
    self.assertEqual(query.count(f'embedding {operator} CAST('), 2)

  def test_custom_columns(self):
    query, _ = _build_query(
        PostgresVectorSearchParameters(
            table_name='public.docs',
            embedding_column='vec',
            id_column='doc_id',
            columns=[]), [_item('a', [1.0])])
    self.assertIn(
        'SELECT 0 AS __beam_query_index, doc_id, '
        'vec <=> CAST(:query_vector_0 AS vector) AS __beam_distance '
        'FROM public.docs ORDER BY vec <=>',
        query)

  def test_filter_parameters_are_bound_per_item(self):
    params = PostgresVectorSearchParameters(
        table_name='docs',
        filter_template="metadata->>'lang' = :lang AND (n)::int > :n",
        filter_params_fn=lambda item: {
            'lang': item.metadata['lang'], 'n': 1, 'unused': 2
        })
    query, bind_params = _build_query(
        params, [_item('a', [1.0], lang='en'), _item('b', [1.0], lang='fr')])

    self.assertIn(
        "WHERE (metadata->>'lang' = :filter_0_lang AND (n)::int > "
        ":filter_0_n) ORDER BY",
        query)
    self.assertIn(
        "WHERE (metadata->>'lang' = :filter_1_lang AND (n)::int > "
        ":filter_1_n) ORDER BY",
        query)
    self.assertEqual(
        bind_params,
        {
            'query_vector_0': '[1.0]',
            'filter_0_lang': 'en',
            'filter_0_n': 1,
            'query_vector_1': '[1.0]',
            'filter_1_lang': 'fr',
            'filter_1_n': 1,
        })

  def test_filter_values_are_not_part_of_the_query(self):
    value = "en' OR '1'='1"
    params = PostgresVectorSearchParameters(
        table_name='docs',
        filter_template="metadata->>'lang' = :lang",
        filter_params_fn=lambda item: {'lang': item.metadata['lang']})
    query, bind_params = _build_query(params, [_item('a', [1.0], lang=value)])

    self.assertNotIn(value, query)
    self.assertEqual(bind_params['filter_0_lang'], value)

  def test_filter_without_parameters(self):
    query, bind_params = _build_query(
        PostgresVectorSearchParameters(
            table_name='docs', filter_template="metadata->>'lang' = 'en'"),
        [_item('a', [1.0])])
    self.assertIn("WHERE (metadata->>'lang' = 'en') ORDER BY", query)
    self.assertEqual(list(bind_params), ['query_vector_0'])

  def test_missing_filter_value(self):
    params = PostgresVectorSearchParameters(
        table_name='docs',
        filter_template='a = :a AND b = :b',
        filter_params_fn=lambda item: {'a': 1})
    with self.assertRaisesRegex(ValueError,
                                r"no value for parameters \['b'\].*item x"):
      _build_query(params, [_item('x', [1.0])])

  def test_filter_params_fn_must_return_dict(self):
    params = PostgresVectorSearchParameters(
        table_name='docs',
        filter_template='a = :a',
        filter_params_fn=lambda item: [1])
    with self.assertRaisesRegex(ValueError, 'must return a dict.*got list'):
      _build_query(params, [_item('x', [1.0])])

  def test_missing_embedding(self):
    with self.assertRaisesRegex(ValueError, 'Item x is missing the dense'):
      _build_query(
          PostgresVectorSearchParameters(table_name='docs'), [_item('x')])

  def test_vector_values_are_floats(self):
    _, params = _build_query(
        PostgresVectorSearchParameters(table_name='docs'),
        [_item('a', [1, 0.5, -2e-07])])
    self.assertEqual(params['query_vector_0'], '[1.0,0.5,-2e-07]')


class TestPostgresVectorSearchEnrichmentHandler(unittest.TestCase):
  def test_requires_postgres_adapter(self):
    connection = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.MYSQL, host='localhost', port=3306)
    with self.assertRaisesRegex(ValueError, 'requires DatabaseTypeAdapter'):
      PostgresVectorSearchEnrichmentHandler(
          connection, PostgresVectorSearchParameters(table_name='docs'))

  def test_batch_elements_kwargs(self):
    handler = PostgresVectorSearchEnrichmentHandler(
        _CONNECTION,
        PostgresVectorSearchParameters(table_name='docs'),
        min_batch_size=2,
        max_batch_size=8)
    self.assertEqual(
        handler.batch_elements_kwargs(), {
            'min_batch_size': 2, 'max_batch_size': 8
        })

  def test_default_batch_size(self):
    handler = PostgresVectorSearchEnrichmentHandler(
        _CONNECTION, PostgresVectorSearchParameters(table_name='docs'))
    self.assertEqual(
        handler.batch_elements_kwargs(), {
            'min_batch_size': 1, 'max_batch_size': 100
        })

  def test_enter_creates_engine_and_exit_disposes_it(self):
    handler = PostgresVectorSearchEnrichmentHandler(
        _CONNECTION, PostgresVectorSearchParameters(table_name='docs'))
    engine = _FakeEngine()
    with patch('apache_beam.ml.rag.enrichment.postgres_search.create_engine',
               return_value=engine) as create_engine:
      with handler as entered:
        self.assertIs(entered, handler)
        self.assertIs(handler._engine, engine)
    self.assertEqual(
        create_engine.call_args.kwargs['url'], 'postgresql+pg8000://')
    self.assertTrue(callable(create_engine.call_args.kwargs['creator']))
    self.assertTrue(engine.disposed)
    self.assertIsNone(handler._engine)

  def test_results_in_input_order(self):
    engine = _FakeEngine([
        _row(0, 'a', 0.0, content='east', metadata={'lang': 'en'}),
        _row(0, 'd', 0.1, content='east-ish', metadata={}),
        _row(2, 'b', 0.5, content='north', metadata={'lang': 'fr'}),
    ])
    items = [
        _item('q0', [1.0, 0.0]),
        _item('q1', [0.5, 0.5]),
        _item('q2', [0.0, 1.0])
    ]
    results = _handler(engine)(items)

    self.assertEqual(
        results,
        [
            (
                items[0],
                {
                    'id': ['a', 'd'],
                    'distance': [0.0, 0.1],
                    'fields': [{
                        'content': 'east', 'metadata': {
                            'lang': 'en'
                        }
                    }, {
                        'content': 'east-ish', 'metadata': {}
                    }],
                }),
            (items[1], {
                'id': [], 'distance': [], 'fields': []
            }),
            (
                items[2],
                {
                    'id': ['b'],
                    'distance': [0.5],
                    'fields': [{
                        'content': 'north', 'metadata': {
                            'lang': 'fr'
                        }
                    }],
                }),
        ])
    self.assertEqual(len(engine.queries), 1)

  def test_items_with_the_same_id_get_their_own_results(self):
    # Results are matched to items by position in the batch, not by id.
    engine = _FakeEngine([
        _row(0, 'a', 0.0, content='east', metadata={}),
        _row(1, 'b', 0.0, content='north', metadata={}),
    ])
    items = [_item('same', [1.0, 0.0]), _item('same', [0.0, 1.0])]
    results = _handler(engine)(items)
    self.assertEqual([r[1]['id'] for r in results], [['a'], ['b']])
    self.assertIs(results[0][0], items[0])
    self.assertIs(results[1][0], items[1])

  def test_single_item(self):
    engine = _FakeEngine([_row(0, 'a', 0.0, content='c', metadata={})])
    item = _item('q', [1.0, 0.0])
    result = _handler(engine)(item)
    self.assertEqual(result[0][0], item)
    self.assertEqual(result[0][1]['id'], ['a'])

  def test_column_names_are_case_insensitive(self):
    # Postgres returns unquoted names in lower case.
    engine = _FakeEngine([{
        '__beam_query_index': 0,
        'doc_id': 7,
        '__beam_distance': 0.0,
        'title': 'Beam'
    }])
    handler = _handler(
        engine,
        search_parameters=PostgresVectorSearchParameters(
            table_name='docs', id_column='Doc_Id', columns=['Title']))
    result = handler([_item('q', [1.0])])
    self.assertEqual(
        result[0][1], {
            'id': [7], 'distance': [0.0], 'fields': [{
                'Title': 'Beam'
            }]
        })

  def test_empty_request(self):
    engine = _FakeEngine()
    self.assertEqual(_handler(engine)([]), [])
    self.assertEqual(engine.queries, [])

  @parameterized.expand([
      ('statement_timeout', _db_error('57014'), UserCodeTimeoutException),
      ('too_many_connections', _db_error('53300'), UserCodeQuotaException),
      (
          'pool_timeout',
          sqlalchemy_exc.TimeoutError('QueuePool limit reached'),
          UserCodeTimeoutException),
  ])
  def test_retryable_errors(self, _, error, expected):
    handler = _handler(_FakeEngine(error))
    with self.assertRaises(expected) as context:
      handler([_item('q', [1.0])])
    self.assertIs(context.exception.__cause__, error)

  @parameterized.expand([
      ('undefined_table', _db_error('42P01')),
      ('no_sqlstate', sqlalchemy_exc.OperationalError('', {}, OSError('x'))),
  ])
  def test_other_errors_propagate(self, _, error):
    handler = _handler(_FakeEngine(error))
    with self.assertRaises(type(error)) as context:
      handler([_item('q', [1.0])])
    self.assertIs(context.exception, error)

  def test_default_repeater_recovers_from_retryable_errors(self):
    engine = _FakeEngine(
        _db_error('57014'),
        _db_error('53300'), [_row(0, 'a', 0.0, content='c', metadata={})])
    item = _item('q', [1.0])
    with patch('apache_beam.utils.retry.time.sleep'):
      result = ExponentialBackOffRepeater().repeat(_handler(engine), [item], 30)

    self.assertEqual(result[0][1]['id'], ['a'])
    self.assertEqual(len(engine.queries), 3)

  def test_default_repeater_does_not_retry_other_errors(self):
    error = _db_error('42P01')
    engine = _FakeEngine(error)
    with self.assertRaises(sqlalchemy_exc.OperationalError) as context:
      ExponentialBackOffRepeater().repeat(
          _handler(engine), [_item('q', [1.0])], 30)
    self.assertIs(context.exception, error)
    self.assertEqual(len(engine.queries), 1)

  def test_join_fn(self):
    item = _item('q', [1.0], source='docs')
    result = {'id': ['a'], 'distance': [0.0], 'fields': [{}]}
    joined = join_fn(item, result)
    self.assertEqual(
        joined.metadata, {
            'source': 'docs', 'enrichment_data': result
        })
    # The input item is not modified.
    self.assertEqual(item.metadata, {'source': 'docs'})


if __name__ == '__main__':
  unittest.main()
