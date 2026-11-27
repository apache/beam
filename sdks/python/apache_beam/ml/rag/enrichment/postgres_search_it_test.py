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

import pytest

import apache_beam as beam
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  from sqlalchemy import create_engine
  from sqlalchemy import text

  from apache_beam.ml.rag.enrichment.postgres_search import DatabaseTypeAdapter
  from apache_beam.ml.rag.enrichment.postgres_search import ExternalSQLDBConnectionConfig
  from apache_beam.ml.rag.enrichment.postgres_search import PostgresVectorDistance
  from apache_beam.ml.rag.enrichment.postgres_search import PostgresVectorSearchEnrichmentHandler
  from apache_beam.ml.rag.enrichment.postgres_search import PostgresVectorSearchParameters
  from apache_beam.ml.rag.enrichment.postgres_search import _build_query
  from apache_beam.transforms.enrichment import Enrichment

  POSTGRES_AVAILABLE = True
except ImportError:
  POSTGRES_AVAILABLE = False

try:
  from testcontainers.postgres import PostgresContainer
except ImportError:
  PostgresContainer = None
# pylint: enable=ungrouped-imports

_TABLE = 'docs'

# Rows in the test table. A cosine search for [1.0, 0.0] ranks a, d, b, c.
_ROWS = [
    ('a', '[1,0]', 'east', '{"lang": "en"}'),
    ('b', '[0,1]', 'north', '{"lang": "fr"}'),
    ('c', '[-1,0]', 'west', '{"lang": "en"}'),
    ('d', '[0.9,0.1]', 'east-ish', '{"lang": "fr"}'),
]


def _query(id, dense, **metadata):
  return EmbeddableItem(
      id=id,
      content=Content(text=f'query {id}'),
      metadata=metadata,
      embedding=Embedding(dense_embedding=dense))


def _ids_and_distances(item):
  data = item.metadata['enrichment_data']
  return item.id, data['id'], [round(d, 4) for d in data['distance']]


@unittest.skipIf(not POSTGRES_AVAILABLE, 'postgres dependencies not installed.')
@unittest.skipIf(
    not PostgresContainer, 'postgres test_container not installed.')
@pytest.mark.require_docker_in_docker
class TestPostgresVectorSearchEnrichment(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls._container = PostgresContainer(
        image='pgvector/pgvector:pg16',
        username='test',
        password='test',
        dbname='test',
        driver='pg8000')
    cls._container.start()
    cls._engine = create_engine(cls._container.get_connection_url())
    with cls._engine.begin() as connection:
      connection.execute(text('CREATE EXTENSION IF NOT EXISTS vector'))
      connection.execute(
          text(
              f'CREATE TABLE {_TABLE} (id TEXT PRIMARY KEY, '
              'embedding VECTOR(2), content TEXT, metadata JSONB)'))
      for id, embedding, content, metadata in _ROWS:
        connection.execute(
            text(
                f'INSERT INTO {_TABLE} VALUES (:id, '
                'CAST(:embedding AS vector), :content, '
                'CAST(:metadata AS jsonb))'),
            {
                'id': id,
                'embedding': embedding,
                'content': content,
                'metadata': metadata
            })
    cls._connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host=cls._container.get_container_host_ip(),
        port=int(cls._container.get_exposed_port(5432)),
        user='test',
        password='test',
        db_id='test')

  @classmethod
  def tearDownClass(cls):
    cls._engine.dispose()
    cls._container.stop()

  def _handler(self, **kwargs):
    batching = {
        key: kwargs.pop(key)
        for key in ('min_batch_size', 'max_batch_size') if key in kwargs
    }
    return PostgresVectorSearchEnrichmentHandler(
        self._connection_config,
        PostgresVectorSearchParameters(table_name=_TABLE, **kwargs),
        **batching)

  def test_cosine_search(self):
    queries = [_query('east', [1.0, 0.0]), _query('north', [0.0, 1.0])]
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(queries)
          | Enrichment(self._handler(limit=2, min_batch_size=2))
          | beam.Map(_ids_and_distances))
      assert_that(
          result,
          equal_to([
              ('east', ['a', 'd'], [0.0, 0.0061]),
              ('north', ['b', 'd'], [0.0, 0.8896]),
          ]))

  def test_returns_columns(self):
    with TestPipeline() as p:
      result = (
          p
          | beam.Create([_query('east', [1.0, 0.0])])
          | Enrichment(self._handler(limit=1))
          | beam.Map(lambda item: item.metadata['enrichment_data']['fields']))
      assert_that(
          result, equal_to([[{
              'content': 'east', 'metadata': {
                  'lang': 'en'
              }
          }]]))

  def test_l2_and_inner_product(self):
    with TestPipeline() as p:
      queries = p | beam.Create([_query('east', [1.0, 0.0])])
      l2 = (
          queries
          | 'L2' >> Enrichment(
              self._handler(limit=2, distance=PostgresVectorDistance.L2))
          | 'L2 result' >> beam.Map(_ids_and_distances))
      inner_product = (
          queries
          | 'InnerProduct' >> Enrichment(
              self._handler(
                  limit=2, distance=PostgresVectorDistance.INNER_PRODUCT))
          | 'InnerProduct result' >> beam.Map(_ids_and_distances))
      assert_that(
          l2, equal_to([('east', ['a', 'd'], [0.0, 0.1414])]), label='l2')
      assert_that(
          inner_product,
          equal_to([('east', ['a', 'd'], [-1.0, -0.9])]),
          label='inner_product')

  def test_filter_with_item_values(self):
    queries = [
        _query('en', [1.0, 0.0], lang='en'),
        _query('fr', [1.0, 0.0], lang='fr'),
        _query('none', [1.0, 0.0], lang='de'),
    ]
    handler = self._handler(
        limit=3,
        columns=[],
        filter_template="metadata->>'lang' = :lang",
        filter_params_fn=lambda item: {'lang': item.metadata['lang']},
        min_batch_size=3)
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(queries)
          | Enrichment(handler)
          | beam.Map(
              lambda item: (item.id, item.metadata['enrichment_data']['id'])))
      assert_that(
          result,
          equal_to([('en', ['a', 'c']), ('fr', ['d', 'b']), ('none', [])]))

  def test_each_item_uses_the_vector_index(self):
    query, params = _build_query(
        PostgresVectorSearchParameters(table_name=_TABLE, limit=2),
        [_query('east', [1.0, 0.0]), _query('north', [0.0, 1.0])])
    with self._engine.connect() as connection:
      # Roll back so the index doesn't affect the other tests.
      transaction = connection.begin()
      try:
        connection.execute(
            text(
                f'CREATE INDEX {_TABLE}_hnsw ON {_TABLE} '
                'USING hnsw (embedding vector_cosine_ops)'))
        # The table is tiny, so make the planner use the index if it can.
        connection.execute(text('SET LOCAL enable_seqscan = off'))
        plan = '\n'.join(
            row[0]
            for row in connection.execute(text('EXPLAIN ' + query), params))
      finally:
        transaction.rollback()
    self.assertEqual(plan.count(f'Index Scan using {_TABLE}_hnsw'), 2, plan)


if __name__ == '__main__':
  unittest.main()
