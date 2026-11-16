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
from unittest import mock

# pylint: disable=ungrouped-imports
try:
  from google.cloud import bigquery
  from apache_beam.ml.rag.enrichment.bigquery_vector_search import BigQueryVectorSearchEnrichmentHandler
  from apache_beam.ml.rag.enrichment.bigquery_vector_search import BigQueryVectorSearchParameters
  from apache_beam.ml.rag.types import Chunk
  from apache_beam.ml.rag.types import Content
  from apache_beam.ml.rag.types import Embedding
except ImportError:
  raise unittest.SkipTest('BigQuery dependencies not installed')


class TestBigQueryVectorSearchEnrichment(unittest.TestCase):
  def setUp(self) -> None:
    self.project = 'apache-beam-testing'
    self.table_name = 'apache-beam-testing.dataset.embeddings'

  def test_vector_search_binds_query_parameters_for_id_and_metadata(self):
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
    self.assertIn('SELECT @_bq_item_id_0 as id', query_str)
    self.assertEqual(
        list(job_config.query_parameters),
        [
            bigquery.ScalarQueryParameter(
                '_bq_meta_0', 'STRING', malicious_lang),
            bigquery.ScalarQueryParameter(
                '_bq_item_id_0', 'STRING', malicious_id),
        ],
    )

  def test_invalid_table_or_column_names_raise_value_error(self):
    with self.assertRaises(ValueError):
      _ = BigQueryVectorSearchParameters(
          project=self.project,
          table_name='project.dataset.table`; DROP TABLE x; --',
          embedding_column='embedding',
          columns=['content'],
          neighbor_count=2,
      )
    with self.assertRaises(ValueError):
      _ = BigQueryVectorSearchParameters(
          project=self.project,
          table_name=self.table_name,
          embedding_column='embedding; DROP TABLE x',
          columns=['content'],
          neighbor_count=2,
      )


if __name__ == '__main__':
  unittest.main()
