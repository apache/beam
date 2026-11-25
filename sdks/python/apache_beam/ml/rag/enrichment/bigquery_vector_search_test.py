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

import json
import re
import unittest

from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding

# pylint: disable=ungrouped-imports
try:
  from apache_beam.ml.rag.enrichment.bigquery_vector_search import BigQueryVectorSearchParameters
except ImportError:
  raise unittest.SkipTest('BigQuery dependencies not installed')


class BigQueryVectorSearchParametersTest(unittest.TestCase):
  def setUp(self):
    self.item = EmbeddableItem(
        id='1',
        content=Content(text='hello'),
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        metadata={'language': 'en'})

  def test_format_query_serializes_options_as_json_string(self):
    options = {'use_brute_force': True, 'fraction_lists_to_search': 0.01}
    params = BigQueryVectorSearchParameters(
        project='my-proj',
        table_name='my-proj.ds.tbl',
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=5,
        options=options)

    query = params.format_query([self.item])
    expected_json = json.dumps(options)
    self.assertIn(f", options => '{expected_json}'", query)

    match = re.search(r"options => '([^']+)'", query)
    assert match is not None
    self.assertEqual(json.loads(match.group(1)), options)

  def test_format_query_escapes_special_chars_in_options(self):
    options = {'note': "it's a \\test"}
    params = BigQueryVectorSearchParameters(
        project='my-proj',
        table_name='my-proj.ds.tbl',
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=5,
        options=options)

    query = params.format_query([self.item])
    self.assertIn(", options => '{\"note\": \"it\\'s a \\\\\\\\test\"}'", query)

  def test_format_query_omits_options_when_none_or_empty(self):
    params_none = BigQueryVectorSearchParameters(
        project='my-proj',
        table_name='my-proj.ds.tbl',
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=5,
        options=None)
    self.assertNotIn('options =>', params_none.format_query([self.item]))

    params_empty = BigQueryVectorSearchParameters(
        project='my-proj',
        table_name='my-proj.ds.tbl',
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=5,
        options={})
    self.assertNotIn('options =>', params_empty.format_query([self.item]))

  def test_format_query_deduplicates_embedding_column_in_subquery(self):
    params = BigQueryVectorSearchParameters(
        project='my-proj',
        table_name='my-proj.ds.tbl',
        embedding_column='embedding',
        columns=['content', 'embedding'],
        neighbor_count=5)

    query = params.format_query([self.item])
    self.assertIn('(SELECT content, embedding\n', query)
    self.assertNotIn('embedding, embedding', query)
    self.assertIn('base.content, base.embedding', query)

  def test_format_query_appends_embedding_column_when_not_in_columns(self):
    params = BigQueryVectorSearchParameters(
        project='my-proj',
        table_name='my-proj.ds.tbl',
        embedding_column='embedding',
        columns=['content', 'metadata'],
        neighbor_count=5)

    query = params.format_query([self.item])
    self.assertIn('(SELECT content, metadata, embedding\n', query)
    self.assertIn('base.content, base.metadata', query)

  def test_format_query_with_distance_type_and_include_distance(self):
    params = BigQueryVectorSearchParameters(
        project='my-proj',
        table_name='my-proj.ds.tbl',
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=3,
        distance_type='COSINE',
        include_distance=True)

    query = params.format_query([self.item])
    self.assertIn("distance_type => 'COSINE'", query)
    self.assertRegex(query, r'STRUCT\(distance,\s+base\.content\)')


if __name__ == '__main__':
  unittest.main()
