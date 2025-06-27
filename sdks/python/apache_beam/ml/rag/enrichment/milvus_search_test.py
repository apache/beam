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

from parameterized import parameterized

try:
  from apache_beam.ml.rag.types import Chunk
  from apache_beam.ml.rag.types import Embedding
  from apache_beam.ml.rag.types import Content
  from apache_beam.ml.rag.enrichment.milvus_search import (
      MilvusSearchEnrichmentHandler,
      MilvusConnectionParameters,
      MilvusSearchParameters,
      MilvusCollectionLoadParameters,
      VectorSearchParameters,
      KeywordSearchParameters,
      HybridSearchParameters,
      MilvusBaseRanker,
      unpack_dataclass_with_kwargs)
except ImportError as e:
  raise unittest.SkipTest(f'Milvus dependencies not installed: {str(e)}')


class MockRanker(MilvusBaseRanker):
  def dict(self):
    return {"name": "mock_ranker"}


class TestMilvusSearchEnrichment(unittest.TestCase):
  """Unit tests for general search functionality in the Enrichment Handler."""
  def test_invalid_connection_parameters(self):
    """Test validation errors for invalid connection parameters."""
    # Empty URI in connection parameters.
    with self.assertRaises(ValueError) as context:
      connection_params = MilvusConnectionParameters(uri="")
      search_params = MilvusSearchParameters(
          collection_name="test_collection",
          search_strategy=VectorSearchParameters(anns_field="embedding"))
      collection_load_params = MilvusCollectionLoadParameters()

      _ = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

    self.assertIn(
        "URI must be provided for Milvus connection", str(context.exception))

  @parameterized.expand([
      # Empty collection name.
      (
          lambda: MilvusSearchParameters(
              collection_name="",
              search_strategy=VectorSearchParameters(anns_field="embedding")),
          "Collection name must be provided"
      ),
      # Missing search strategy.
      (
          lambda: MilvusSearchParameters(
              collection_name="test_collection",
              search_strategy=None),  # type: ignore[arg-type]
          "Search strategy must be provided"
      ),
  ])
  def test_invalid_search_parameters(self, create_params, expected_error_msg):
    """Test validation errors for invalid general search parameters."""
    with self.assertRaises(ValueError) as context:
      connection_params = MilvusConnectionParameters(
          uri="http://localhost:19530")
      search_params = create_params()
      collection_load_params = MilvusCollectionLoadParameters()

      _ = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

    self.assertIn(expected_error_msg, str(context.exception))

  def test_unpack_dataclass_with_kwargs(self):
    """Test the unpack_dataclass_with_kwargs function."""
    # Create a test dataclass instance.
    connection_params = MilvusConnectionParameters(
        uri="http://localhost:19530",
        user="test_user",
        kwargs={"custom_param": "value"})

    # Call the actual function.
    result = unpack_dataclass_with_kwargs(connection_params)

    # Verify the function correctly unpacks the dataclass and merges kwargs.
    self.assertEqual(result["uri"], "http://localhost:19530")
    self.assertEqual(result["user"], "test_user")
    self.assertEqual(result["custom_param"], "value")

    # Verify that kwargs take precedence over existing attributes.
    connection_params_with_override = MilvusConnectionParameters(
        uri="http://localhost:19530",
        user="test_user",
        kwargs={"user": "override_user"})

    result_with_override = unpack_dataclass_with_kwargs(
        connection_params_with_override)
    self.assertEqual(result_with_override["user"], "override_user")


class TestMilvusVectorSearchEnrichment(unittest.TestCase):
  """Unit tests specific to vector search functionality"""

  @parameterized.expand([
      # Negative limit in vector search parameters.
      (
          lambda: VectorSearchParameters(anns_field="embedding", limit=-1),
          "Search limit must be positive, got -1"
      ),
      # Missing anns_field in vector search parameters.
      (
          lambda: VectorSearchParameters(anns_field=None),  # type: ignore[arg-type]
          "Approximate Nearest Neighbor Search (ANNS) field must be provided"
      ),
  ])
  def test_invalid_search_parameters(self, create_params, expected_error_msg):
    """Test validation errors for invalid vector search parameters."""
    with self.assertRaises(ValueError) as context:
      connection_params = MilvusConnectionParameters(
          uri="http://localhost:19530")
      vector_search_params = create_params()
      search_params = MilvusSearchParameters(
          collection_name="test_collection",
          search_strategy=vector_search_params)
      collection_load_params = MilvusCollectionLoadParameters()

      _ = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

    self.assertIn(expected_error_msg, str(context.exception))

  def test_missing_dense_embedding(self):
    with self.assertRaises(ValueError) as context:
      chunk = Chunk(
          id=1, content=None, embedding=Embedding(dense_embedding=None))
      connection_params = MilvusConnectionParameters(
          uri="http://localhost:19530")
      vector_search_params = VectorSearchParameters(anns_field="embedding")
      search_params = MilvusSearchParameters(
          collection_name="test_collection",
          search_strategy=vector_search_params)
      collection_load_params = MilvusCollectionLoadParameters()
      handler = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

      _ = handler._get_vector_search_data(chunk)

    err_msg = "Chunk 1 missing dense embedding required for vector search"
    self.assertIn(err_msg, str(context.exception))


class TestMilvusKeywordSearchEnrichment(unittest.TestCase):
  """Unit tests specific to keyword search functionality"""

  @parameterized.expand([
      # Negative limit in keyword search parameters.
      (
          lambda: KeywordSearchParameters(
              anns_field="sparse_embedding", limit=-1),
          "Search limit must be positive, got -1"
      ),
      # Missing anns_field in keyword search parameters.
      (
          lambda: KeywordSearchParameters(anns_field=None),  # type: ignore[arg-type]
          "Approximate Nearest Neighbor Search (ANNS) field must be provided"
      ),
  ])
  def test_invalid_search_parameters(self, create_params, expected_error_msg):
    """Test validation errors for invalid keyword search parameters."""
    with self.assertRaises(ValueError) as context:
      connection_params = MilvusConnectionParameters(
          uri="http://localhost:19530")
      keyword_search_params = create_params()
      search_params = MilvusSearchParameters(
          collection_name="test_collection",
          search_strategy=keyword_search_params)
      collection_load_params = MilvusCollectionLoadParameters()

      _ = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

    self.assertIn(expected_error_msg, str(context.exception))

  def test_missing_text_content_and_sparse_embedding(self):
    with self.assertRaises(ValueError) as context:
      chunk = Chunk(
          id=1,
          content=Content(text=None),
          embedding=Embedding(sparse_embedding=None))
      connection_params = MilvusConnectionParameters(
          uri="http://localhost:19530")
      vector_search_params = VectorSearchParameters(anns_field="embedding")
      search_params = MilvusSearchParameters(
          collection_name="test_collection",
          search_strategy=vector_search_params)
      collection_load_params = MilvusCollectionLoadParameters()
      handler = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

      _ = handler._get_keyword_search_data(chunk)

    err_msg = (
        "Chunk 1 missing both text content and sparse embedding "
        "required for keyword search")
    self.assertIn(err_msg, str(context.exception))

  def test_missing_text_content_only(self):
    try:
      chunk = Chunk(
          id=1,
          content=Content(text=None),
          embedding=Embedding(
              sparse_embedding=([1, 2, 3, 4], [0.05, 0.41, 0.05, 0.41])))
      connection_params = MilvusConnectionParameters(
          uri="http://localhost:19530")
      vector_search_params = VectorSearchParameters(anns_field="embedding")
      search_params = MilvusSearchParameters(
          collection_name="test_collection",
          search_strategy=vector_search_params)
      collection_load_params = MilvusCollectionLoadParameters()
      handler = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

      _ = handler._get_keyword_search_data(chunk)
    except Exception as e:
      self.fail(f"raised an unexpected exception: {e}")

  def test_missing_sparse_embedding_only(self):
    try:
      chunk = Chunk(
          id=1,
          content=Content(text="what is apache beam?"),
          embedding=Embedding(sparse_embedding=None))
      connection_params = MilvusConnectionParameters(
          uri="http://localhost:19530")
      vector_search_params = VectorSearchParameters(anns_field="embedding")
      search_params = MilvusSearchParameters(
          collection_name="test_collection",
          search_strategy=vector_search_params)
      collection_load_params = MilvusCollectionLoadParameters()
      handler = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

      _ = handler._get_keyword_search_data(chunk)
    except Exception as e:
      self.fail(f"raised an unexpected exception: {e}")
    pass


class TestMilvusHybridSearchEnrichment(unittest.TestCase):
  """Tests specific to hybrid search functionality"""

  @parameterized.expand([
      # Missing vector in hybrid search parameters.
      (
          lambda: HybridSearchParameters(
              vector=None,  # type: ignore[arg-type]
              keyword=KeywordSearchParameters(anns_field="sparse_embedding"),
              ranker=MockRanker()),
          "Vector and keyword search parameters must be provided for hybrid "
          "search"
      ),
      # Missing keyword in hybrid search parameters.
      (
          lambda: HybridSearchParameters(
              vector=VectorSearchParameters(anns_field="embedding"),
              keyword=None,  # type: ignore[arg-type]
              ranker=MockRanker()),
          "Vector and keyword search parameters must be provided for hybrid "
          "search"
      ),
      # Missing ranker in hybrid search parameters.
      (
          lambda: HybridSearchParameters(
              vector=VectorSearchParameters(anns_field="embedding"),
              keyword=KeywordSearchParameters(anns_field="sparse_embedding"),
              ranker=None),  # type: ignore[arg-type]
          "Ranker must be provided for hybrid search"
      ),
      # Negative limit in hybrid search parameters.
      (
          lambda: HybridSearchParameters(
              vector=VectorSearchParameters(anns_field="embedding"),
              keyword=KeywordSearchParameters(anns_field="sparse_embedding"),
              ranker=MockRanker(),
              limit=-1),
          "Search limit must be positive, got -1"
      ),
  ])
  def test_invalid_search_parameters(self, create_params, expected_error_msg):
    """Test validation errors for invalid hybrid search parameters."""
    with self.assertRaises(ValueError) as context:
      connection_params = MilvusConnectionParameters(
          uri="http://localhost:19530")
      hybrid_search_params = create_params()
      search_params = MilvusSearchParameters(
          collection_name="test_collection",
          search_strategy=hybrid_search_params)
      collection_load_params = MilvusCollectionLoadParameters()

      _ = MilvusSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params,
          collection_load_parameters=collection_load_params)

    self.assertIn(expected_error_msg, str(context.exception))


if __name__ == '__main__':
  unittest.main()
