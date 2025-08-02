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
  from apache_beam.ml.rag.types import Chunk, Embedding
  from apache_beam.ml.rag.enrichment.qdrant_search import (
      QdrantSearchEnrichmentHandler,
      QdrantConnectionParameters,
      QdrantSearchParameters,
      QdrantDenseSearchParameters,
      QdrantSparseSearchParameters,
      QdrantHybridSearchParameters,
  )
except ImportError as e:
  raise unittest.SkipTest(f"Qdrant dependencies not installed: {str(e)}")


class TestQdrantSearchEnrichment(unittest.TestCase):
  def test_invalid_connection_parameters(self):
    with self.assertRaises(ValueError) as context:
      connection_params = QdrantConnectionParameters(url="")
      search_params = QdrantSearchParameters(
          collection_name="test_collection",
          search_strategy=QdrantDenseSearchParameters(vector_name="dense"),
      )
      _ = QdrantSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params)
    self.assertIn(
        "One of location, url, host, or path must be provided for Qdrant",
        str(context.exception),
    )

  @parameterized.expand([
      (
          lambda: QdrantSearchParameters(
              collection_name="",
              search_strategy=QdrantDenseSearchParameters(vector_name="dense"),
          ),
          "Collection name must be provided",
      ),
      (
          lambda: QdrantSearchParameters(
              collection_name="test_collection", search_strategy=None),
          "Search strategy must be provided",
      ),
  ])
  def test_invalid_search_parameters(self, create_params, expected_error_msg):
    with self.assertRaises(ValueError) as context:
      connection_params = QdrantConnectionParameters(
          url="http://localhost:6333")
      search_params = create_params()
      _ = QdrantSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params)
    self.assertIn(expected_error_msg, str(context.exception))


class TestQdrantDenseSearchEnrichment(unittest.TestCase):
  @parameterized.expand([
      (
          lambda: QdrantDenseSearchParameters(vector_name="dense", limit=-1),
          "Search limit must be positive, got -1",
      ),
  ])
  def test_invalid_dense_search_parameters(
      self, create_params, expected_error_msg):
    with self.assertRaises(ValueError) as context:
      connection_params = QdrantConnectionParameters(
          url="http://localhost:6333")
      dense_search_params = create_params()
      search_params = QdrantSearchParameters(
          collection_name="test_collection",
          search_strategy=dense_search_params)
      _ = QdrantSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params)
    self.assertIn(expected_error_msg, str(context.exception))

  def test_missing_dense_embedding(self):
    with self.assertRaises(ValueError) as context:
      chunk = Chunk(
          id=1, content=None, embedding=Embedding(dense_embedding=None))
      connection_params = QdrantConnectionParameters(
          url="http://localhost:6333")
      dense_search_params = QdrantDenseSearchParameters(vector_name="dense")
      search_params = QdrantSearchParameters(
          collection_name="test_collection",
          search_strategy=dense_search_params)
      handler = QdrantSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params)
      _ = handler._dense_search([chunk])
    self.assertIn("missing dense embedding", str(context.exception))


class TestQdrantSparseSearchEnrichment(unittest.TestCase):
  @parameterized.expand([
      (
          lambda: QdrantSparseSearchParameters(vector_name="sparse", limit=-1),
          "Search limit must be positive, got -1",
      ),
  ])
  def test_invalid_sparse_search_parameters(
      self, create_params, expected_error_msg):
    with self.assertRaises(ValueError) as context:
      connection_params = QdrantConnectionParameters(
          url="http://localhost:6333")
      sparse_search_params = create_params()
      search_params = QdrantSearchParameters(
          collection_name="test_collection",
          search_strategy=sparse_search_params)
      _ = QdrantSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params)
    self.assertIn(expected_error_msg, str(context.exception))

  def test_missing_sparse_embedding(self):
    with self.assertRaises(ValueError) as context:
      chunk = Chunk(
          id=1, content=None, embedding=Embedding(sparse_embedding=None))
      connection_params = QdrantConnectionParameters(
          url="http://localhost:6333")
      sparse_search_params = QdrantSparseSearchParameters(vector_name="sparse")
      search_params = QdrantSearchParameters(
          collection_name="test_collection",
          search_strategy=sparse_search_params)
      handler = QdrantSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params)
      _ = handler._sparse_search([chunk])
    self.assertIn(
        "missing sparse embedding",
        str(context.exception),
    )


class TestQdrantHybridSearchEnrichment(unittest.TestCase):
  @parameterized.expand([
      (
          lambda: QdrantHybridSearchParameters(
              dense=None,
              sparse=QdrantSparseSearchParameters(vector_name="sparse"), ),
          "Both dense and sparse search params are required",
      ),
      (
          lambda: QdrantHybridSearchParameters(
              dense=QdrantDenseSearchParameters(vector_name="dense"), sparse=
              None),
          "Both dense and sparse search params are required",
      ),
      (
          lambda: QdrantHybridSearchParameters(
              dense=QdrantDenseSearchParameters(vector_name="dense"),
              sparse=QdrantSparseSearchParameters(vector_name="sparse"),
              limit=-1, ),
          "Search limit must be positive, got -1",
      ),
  ])
  def test_invalid_hybrid_search_parameters(
      self, create_params, expected_error_msg):
    with self.assertRaises(ValueError) as context:
      connection_params = QdrantConnectionParameters(
          url="http://localhost:6333")
      hybrid_search_params = create_params()
      search_params = QdrantSearchParameters(
          collection_name="test_collection",
          search_strategy=hybrid_search_params)
      _ = QdrantSearchEnrichmentHandler(
          connection_parameters=connection_params,
          search_parameters=search_params)
    self.assertIn(expected_error_msg, str(context.exception))


if __name__ == "__main__":
  unittest.main()
