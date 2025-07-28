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
from dataclasses import dataclass

import pytest
from testcontainers.qdrant import QdrantContainer

import apache_beam as beam
from apache_beam.ml.rag.types import Chunk, Content, Embedding
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

try:
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.ml.rag.enrichment.qdrant_search import (
      QdrantSearchEnrichmentHandler,
      QdrantConnectionParameters,
      QdrantSearchParameters,
      QdrantDenseSearchParameters,
  )
except ImportError as e:
  raise unittest.SkipTest(f"Qdrant dependencies not installed: {str(e)}")


@dataclass
class QdrantITDataConstruct:
  id: int
  content: str
  metadata: dict
  dense_embedding: list
  sparse_embedding: dict

  def __getitem__(self, key):
    return getattr(self, key)


QDRANT_IT_CONFIG = {
    "collection_name": "docs_catalog",
    "corpus": [
        QdrantITDataConstruct(
            id=1,
            content="This is a test document",
            metadata={"language": "en"},
            dense_embedding=[0.1, 0.2, 0.3],
            sparse_embedding={
                1: 0.05, 2: 0.41, 3: 0.05, 4: 0.41
            },
        ),
        QdrantITDataConstruct(
            id=2,
            content="Another test document",
            metadata={"language": "en"},
            dense_embedding=[0.2, 0.3, 0.4],
            sparse_embedding={
                1: 0.07, 3: 3.07, 0: 0.53
            },
        ),
        QdrantITDataConstruct(
            id=3,
            content="وثيقة اختبار",
            metadata={"language": "ar"},
            dense_embedding=[0.3, 0.4, 0.5],
            sparse_embedding={
                6: 0.62, 5: 0.62
            },
        ),
    ],
}


def _to_sparse_dict(sparse_embedding):
  if isinstance(sparse_embedding, dict):
    return sparse_embedding
  if sparse_embedding is None:
    return None
  indices, values = sparse_embedding
  return dict(zip(indices, values))


def assert_chunks_equivalent(
    actual_chunks,
    expected_chunks,
    check_dense=True,
    check_sparse=True,
):
  actual_sorted = sorted(actual_chunks, key=lambda c: c.id)
  expected_sorted = sorted(expected_chunks, key=lambda c: c.id)
  assert len(actual_sorted) == len(expected_sorted)
  for actual, expected in zip(actual_sorted, expected_sorted):
    assert actual.id == expected.id
    if check_dense:
      assert (
          actual.embedding.dense_embedding == expected.embedding.dense_embedding
      )
    if check_sparse:
      assert _to_sparse_dict(
          actual.embedding.sparse_embedding) == _to_sparse_dict(
              expected.embedding.sparse_embedding)
    assert "enrichment_data" in actual.metadata
    actual_data = actual.metadata["enrichment_data"]
    expected_data = expected.metadata["enrichment_data"]
    assert actual_data["id"] == expected_data["id"]
    for a, e in zip(actual_data["payload"], expected_data["payload"]):
      assert a["content"] == e["content"]
      assert a["metadata"] == e["metadata"]


@pytest.mark.uses_testcontainer
class TestQdrantSearchEnrichmentIT(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls._container = QdrantContainer()
    cls._container.start()
    cls._host = cls._container.get_container_host_ip()
    cls._port = int(cls._container.get_exposed_port(6333))
    cls._connection_params = QdrantConnectionParameters(
        host=cls._host, port=cls._port)
    from qdrant_client import QdrantClient, models

    client = QdrantClient(
        host=cls._host, port=cls._port, check_compatibility=False)
    points = []
    for doc in QDRANT_IT_CONFIG["corpus"]:
      points.append(
          models.PointStruct(
              id=doc.id,
              vector={
                  "dense": doc.dense_embedding,
                  "sparse": models.SparseVector(
                      indices=list(doc.sparse_embedding.keys()),
                      values=list(doc.sparse_embedding.values()),
                  ),
              },
              payload={
                  "content": doc.content, "metadata": doc.metadata
              },
          ))
    client.create_collection(
        collection_name=QDRANT_IT_CONFIG["collection_name"],
        vectors_config={
            "dense": models.VectorParams(
                size=3, distance=models.Distance.COSINE),
        },
        sparse_vectors_config={
            "sparse": models.SparseVectorParams(),
        },
    )
    client.upsert(
        collection_name=QDRANT_IT_CONFIG["collection_name"], points=points)

  @classmethod
  def tearDownClass(cls):
    cls._container.stop()

  def test_dense_search(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(),
        ),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            content=Content(),
        ),
    ]
    search_parameters = QdrantSearchParameters(
        collection_name=QDRANT_IT_CONFIG["collection_name"],
        search_strategy=QdrantDenseSearchParameters(
            vector_name="dense", limit=2),
    )
    handler = QdrantSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
    )
    with TestPipeline(is_integration_test=True) as p:
      result = p | beam.Create(test_chunks) | Enrichment(handler)

      assert_that(result, lambda actual: len(actual) == 2)

  def test_dense_search_empty(self):
    test_chunks = []
    search_parameters = QdrantSearchParameters(
        collection_name=QDRANT_IT_CONFIG["collection_name"],
        search_strategy=QdrantDenseSearchParameters(
            vector_name="dense", limit=2),
    )
    handler = QdrantSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
    )
    with TestPipeline(is_integration_test=True) as p:
      result = p | beam.Create(test_chunks) | Enrichment(handler)
      assert_that(result, equal_to([]))

  def test_invalid_collection_name(self):
    search_parameters = QdrantSearchParameters(
        collection_name="nonexistent_collection",
        search_strategy=QdrantDenseSearchParameters(
            vector_name="dense", limit=2),
    )
    handler = QdrantSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
    )
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(),
        )
    ]
    with self.assertRaises(Exception) as context:
      with TestPipeline() as p:
        _ = p | beam.Create(test_chunks) | Enrichment(handler)
    self.assertIn("not found", str(context.exception).lower())

  def test_missing_embedding(self):
    search_parameters = QdrantSearchParameters(
        collection_name=QDRANT_IT_CONFIG["collection_name"],
        search_strategy=QdrantDenseSearchParameters(
            vector_name="dense", limit=2),
    )
    handler = QdrantSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
    )
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=None),
            content=Content(),
        )
    ]
    with self.assertRaises(ValueError) as context:
      with TestPipeline() as p:
        _ = p | beam.Create(test_chunks) | Enrichment(handler)
    self.assertIn("missing dense embedding", str(context.exception).lower())

  def test_batched_dense_search(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(),
        ),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            content=Content(),
        ),
        Chunk(
            id="query3",
            embedding=Embedding(dense_embedding=[0.3, 0.4, 0.5]),
            content=Content(),
        ),
    ]
    search_parameters = QdrantSearchParameters(
        collection_name=QDRANT_IT_CONFIG["collection_name"],
        search_strategy=QdrantDenseSearchParameters(
            vector_name="dense", limit=2),
    )
    handler = QdrantSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
    )
    with TestPipeline(is_integration_test=True) as p:
      result = p | beam.Create(test_chunks) | Enrichment(handler)
      assert_that(result, lambda actual: len(actual) == 3)

  def test_dense_search_values(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(),
        ),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            content=Content(),
        ),
    ]
    search_parameters = QdrantSearchParameters(
        collection_name=QDRANT_IT_CONFIG["collection_name"],
        search_strategy=QdrantDenseSearchParameters(
            vector_name="dense", limit=2),
    )
    handler = QdrantSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
    )
    expected_chunks = [
        Chunk(
            id="query1",
            content=Content(),
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            metadata={
                "enrichment_data": {
                    "id": [1, 2],
                    "score": None,
                    "payload": [
                        {
                            "content": "This is a test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                        {
                            "content": "Another test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                    ],
                }
            },
        ),
        Chunk(
            id="query2",
            content=Content(),
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            metadata={
                "enrichment_data": {
                    "id": [2, 3],
                    "score": None,
                    "payload": [
                        {
                            "content": "Another test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                        {
                            "content": "وثيقة اختبار",
                            "metadata": {
                                "language": "ar"
                            }
                        },
                    ],
                }
            },
        ),
    ]
    with TestPipeline(is_integration_test=True) as p:
      result = p | beam.Create(test_chunks) | Enrichment(handler)
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(
              actual,
              expected_chunks,
              check_dense=True,
              check_sparse=False, ),
      )

  def test_sparse_search_values(self):
    from apache_beam.ml.rag.enrichment.qdrant_search import (
        QdrantSparseSearchParameters, )

    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(
                dense_embedding=None,
                sparse_embedding=([1, 2, 3, 4], [0.05, 0.41, 0.05, 0.41]),
            ),
            content=Content(),
        ),
        Chunk(
            id="query2",
            embedding=Embedding(
                dense_embedding=None,
                sparse_embedding=([1, 3, 0], [0.07, 3.07, 0.53]),
            ),
            content=Content(),
        ),
    ]
    search_parameters = QdrantSearchParameters(
        collection_name=QDRANT_IT_CONFIG["collection_name"],
        search_strategy=QdrantSparseSearchParameters(
            vector_name="sparse", limit=2),
    )
    handler = QdrantSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
    )
    expected_chunks = [
        Chunk(
            id="query1",
            content=Content(),
            embedding=Embedding(
                dense_embedding=None,
                sparse_embedding={
                    1: 0.05, 2: 0.41, 3: 0.05, 4: 0.41
                },
            ),
            metadata={
                "enrichment_data": {
                    "id": [1, 2],
                    "score": None,
                    "payload": [
                        {
                            "content": "This is a test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                        {
                            "content": "Another test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                    ],
                }
            },
        ),
        Chunk(
            id="query2",
            content=Content(),
            embedding=Embedding(
                dense_embedding=None,
                sparse_embedding={
                    1: 0.07, 3: 3.07, 0: 0.53
                }),
            metadata={
                "enrichment_data": {
                    "id": [2, 1],
                    "score": None,
                    "payload": [
                        {
                            "content": "Another test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                        {
                            "content": "This is a test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                    ],
                }
            },
        ),
    ]
    with TestPipeline(is_integration_test=True) as p:
      result = p | beam.Create(test_chunks) | Enrichment(handler)
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(
              actual,
              expected_chunks,
              check_dense=False,
              check_sparse=True, ),
      )

  def test_hybrid_search_values(self):
    from apache_beam.ml.rag.enrichment.qdrant_search import (
        QdrantHybridSearchParameters,
        QdrantDenseSearchParameters,
        QdrantSparseSearchParameters,
    )

    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(
                dense_embedding=[0.1, 0.2, 0.3],
                sparse_embedding=([1, 2, 3, 4], [0.05, 0.41, 0.05, 0.41]),
            ),
            content=Content(),
        )
    ]
    search_parameters = QdrantSearchParameters(
        collection_name=QDRANT_IT_CONFIG["collection_name"],
        search_strategy=QdrantHybridSearchParameters(
            dense=QdrantDenseSearchParameters(vector_name="dense", limit=2),
            sparse=QdrantSparseSearchParameters(vector_name="sparse", limit=2),
            limit=2,
        ),
    )
    handler = QdrantSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
    )
    expected_chunks = [
        Chunk(
            id="query1",
            content=Content(),
            embedding=Embedding(
                dense_embedding=[0.1, 0.2, 0.3],
                sparse_embedding={
                    1: 0.05, 2: 0.41, 3: 0.05, 4: 0.41
                },
            ),
            metadata={
                "enrichment_data": {
                    "id": [1, 2],
                    "score": None,
                    "payload": [
                        {
                            "content": "This is a test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                        {
                            "content": "Another test document",
                            "metadata": {
                                "language": "en"
                            },
                        },
                    ],
                }
            },
        )
    ]
    with TestPipeline(is_integration_test=True) as p:
      result = p | beam.Create(test_chunks) | Enrichment(handler)
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(
              actual,
              expected_chunks,
              check_dense=True,
              check_sparse=True, ),
      )


if __name__ == "__main__":
  unittest.main()
