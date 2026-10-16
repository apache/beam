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

import grpc
from parameterized import parameterized

try:
  import httpx
  from qdrant_client import models
  from qdrant_client.common.client_exceptions import ResourceExhaustedResponse
  from qdrant_client.http.exceptions import ResponseHandlingException
  from qdrant_client.http.exceptions import UnexpectedResponse
  from qdrant_client.http.models import QueryResponse

  from apache_beam.io.requestresponse import ExponentialBackOffRepeater
  from apache_beam.io.requestresponse import UserCodeQuotaException
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantConnectionParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantDenseSearchParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantHybridSearchParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantSearchEnrichmentHandler
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantSearchParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantSparseSearchParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import join_fn
  from apache_beam.ml.rag.types import Content
  from apache_beam.ml.rag.types import EmbeddableItem
  from apache_beam.ml.rag.types import Embedding
except ImportError as e:
  raise unittest.SkipTest(f'Qdrant dependencies not installed: {str(e)}')

_COLLECTION = "test_collection"

# Points stored in the in-memory collection. Dense vectors use cosine
# distance, so a query of [1.0, 0.0] ranks point 1 first, then 2, then 3.
_POINTS = [
    models.PointStruct(
        id=1,
        vector={
            "dense": [1.0, 0.0],
            "sparse": models.SparseVector(indices=[0, 1], values=[1.0, 0.5]),
        },
        payload={
            "content": "doc one", "source": "a"
        },
    ),
    models.PointStruct(
        id=2,
        vector={
            "dense": [0.8, 0.6],
            "sparse": models.SparseVector(indices=[1, 2], values=[1.0, 0.5]),
        },
        payload={
            "content": "doc two", "source": "b"
        },
    ),
    models.PointStruct(
        id=3,
        vector={
            "dense": [-1.0, 0.0],
            "sparse": models.SparseVector(indices=[2, 3], values=[1.0, 0.5]),
        },
        payload={
            "content": "doc three", "source": "a"
        },
    ),
]


def _item(id, dense=None, sparse=None):
  return EmbeddableItem(
      id=id,
      content=Content(text=f"query {id}"),
      embedding=Embedding(dense_embedding=dense, sparse_embedding=sparse))


class _RpcError(grpc.RpcError):
  def __init__(self, status_code):
    self._status_code = status_code

  def code(self):
    return self._status_code


class TestQdrantSearchParameters(unittest.TestCase):
  """Unit tests for validation of Qdrant search parameters."""
  @parameterized.expand([
      (lambda: QdrantDenseSearchParameters(limit=0), "positive"),
      (lambda: QdrantDenseSearchParameters(vector_name=""), "Vector name"),
      (lambda: QdrantSparseSearchParameters(limit=-1), "positive"),
      (lambda: QdrantSparseSearchParameters(vector_name=""), "Vector name"),
      (
          lambda: QdrantHybridSearchParameters(
              dense=QdrantDenseSearchParameters(), sparse=
              QdrantSparseSearchParameters(), limit=0),
          "positive"),
      (
          lambda: QdrantHybridSearchParameters(
              dense=None, sparse=QdrantSparseSearchParameters()),
          "Dense and sparse"),
      (
          lambda: QdrantHybridSearchParameters(
              dense=QdrantDenseSearchParameters(), sparse=
              QdrantSparseSearchParameters(), fusion=None),
          "Fusion"),
      (
          lambda: QdrantSearchParameters(
              collection_name="", search_strategy=QdrantDenseSearchParameters()
          ),
          "Collection name"),
      (
          lambda: QdrantSearchParameters(
              collection_name=_COLLECTION, search_strategy=None),
          "Search strategy"),
  ])
  def test_invalid_parameters(self, create, expected_message):
    with self.assertRaises(ValueError) as context:
      create()
    self.assertIn(expected_message, str(context.exception))

  def test_batch_elements_kwargs(self):
    handler = QdrantSearchEnrichmentHandler(
        QdrantConnectionParameters.in_memory(),
        QdrantSearchParameters(
            collection_name=_COLLECTION,
            search_strategy=QdrantDenseSearchParameters()),
        min_batch_size=5,
        max_batch_size=50)
    self.assertEqual(
        handler.batch_elements_kwargs(), {
            'min_batch_size': 5, 'max_batch_size': 50
        })


class TestQdrantSearchEnrichmentHandler(unittest.TestCase):
  """Runs the handler against an in-memory Qdrant collection."""
  def _create_handler(self, search_strategy, **search_kwargs):
    handler = QdrantSearchEnrichmentHandler(
        QdrantConnectionParameters.in_memory(),
        QdrantSearchParameters(
            collection_name=_COLLECTION,
            search_strategy=search_strategy,
            **search_kwargs))
    handler.__enter__()
    self.addCleanup(handler.__exit__, None, None, None)
    # The in-memory client is created on __enter__, so populate it here.
    client = handler._client
    client.create_collection(
        collection_name=_COLLECTION,
        vectors_config={
            "dense": models.VectorParams(
                size=2, distance=models.Distance.COSINE)
        },
        sparse_vectors_config={"sparse": models.SparseVectorParams()})
    client.upsert(collection_name=_COLLECTION, points=_POINTS)
    return handler

  def test_dense_search(self):
    handler = self._create_handler(QdrantDenseSearchParameters(limit=2))
    item = _item("q", dense=[1.0, 0.0])

    [(returned_item, result)] = handler(item)

    self.assertIs(returned_item, item)
    self.assertEqual(result["id"], [1, 2])
    self.assertAlmostEqual(result["score"][0], 1.0, places=5)
    self.assertAlmostEqual(result["score"][1], 0.8, places=5)
    self.assertEqual(
        result["payload"][0], {
            "content": "doc one", "source": "a"
        })

  def test_dense_search_with_filter_and_score_threshold(self):
    handler = self._create_handler(
        QdrantDenseSearchParameters(
            limit=3,
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="source", match=models.MatchValue(value="a"))
                ]),
            score_threshold=0.0))

    [(_, result)] = handler(_item("q", dense=[1.0, 0.0]))

    # Point 2 is filtered out by payload and point 3 by score (-1.0).
    self.assertEqual(result["id"], [1])

  def test_sparse_search(self):
    handler = self._create_handler(QdrantSparseSearchParameters(limit=3))

    [(_, result)] = handler(_item("q", sparse=([2, 3], [1.0, 1.0])))

    # Dot products: point 3 = 1.5, point 2 = 0.5, point 1 shares no index.
    self.assertEqual(result["id"], [3, 2])
    self.assertEqual(result["score"], [1.5, 0.5])

  @parameterized.expand([(models.Fusion.RRF, ), (models.Fusion.DBSF, )])
  def test_hybrid_search(self, fusion):
    handler = self._create_handler(
        QdrantHybridSearchParameters(
            dense=QdrantDenseSearchParameters(limit=3),
            sparse=QdrantSparseSearchParameters(limit=3),
            fusion=fusion,
            limit=2))

    [(_, result)
     ] = handler(_item("q", dense=[0.8, 0.6], sparse=([1, 2], [1.0, 1.0])))

    # Point 2 ranks first in both prefetches, so it wins after fusion.
    self.assertEqual(len(result["id"]), 2)
    self.assertEqual(result["id"][0], 2)
    self.assertEqual(result["payload"][0]["content"], "doc two")

  def test_hybrid_common_filter_applies_before_prefetch_limit(self):
    handler = self._create_handler(
        QdrantHybridSearchParameters(
            dense=QdrantDenseSearchParameters(limit=1),
            sparse=QdrantSparseSearchParameters(limit=1),
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="source", match=models.MatchValue(value="a"))
                ])))

    [(_, result)] = handler(_item("q", dense=[0.8, 0.6], sparse=([1], [1.0])))

    # Point 2 would win both prefetches without the common filter.
    self.assertEqual(result["id"], [1])

  def test_hybrid_fused_score_threshold(self):
    handler = self._create_handler(
        QdrantHybridSearchParameters(
            dense=QdrantDenseSearchParameters(limit=3),
            sparse=QdrantSparseSearchParameters(limit=3),
            score_threshold=0.8))

    [(_, result)
     ] = handler(_item("q", dense=[0.8, 0.6], sparse=([1, 2], [1.0, 1.0])))

    self.assertEqual(result["id"], [2])
    self.assertEqual(result["score"], [1.0])

  @parameterized.expand([
      (ResourceExhaustedResponse("rate limited", 1), ),
      (UnexpectedResponse(429, "rate limited", b"", httpx.Headers()), ),
      (UnexpectedResponse(408, "timeout", b"", httpx.Headers()), ),
      (UnexpectedResponse(504, "timeout", b"", httpx.Headers()), ),
      (ResponseHandlingException(httpx.ReadTimeout("timeout")), ),
      (_RpcError(grpc.StatusCode.RESOURCE_EXHAUSTED), ),
      (_RpcError(grpc.StatusCode.DEADLINE_EXCEEDED), ),
  ])
  def test_default_repeater_recovers_from_quota_and_timeout(self, error):
    handler = self._create_handler(QdrantDenseSearchParameters(limit=1))
    item = _item("q", dense=[1.0, 0.0])
    response = QueryResponse(points=[])
    with patch.object(
        handler._client, 'query_batch_points',
        side_effect=[error, [response]]) as query, patch(
            'apache_beam.utils.retry.time.sleep'):
      result = ExponentialBackOffRepeater().repeat(handler, [item], 30)

    self.assertEqual(result, [(item, {"id": [], "score": [], "payload": []})])
    self.assertEqual(query.call_count, 2)

  @parameterized.expand([
      (UnexpectedResponse(400, "bad request", b"", httpx.Headers()), ),
      (UnexpectedResponse(401, "unauthorized", b"", httpx.Headers()), ),
      (UnexpectedResponse(404, "not found", b"", httpx.Headers()), ),
      (ResponseHandlingException(ValueError("invalid response")), ),
      (_RpcError(grpc.StatusCode.INVALID_ARGUMENT), ),
      (ValueError("invalid query"), ),
  ])
  def test_default_repeater_does_not_retry_permanent_errors(self, error):
    handler = self._create_handler(QdrantDenseSearchParameters())
    with patch.object(handler._client, 'query_batch_points',
                      side_effect=error) as query:
      with self.assertRaises(type(error)) as context:
        ExponentialBackOffRepeater().repeat(
            handler, [_item("q", dense=[1.0, 0.0])], 30)

    self.assertIs(context.exception, error)
    self.assertEqual(query.call_count, 1)

  def test_default_repeater_exhausts_retries(self):
    handler = self._create_handler(QdrantDenseSearchParameters())
    error = ResourceExhaustedResponse("rate limited", 1)
    with patch.object(handler._client, 'query_batch_points',
                      side_effect=error) as query, \
        patch('apache_beam.utils.retry.time.sleep'):
      with self.assertRaises(UserCodeQuotaException) as context:
        ExponentialBackOffRepeater().repeat(
            handler, [_item("q", dense=[1.0, 0.0])], 30)

    self.assertIs(context.exception.__cause__, error)
    self.assertEqual(query.call_count, 3)

  def test_batched_request_returns_result_per_item(self):
    handler = self._create_handler(QdrantDenseSearchParameters(limit=1))
    items = [
        _item("q1", dense=[1.0, 0.0]),
        _item("q2", dense=[-1.0, 0.0]),
    ]

    response = handler(items)

    self.assertEqual([item for item, _ in response], items)
    self.assertEqual([result["id"] for _, result in response], [[1], [3]])

  def test_with_payload_keys(self):
    handler = self._create_handler(
        QdrantDenseSearchParameters(limit=1), with_payload=["content"])

    [(_, result)] = handler(_item("q", dense=[1.0, 0.0]))

    self.assertEqual(result["payload"], [{"content": "doc one"}])

  def test_without_payload(self):
    handler = self._create_handler(
        QdrantDenseSearchParameters(limit=1), with_payload=False)

    [(_, result)] = handler(_item("q", dense=[1.0, 0.0]))

    self.assertEqual(result["id"], [1])
    self.assertEqual(result["payload"], [{}])

  def test_empty_request(self):
    handler = self._create_handler(QdrantDenseSearchParameters())
    self.assertEqual(handler([]), [])

  @parameterized.expand([
      (QdrantDenseSearchParameters(), _item("q", sparse=([0], [1.0])), "dense"),
      (QdrantSparseSearchParameters(), _item("q", dense=[1.0, 0.0]), "sparse"),
      (
          QdrantHybridSearchParameters(
              dense=QdrantDenseSearchParameters(),
              sparse=QdrantSparseSearchParameters()),
          _item("q", dense=[1.0, 0.0]),
          "sparse"),
  ])
  def test_missing_embedding(self, search_strategy, item, embedding_type):
    handler = self._create_handler(search_strategy)
    with self.assertRaises(ValueError) as context:
      handler(item)
    self.assertIn(
        f"Item q missing {embedding_type} embedding", str(context.exception))

  def test_unsupported_search_strategy(self):
    handler = self._create_handler({"vector_name": "dense"})
    with self.assertRaises(ValueError) as context:
      handler(_item("q", dense=[1.0, 0.0]))
    self.assertIn("Not supported search strategy", str(context.exception))

  def test_join_fn(self):
    item = _item("q", dense=[1.0, 0.0])
    item.metadata = {"source": "query", "enrichment_data": {"previous": True}}
    original_metadata = dict(item.metadata)
    result = {"id": [1], "score": [1.0], "payload": [{"content": "doc one"}]}

    joined = join_fn(item, result)

    self.assertEqual(joined.metadata["enrichment_data"], result)
    self.assertEqual(joined.metadata["source"], "query")
    self.assertEqual(item.metadata, original_metadata)
    self.assertIsNot(joined, item)
    self.assertIsNot(joined.metadata, item.metadata)
    self.assertEqual(joined.id, item.id)
    self.assertEqual(joined.content, item.content)
    self.assertEqual(joined.embedding, item.embedding)


if __name__ == '__main__':
  unittest.main()
