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
  from qdrant_client import models

  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantConnectionParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantDenseSearchParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantHybridSearchParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantSearchEnrichmentHandler
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantSearchParameters
  from apache_beam.ml.rag.enrichment.qdrant_search import QdrantSparseSearchParameters
  from apache_beam.ml.rag.ingestion.qdrant import QdrantWriteConfig
  from apache_beam.transforms.enrichment import Enrichment

  QDRANT_AVAILABLE = True
except ImportError:
  QDRANT_AVAILABLE = False

try:
  from testcontainers.qdrant import QdrantContainer
except ImportError:
  QdrantContainer = None
# pylint: enable=ungrouped-imports

TEST_CORPUS = [
    EmbeddableItem(
        id="1",
        content=Content(text="Beam runs batch and streaming pipelines."),
        metadata={"source": "beam"},
        embedding=Embedding(
            dense_embedding=[1.0, 0.0], sparse_embedding=([0, 1], [1.0, 0.5])),
    ),
    EmbeddableItem(
        id="2",
        content=Content(text="Qdrant is a vector database."),
        metadata={"source": "qdrant"},
        embedding=Embedding(
            dense_embedding=[0.0, 1.0], sparse_embedding=([2, 3], [1.0, 0.5])),
    ),
    EmbeddableItem(
        id="3",
        content=Content(text="Enrichment joins external data."),
        metadata={"source": "beam"},
        embedding=Embedding(
            dense_embedding=[-1.0, 0.0], sparse_embedding=([4, 5], [1.0, 0.5])),
    ),
]


def _query(id, dense=None, sparse=None):
  return EmbeddableItem(
      id=id,
      content=Content(text=f"query {id}"),
      embedding=Embedding(dense_embedding=dense, sparse_embedding=sparse))


def _retrieved(item):
  """Extracts (query id, retrieved ids, retrieved contents) for assertions."""
  data = item.metadata["enrichment_data"]
  return (
      item.id,
      data["id"], [payload.get("content") for payload in data["payload"]])


@unittest.skipIf(not QDRANT_AVAILABLE, "qdrant dependencies not installed.")
@unittest.skipIf(not QdrantContainer, "qdrant test_container not installed.")
@pytest.mark.require_docker_in_docker
class TestQdrantSearchEnrichment(unittest.TestCase):
  """Writes TEST_CORPUS with QdrantWriteConfig, then retrieves it."""
  _collection_name = "test_qdrant_search"

  @classmethod
  def setUpClass(cls):
    cls._container = QdrantContainer()
    cls._container.start()
    cls.addClassCleanup(cls._container.stop)
    cls._connection_params = QdrantConnectionParameters(
        host=cls._container.get_container_host_ip(),
        port=int(cls._container.get_exposed_port(6333)))

    client = cls._container.get_client()
    client.create_collection(
        collection_name=cls._collection_name,
        vectors_config={
            "dense": models.VectorParams(
                size=2, distance=models.Distance.COSINE)
        },
        sparse_vectors_config={"sparse": models.SparseVectorParams()})
    client.close()

    write_config = QdrantWriteConfig(
        connection_params=cls._connection_params,
        collection_name=cls._collection_name,
        content_payload_key="content")
    with TestPipeline() as p:
      _ = (p | beam.Create(TEST_CORPUS) | write_config.create_write_transform())

  def _run_enrichment(self, search_strategy, queries, expected, **kwargs):
    handler = QdrantSearchEnrichmentHandler(
        self._connection_params,
        QdrantSearchParameters(
            collection_name=self._collection_name,
            search_strategy=search_strategy),
        **kwargs)
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(queries)
          | Enrichment(handler)
          | beam.Map(_retrieved))
      assert_that(result, equal_to(expected))

  def test_dense_search(self):
    self._run_enrichment(
        QdrantDenseSearchParameters(limit=1),
        [_query("a", dense=[0.9, 0.1]), _query("b", dense=[0.1, 0.9])],
        [
            ("a", [1], ["Beam runs batch and streaming pipelines."]),
            ("b", [2], ["Qdrant is a vector database."]),
        ])

  def test_dense_search_with_filter(self):
    self._run_enrichment(
        QdrantDenseSearchParameters(
            limit=3,
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="source", match=models.MatchValue(value="beam"))
                ])), [_query("a", dense=[0.1, 0.9])],
        [(
            "a", [1, 3],
            [
                "Beam runs batch and streaming pipelines.",
                "Enrichment joins external data."
            ])])

  def test_sparse_search(self):
    self._run_enrichment(
        QdrantSparseSearchParameters(limit=3),
        [_query("a", sparse=([4], [1.0]))],
        [("a", [3], ["Enrichment joins external data."])])

  def test_hybrid_search(self):
    self._run_enrichment(
        QdrantHybridSearchParameters(
            dense=QdrantDenseSearchParameters(limit=3),
            sparse=QdrantSparseSearchParameters(limit=3),
            limit=1),
        [_query("a", dense=[0.0, 1.0], sparse=([2, 3], [1.0, 1.0]))],
        [("a", [2], ["Qdrant is a vector database."])])

  def test_batched_search(self):
    queries = [_query(str(i), dense=[1.0, 0.0]) for i in range(10)]
    self._run_enrichment(
        QdrantDenseSearchParameters(limit=1),
        queries,
        [(q.id, [1], ["Beam runs batch and streaming pipelines."])
         for q in queries],
        min_batch_size=5,
        max_batch_size=5)

  def test_hybrid_search_with_filter_and_fused_score_threshold(self):
    self._run_enrichment(
        QdrantHybridSearchParameters(
            dense=QdrantDenseSearchParameters(limit=3),
            sparse=QdrantSparseSearchParameters(limit=3),
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="source", match=models.MatchValue(value="beam"))
                ]),
            score_threshold=0.7),
        [_query("a", dense=[0.1, 0.9], sparse=([4], [1.0]))],
        [("a", [3], ["Enrichment joins external data."])])

  def test_empty_input(self):
    self._run_enrichment(QdrantDenseSearchParameters(), [], [])


if __name__ == '__main__':
  unittest.main()
