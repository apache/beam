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

import contextlib
import tempfile
import unittest

import apache_beam as beam
from apache_beam.ml.rag.ingestion.qdrant import QdrantConnectionParameters
from apache_beam.ml.rag.ingestion.qdrant import QdrantWriteConfig
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from qdrant_client import QdrantClient, models
  QDRANT_AVAILABLE = True
except ImportError:
  QDRANT_AVAILABLE = False
# pylint: enable=ungrouped-imports

TEST_CORPUS = [
    EmbeddableItem(
        id="1",
        content=Content(text="Test document one"),
        metadata={"source": "test1"},
        embedding=Embedding(dense_embedding=[1.0, 0.0]),
    ),
    EmbeddableItem(
        id="2",
        content=Content(text="Test document two"),
        metadata={"source": "test2"},
        embedding=Embedding(dense_embedding=[0.0, 1.0]),
    ),
    EmbeddableItem(
        id="3",
        content=Content(text="Test document three"),
        metadata={"source": "test3"},
        embedding=Embedding(dense_embedding=[-1.0, 0.0]),
    ),
]


@unittest.skipIf(not QDRANT_AVAILABLE, "qdrant dependencies not installed.")
class TestQdrantIngestion(unittest.TestCase):
  @contextlib.contextmanager
  def qdrant_client(self) -> 'QdrantClient':
    client = QdrantClient(path=self._temp_dir.name)
    try:
      yield client
    finally:
      client.close()

  def setUp(self):
    self._temp_dir = tempfile.TemporaryDirectory()
    self._collection_name = f"test_collection_{self._testMethodName}"

    with self.qdrant_client() as client:
      client.create_collection(
          collection_name=self._collection_name,
          vectors_config={
              "dense": models.VectorParams(
                  size=2, distance=models.Distance.COSINE)
          },
          sparse_vectors_config={"sparse": models.SparseVectorParams()},
      )
      assert client.collection_exists(collection_name=self._collection_name)

    self._connection_params = QdrantConnectionParameters(
        path=self._temp_dir.name)

  def tearDown(self):
    self._temp_dir.cleanup()

  def test_write_on_non_existent_collection(self):
    non_existent = "nonexistent_collection"
    write_config = QdrantWriteConfig(
        connection_params=self._connection_params,
        collection_name=non_existent,
        batch_size=1,
    )

    with self.assertRaises(Exception):
      with TestPipeline() as p:
        _ = p | beam.Create(TEST_CORPUS) | write_config.create_write_transform()

  def test_write_dense_embeddings_only(self):
    write_config = QdrantWriteConfig(
        connection_params=self._connection_params,
        collection_name=self._collection_name,
        batch_size=len(TEST_CORPUS),
    )

    with TestPipeline() as p:
      _ = p | beam.Create(TEST_CORPUS) | write_config.create_write_transform()

    with self.qdrant_client() as client:
      count_result = client.count(collection_name=self._collection_name)
      self.assertEqual(count_result.count, len(TEST_CORPUS))

      points, _ = client.scroll(
        collection_name=self._collection_name,
        limit=100,
        with_payload=True,
        with_vectors=True,
      )
    points_by_id = {p.id: p for p in points}

    for item in TEST_CORPUS:
      expected_record = models.Record(
          id=int(item.id),
          vector={"dense": item.dense_embedding},
          payload=item.metadata,
      )
      self.assertEqual(expected_record, points_by_id[int(item.id)])

  def test_write_sparse_embeddings_only(self):
    sparse_corpus = [
        EmbeddableItem(
            id="1",
            content=Content(text="Sparse doc one"),
            metadata={"source": "sparse1"},
            embedding=Embedding(sparse_embedding=([0, 1, 2], [0.1, 0.2, 0.3])),
        ),
        EmbeddableItem(
            id="2",
            content=Content(text="Sparse doc two"),
            metadata={"source": "sparse2"},
            embedding=Embedding(sparse_embedding=([1, 3, 5], [0.4, 0.5, 0.6])),
        ),
    ]

    write_config = QdrantWriteConfig(
        connection_params=self._connection_params,
        collection_name=self._collection_name,
        batch_size=len(sparse_corpus),
    )

    with TestPipeline() as p:
      _ = p | beam.Create(sparse_corpus) | write_config.create_write_transform()

    with self.qdrant_client() as client:
      count_result = client.count(collection_name=self._collection_name)
      self.assertEqual(count_result.count, len(sparse_corpus))

      points, _ = client.scroll(
        collection_name=self._collection_name,
        limit=100,
        with_payload=True,
        with_vectors=True,
      )
    points_by_id = {p.id: p for p in points}

    for item in sparse_corpus:
      expected_record = models.Record(
          id=int(item.id),
          vector={
              "sparse": models.SparseVector(
                  indices=item.sparse_embedding[0],
                  values=item.sparse_embedding[1],
              )
          },
          payload=item.metadata,
      )
      self.assertEqual(expected_record, points_by_id[int(item.id)])

  def test_write_both_dense_and_sparse(self):
    hybrid_corpus = [
        EmbeddableItem(
            id="1",
            content=Content(text="Hybrid doc one"),
            metadata={"source": "hybrid1"},
            embedding=Embedding(
                dense_embedding=[1.0, 0.0],
                sparse_embedding=([0, 1], [0.1, 0.2])),
        ),
        EmbeddableItem(
            id="2",
            content=Content(text="Hybrid doc two"),
            metadata={"source": "hybrid2"},
            embedding=Embedding(
                dense_embedding=[0.0, 1.0],
                sparse_embedding=([2, 3], [0.3, 0.4])),
        ),
    ]

    write_config = QdrantWriteConfig(
        connection_params=self._connection_params,
        collection_name=self._collection_name,
        batch_size=len(hybrid_corpus),
    )

    with TestPipeline() as p:
      _ = p | beam.Create(hybrid_corpus) | write_config.create_write_transform()

    with self.qdrant_client() as client:
      count_result = client.count(collection_name=self._collection_name)
      self.assertEqual(count_result.count, len(hybrid_corpus))

      points, _ = client.scroll(
        collection_name=self._collection_name,
        limit=100,
        with_payload=True,
        with_vectors=True,
      )
    points_by_id = {p.id: p for p in points}

    for item in hybrid_corpus:
      expected_record = models.Record(
          id=int(item.id),
          vector={
              "dense": item.dense_embedding,
              "sparse": models.SparseVector(
                  indices=item.sparse_embedding[0],
                  values=item.sparse_embedding[1]),
          },
          payload=item.metadata,
      )
      self.assertEqual(expected_record, points_by_id[int(item.id)])

  def test_write_with_batching(self):
    batch_corpus = [
        EmbeddableItem(
            id=str(i),
            content=Content(text=f"Batch doc {i}"),
            metadata={"batch_id": i},
            embedding=Embedding(dense_embedding=[1.0, 0.0]),
        ) for i in range(1, 8)
    ]

    write_config = QdrantWriteConfig(
        connection_params=self._connection_params,
        collection_name=self._collection_name,
        batch_size=3,
    )

    with TestPipeline() as p:
      _ = p | beam.Create(batch_corpus) | write_config.create_write_transform()

    with self.qdrant_client() as client:
      count_result = client.count(collection_name=self._collection_name)
      self.assertEqual(count_result.count, len(batch_corpus))

      points, _ = client.scroll(
        collection_name=self._collection_name,
        limit=100,
        with_payload=True,
        with_vectors=True,
      )
    points_by_id = {p.id: p for p in points}

    for item in batch_corpus:
      expected_record = models.Record(
          id=int(item.id),
          vector={
              "dense": item.dense_embedding,
          },
          payload=item.metadata,
      )
      self.assertEqual(expected_record, points_by_id[int(item.id)])


if __name__ == "__main__":
  unittest.main()
