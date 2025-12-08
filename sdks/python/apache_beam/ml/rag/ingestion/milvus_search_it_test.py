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

import platform
import unittest
import uuid
from typing import Callable
from typing import cast

import pytest
from pymilvus import CollectionSchema
from pymilvus import DataType
from pymilvus import FieldSchema
from pymilvus import MilvusClient
from pymilvus.exceptions import MilvusException
from pymilvus.milvus_client import IndexParams

import apache_beam as beam
from apache_beam.ml.rag.ingestion.jdbc_common import WriteConfig
from apache_beam.ml.rag.test_utils import MilvusTestHelpers
from apache_beam.ml.rag.test_utils import VectorDBContainerInfo
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.ml.rag.utils import MilvusConnectionParameters
from apache_beam.ml.rag.utils import retry_with_backoff
from apache_beam.ml.rag.utils import unpack_dataclass_with_kwargs
from apache_beam.testing.test_pipeline import TestPipeline

try:
  from apache_beam.ml.rag.ingestion.milvus_search import MilvusVectorWriterConfig
  from apache_beam.ml.rag.ingestion.milvus_search import MilvusWriteConfig
except ImportError as e:
  raise unittest.SkipTest(f'Milvus dependencies not installed: {str(e)}')


def _construct_index_params():
  index_params = IndexParams()

  # Dense vector index for dense embeddings.
  index_params.add_index(
      field_name="embedding",
      index_name="embedding_ivf_flat",
      index_type="IVF_FLAT",
      metric_type="COSINE",
      params={"nlist": 1})

  # Sparse vector index for sparse embeddings.
  index_params.add_index(
      field_name="sparse_embedding",
      index_name="sparse_embedding_inverted_index",
      index_type="SPARSE_INVERTED_INDEX",
      metric_type="IP",
      params={"inverted_index_algo": "TAAT_NAIVE"})

  return index_params


MILVUS_INGESTION_IT_CONFIG = {
    "fields": [
        FieldSchema(
            name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=1000),
        FieldSchema(name="metadata", dtype=DataType.JSON),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=3),
        FieldSchema(
            name="sparse_embedding", dtype=DataType.SPARSE_FLOAT_VECTOR)
    ],
    "index": _construct_index_params,
    "corpus": [
        Chunk(
            id=1,  # type: ignore[arg-type]
            content=Content(text="Test document one"),
            metadata={"source": "test1"},
            embedding=Embedding(
                dense_embedding=[0.1, 0.2, 0.3],
                sparse_embedding=([1, 2], [0.1, 0.2])),
        ),
        Chunk(
            id=2,  # type: ignore[arg-type]
            content=Content(text="Test document two"),
            metadata={"source": "test2"},
            embedding=Embedding(
                dense_embedding=[0.2, 0.3, 0.4],
                sparse_embedding=([2, 3], [0.3, 0.1]),
            ),
        ),
        Chunk(
            id=3,  # type: ignore[arg-type]
            content=Content(text="Test document three"),
            metadata={"source": "test3"},
            embedding=Embedding(
                dense_embedding=[0.3, 0.4, 0.5],
                sparse_embedding=([3, 4], [0.4, 0.2]),
            ),
        )
    ]
}


def create_collection_with_partition(
    client: MilvusClient,
    collection_name: str,
    partition_name: str = '',
    fields=None):

  if fields is None:
    fields = MILVUS_INGESTION_IT_CONFIG["fields"]

  # Configure schema.
  schema = CollectionSchema(fields=fields)

  # Configure index.
  index_function: Callable[[], IndexParams] = cast(
      Callable[[], IndexParams], MILVUS_INGESTION_IT_CONFIG["index"])

  # Create collection with schema.
  client.create_collection(
      collection_name=collection_name,
      schema=schema,
      index_params=index_function())

  # Create partition within the collection.
  client.create_partition(
      collection_name=collection_name, partition_name=partition_name)

  msg = f"Expected collection '{collection_name}' to be created."
  assert client.has_collection(collection_name), msg

  msg = f"Expected partition '{partition_name}' to be created."
  assert client.has_partition(collection_name, partition_name), msg

  # Release the collection from memory. We don't need that on pure writing.
  client.release_collection(collection_name)


def drop_collection(client: MilvusClient, collection_name: str):
  try:
    client.drop_collection(collection_name)
    assert not client.has_collection(collection_name)
  except Exception:
    # Silently ignore connection errors during cleanup.
    pass


@pytest.mark.require_docker_in_docker
@unittest.skipUnless(
    platform.system() == "Linux",
    "Test runs only on Linux due to lack of support, as yet, for nested "
    "virtualization in CI environments on Windows/macOS. Many CI providers run "
    "tests in virtualized environments, and nested virtualization "
    "(Docker inside a VM) is either unavailable or has several issues on "
    "non-Linux platforms.")
class TestMilvusVectorWriterConfig(unittest.TestCase):
  """Integration tests for Milvus vector database ingestion functionality"""

  _db: VectorDBContainerInfo

  @classmethod
  def setUpClass(cls):
    cls._db = MilvusTestHelpers.start_db_container()
    cls._connection_config = MilvusConnectionParameters(
        uri=cls._db.uri,
        user=cls._db.user,
        password=cls._db.password,
        db_name=cls._db.id,
        token=cls._db.token)

  @classmethod
  def tearDownClass(cls):
    MilvusTestHelpers.stop_db_container(cls._db)
    cls._db = None

  def setUp(self):
    self.write_test_pipeline = TestPipeline()
    self.write_test_pipeline.not_use_test_runner_api = True
    self._collection_name = f"test_collection_{self._testMethodName}"
    self._partition_name = f"test_partition_{self._testMethodName}"
    config = unpack_dataclass_with_kwargs(self._connection_config)
    config["alias"] = f"milvus_conn_{uuid.uuid4().hex[:8]}"

    # Use retry_with_backoff for test client connection.
    def create_client():
      return MilvusClient(**config)

    self._test_client = retry_with_backoff(
        create_client,
        max_retries=3,
        retry_delay=1.0,
        operation_name="Test Milvus client connection",
        exception_types=(MilvusException, ))

    create_collection_with_partition(
        self._test_client, self._collection_name, self._partition_name)

  def tearDown(self):
    drop_collection(self._test_client, self._collection_name)
    self._test_client.close()

  def test_invalid_write_on_non_existent_collection(self):
    non_existent_collection = "nonexistent_collection"

    test_chunks = MILVUS_INGESTION_IT_CONFIG["corpus"]

    write_config = MilvusWriteConfig(
        collection_name=non_existent_collection,
        write_config=WriteConfig(write_batch_size=1))
    config = MilvusVectorWriterConfig(
        connection_params=self._connection_config,
        write_config=write_config,
    )

    # Write pipeline.
    with self.assertRaises(Exception) as context:
      with TestPipeline() as p:
        _ = (p | beam.Create(test_chunks) | config.create_write_transform())

    # Assert on what should happen.
    self.assertIn("can't find collection", str(context.exception).lower())

  def test_invalid_write_on_non_existent_partition(self):
    non_existent_partition = "nonexistent_partition"

    test_chunks = MILVUS_INGESTION_IT_CONFIG["corpus"]

    write_config = MilvusWriteConfig(
        collection_name=self._collection_name,
        partition_name=non_existent_partition,
        write_config=WriteConfig(write_batch_size=1))
    config = MilvusVectorWriterConfig(
        connection_params=self._connection_config, write_config=write_config)

    # Write pipeline.
    with self.assertRaises(Exception) as context:
      with TestPipeline() as p:
        _ = (p | beam.Create(test_chunks) | config.create_write_transform())

    # Assert on what should happen.
    self.assertIn("partition not found", str(context.exception).lower())

  def test_invalid_write_on_missing_primary_key_in_entity(self):
    test_chunks = [
        Chunk(
            content=Content(text="Test content without ID"),
            embedding=Embedding(
                dense_embedding=[0.1, 0.2, 0.3],
                sparse_embedding=([1, 2], [0.1, 0.2])),
            metadata={"source": "test"})
    ]

    write_config = MilvusWriteConfig(
        collection_name=self._collection_name,
        partition_name=self._partition_name,
        write_config=WriteConfig(write_batch_size=1))

    # Deliberately remove id primary key from the entity.
    specs = MilvusVectorWriterConfig.default_column_specs()
    for i, spec in enumerate(specs):
      if spec.column_name == "id":
        del specs[i]
        break

    config = MilvusVectorWriterConfig(
        connection_params=self._connection_config,
        write_config=write_config,
        column_specs=specs)

    # Write pipeline.
    with self.assertRaises(Exception) as context:
      with TestPipeline() as p:
        _ = (p | beam.Create(test_chunks) | config.create_write_transform())

    # Assert on what should happen.
    self.assertIn(
        "insert missed an field `id` to collection",
        str(context.exception).lower())

  def test_write_on_auto_id_primary_key(self):
    auto_id_collection = f"auto_id_collection_{self._testMethodName}"
    auto_id_partition = f"auto_id_partition_{self._testMethodName}"
    auto_id_fields = [
        FieldSchema(
            name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=1000),
        FieldSchema(name="metadata", dtype=DataType.JSON),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=3),
        FieldSchema(
            name="sparse_embedding", dtype=DataType.SPARSE_FLOAT_VECTOR)
    ]

    # Create collection with an auto id field.
    create_collection_with_partition(
        client=self._test_client,
        collection_name=auto_id_collection,
        partition_name=auto_id_partition,
        fields=auto_id_fields)

    test_chunks = [
        Chunk(
            id=1,
            content=Content(text="Test content without ID"),
            embedding=Embedding(
                dense_embedding=[0.1, 0.2, 0.3],
                sparse_embedding=([1, 2], [0.1, 0.2])),
            metadata={"source": "test"})
    ]

    write_config = MilvusWriteConfig(
        collection_name=auto_id_collection,
        partition_name=auto_id_partition,
        write_config=WriteConfig(write_batch_size=1))

    config = MilvusVectorWriterConfig(
        connection_params=self._connection_config, write_config=write_config)

    with self.write_test_pipeline as p:
      _ = (p | beam.Create(test_chunks) | config.create_write_transform())

    self._test_client.flush(auto_id_collection)
    self._test_client.load_collection(auto_id_collection)
    result = self._test_client.query(
        collection_name=auto_id_collection,
        partition_names=[auto_id_partition],
        limit=3)

    # Test there is only one item in the result and the ID is not equal to one.
    self.assertEqual(len(result), len(test_chunks))
    result_item = dict(result[0])
    self.assertNotEqual(result_item["id"], 1)

  def test_write_on_existent_collection_with_default_schema(self):
    test_chunks = MILVUS_INGESTION_IT_CONFIG["corpus"]

    write_config = MilvusWriteConfig(
        collection_name=self._collection_name,
        partition_name=self._partition_name,
        write_config=WriteConfig(write_batch_size=3))
    config = MilvusVectorWriterConfig(
        connection_params=self._connection_config, write_config=write_config)

    with self.write_test_pipeline as p:
      _ = (p | beam.Create(test_chunks) | config.create_write_transform())

    # Verify data was written successfully.
    self._test_client.flush(self._collection_name)
    self._test_client.load_collection(self._collection_name)
    result = self._test_client.query(
        collection_name=self._collection_name,
        partition_names=[self._partition_name],
        limit=10)

    self.assertEqual(len(result), len(test_chunks))

    # Verify each chunk was written correctly.
    result_by_id = {item["id"]: item for item in result}
    for chunk in test_chunks:
      self.assertIn(chunk.id, result_by_id)
      result_item = result_by_id[chunk.id]
      self.assertEqual(result_item["content"], chunk.content.text)
      self.assertEqual(result_item["metadata"], chunk.metadata)

      # Verify embedding is present and has correct length.
      expected_embedding = chunk.embedding.dense_embedding
      actual_embedding = result_item["embedding"]
      self.assertIsNotNone(actual_embedding)
      self.assertEqual(len(actual_embedding), len(expected_embedding))

  def test_write_with_custom_column_specifications(self):
    from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpec
    from apache_beam.ml.rag.utils import MilvusHelpers

    custom_column_specs = [
        ColumnSpec("id", int, lambda chunk: int(chunk.id) if chunk.id else 0),
        ColumnSpec("content", str, lambda chunk: chunk.content.text),
        ColumnSpec("metadata", dict, lambda chunk: chunk.metadata or {}),
        ColumnSpec(
            "embedding",
            list, lambda chunk: chunk.embedding.dense_embedding or []),
        ColumnSpec(
            "sparse_embedding",
            dict, lambda chunk: (
                MilvusHelpers.sparse_embedding(
                    chunk.embedding.sparse_embedding) if chunk.embedding and
                chunk.embedding.sparse_embedding else {}))
    ]

    test_chunks = [
        Chunk(
            id=10,
            content=Content(text="Custom column spec test"),
            embedding=Embedding(
                dense_embedding=[0.8, 0.9, 1.0],
                sparse_embedding=([1, 3, 5], [0.8, 0.9, 1.0])),
            metadata={"custom": "spec_test"})
    ]

    write_config = MilvusWriteConfig(
        collection_name=self._collection_name,
        partition_name=self._partition_name,
        write_config=WriteConfig(write_batch_size=1))
    config = MilvusVectorWriterConfig(
        connection_params=self._connection_config,
        write_config=write_config,
        column_specs=custom_column_specs)

    with self.write_test_pipeline as p:
      _ = (p | beam.Create(test_chunks) | config.create_write_transform())

    # Verify data was written successfully.
    self._test_client.flush(self._collection_name)
    self._test_client.load_collection(self._collection_name)
    result = self._test_client.query(
        collection_name=self._collection_name,
        partition_names=[self._partition_name],
        filter="id == 10",
        limit=1)

    self.assertEqual(len(result), 1)
    result_item = result[0]

    # Verify custom column specs worked correctly.
    self.assertEqual(result_item["id"], 10)
    self.assertEqual(result_item["content"], "Custom column spec test")
    self.assertEqual(result_item["metadata"], {"custom": "spec_test"})

    # Verify embedding is present and has correct length.
    expected_embedding = [0.8, 0.9, 1.0]
    actual_embedding = result_item["embedding"]
    self.assertIsNotNone(actual_embedding)
    self.assertEqual(len(actual_embedding), len(expected_embedding))

    # Verify sparse embedding was converted correctly - check keys are present.
    expected_sparse_keys = {1, 3, 5}
    actual_sparse = result_item["sparse_embedding"]
    self.assertIsNotNone(actual_sparse)
    self.assertEqual(set(actual_sparse.keys()), expected_sparse_keys)

  def test_write_with_batching(self):
    test_chunks = [
        Chunk(
            id=i,
            content=Content(text=f"Batch test document {i}"),
            embedding=Embedding(
                dense_embedding=[0.1 * i, 0.2 * i, 0.3 * i],
                sparse_embedding=([i, i + 1], [0.1 * i, 0.2 * i])),
            metadata={"batch_id": i}) for i in range(1, 8)  # 7 chunks
    ]

    # Set small batch size to force batching (7 chunks with batch size 3).
    batch_write_config = WriteConfig(write_batch_size=3)
    write_config = MilvusWriteConfig(
        collection_name=self._collection_name,
        partition_name=self._partition_name,
        write_config=batch_write_config)
    config = MilvusVectorWriterConfig(
        connection_params=self._connection_config, write_config=write_config)

    with self.write_test_pipeline as p:
      _ = (p | beam.Create(test_chunks) | config.create_write_transform())

    # Verify all data was written successfully.
    # Flush to persist all data to disk, then load collection for querying.
    self._test_client.flush(self._collection_name)
    self._test_client.load_collection(self._collection_name)

    result = self._test_client.query(
        collection_name=self._collection_name,
        partition_names=[self._partition_name],
        limit=10)

    self.assertEqual(len(result), len(test_chunks))

    # Verify each batch was written correctly.
    result_by_id = {item["id"]: item for item in result}
    for chunk in test_chunks:
      self.assertIn(chunk.id, result_by_id)
      result_item = result_by_id[chunk.id]

      # Verify content and metadata.
      self.assertEqual(result_item["content"], chunk.content.text)
      self.assertEqual(result_item["metadata"], chunk.metadata)

      # Verify embeddings are present and have correct length.
      expected_embedding = chunk.embedding.dense_embedding
      actual_embedding = result_item["embedding"]
      self.assertIsNotNone(actual_embedding)
      self.assertEqual(len(actual_embedding), len(expected_embedding))

      # Verify sparse embedding keys are present.
      expected_sparse_keys = {chunk.id, chunk.id + 1}
      actual_sparse = result_item["sparse_embedding"]
      self.assertIsNotNone(actual_sparse)
      self.assertEqual(set(actual_sparse.keys()), expected_sparse_keys)

  def test_idempotent_write(self):
    # Step 1: Insert initial data that doesn't exist.
    initial_chunks = [
        Chunk(
            id=100,
            content=Content(text="Initial document"),
            embedding=Embedding(
                dense_embedding=[1.0, 2.0, 3.0],
                sparse_embedding=([100, 101], [1.0, 2.0])),
            metadata={"version": 1}),
        Chunk(
            id=200,
            content=Content(text="Another initial document"),
            embedding=Embedding(
                dense_embedding=[2.0, 3.0, 4.0],
                sparse_embedding=([200, 201], [2.0, 3.0])),
            metadata={"version": 1})
    ]

    write_config = MilvusWriteConfig(
        collection_name=self._collection_name,
        partition_name=self._partition_name,
        write_config=WriteConfig(write_batch_size=2))
    config = MilvusVectorWriterConfig(
        connection_params=self._connection_config, write_config=write_config)

    # Insert initial data.
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p | "Create initial" >> beam.Create(initial_chunks)
          | "Write initial" >> config.create_write_transform())

    # Verify initial data was inserted (not existed before).
    self._test_client.flush(self._collection_name)
    self._test_client.load_collection(self._collection_name)
    result = self._test_client.query(
        collection_name=self._collection_name,
        partition_names=[self._partition_name],
        limit=10)

    self.assertEqual(len(result), 2)
    result_by_id = {item["id"]: item for item in result}

    # Verify initial state.
    self.assertEqual(result_by_id[100]["content"], "Initial document")
    self.assertEqual(result_by_id[100]["metadata"], {"version": 1})
    self.assertEqual(result_by_id[200]["content"], "Another initial document")
    self.assertEqual(result_by_id[200]["metadata"], {"version": 1})

    # Step 2: Update existing data (same IDs, different content).
    updated_chunks = [
        Chunk(
            id=100,
            content=Content(text="Updated document"),
            embedding=Embedding(
                dense_embedding=[1.1, 2.1, 3.1],
                sparse_embedding=([100, 102], [1.1, 2.1])),
            metadata={"version": 2}),
        Chunk(
            id=200,
            content=Content(text="Another updated document"),
            embedding=Embedding(
                dense_embedding=[2.1, 3.1, 4.1],
                sparse_embedding=([200, 202], [2.1, 3.1])),
            metadata={"version": 2})
    ]

    # Perform first update.
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p | "Create update1" >> beam.Create(updated_chunks)
          | "Write update1" >> config.create_write_transform())

    # Verify update worked.
    self._test_client.flush(self._collection_name)
    self._test_client.load_collection(self._collection_name)
    result = self._test_client.query(
        collection_name=self._collection_name,
        partition_names=[self._partition_name],
        limit=10)

    self.assertEqual(len(result), 2)  # Still only 2 records.
    result_by_id = {item["id"]: item for item in result}

    # Verify updated state.
    self.assertEqual(result_by_id[100]["content"], "Updated document")
    self.assertEqual(result_by_id[100]["metadata"], {"version": 2})
    self.assertEqual(result_by_id[200]["content"], "Another updated document")
    self.assertEqual(result_by_id[200]["metadata"], {"version": 2})

    # Step 3: Repeat the same update operation 3 more times (idempotence test).
    for i in range(3):
      with TestPipeline() as p:
        p.not_use_test_runner_api = True
        _ = (
            p | f"Create repeat{i+2}" >> beam.Create(updated_chunks)
            | f"Write repeat{i+2}" >> config.create_write_transform())

      # Verify state hasn't changed after repeated updates.
      self._test_client.flush(self._collection_name)
      self._test_client.load_collection(self._collection_name)
      result = self._test_client.query(
          collection_name=self._collection_name,
          partition_names=[self._partition_name],
          limit=10)

      # Still only 2 records.
      self.assertEqual(len(result), 2)
      result_by_id = {item["id"]: item for item in result}

      # Final state should remain unchanged.
      self.assertEqual(result_by_id[100]["content"], "Updated document")
      self.assertEqual(result_by_id[100]["metadata"], {"version": 2})
      self.assertEqual(result_by_id[200]["content"], "Another updated document")
      self.assertEqual(result_by_id[200]["metadata"], {"version": 2})

      # Verify embeddings are still correct.
      self.assertIsNotNone(result_by_id[100]["embedding"])
      self.assertEqual(len(result_by_id[100]["embedding"]), 3)
      self.assertIsNotNone(result_by_id[200]["embedding"])
      self.assertEqual(len(result_by_id[200]["embedding"]), 3)


if __name__ == '__main__':
  unittest.main()
