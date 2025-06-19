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

import logging
import platform
import unittest
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from typing import Dict
from typing import List
from typing import Optional

import pytest
from pymilvus import CollectionSchema
from pymilvus import DataType
from pymilvus import FieldSchema
from pymilvus import Function
from pymilvus import FunctionType
from pymilvus import MilvusClient
from pymilvus.milvus_client import IndexParams
from testcontainers.core.generic import DbContainer
from testcontainers.milvus import MilvusContainer

import apache_beam as beam
from apache_beam.ml.rag.types import Chunk

try:
  from apache_beam.ml.rag.enrichment.milvus_search import (
      MilvusConnectionParameters,
      MilvusCollectionLoadParameters,
      VectorSearchMetrics,
      KeywordSearchMetrics)
except ImportError:
  raise unittest.SkipTest('Milvus dependencies not installed')

_LOGGER = logging.getLogger(__name__)


def _create_index_params():
  index_params = IndexParams()

  # Create an index on the dense embedding for vector search.
  index_params.add_index(
      field_name="dense_embedding",
      index_name="dense_embedding_ivf_flat",
      index_type="IVF_FLAT",
      metric_type=VectorSearchMetrics.COSINE.value,
      params={"nlist": 1024})

  # Create an index on the sparse embedding for keyword/text search.
  index_params.add_index(
      field_name="sparse_embedding",
      index_name="sparse_embedding_inverted_index",
      index_type="SPARSE_INVERTED_INDEX",
      metric_type=KeywordSearchMetrics.BM25.value,
      params={
          "inverted_index_algo": "DAAT_MAXSCORE",
          "bm25_k1": 1.2,
          "bm25_b": 0.75,
      })

  return index_params


@dataclass
class MilvusITDataConstruct:
  id: int
  content: str
  domain: str
  cost: int
  metadata: dict
  dense_embedding: list[float]
  vocabulary: Dict[str, int] = field(default_factory=dict)

  def __getitem__(self, key):
    return getattr(self, key)


MILVUS_IT_CONFIG = {
    "collection_name": "docs_catalog",
    "fields": [
        FieldSchema(
            name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(
            name="content",
            dtype=DataType.VARCHAR,
            max_length=512,
            enable_analyzer=True),
        FieldSchema(name="domain", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="cost", dtype=DataType.INT32),
        FieldSchema(name="metadata", dtype=DataType.JSON),
        FieldSchema(name="dense_embedding", dtype=DataType.FLOAT_VECTOR, dim=3),
        FieldSchema(
            name="sparse_embedding", dtype=DataType.SPARSE_FLOAT_VECTOR),
    ],
    "functions": [
        Function(
            name="content_bm25_emb",
            input_field_names=["content"],
            output_field_names=["sparse_embedding"],
            function_type=FunctionType.BM25)
    ],
    "index": _create_index_params(),
    "corpus": [
        MilvusITDataConstruct(
            id=1,
            content="This is a test document",
            domain="medical",
            cost=49,
            metadata={"language": "en"},
            dense_embedding=[0.1, 0.2, 0.3]),
        MilvusITDataConstruct(
            id=2,
            content="Another test document",
            domain="legal",
            cost=75,
            metadata={"language": "en"},
            dense_embedding=[0.2, 0.3, 0.4]),
        MilvusITDataConstruct(
            id=3,
            content="وثيقة اختبار",
            domain="financial",
            cost=149,
            metadata={"language": "ar"},
            dense_embedding=[0.3, 0.4, 0.5]),
    ],
    "sparse_embeddings": {
        "doc1": {
            "indices": [1, 2, 3, 4],
            "values": [0.05, 0.41, 0.05, 0.41],
        },
        "doc2": {
            "indices": [1, 3, 0],
            "values": [0.07, 0.07, 0.53],
        },
        "doc3": {
            "indices": [6, 5], "values": [0.62, 0.62]
        }
    },
    "vocabulary": {
        "this": 4,
        "is": 2,
        "test": 3,
        "document": 1,
        "another": 0,
        "وثيقة": 6,
        "اختبار": 5
    }
}


@dataclass
class MilvusDBContainerInfo:
  container: DbContainer
  host: str
  port: int
  user: Optional[str] = ""
  password: Optional[str] = ""
  token: Optional[str] = ""
  id: Optional[str] = "default"

  @property
  def uri(self) -> str:
    return f"http://{self.host}:{self.port}"


class MilvusEnrichmentTestHelper:
  @staticmethod
  def start_db_container(
      image="milvusdb/milvus:v2.5.10",
      vector_client_retries=3) -> Optional[MilvusDBContainerInfo]:
    info = None
    for i in range(vector_client_retries):
      try:
        vector_db_container = MilvusContainer(image=image, port=19530)
        vector_db_container.start()
        host = vector_db_container.get_container_host_ip()
        port = vector_db_container.get_exposed_port(19530)

        info = MilvusDBContainerInfo(vector_db_container, host, port)
        _LOGGER.info(
            "milvus db container started successfully on %s.", info.uri)
        break
      except Exception as e:
        _LOGGER.warning(
            "Retry %d/%d: Failed to start milvus db container. Reason: %s",
            i + 1,
            vector_client_retries,
            e)
        if i == vector_client_retries - 1:
          _LOGGER.error(
              "Unable to start milvus db container for I/O tests after %d "
              "retries. Tests cannot proceed.",
              vector_client_retries)
          raise e
    return info

  @staticmethod
  def stop_db_container(db_info: MilvusDBContainerInfo):
    try:
      _LOGGER.debug("Stopping milvus db container.")
      db_info.container.stop()
      _LOGGER.info("milvus db container stopped successfully.")
    except Exception as e:
      _LOGGER.warning(
          "Error encountered while stopping milvus db container: %s", e)

  @staticmethod
  def initialize_db_with_data(connc_params: MilvusConnectionParameters):
    # Open the connection to the milvus db.
    client = MilvusClient(**connc_params.__dict__)

    # Configure schema.
    fields: List[FieldSchema] = MILVUS_IT_CONFIG["fields"]
    schema = CollectionSchema(
        fields=fields, functions=MILVUS_IT_CONFIG["functions"])

    # Create collection with the schema.
    collection_name = MILVUS_IT_CONFIG["collection_name"]
    client.create_collection(
        collection_name=collection_name,
        schema=schema,
        index_params=MILVUS_IT_CONFIG["index"])

    # Assert that collection was created.
    collection_error = f"Expected collection '{collection_name}' to be created."
    assert client.has_collection(collection_name), collection_error

    # Gather all fields we have excluding 'sparse_embedding' special field. It
    # is not possible yet to insert data into it manually in Milvus db.
    field_schemas: List[FieldSchema] = MILVUS_IT_CONFIG["fields"]
    fields = []
    for field_schema in field_schemas:
      if field_schema.name != "sparse_embedding":
        fields.append(field_schema.name)
      else:
        continue

    # Prep data for indexing.
    data_ready_to_index = []
    for doc in MILVUS_IT_CONFIG["corpus"]:
      item = {field: doc[field] for field in fields}
      data_ready_to_index.append(item)

    # Index data.
    result = client.insert(
        collection_name=collection_name, data=data_ready_to_index)

    # Assert that the intended data has been properly indexed.
    insertion_err = f'failed to insert the {result["insert_count"]} data points'
    assert result["insert_count"] == len(data_ready_to_index), insertion_err

    # Release the collection from memory. It will be loaded lazily when the
    # enrichment handler is invoked.
    client.release_collection(collection_name)

    # Close the connection to the Milvus database, as no further preparation
    # operations are needed  before executing the enrichment handler.
    client.close()

    return collection_name


@pytest.mark.uses_testcontainer
@unittest.skipUnless(
    platform.system() == "Linux",
    "Test runs only on Linux due to lack of support, as yet, for nested "
    "virtualization in CI environments on Windows/macOS. Many CI providers run "
    "tests in virtualized environments, and nested virtualization "
    "(Docker inside a VM) is either unavailable or has several issues on "
    "non-Linux platforms.")
class TestMilvusSearchEnrichment(unittest.TestCase):
  """Tests for search functionality across all search strategies"""

  _db: MilvusDBContainerInfo
  _version = "milvusdb/milvus:v2.5.10"

  @classmethod
  def setUpClass(cls):
    cls._db = MilvusEnrichmentTestHelper.start_db_container(cls._version)
    cls._connection_params = MilvusConnectionParameters(
        uri=cls._db.uri,
        user=cls._db.user,
        password=cls._db.password,
        db_id=cls._db.id,
        token=cls._db.token)
    cls._collection_load_params = MilvusCollectionLoadParameters()
    cls._collection_name = MilvusEnrichmentTestHelper.initialize_db_with_data(
        cls._connection_params)

  @classmethod
  def tearDownClass(cls):
    MilvusEnrichmentTestHelper.stop_db_container(cls._db)
    cls._db = None

  def test_invalid_query(self):
    pass

  def test_empty_input_chunks(self):
    pass

  def test_filtered_search(self):
    pass

  def test_chunks_batching(self):
    pass

  def test_basic_vector_search_COSINE(self):
    pass

  def test_basic_vector_search_EUCLIDEAN_DISTANCE(self):
    pass

  def test_basic_vector_search_INNER_PRODUCT(self):
    pass

  def test_basic_keyword_search_BM25(self):
    pass

  def test_basic_hybrid_search(self):
    pass


class MilvusITSearchResultsFormatter(beam.PTransform):
  """
  A PTransform that formats Milvus integration test search results to ensure
  deterministic behavior.
  
  Since Python dictionaries do not guarantee order, this transformer sorts
  dictionary fields lexicographically by keys. This ensures:
  1. Deterministic behavior for returned search results
  2. Avoids flaky test cases when used in testing environments
  """
  def expand(self, pcoll):
    return pcoll | beam.Map(self.format)

  @staticmethod
  def format(chunk: Chunk):
    enrichment_data = chunk.metadata.get('enrichment_data', defaultdict(list))
    fields = enrichment_data['fields']
    for i, field in enumerate(fields):
      if isinstance(field, dict):
        # Sort the dictionary by creating a new ordered dictionary.
        sorted_field = {k: field[k] for k in sorted(field.keys())}
        fields[i] = sorted_field
    # Update the metadata with sorted fields.
    chunk.metadata['enrichment_data']['fields'] = fields
    return chunk


if __name__ == '__main__':
  unittest.main()
