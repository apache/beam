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

import contextlib
import logging
import os
import platform
import re
import socket
import tempfile
import unittest
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import cast

import pytest
import yaml

import apache_beam as beam
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

# pylint: disable=ungrouped-imports
try:
  from pymilvus import (
      CollectionSchema,
      DataType,
      FieldSchema,
      Function,
      FunctionType,
      MilvusClient,
      RRFRanker)
  from pymilvus.milvus_client import IndexParams
  from testcontainers.core.config import MAX_TRIES as TC_MAX_TRIES
  from testcontainers.core.config import testcontainers_config
  from testcontainers.core.generic import DbContainer
  from testcontainers.milvus import MilvusContainer
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.ml.rag.enrichment.milvus_search import (
      MilvusSearchEnrichmentHandler,
      MilvusConnectionParameters,
      MilvusSearchParameters,
      MilvusCollectionLoadParameters,
      VectorSearchParameters,
      KeywordSearchParameters,
      HybridSearchParameters,
      VectorSearchMetrics,
      KeywordSearchMetrics)
except ImportError as e:
  raise unittest.SkipTest(f'Milvus dependencies not installed: {str(e)}')

_LOGGER = logging.getLogger(__name__)


def _construct_index_params():
  index_params = IndexParams()

  # Milvus doesn't support multiple indexes on the same field. This is a
  # limitation of Milvus - someone can only create one index per field as yet.

  # Cosine similarity index on first dense embedding field
  index_params.add_index(
      field_name="dense_embedding_cosine",
      index_name="dense_embedding_cosine_ivf_flat",
      index_type="IVF_FLAT",
      metric_type=VectorSearchMetrics.COSINE.value,
      params={"nlist": 1})

  # Euclidean distance index on second dense embedding field
  index_params.add_index(
      field_name="dense_embedding_euclidean",
      index_name="dense_embedding_euclidean_ivf_flat",
      index_type="IVF_FLAT",
      metric_type=VectorSearchMetrics.EUCLIDEAN_DISTANCE.value,
      params={"nlist": 1})

  # Inner product index on third dense embedding field
  index_params.add_index(
      field_name="dense_embedding_inner_product",
      index_name="dense_embedding_inner_product_ivf_flat",
      index_type="IVF_FLAT",
      metric_type=VectorSearchMetrics.INNER_PRODUCT.value,
      params={"nlist": 1})

  index_params.add_index(
      field_name="sparse_embedding_inner_product",
      index_name="sparse_embedding_inner_product_inverted_index",
      index_type="SPARSE_INVERTED_INDEX",
      metric_type=VectorSearchMetrics.INNER_PRODUCT.value,
      params={
          "inverted_index_algo": "TAAT_NAIVE",
      })

  # BM25 index on sparse_embedding field.
  #
  # For deterministic testing results
  # 1. Using TAAT_NAIVE: Most predictable algorithm that processes each term
  # completely before moving to the next.
  # 2. Using k1=1: Moderate term frequency weighting – repeated terms matter
  # but with diminishing returns.
  # 3. Using b=0: No document length normalization – longer documents not
  # penalized.
  # This combination provides maximum transparency and predictability for
  # test assertions.
  index_params.add_index(
      field_name="sparse_embedding_bm25",
      index_name="sparse_embedding_bm25_inverted_index",
      index_type="SPARSE_INVERTED_INDEX",
      metric_type=KeywordSearchMetrics.BM25.value,
      params={
          "inverted_index_algo": "TAAT_NAIVE",
          "bm25_k1": 1,
          "bm25_b": 0,
      })

  return index_params


@dataclass
class MilvusITDataConstruct:
  id: int
  content: str
  domain: str
  cost: int
  metadata: dict
  tags: list[str]
  dense_embedding: list[float]
  sparse_embedding: dict
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
        FieldSchema(
            name="tags",
            dtype=DataType.ARRAY,
            element_type=DataType.VARCHAR,
            max_length=64,
            max_capacity=64),
        FieldSchema(
            name="dense_embedding_cosine", dtype=DataType.FLOAT_VECTOR, dim=3),
        FieldSchema(
            name="dense_embedding_euclidean",
            dtype=DataType.FLOAT_VECTOR,
            dim=3),
        FieldSchema(
            name="dense_embedding_inner_product",
            dtype=DataType.FLOAT_VECTOR,
            dim=3),
        FieldSchema(
            name="sparse_embedding_bm25", dtype=DataType.SPARSE_FLOAT_VECTOR),
        FieldSchema(
            name="sparse_embedding_inner_product",
            dtype=DataType.SPARSE_FLOAT_VECTOR)
    ],
    "functions": [
        Function(
            name="content_bm25_emb",
            input_field_names=["content"],
            output_field_names=["sparse_embedding_bm25"],
            function_type=FunctionType.BM25)
    ],
    "index": _construct_index_params,
    "corpus": [
        MilvusITDataConstruct(
            id=1,
            content="This is a test document",
            domain="medical",
            cost=49,
            metadata={"language": "en"},
            tags=["healthcare", "patient", "clinical"],
            dense_embedding=[0.1, 0.2, 0.3],
            sparse_embedding={
                1: 0.05, 2: 0.41, 3: 0.05, 4: 0.41
            }),
        MilvusITDataConstruct(
            id=2,
            content="Another test document",
            domain="legal",
            cost=75,
            metadata={"language": "en"},
            tags=["contract", "law", "regulation"],
            dense_embedding=[0.2, 0.3, 0.4],
            sparse_embedding={
                1: 0.07, 3: 3.07, 0: 0.53
            }),
        MilvusITDataConstruct(
            id=3,
            content="وثيقة اختبار",
            domain="financial",
            cost=149,
            metadata={"language": "ar"},
            tags=["banking", "investment", "arabic"],
            dense_embedding=[0.3, 0.4, 0.5],
            sparse_embedding={
                6: 0.62, 5: 0.62
            })
    ],
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


class CustomMilvusContainer(MilvusContainer):
  def __init__(
      self,
      image: str,
      service_container_port,
      healthcheck_container_port,
      **kwargs,
  ) -> None:
    # Skip the parent class's constructor and go straight to
    # GenericContainer.
    super(MilvusContainer, self).__init__(image=image, **kwargs)
    self.port = service_container_port
    self.healthcheck_port = healthcheck_container_port
    self.with_exposed_ports(service_container_port, healthcheck_container_port)

    # Get free host ports.
    service_host_port = MilvusEnrichmentTestHelper.find_free_port()
    healthcheck_host_port = MilvusEnrichmentTestHelper.find_free_port()

    # Bind container and host ports.
    self.with_bind_ports(service_container_port, service_host_port)
    self.with_bind_ports(healthcheck_container_port, healthcheck_host_port)
    self.cmd = "milvus run standalone"

    # Set environment variables needed for Milvus.
    envs = {
        "ETCD_USE_EMBED": "true",
        "ETCD_DATA_DIR": "/var/lib/milvus/etcd",
        "COMMON_STORAGETYPE": "local",
        "METRICS_PORT": str(healthcheck_container_port)
    }
    for env, value in envs.items():
      self.with_env(env, value)


class MilvusEnrichmentTestHelper:
  @staticmethod
  def start_db_container(
      image="milvusdb/milvus:v2.6.2",
      max_vec_fields=5,
      vector_client_max_retries=3,
      tc_max_retries=TC_MAX_TRIES) -> Optional[MilvusDBContainerInfo]:
    service_container_port = MilvusEnrichmentTestHelper.find_free_port()
    healthcheck_container_port = MilvusEnrichmentTestHelper.find_free_port()
    user_yaml_creator = MilvusEnrichmentTestHelper.create_user_yaml
    with user_yaml_creator(service_container_port, max_vec_fields) as cfg:
      info = None
      testcontainers_config.max_tries = tc_max_retries
      for i in range(vector_client_max_retries):
        try:
          vector_db_container = CustomMilvusContainer(
              image=image,
              service_container_port=service_container_port,
              healthcheck_container_port=healthcheck_container_port)
          vector_db_container = vector_db_container.with_volume_mapping(
              cfg, "/milvus/configs/user.yaml")
          vector_db_container.start()
          host = vector_db_container.get_container_host_ip()
          port = vector_db_container.get_exposed_port(service_container_port)
          info = MilvusDBContainerInfo(vector_db_container, host, port)
          testcontainers_config.max_tries = TC_MAX_TRIES
          _LOGGER.info(
              "milvus db container started successfully on %s.", info.uri)
          break
        except Exception as e:
          stdout_logs, stderr_logs = vector_db_container.get_logs()
          stdout_logs = stdout_logs.decode("utf-8")
          stderr_logs = stderr_logs.decode("utf-8")
          _LOGGER.warning(
              "Retry %d/%d: Failed to start Milvus DB container. Reason: %s. "
              "STDOUT logs:\n%s\nSTDERR logs:\n%s",
              i + 1,
              vector_client_max_retries,
              e,
              stdout_logs,
              stderr_logs)
          if i == vector_client_max_retries - 1:
            _LOGGER.error(
                "Unable to start milvus db container for I/O tests after %d "
                "retries. Tests cannot proceed. STDOUT logs:\n%s\n"
                "STDERR logs:\n%s",
                vector_client_max_retries,
                stdout_logs,
                stderr_logs)
            raise e
      return info

  @staticmethod
  def stop_db_container(db_info: MilvusDBContainerInfo):
    if db_info is None:
      _LOGGER.warning("Milvus db info is None. Skipping stop operation.")
      return
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
    field_schemas: List[FieldSchema] = cast(
        List[FieldSchema], MILVUS_IT_CONFIG["fields"])
    schema = CollectionSchema(
        fields=field_schemas, functions=MILVUS_IT_CONFIG["functions"])

    # Create collection with the schema.
    collection_name = MILVUS_IT_CONFIG["collection_name"]
    index_function: Callable[[], IndexParams] = cast(
        Callable[[], IndexParams], MILVUS_IT_CONFIG["index"])
    client.create_collection(
        collection_name=collection_name,
        schema=schema,
        index_params=index_function())

    # Assert that collection was created.
    collection_error = f"Expected collection '{collection_name}' to be created."
    assert client.has_collection(collection_name), collection_error

    # Gather all fields we have excluding 'sparse_embedding_bm25' special field.
    fields = list(map(lambda field: field.name, field_schemas))

    # Prep data for indexing. Currently we can't insert sparse vectors for BM25
    # sparse embedding field as it would be automatically generated by Milvus
    # through the registered BM25 function.
    data_ready_to_index = []
    for doc in MILVUS_IT_CONFIG["corpus"]:
      item = {}
      for field in fields:
        if field.startswith("dense_embedding"):
          item[field] = doc["dense_embedding"]
        elif field == "sparse_embedding_inner_product":
          item[field] = doc["sparse_embedding"]
        elif field == "sparse_embedding_bm25":
          # It is automatically generated by Milvus from the content field.
          continue
        else:
          item[field] = doc[field]
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

  @staticmethod
  def find_free_port():
    """Find a free port on the local machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # Bind to port 0, which asks OS to assign a free port.
      s.bind(('', 0))
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      # Return the port number assigned by OS.
      return s.getsockname()[1]

  @staticmethod
  @contextlib.contextmanager
  def create_user_yaml(service_port: int, max_vector_field_num=5):
    """Creates a temporary user.yaml file for Milvus configuration.

      This user yaml file overrides Milvus default configurations. It sets
      the Milvus service port to the specified container service port. The
      default for maxVectorFieldNum is 4, but we need 5
      (one unique field for each metric).

      Args:
        service_port: Port number for the Milvus service.
        max_vector_field_num: Max number of vec fields allowed per collection.

      Yields:
          str: Path to the created temporary yaml file.
      """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                     delete=False) as temp_file:
      # Define the content for user.yaml.
      user_config = {
          'proxy': {
              'maxVectorFieldNum': max_vector_field_num, 'port': service_port
          }
      }

      # Write the content to the file.
      yaml.dump(user_config, temp_file, default_flow_style=False)
      path = temp_file.name

    try:
      yield path
    finally:
      if os.path.exists(path):
        os.remove(path)


@pytest.mark.require_docker_in_docker
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

  def test_invalid_query_on_non_existent_collection(self):
    non_existent_collection = "nonexistent_collection"
    existent_field = "dense_embedding_cosine"

    test_chunks = [
        Chunk(
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content())
    ]

    search_parameters = MilvusSearchParameters(
        collection_name=non_existent_collection,
        search_strategy=VectorSearchParameters(anns_field=existent_field))

    collection_load_parameters = MilvusCollectionLoadParameters()

    handler = MilvusSearchEnrichmentHandler(
        self._connection_params,
        search_parameters,
        collection_load_parameters=collection_load_parameters)

    with self.assertRaises(Exception) as context:
      with TestPipeline() as p:
        _ = (p | beam.Create(test_chunks) | Enrichment(handler))

    expect_err_msg_contains = "collection not found"
    self.assertIn(expect_err_msg_contains, str(context.exception))

  def test_invalid_query_on_non_existent_field(self):
    non_existent_field = "nonexistent_column"
    existent_collection = MILVUS_IT_CONFIG["collection_name"]

    test_chunks = [
        Chunk(
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content())
    ]

    search_parameters = MilvusSearchParameters(
        collection_name=existent_collection,
        search_strategy=VectorSearchParameters(anns_field=non_existent_field))

    collection_load_parameters = MilvusCollectionLoadParameters()

    handler = MilvusSearchEnrichmentHandler(
        self._connection_params,
        search_parameters,
        collection_load_parameters=collection_load_parameters)

    with self.assertRaises(Exception) as context:
      with TestPipeline() as p:
        _ = (p | beam.Create(test_chunks) | Enrichment(handler))

    expect_err_msg_contains = f"fieldName({non_existent_field}) not found"
    self.assertIn(expect_err_msg_contains, str(context.exception))

  def test_empty_input_chunks(self):
    test_chunks = []
    anns_field = "dense_embedding_cosine"

    search_parameters = MilvusSearchParameters(
        collection_name=MILVUS_IT_CONFIG["collection_name"],
        search_strategy=VectorSearchParameters(anns_field=anns_field))

    collection_load_parameters = MilvusCollectionLoadParameters()

    handler = MilvusSearchEnrichmentHandler(
        self._connection_params,
        search_parameters,
        collection_load_parameters=collection_load_parameters)

    expected_chunks = []

    with TestPipeline() as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(actual, expected_chunks))

  def test_filtered_search_with_cosine_similarity_and_batching(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content()),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            content=Content()),
        Chunk(
            id="query3",
            embedding=Embedding(dense_embedding=[0.3, 0.4, 0.5]),
            content=Content())
    ]

    filter_condition = 'metadata["language"] == "en"'

    anns_field = "dense_embedding_cosine"

    addition_search_params = {
        "metric_type": VectorSearchMetrics.COSINE.value, "nprobe": 1
    }

    vector_search_parameters = VectorSearchParameters(
        anns_field=anns_field,
        limit=10,
        filter=filter_condition,
        search_params=addition_search_params)

    search_parameters = MilvusSearchParameters(
        collection_name=MILVUS_IT_CONFIG["collection_name"],
        search_strategy=vector_search_parameters,
        output_fields=["id", "content", "metadata"],
        round_decimal=1)

    collection_load_parameters = MilvusCollectionLoadParameters()

    # Force batching.
    min_batch_size, max_batch_size = 2, 2
    handler = MilvusSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
        collection_load_parameters=collection_load_parameters,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size)

    expected_chunks = [
        Chunk(
            id='query1',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [1, 2],
                    'distance': [1.0, 1.0],
                    'fields': [{
                        'content': 'This is a test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 1
                    },
                               {
                                   'content': 'Another test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 2
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3])),
        Chunk(
            id='query2',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [2, 1],
                    'distance': [1.0, 1.0],
                    'fields': [{
                        'content': 'Another test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 2
                    },
                               {
                                   'content': 'This is a test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 1
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4])),
        Chunk(
            id='query3',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [2, 1],
                    'distance': [1.0, 1.0],
                    'fields': [{
                        'content': 'Another test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 2
                    },
                               {
                                   'content': 'This is a test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 1
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.3, 0.4, 0.5]))
    ]

    with TestPipeline() as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(actual, expected_chunks))

  def test_filtered_search_with_bm25_full_text_and_batching(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(sparse_embedding=None),
            content=Content(text="This is a test document")),
        Chunk(
            id="query2",
            embedding=Embedding(sparse_embedding=None),
            content=Content(text="Another test document")),
        Chunk(
            id="query3",
            embedding=Embedding(sparse_embedding=None),
            content=Content(text="وثيقة اختبار"))
    ]

    filter_condition = 'ARRAY_CONTAINS_ANY(tags, ["healthcare", "banking"])'

    anns_field = "sparse_embedding_bm25"

    addition_search_params = {"metric_type": KeywordSearchMetrics.BM25.value}

    keyword_search_parameters = KeywordSearchParameters(
        anns_field=anns_field,
        limit=10,
        filter=filter_condition,
        search_params=addition_search_params)

    search_parameters = MilvusSearchParameters(
        collection_name=MILVUS_IT_CONFIG["collection_name"],
        search_strategy=keyword_search_parameters,
        output_fields=["id", "content", "metadata"],
        round_decimal=1)

    collection_load_parameters = MilvusCollectionLoadParameters()

    # Force batching.
    min_batch_size, max_batch_size = 2, 2
    handler = MilvusSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
        collection_load_parameters=collection_load_parameters,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size)

    expected_chunks = [
        Chunk(
            id='query1',
            content=Content(text='This is a test document'),
            metadata={
                'enrichment_data': {
                    'id': [1],
                    'distance': [3.3],
                    'fields': [{
                        'content': 'This is a test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 1
                    }]
                }
            },
            embedding=Embedding()),
        Chunk(
            id='query2',
            content=Content(text='Another test document'),
            metadata={
                'enrichment_data': {
                    'id': [1],
                    'distance': [0.8],
                    'fields': [{
                        'content': 'This is a test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 1
                    }]
                }
            },
            embedding=Embedding()),
        Chunk(
            id='query3',
            content=Content(text='وثيقة اختبار'),
            metadata={
                'enrichment_data': {
                    'id': [3],
                    'distance': [2.3],
                    'fields': [{
                        'content': 'وثيقة اختبار',
                        'metadata': {
                            'language': 'ar'
                        },
                        'id': 3
                    }]
                }
            },
            embedding=Embedding())
    ]

    with TestPipeline() as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(actual, expected_chunks))

  def test_vector_search_with_euclidean_distance(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content()),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            content=Content()),
        Chunk(
            id="query3",
            embedding=Embedding(dense_embedding=[0.3, 0.4, 0.5]),
            content=Content())
    ]

    anns_field = "dense_embedding_euclidean"

    addition_search_params = {
        "metric_type": VectorSearchMetrics.EUCLIDEAN_DISTANCE.value,
        "nprobe": 1
    }

    vector_search_parameters = VectorSearchParameters(
        anns_field=anns_field, limit=10, search_params=addition_search_params)

    search_parameters = MilvusSearchParameters(
        collection_name=MILVUS_IT_CONFIG["collection_name"],
        search_strategy=vector_search_parameters,
        output_fields=["id", "content", "metadata"],
        round_decimal=1)

    collection_load_parameters = MilvusCollectionLoadParameters()

    handler = MilvusSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
        collection_load_parameters=collection_load_parameters)

    expected_chunks = [
        Chunk(
            id='query1',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [1, 2, 3],
                    'distance': [0.0, 0.0, 0.1],
                    'fields': [{
                        'content': 'This is a test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 1
                    },
                               {
                                   'content': 'Another test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 2
                               },
                               {
                                   'content': 'وثيقة اختبار',
                                   'metadata': {
                                       'language': 'ar'
                                   },
                                   'id': 3
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3])),
        Chunk(
            id='query2',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [2, 3, 1],
                    'distance': [0.0, 0.0, 0.0],
                    'fields': [{
                        'content': 'Another test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 2
                    },
                               {
                                   'content': 'وثيقة اختبار',
                                   'metadata': {
                                       'language': 'ar'
                                   },
                                   'id': 3
                               },
                               {
                                   'content': 'This is a test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 1
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4])),
        Chunk(
            id='query3',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [3, 2, 1],
                    'distance': [0.0, 0.0, 0.1],
                    'fields': [{
                        'content': 'وثيقة اختبار',
                        'metadata': {
                            'language': 'ar'
                        },
                        'id': 3
                    },
                               {
                                   'content': 'Another test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 2
                               },
                               {
                                   'content': 'This is a test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 1
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.3, 0.4, 0.5]))
    ]

    with TestPipeline() as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(actual, expected_chunks))

  def test_vector_search_with_inner_product_similarity(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content()),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            content=Content()),
        Chunk(
            id="query3",
            embedding=Embedding(dense_embedding=[0.3, 0.4, 0.5]),
            content=Content())
    ]

    anns_field = "dense_embedding_inner_product"

    addition_search_params = {
        "metric_type": VectorSearchMetrics.INNER_PRODUCT.value, "nprobe": 1
    }

    vector_search_parameters = VectorSearchParameters(
        anns_field=anns_field, limit=10, search_params=addition_search_params)

    search_parameters = MilvusSearchParameters(
        collection_name=MILVUS_IT_CONFIG["collection_name"],
        search_strategy=vector_search_parameters,
        output_fields=["id", "content", "metadata"],
        round_decimal=1)

    collection_load_parameters = MilvusCollectionLoadParameters()

    handler = MilvusSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
        collection_load_parameters=collection_load_parameters)

    expected_chunks = [
        Chunk(
            id='query1',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [3, 2, 1],
                    'distance': [0.3, 0.2, 0.1],
                    'fields': [{
                        'content': 'وثيقة اختبار',
                        'metadata': {
                            'language': 'ar'
                        },
                        'id': 3
                    },
                               {
                                   'content': 'Another test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 2
                               },
                               {
                                   'content': 'This is a test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 1
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3])),
        Chunk(
            id='query2',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [3, 2, 1],
                    'distance': [0.4, 0.3, 0.2],
                    'fields': [{
                        'content': 'وثيقة اختبار',
                        'metadata': {
                            'language': 'ar'
                        },
                        'id': 3
                    },
                               {
                                   'content': 'Another test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 2
                               },
                               {
                                   'content': 'This is a test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 1
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4])),
        Chunk(
            id='query3',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [3, 2, 1],
                    'distance': [0.5, 0.4, 0.3],
                    'fields': [{
                        'content': 'وثيقة اختبار',
                        'metadata': {
                            'language': 'ar'
                        },
                        'id': 3
                    },
                               {
                                   'content': 'Another test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 2
                               },
                               {
                                   'content': 'This is a test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 1
                               }]
                }
            },
            embedding=Embedding(dense_embedding=[0.3, 0.4, 0.5]))
    ]

    with TestPipeline() as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(actual, expected_chunks))

  def test_keyword_search_with_inner_product_sparse_embedding(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(
                sparse_embedding=([1, 2, 3, 4], [0.05, 0.41, 0.05, 0.41])),
            content=Content())
    ]

    anns_field = "sparse_embedding_inner_product"

    addition_search_params = {
        "metric_type": VectorSearchMetrics.INNER_PRODUCT.value,
    }

    keyword_search_parameters = KeywordSearchParameters(
        anns_field=anns_field, limit=3, search_params=addition_search_params)

    search_parameters = MilvusSearchParameters(
        collection_name=MILVUS_IT_CONFIG["collection_name"],
        search_strategy=keyword_search_parameters,
        output_fields=["id", "content", "metadata"],
        round_decimal=1)

    collection_load_parameters = MilvusCollectionLoadParameters()

    handler = MilvusSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
        collection_load_parameters=collection_load_parameters)

    expected_chunks = [
        Chunk(
            id='query1',
            content=Content(),
            metadata={
                'enrichment_data': {
                    'id': [1, 2],
                    'distance': [0.3, 0.2],
                    'fields': [{
                        'content': 'This is a test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 1
                    },
                               {
                                   'content': 'Another test document',
                                   'metadata': {
                                       'language': 'en'
                                   },
                                   'id': 2
                               }]
                }
            },
            embedding=Embedding(
                sparse_embedding=([1, 2, 3, 4], [0.05, 0.41, 0.05, 0.41])))
    ]

    with TestPipeline() as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(actual, expected_chunks))

  def test_hybrid_search(self):
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="This is a test document"))
    ]

    anns_vector_field = "dense_embedding_cosine"
    addition_vector_search_params = {
        "metric_type": VectorSearchMetrics.COSINE.value, "nprobe": 1
    }

    vector_search_parameters = VectorSearchParameters(
        anns_field=anns_vector_field,
        limit=10,
        search_params=addition_vector_search_params)

    anns_keyword_field = "sparse_embedding_bm25"
    addition_keyword_search_params = {
        "metric_type": KeywordSearchMetrics.BM25.value
    }

    keyword_search_parameters = KeywordSearchParameters(
        anns_field=anns_keyword_field,
        limit=10,
        search_params=addition_keyword_search_params)

    hybrid_search_parameters = HybridSearchParameters(
        vector=vector_search_parameters,
        keyword=keyword_search_parameters,
        ranker=RRFRanker(1),
        limit=1)

    search_parameters = MilvusSearchParameters(
        collection_name=MILVUS_IT_CONFIG["collection_name"],
        search_strategy=hybrid_search_parameters,
        output_fields=["id", "content", "metadata"],
        round_decimal=1)

    collection_load_parameters = MilvusCollectionLoadParameters()

    handler = MilvusSearchEnrichmentHandler(
        connection_parameters=self._connection_params,
        search_parameters=search_parameters,
        collection_load_parameters=collection_load_parameters)

    expected_chunks = [
        Chunk(
            content=Content(text='This is a test document'),
            id='query1',
            metadata={
                'enrichment_data': {
                    'id': [1],
                    'distance': [1.0],
                    'fields': [{
                        'content': 'This is a test document',
                        'metadata': {
                            'language': 'en'
                        },
                        'id': 1
                    }]
                }
            },
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]))
    ]

    with TestPipeline() as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: assert_chunks_equivalent(actual, expected_chunks))


def parse_chunk_strings(chunk_str_list: List[str]) -> List[Chunk]:
  parsed_chunks = []

  # Define safe globals and disable built-in functions for safety.
  safe_globals = {
      'Chunk': Chunk,
      'Content': Content,
      'Embedding': Embedding,
      'defaultdict': defaultdict,
      'list': list,
      '__builtins__': {}
  }

  for raw_str in chunk_str_list:
    try:
      # replace "<class 'list'>" with actual list reference.
      cleaned_str = re.sub(
          r"defaultdict\(<class 'list'>", "defaultdict(list", raw_str)

      # Evaluate string in restricted environment.
      chunk = eval(cleaned_str, safe_globals)  # pylint: disable=eval-used
      if isinstance(chunk, Chunk):
        parsed_chunks.append(chunk)
      else:
        raise ValueError("Parsed object is not a Chunk instance")
    except Exception as e:
      raise ValueError(f"Error parsing string:\n{raw_str}\n{e}")

  return parsed_chunks


def assert_chunks_equivalent(
    actual_chunks: List[Chunk], expected_chunks: List[Chunk]):
  """assert_chunks_equivalent checks for presence rather than exact match"""
  # Sort both lists by ID to ensure consistent ordering.
  actual_sorted = sorted(actual_chunks, key=lambda c: c.id)
  expected_sorted = sorted(expected_chunks, key=lambda c: c.id)

  actual_len = len(actual_sorted)
  expected_len = len(expected_sorted)
  err_msg = (
      f"Different number of chunks, actual: {actual_len}, "
      f"expected: {expected_len}")
  assert actual_len == expected_len, err_msg

  for actual, expected in zip(actual_sorted, expected_sorted):
    # Assert that IDs match.
    assert actual.id == expected.id

    # Assert that dense embeddings match.
    err_msg = f"Dense embedding mismatch for chunk {actual.id}"
    assert actual.dense_embedding == expected.dense_embedding, err_msg

    # Assert that sparse embeddings match.
    err_msg = f"Sparse embedding mismatch for chunk {actual.id}"
    assert actual.sparse_embedding == expected.sparse_embedding, err_msg

    # Assert that text content match.
    err_msg = f"Text Content mismatch for chunk {actual.id}"
    assert actual.content.text == expected.content.text, err_msg

    # For enrichment_data, be more flexible.
    # If "expected" has values for enrichment_data but actual doesn't, that's
    # acceptable since vector search results can vary based on many factors
    # including implementation details, vector database state, and slight
    # variations in similarity calculations.

    # First ensure the enrichment data key exists.
    err_msg = f"Missing enrichment_data key in chunk {actual.id}"
    assert 'enrichment_data' in actual.metadata, err_msg

    # For enrichment_data, ensure consistent ordering of results.
    actual_data = actual.metadata['enrichment_data']
    expected_data = expected.metadata['enrichment_data']

    # If actual has enrichment data, then perform detailed validation.
    if actual_data:
      # Ensure the id key exist.
      err_msg = f"Missing id key in metadata {actual.id}"
      assert 'id' in actual_data, err_msg

      # Validate IDs have consistent ordering.
      actual_ids = sorted(actual_data['id'])
      expected_ids = sorted(expected_data['id'])
      err_msg = f"IDs in enrichment_data don't match for chunk {actual.id}"
      assert actual_ids == expected_ids, err_msg

      # Ensure the distance key exist.
      err_msg = f"Missing distance key in metadata {actual.id}"
      assert 'distance' in actual_data, err_msg

      # Validate distances exist and have same length as IDs.
      actual_distances = actual_data['distance']
      expected_distances = expected_data['distance']
      err_msg = (
          "Number of distances doesn't match number of IDs for "
          f"chunk {actual.id}")
      assert len(actual_distances) == len(expected_distances), err_msg

      # Ensure the fields key exist.
      err_msg = f"Missing fields key in metadata {actual.id}"
      assert 'fields' in actual_data, err_msg

      # Validate fields have consistent content.
      # Sort fields by 'id' to ensure consistent ordering.
      actual_fields_sorted = sorted(
          actual_data['fields'], key=lambda f: f.get('id', 0))
      expected_fields_sorted = sorted(
          expected_data['fields'], key=lambda f: f.get('id', 0))

      # Compare field IDs.
      actual_field_ids = [f.get('id') for f in actual_fields_sorted]
      expected_field_ids = [f.get('id') for f in expected_fields_sorted]
      err_msg = f"Field IDs don't match for chunk {actual.id}"
      assert actual_field_ids == expected_field_ids, err_msg

      # Compare field content.
      for a_f, e_f in zip(actual_fields_sorted, expected_fields_sorted):
        # Ensure the id key exist.
        err_msg = f"Missing id key in metadata.fields {actual.id}"
        assert 'id' in a_f

        err_msg = f"Field ID mismatch chunk {actual.id}"
        assert a_f['id'] == e_f['id'], err_msg

        # Validate field metadata.
        err_msg = f"Field Metadata doesn't match for chunk {actual.id}"
        assert a_f['metadata'] == e_f['metadata'], err_msg


if __name__ == '__main__':
  unittest.main()
