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

from dataclasses import dataclass, field
import platform
import unittest
from typing import Callable, Dict
from typing import List
from typing import cast
import uuid

import pytest
from pymilvus import CollectionSchema
from pymilvus import DataType
from pymilvus import FieldSchema
from pymilvus import Function
from pymilvus import FunctionType
from pymilvus import MilvusClient
from pymilvus import RRFRanker
from pymilvus.milvus_client import IndexParams

import apache_beam as beam
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.ml.rag.utils import (
    MilvusConnectionConfig,
    unpack_dataclass_with_kwargs)
from apache_beam.ml.rag.test_utils import (
    VectorDBContainerInfo,
    MilvusTestHelpers,)
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

try:
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.ml.rag.enrichment.milvus_search import (
      MilvusSearchEnrichmentHandler,
      MilvusSearchParameters,
      MilvusCollectionLoadParameters,
      VectorSearchParameters,
      KeywordSearchParameters,
      HybridSearchParameters,
      VectorSearchMetrics,
      KeywordSearchMetrics)
except ImportError as e:
  raise unittest.SkipTest(f'Milvus dependencies not installed: {str(e)}')


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

MILVUS_ENRICHMENT_IT_CONFIG = {
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

def initialize_db_with_data(connc_params: MilvusConnectionConfig):
    # Open the connection to the milvus db.
    config = unpack_dataclass_with_kwargs(connc_params)
    config["alias"] = f"milvus_conn_{uuid.uuid4().hex[:8]}"
    client = MilvusClient(**config)

    # Configure schema.
    field_schemas: List[FieldSchema] = cast(
        List[FieldSchema], MILVUS_ENRICHMENT_IT_CONFIG["fields"])
    schema = CollectionSchema(
        fields=field_schemas,
        functions=MILVUS_ENRICHMENT_IT_CONFIG["functions"])

    # Create collection with the schema.
    collection_name = MILVUS_ENRICHMENT_IT_CONFIG["collection_name"]
    index_function: Callable[[], IndexParams] = cast(
        Callable[[], IndexParams], MILVUS_ENRICHMENT_IT_CONFIG["index"])
    client.create_collection(
        collection_name=collection_name,
        schema=schema,
        index_params=index_function())

    # Assert that collection was created.
    collection_error = f"Expected collection '{collection_name}' to be created."
    assert client.has_collection(collection_name), collection_error

    # Gather all fields we have excluding 'sparse_embedding_bm25' special field.
    fields = [field.name for field in field_schemas]

    # Prep data for indexing. Currently we can't insert sparse vectors for BM25
    # sparse embedding field as it would be automatically generated by Milvus
    # through the registered BM25 function.
    data_ready_to_index = []
    for doc in MILVUS_ENRICHMENT_IT_CONFIG["corpus"]:
        item = {}
        for field in fields:
            if field.startswith("dense_embedding"):
                item[field] = doc["dense_embedding"]
            elif field == "sparse_embedding_inner_product":
                item[field] = doc["sparse_embedding"]
            elif field == "sparse_embedding_bm25":
                # It is automatically generated by Milvus from content field.
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

  _db: VectorDBContainerInfo
  _version = "milvusdb/milvus:v2.5.10"

  @classmethod
  def setUpClass(cls):
    try:
        cls._db = MilvusTestHelpers.start_db_container(
            cls._version, vector_client_max_retries=1, tc_max_retries=1)
        cls._connection_params = MilvusConnectionConfig(
            uri=cls._db.uri,
            user=cls._db.user,
            password=cls._db.password,
            db_name=cls._db.db_id,
            token=cls._db.token)
        cls._collection_load_params = MilvusCollectionLoadParameters()
        cls._collection_name = initialize_db_with_data(cls._connection_params)
    except Exception as e:
      pytest.skip(
          f"Skipping all tests in {cls.__name__} due to DB startup failure: {e}"
      )

  @classmethod
  def tearDownClass(cls):
    MilvusTestHelpers.stop_db_container(cls._db)
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
    existent_collection = MILVUS_ENRICHMENT_IT_CONFIG["collection_name"]

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
        collection_name=MILVUS_ENRICHMENT_IT_CONFIG["collection_name"],
        search_strategy=VectorSearchParameters(anns_field=anns_field))

    collection_load_parameters = MilvusCollectionLoadParameters()

    handler = MilvusSearchEnrichmentHandler(
        self._connection_params,
        search_parameters,
        collection_load_parameters=collection_load_parameters)

    expected_chunks = []

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: MilvusTestHelpers.assert_chunks_equivalent(
            actual, expected_chunks))

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
        collection_name=MILVUS_ENRICHMENT_IT_CONFIG["collection_name"],
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

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: MilvusTestHelpers.assert_chunks_equivalent(
            actual, expected_chunks))

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
        collection_name=MILVUS_ENRICHMENT_IT_CONFIG["collection_name"],
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

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: MilvusTestHelpers.assert_chunks_equivalent(
            actual, expected_chunks))

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
        collection_name=MILVUS_ENRICHMENT_IT_CONFIG["collection_name"],
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

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: MilvusTestHelpers.assert_chunks_equivalent(
            actual, expected_chunks))

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
        collection_name=MILVUS_ENRICHMENT_IT_CONFIG["collection_name"],
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

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: MilvusTestHelpers.assert_chunks_equivalent(
            actual, expected_chunks))

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
        collection_name=MILVUS_ENRICHMENT_IT_CONFIG["collection_name"],
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

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: MilvusTestHelpers.assert_chunks_equivalent(
            actual, expected_chunks))

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
        collection_name=MILVUS_ENRICHMENT_IT_CONFIG["collection_name"],
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

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))
      assert_that(
          result,
          lambda actual: MilvusTestHelpers.assert_chunks_equivalent(
            actual, expected_chunks))


if __name__ == '__main__':
  unittest.main()
