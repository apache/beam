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
import secrets
import time
import unittest

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  from google.api_core.exceptions import BadRequest
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.ml.rag.enrichment.bigquery_vector_search import \
    BigQueryVectorSearchEnrichmentHandler
  from apache_beam.ml.rag.enrichment.bigquery_vector_search import \
    BigQueryVectorSearchParameters
except ImportError:
  raise unittest.SkipTest('BigQuery dependencies not installed')

_LOGGER = logging.getLogger(__name__)


class BigQueryVectorSearchIT(unittest.TestCase):
  bigquery_dataset_id = 'python_vector_search_test_'
  project = "apache-beam-testing"

  @classmethod
  def setUpClass(cls):
    cls.bigquery_client = BigQueryWrapper()
    cls.dataset_id = '%s%d%s' % (
        cls.bigquery_dataset_id, int(time.time()), secrets.token_hex(3))
    cls.bigquery_client.get_or_create_dataset(cls.project, cls.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", cls.dataset_id, cls.project)

  @classmethod
  def tearDownClass(cls):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=cls.project, datasetId=cls.dataset_id, deleteContents=True)
    try:
      cls.bigquery_client.client.datasets.Delete(request)
    except Exception:
      _LOGGER.warning(
          'Failed to clean up dataset %s in project %s',
          cls.dataset_id,
          cls.project)


class TestBigQueryVectorSearchIT(BigQueryVectorSearchIT):
  # Test data with embeddings
  table_data = [{
      "id": "doc1",
      "content": "This is a test document",
      "domain": "medical",
      "embedding": [0.1, 0.2, 0.3],
      "metadata": [{
          "key": "language", "value": "en"
      }]
  },
                {
                    "id": "doc2",
                    "content": "Another test document",
                    "domain": "legal",
                    "embedding": [0.2, 0.3, 0.4],
                    "metadata": [{
                        "key": "language", "value": "en"
                    }]
                },
                {
                    "id": "doc3",
                    "content": "Un document de test",
                    "domain": "financial",
                    "embedding": [0.3, 0.4, 0.5],
                    "metadata": [{
                        "key": "language", "value": "fr"
                    }]
                }]

  @classmethod
  def create_table(cls, table_name):
    fields = [('id', 'STRING'), ('content', 'STRING'), ('domain', 'STRING'),
              ('embedding', 'FLOAT64', 'REPEATED'),
              (
                  'metadata',
                  'RECORD',
                  'REPEATED', [('key', 'STRING'), ('value', 'STRING')])]
    table_schema = bigquery.TableSchema()
    for field_def in fields:
      field = bigquery.TableFieldSchema()
      field.name = field_def[0]
      field.type = field_def[1]
      if len(field_def) > 2:
        field.mode = field_def[2]
        if len(field_def) > 3:
          for subfield_def in field_def[3]:
            subfield = bigquery.TableFieldSchema()
            subfield.name = subfield_def[0]
            subfield.type = subfield_def[1]
            field.fields.append(subfield)
      table_schema.fields.append(field)

    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=cls.project, datasetId=cls.dataset_id,
            tableId=table_name),
        schema=table_schema)

    request = bigquery.BigqueryTablesInsertRequest(
        projectId=cls.project, datasetId=cls.dataset_id, table=table)
    cls.bigquery_client.client.tables.Insert(request)
    cls.bigquery_client.insert_rows(
        cls.project, cls.dataset_id, table_name, cls.table_data)
    cls.table_name = f"{cls.project}.{cls.dataset_id}.{table_name}"

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls.create_table('vector_test')

  def test_basic_vector_search(self):
    """Test basic vector similarity search."""
    test_chunks = [
        Chunk(
            id="query1",
            index=0,
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="test query"),
            metadata={"language": "en"})
    ]
    # Expected chunk will have enrichment_data in metadata
    expected_chunks = [
        Chunk(
            id="query1",
            index=0,
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="test query"),
            metadata={
                "language": "en",
                "enrichment_data": {
                    "id": "query1",
                    "chunks": [{
                        "content": "This is a test document",
                        "metadata": [{
                            "key": "language", "value": "en"
                        }]
                    },
                               {
                                   "content": "Another test document",
                                   "metadata": [{
                                       "key": "language", "value": "en"
                                   }]
                               }]
                }
            })
    ]

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content', 'metadata'],
        neighbor_count=2,
        metadata_restriction_template=(
            "check_metadata(metadata, 'language', '{language}')"))

    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))

      assert_that(result, equal_to(expected_chunks))

  def test_batched_metadata_filter_vector_search(self):
    """Test vector similarity search with batching."""
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="test query 1"),
            metadata={"language": "en"},
            index=0),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            content=Content(text="test query 2"),
            metadata={"language": "en"},
            index=1),
        Chunk(
            id="query3",
            embedding=Embedding(dense_embedding=[0.3, 0.4, 0.5]),
            content=Content(text="test query 3"),
            metadata={"language": "fr"},
            index=2)
    ]

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content', 'metadata'],
        neighbor_count=2,
        metadata_restriction_template=(
            "check_metadata(metadata, 'language', '{language}')"))

    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params,
        min_batch_size=2,  # Force batching
        max_batch_size=2   # Process 2 chunks at a time
    )

    expected_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(
                dense_embedding=[0.1, 0.2, 0.3], sparse_embedding=None),
            content=Content(text="test query 1"),
            metadata={
                "language": "en",
                "enrichment_data": {
                    "id": "query1",
                    "chunks": [{
                        "content": "This is a test document",
                        "metadata": [{
                            "key": "language", "value": "en"
                        }]
                    },
                               {
                                   "content": "Another test document",
                                   "metadata": [{
                                       "key": "language", "value": "en"
                                   }]
                               }]
                }
            },
            index=0),
        Chunk(
            id="query2",
            embedding=Embedding(
                dense_embedding=[0.2, 0.3, 0.4], sparse_embedding=None),
            content=Content(text="test query 2"),
            metadata={
                "language": "en",
                "enrichment_data": {
                    "id": "query2",
                    "chunks": [{
                        "content": "Another test document",
                        "metadata": [{
                            "key": "language", "value": "en"
                        }]
                    },
                               {
                                   "content": "This is a test document",
                                   "metadata": [{
                                       "key": "language", "value": "en"
                                   }]
                               }]
                }
            },
            index=1),
        Chunk(
            id="query3",
            embedding=Embedding(
                dense_embedding=[0.3, 0.4, 0.5], sparse_embedding=None),
            content=Content(text="test query 3"),
            metadata={
                "language": "fr",
                "enrichment_data": {
                    "id": "query3",
                    "chunks": [{
                        "content": "Un document de test",
                        "metadata": [{
                            "key": "language", "value": "fr"
                        }]
                    }]
                }
            },
            index=2)
    ]

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))

      assert_that(result, equal_to(expected_chunks))

  def test_euclidean_distance_search(self):
    """Test vector similarity search using Euclidean distance."""
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="test query 1"),
            metadata={"language": "en"},
            index=0),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.2, 0.3, 0.4]),
            content=Content(text="test query 2"),
            metadata={"language": "en"},
            index=1)
    ]

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content', 'metadata'],
        neighbor_count=2,
        metadata_restriction_template=(
            "check_metadata(metadata, 'language', '{language}')"),
        distance_type='EUCLIDEAN'  # Use Euclidean distance
    )

    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params, min_batch_size=2, max_batch_size=2)

    expected_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(
                dense_embedding=[0.1, 0.2, 0.3], sparse_embedding=None),
            content=Content(text="test query 1"),
            metadata={
                "language": "en",
                "enrichment_data": {
                    "id": "query1",
                    "chunks": [{
                        "content": "This is a test document",
                        "metadata": [{
                            "key": "language", "value": "en"
                        }]
                    },
                               {
                                   "content": "Another test document",
                                   "metadata": [{
                                       "key": "language", "value": "en"
                                   }]
                               }]
                }
            },
            index=0),
        Chunk(
            id="query2",
            embedding=Embedding(
                dense_embedding=[0.2, 0.3, 0.4], sparse_embedding=None),
            content=Content(text="test query 2"),
            metadata={
                "language": "en",
                "enrichment_data": {
                    "id": "query2",
                    "chunks": [{
                        "content": "Another test document",
                        "metadata": [{
                            "key": "language", "value": "en"
                        }]
                    },
                               {
                                   "content": "This is a test document",
                                   "metadata": [{
                                       "key": "language", "value": "en"
                                   }]
                               }]
                }
            },
            index=1)
    ]

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))

      assert_that(result, equal_to(expected_chunks))

  def test_no_metadata_restriction(self):
    """Test vector search without metadata filtering."""
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="test query"),
            metadata={'language': 'fr'},
            index=0)
    ]

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content', 'metadata'],
        neighbor_count=2,  # Get top 2 matches
        metadata_restriction_template=None  # No filtering
    )

    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))

      # Should get matches regardless of metadata
      assert_that(
          result,
          equal_to([
              Chunk(
                  id="query1",
                  embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
                  content=Content(text="test query"),
                  metadata={
                      "language": "fr",
                      "enrichment_data": {
                          "id": "query1",
                          "chunks": [{
                              "content": "This is a test document",
                              "metadata": [{
                                  "key": "language", "value": "en"
                              }]
                          },
                                     {
                                         "content": "Another test document",
                                         "metadata": [{
                                             "key": "language", "value": "en"
                                         }]
                                     }]
                      }
                  },
                  index=0)
          ]))

  def test_metadata_filter_leakage(self):
    """Test that metadata filters don't leak between batched chunks."""

    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="medical query"),
            metadata={
                "domain": "medical", "language": "en"
            },
            index=0),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="unmatched query"),
            metadata={
                "domain": "doesntexist", "language": "en"
            },
            index=1)
    ]

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content', 'metadata', 'domain'],
        neighbor_count=1,
        metadata_restriction_template=(
            "domain = '{domain}' AND "
            "check_metadata(metadata, 'language', '{language}')"))

    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params,
        min_batch_size=2,  # Force batching
        max_batch_size=2
    )

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))

      assert_that(
          result,
          equal_to([
              Chunk(
                  id="query1",
                  embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
                  content=Content(text="medical query"),
                  metadata={
                      "domain": "medical",
                      "language": "en",
                      "enrichment_data": {
                          "id": "query1",
                          "chunks": [{
                              "content": "This is a test document",
                              "domain": "medical",
                              "metadata": [{
                                  "key": "language", "value": "en"
                              }]
                          }]
                      }
                  },
                  index=0),
              Chunk(
                  id="query2",
                  embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
                  content=Content(text="unmatched query"),
                  metadata={
                      "domain": "doesntexist",
                      "language": "en",
                      "enrichment_data": {}
                  },
                  index=1)
          ]))

  def test_condition_batching(self):
    """Test that queries with same metadata conditions are batched together."""

    # Create three queries with same conditions but different embeddings
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="english query 1"),
            metadata={"language": "en"},
            index=0),
        Chunk(
            id="query2",
            embedding=Embedding(dense_embedding=[0.4, 0.5, 0.6]),
            content=Content(text="english query 2"),
            metadata={"language": "en"},
            index=1),
        Chunk(
            id="query3",
            embedding=Embedding(dense_embedding=[0.7, 0.8, 0.9]),
            content=Content(text="french query 3"),
            metadata={"language": "fr"},
            index=2)
    ]

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content', 'domain', 'metadata'],
        neighbor_count=3,
        metadata_restriction_template=(
            "check_metadata(metadata, 'language', '{language}')"))

    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params,
        min_batch_size=10,  # Force batching
        max_batch_size=100
    )

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create(test_chunks) | Enrichment(handler))

      # All queries should be handled in a single VECTOR_SEARCH
      # Each should get its closest match
      assert_that(
          result,
          equal_to([
              Chunk(
                  id="query1",
                  embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
                  content=Content(text="english query 1"),
                  metadata={
                      "language": "en",
                      "enrichment_data": {
                          "id": "query1",
                          "chunks": [{
                              "content": "This is a test document",
                              "domain": "medical",
                              "metadata": [{
                                  "key": "language", "value": "en"
                              }]
                          },
                                     {
                                         "content": "Another test document",
                                         "domain": "legal",
                                         "metadata": [{
                                             "key": "language", "value": "en"
                                         }]
                                     }]
                      }
                  },
                  index=0),
              Chunk(
                  id="query2",
                  embedding=Embedding(dense_embedding=[0.4, 0.5, 0.6]),
                  content=Content(text="english query 2"),
                  metadata={
                      "language": "en",
                      "enrichment_data": {
                          "id": "query2",
                          "chunks": [{
                              "content": "Another test document",
                              "domain": "legal",
                              "metadata": [{
                                  "key": "language", "value": "en"
                              }]
                          },
                                     {
                                         "content": "This is a test document",
                                         "domain": "medical",
                                         "metadata": [{
                                             "key": "language", "value": "en"
                                         }]
                                     }]
                      }
                  },
                  index=1),
              Chunk(
                  id="query3",
                  embedding=Embedding(dense_embedding=[0.7, 0.8, 0.9]),
                  content=Content(text="french query 3"),
                  metadata={
                      "language": "fr",
                      "enrichment_data": {
                          "id": "query3",
                          "chunks": [{
                              "content": "Un document de test",
                              "domain": "financial",
                              "metadata": [{
                                  "key": "language", "value": "fr"
                              }]
                          }]
                      }
                  },
                  index=2)
          ]))

  def test_invalid_query(self):
    """Test error handling for invalid queries."""
    test_chunks = [
        Chunk(
            id="query1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            content=Content(text="test query"),
            metadata={"language": "en"})
    ]

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='nonexistent_column',  # Invalid column
        columns=['content'],
        neighbor_count=1,
        metadata_restriction_template=(
          "language = '{language}'"  # Invalid template
        )
    )

    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)

    with self.assertRaises(BadRequest):
      with TestPipeline() as p:
        _ = (p | beam.Create(test_chunks) | Enrichment(handler))

  def test_empty_input(self):
    """Test handling of empty input."""
    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=1)
    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)

    with TestPipeline(is_integration_test=True) as p:
      result = (p | beam.Create([]) | Enrichment(handler))
      assert_that(result, equal_to([]))

  def test_missing_embedding(self):
    """Test handling of chunks with missing embeddings."""
    test_chunks = [
        Chunk(
            id="query1",
            embedding=None,  # Missing embedding
            content=Content(text="test query"),
            metadata={"language": "en"},
            index=0
        )
    ]

    params = BigQueryVectorSearchParameters(
        project=self.project,
        table_name=self.table_name,
        embedding_column='embedding',
        columns=['content'],
        neighbor_count=1)
    handler = BigQueryVectorSearchEnrichmentHandler(
        vector_search_parameters=params)

    with self.assertRaises(ValueError) as context:
      with TestPipeline() as p:
        _ = (p | beam.Create(test_chunks) | Enrichment(handler))
    self.assertIn("missing embedding", str(context.exception))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
