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

"""Integration tests for Spanner vector store writer."""

import logging
import os
import time
import unittest
import uuid

import pytest

import apache_beam as beam
from apache_beam.ml.rag.ingestion.spanner import SpannerVectorWriterConfig
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import spanner
except ImportError:
  spanner = None

try:
  from testcontainers.core.container import DockerContainer
except ImportError:
  DockerContainer = None
# pylint: enable=wrong-import-order, wrong-import-position


def retry(fn, retries, err_msg, *args, **kwargs):
  """Retry a function with exponential backoff."""
  for _ in range(retries):
    try:
      return fn(*args, **kwargs)
    except:  # pylint: disable=bare-except
      time.sleep(1)
  logging.error(err_msg)
  raise RuntimeError(err_msg)


class SpannerEmulatorHelper:
  """Helper for managing Spanner emulator lifecycle."""
  def __init__(self, project_id: str, instance_id: str, table_name: str):
    self.project_id = project_id
    self.instance_id = instance_id
    self.table_name = table_name
    self.host = None

    # Start emulator
    self.emulator = DockerContainer(
        'gcr.io/cloud-spanner-emulator/emulator:latest').with_exposed_ports(
            9010, 9020)
    retry(self.emulator.start, 3, 'Could not start spanner emulator.')
    time.sleep(3)

    self.host = f'{self.emulator.get_container_host_ip()}:' \
                f'{self.emulator.get_exposed_port(9010)}'
    os.environ['SPANNER_EMULATOR_HOST'] = self.host

    # Create client and instance
    self.client = spanner.Client(project_id)
    self.instance = self.client.instance(instance_id)
    self.create_instance()

  def create_instance(self):
    """Create Spanner instance in emulator."""
    self.instance.create().result(120)

  def create_database(self, database_id: str):
    """Create database with default vector table schema."""
    database = self.instance.database(
        database_id,
        ddl_statements=[
            f'''
          CREATE TABLE {self.table_name} (
              id STRING(1024) NOT NULL,
              embedding ARRAY<FLOAT32>(vector_length=>3),
              content STRING(MAX),
              metadata JSON
          ) PRIMARY KEY (id)'''
        ])
    database.create().result(120)

  def read_data(self, database_id: str):
    """Read all data from the table."""
    database = self.instance.database(database_id)
    with database.snapshot() as snapshot:
      results = snapshot.execute_sql(
          f'SELECT * FROM {self.table_name} ORDER BY id')
      return list(results) if results else []

  def drop_database(self, database_id: str):
    """Drop the database."""
    database = self.instance.database(database_id)
    database.drop()

  def shutdown(self):
    """Stop the emulator."""
    if self.emulator:
      try:
        self.emulator.stop()
      except:  # pylint: disable=bare-except
        logging.error('Could not stop Spanner emulator.')

  def get_emulator_host(self) -> str:
    """Get the emulator host URL."""
    return f'http://{self.host}'


@pytest.mark.uses_gcp_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
@unittest.skipIf(spanner is None, 'GCP dependencies are not installed.')
@unittest.skipIf(
    DockerContainer is None, 'testcontainers package is not installed.')
class SpannerVectorWriterTest(unittest.TestCase):
  """Integration tests for Spanner vector writer."""
  @classmethod
  def setUpClass(cls):
    """Set up Spanner emulator for all tests."""
    pipeline = TestPipeline(is_integration_test=True)
    runner_name = type(pipeline.runner).__name__
    if 'DataflowRunner' in runner_name:
      pytest.skip("Spanner emulator not compatible with dataflow runner.")

    cls.project_id = 'test-project'
    cls.instance_id = 'test-instance'
    cls.table_name = 'embeddings'

    cls.spanner_helper = SpannerEmulatorHelper(
        cls.project_id, cls.instance_id, cls.table_name)

  @classmethod
  def tearDownClass(cls):
    """Tear down Spanner emulator."""
    cls.spanner_helper.shutdown()

  def setUp(self):
    """Create a unique database for each test."""
    self.database_id = f'test_db_{uuid.uuid4().hex}'[:30]
    self.spanner_helper.create_database(self.database_id)

  def tearDown(self):
    """Drop the test database."""
    self.spanner_helper.drop_database(self.database_id)

  def test_write_default_schema(self):
    """Test writing with default schema (id, embedding, content, metadata)."""
    # Create test chunks
    chunks = [
        Chunk(
            id='doc1',
            embedding=Embedding(dense_embedding=[1.0, 2.0, 3.0]),
            content=Content(text='First document'),
            metadata={
                'source': 'test', 'page': 1
            }),
        Chunk(
            id='doc2',
            embedding=Embedding(dense_embedding=[4.0, 5.0, 6.0]),
            content=Content(text='Second document'),
            metadata={
                'source': 'test', 'page': 2
            }),
    ]

    # Create config with default schema
    config = SpannerVectorWriterConfig(
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_id=self.database_id,
        table_name=self.table_name,
        emulator_host=self.spanner_helper.get_emulator_host(),
    )

    # Write chunks
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (p | beam.Create(chunks) | config.create_write_transform())

    # Verify data was written
    results = self.spanner_helper.read_data(self.database_id)
    self.assertEqual(len(results), 2)

    # Check first row
    row1 = results[0]
    self.assertEqual(row1[0], 'doc1')  # id
    self.assertEqual(list(row1[1]), [1.0, 2.0, 3.0])  # embedding
    self.assertEqual(row1[2], 'First document')  # content
    # metadata is JSON
    metadata1 = row1[3]
    self.assertEqual(metadata1['source'], 'test')
    self.assertEqual(metadata1['page'], 1)

    # Check second row
    row2 = results[1]
    self.assertEqual(row2[0], 'doc2')
    self.assertEqual(list(row2[1]), [4.0, 5.0, 6.0])
    self.assertEqual(row2[2], 'Second document')

  def test_write_flattened_metadata(self):
    """Test writing with flattened metadata fields."""
    from apache_beam.ml.rag.ingestion.spanner import SpannerColumnSpecsBuilder

    # Create custom database with flattened columns
    self.spanner_helper.drop_database(self.database_id)
    database = self.spanner_helper.instance.database(
        self.database_id,
        ddl_statements=[
            f'''
          CREATE TABLE {self.table_name} (
              id STRING(1024) NOT NULL,
              embedding ARRAY<FLOAT32>(vector_length=>3),
              content STRING(MAX),
              source STRING(MAX),
              page_number INT64,
              metadata JSON
          ) PRIMARY KEY (id)'''
        ])
    database.create().result(120)

    # Create test chunks
    chunks = [
        Chunk(
            id='doc1',
            embedding=Embedding(dense_embedding=[1.0, 2.0, 3.0]),
            content=Content(text='First document'),
            metadata={
                'source': 'book.pdf', 'page': 10, 'author': 'John'
            }),
        Chunk(
            id='doc2',
            embedding=Embedding(dense_embedding=[4.0, 5.0, 6.0]),
            content=Content(text='Second document'),
            metadata={
                'source': 'article.txt', 'page': 5, 'author': 'Jane'
            }),
    ]

    # Create config with flattened metadata
    specs = (
        SpannerColumnSpecsBuilder().with_id_spec().with_embedding_spec().
        with_content_spec().add_metadata_field(
            'source', str, column_name='source').add_metadata_field(
                'page', int,
                column_name='page_number').with_metadata_spec().build())

    config = SpannerVectorWriterConfig(
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_id=self.database_id,
        table_name=self.table_name,
        column_specs=specs,
        emulator_host=self.spanner_helper.get_emulator_host(),
    )

    # Write chunks
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (p | beam.Create(chunks) | config.create_write_transform())

    # Verify data
    database = self.spanner_helper.instance.database(self.database_id)
    with database.snapshot() as snapshot:
      results = snapshot.execute_sql(
          f'SELECT id, embedding, content, source, page_number, metadata '
          f'FROM {self.table_name} ORDER BY id')
      rows = list(results)

    self.assertEqual(len(rows), 2)

    # Check first row
    self.assertEqual(rows[0][0], 'doc1')
    self.assertEqual(list(rows[0][1]), [1.0, 2.0, 3.0])
    self.assertEqual(rows[0][2], 'First document')
    self.assertEqual(rows[0][3], 'book.pdf')  # flattened source
    self.assertEqual(rows[0][4], 10)  # flattened page_number

    metadata1 = rows[0][5]
    self.assertEqual(metadata1['author'], 'John')

  def test_write_minimal_schema(self):
    """Test writing with minimal schema (only id and embedding)."""
    from apache_beam.ml.rag.ingestion.spanner import SpannerColumnSpecsBuilder

    # Create custom database with minimal schema
    self.spanner_helper.drop_database(self.database_id)
    database = self.spanner_helper.instance.database(
        self.database_id,
        ddl_statements=[
            f'''
          CREATE TABLE {self.table_name} (
              id STRING(1024) NOT NULL,
              embedding ARRAY<FLOAT32>(vector_length=>3)
          ) PRIMARY KEY (id)'''
        ])
    database.create().result(120)

    # Create test chunks
    chunks = [
        Chunk(
            id='doc1',
            embedding=Embedding(dense_embedding=[1.0, 2.0, 3.0]),
            content=Content(text='First document'),
            metadata={'source': 'test'}),
        Chunk(
            id='doc2',
            embedding=Embedding(dense_embedding=[4.0, 5.0, 6.0]),
            content=Content(text='Second document'),
            metadata={'source': 'test'}),
    ]

    # Create config with minimal schema
    specs = (
        SpannerColumnSpecsBuilder().with_id_spec().with_embedding_spec().build(
        ))

    config = SpannerVectorWriterConfig(
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_id=self.database_id,
        table_name=self.table_name,
        column_specs=specs,
        emulator_host=self.spanner_helper.get_emulator_host(),
    )

    # Write chunks
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (p | beam.Create(chunks) | config.create_write_transform())

    # Verify data
    results = self.spanner_helper.read_data(self.database_id)
    self.assertEqual(len(results), 2)
    self.assertEqual(results[0][0], 'doc1')
    self.assertEqual(list(results[0][1]), [1.0, 2.0, 3.0])

  def test_write_with_converter(self):
    """Test writing with custom converter function."""
    from apache_beam.ml.rag.ingestion.spanner import SpannerColumnSpecsBuilder

    # Create test chunks with embeddings that need normalization
    chunks = [
        Chunk(
            id='doc1',
            embedding=Embedding(dense_embedding=[3.0, 4.0, 0.0]),
            content=Content(text='First document'),
            metadata={'source': 'test'}),
    ]

    # Define normalizer
    def normalize(vec):
      norm = (sum(x**2 for x in vec)**0.5) or 1.0
      return [x / norm for x in vec]

    # Create config with normalized embeddings
    specs = (
        SpannerColumnSpecsBuilder().with_id_spec().with_embedding_spec(
            convert_fn=normalize).with_content_spec().with_metadata_spec().
        build())

    config = SpannerVectorWriterConfig(
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_id=self.database_id,
        table_name=self.table_name,
        column_specs=specs,
        emulator_host=self.spanner_helper.get_emulator_host(),
    )

    # Write chunks
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (p | beam.Create(chunks) | config.create_write_transform())

    # Verify data - embedding should be normalized
    results = self.spanner_helper.read_data(self.database_id)
    self.assertEqual(len(results), 1)

    embedding = list(results[0][1])
    # Original was [3.0, 4.0, 0.0], normalized should be [0.6, 0.8, 0.0]
    self.assertAlmostEqual(embedding[0], 0.6, places=5)
    self.assertAlmostEqual(embedding[1], 0.8, places=5)
    self.assertAlmostEqual(embedding[2], 0.0, places=5)

    # Check norm is 1.0
    norm = sum(x**2 for x in embedding)**0.5
    self.assertAlmostEqual(norm, 1.0, places=5)

  def test_write_update_mode(self):
    """Test writing with UPDATE mode."""
    # First insert data
    chunks_insert = [
        Chunk(
            id='doc1',
            embedding=Embedding(dense_embedding=[1.0, 2.0, 3.0]),
            content=Content(text='Original content'),
            metadata={'version': 1}),
    ]

    config_insert = SpannerVectorWriterConfig(
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_id=self.database_id,
        table_name=self.table_name,
        write_mode='INSERT',
        emulator_host=self.spanner_helper.get_emulator_host(),
    )

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | beam.Create(chunks_insert)
          | config_insert.create_write_transform())

    # Update existing row
    chunks_update = [
        Chunk(
            id='doc1',
            embedding=Embedding(dense_embedding=[4.0, 5.0, 6.0]),
            content=Content(text='Updated content'),
            metadata={'version': 2}),
    ]

    config_update = SpannerVectorWriterConfig(
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_id=self.database_id,
        table_name=self.table_name,
        write_mode='UPDATE',
        emulator_host=self.spanner_helper.get_emulator_host(),
    )

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | beam.Create(chunks_update)
          | config_update.create_write_transform())

    # Verify update succeeded
    results = self.spanner_helper.read_data(self.database_id)
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0][0], 'doc1')
    self.assertEqual(list(results[0][1]), [4.0, 5.0, 6.0])
    self.assertEqual(results[0][2], 'Updated content')

    metadata = results[0][3]
    self.assertEqual(metadata['version'], 2)

  def test_write_custom_column(self):
    """Test writing with custom computed column."""
    from apache_beam.ml.rag.ingestion.spanner import SpannerColumnSpecsBuilder

    # Create custom database with computed column
    self.spanner_helper.drop_database(self.database_id)
    database = self.spanner_helper.instance.database(
        self.database_id,
        ddl_statements=[
            f'''
          CREATE TABLE {self.table_name} (
              id STRING(1024) NOT NULL,
              embedding ARRAY<FLOAT32>(vector_length=>3),
              content STRING(MAX),
              word_count INT64,
              metadata JSON
          ) PRIMARY KEY (id)'''
        ])
    database.create().result(120)

    # Create test chunks
    chunks = [
        Chunk(
            id='doc1',
            embedding=Embedding(dense_embedding=[1.0, 2.0, 3.0]),
            content=Content(text='Hello world test'),
            metadata={}),
        Chunk(
            id='doc2',
            embedding=Embedding(dense_embedding=[4.0, 5.0, 6.0]),
            content=Content(text='This is a longer test document'),
            metadata={}),
    ]

    # Create config with custom word_count column
    specs = (
        SpannerColumnSpecsBuilder().with_id_spec().with_embedding_spec(
        ).with_content_spec().add_column(
            column_name='word_count',
            python_type=int,
            value_fn=lambda chunk: len(chunk.content.text.split())).
        with_metadata_spec().build())

    config = SpannerVectorWriterConfig(
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_id=self.database_id,
        table_name=self.table_name,
        column_specs=specs,
        emulator_host=self.spanner_helper.get_emulator_host(),
    )

    # Write chunks
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (p | beam.Create(chunks) | config.create_write_transform())

    # Verify data
    database = self.spanner_helper.instance.database(self.database_id)
    with database.snapshot() as snapshot:
      results = snapshot.execute_sql(
          f'SELECT id, word_count FROM {self.table_name} ORDER BY id')
      rows = list(results)

    self.assertEqual(len(rows), 2)
    self.assertEqual(rows[0][1], 3)  # "Hello world test" = 3 words
    self.assertEqual(rows[1][1], 6)  # 6 words

  def test_write_with_timestamp(self):
    """Test writing with timestamp columns."""
    from apache_beam.ml.rag.ingestion.spanner import SpannerColumnSpecsBuilder

    # Create database with timestamp column
    self.spanner_helper.drop_database(self.database_id)
    database = self.spanner_helper.instance.database(
        self.database_id,
        ddl_statements=[
            f'''
          CREATE TABLE {self.table_name} (
              id STRING(1024) NOT NULL,
              embedding ARRAY<FLOAT32>(vector_length=>3),
              content STRING(MAX),
              created_at TIMESTAMP,
              metadata JSON
          ) PRIMARY KEY (id)'''
        ])
    database.create().result(120)

    # Create chunks with timestamp
    timestamp_str = "2025-10-28T09:45:00.123456Z"
    chunks = [
        Chunk(
            id='doc1',
            embedding=Embedding(dense_embedding=[1.0, 2.0, 3.0]),
            content=Content(text='Document with timestamp'),
            metadata={'created_at': timestamp_str}),
    ]

    # Create config with timestamp field
    specs = (
        SpannerColumnSpecsBuilder().with_id_spec().with_embedding_spec().
        with_content_spec().add_metadata_field(
            'created_at', str,
            column_name='created_at').with_metadata_spec().build())

    config = SpannerVectorWriterConfig(
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_id=self.database_id,
        table_name=self.table_name,
        column_specs=specs,
        emulator_host=self.spanner_helper.get_emulator_host(),
    )

    # Write chunks
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (p | beam.Create(chunks) | config.create_write_transform())

    # Verify timestamp was written
    database = self.spanner_helper.instance.database(self.database_id)
    with database.snapshot() as snapshot:
      results = snapshot.execute_sql(
          f'SELECT id, created_at FROM {self.table_name}')
      rows = list(results)

    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0][0], 'doc1')
    # Timestamp is returned as datetime object by Spanner client
    self.assertIsNotNone(rows[0][1])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
