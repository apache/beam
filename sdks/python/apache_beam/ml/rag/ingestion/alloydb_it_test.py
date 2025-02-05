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

import hashlib
import json
import logging
import secrets
import time
import unittest
from typing import List
from typing import NamedTuple

import psycopg2

import apache_beam as beam
from apache_beam.coders import registry
from apache_beam.coders.row_coder import RowCoder
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.ml.rag.ingestion.alloydb import AlloyDBConnectionConfig
from apache_beam.ml.rag.ingestion.alloydb import AlloyDBVectorWriterConfig
from apache_beam.ml.rag.ingestion.alloydb import ColumnSpec
from apache_beam.ml.rag.ingestion.alloydb import ColumnSpecsBuilder
from apache_beam.ml.rag.ingestion.alloydb import ConflictResolution
from apache_beam.ml.rag.ingestion.alloydb import chunk_embedding_fn
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

TestRow = NamedTuple(
    'TestRow',
    [('id', str), ('embedding', List[float]), ('content', str),
     ('metadata', str)])
registry.register_coder(TestRow, RowCoder)

CustomSpecsRow = NamedTuple('CustomSpecsRow', [
    ('custom_id', str),          # For id_spec test
    ('embedding_vec', List[float]),  # For embedding_spec test
    ('content_col', str),        # For content_spec test
    ('metadata', str)
])
registry.register_coder(CustomSpecsRow, RowCoder)

MetadataConflictRow = NamedTuple('MetadataConflictRow', [
    ('id', str),
    ('source', str),          # For metadata_spec and composite key
    ('timestamp', str),       # For metadata_spec and composite key
    ('content', str),
    ('embedding', List[float]),
    ('metadata', str)
])
registry.register_coder(MetadataConflictRow, RowCoder)

VECTOR_SIZE = 768


def row_to_chunk(row) -> Chunk:
  # Parse embedding string back to float list
  embedding_list = [float(x) for x in row.embedding.strip('[]').split(',')]
  return Chunk(
      id=row.id,
      content=Content(text=row.content if hasattr(row, 'content') else None),
      embedding=Embedding(dense_embedding=embedding_list),
      metadata=json.loads(row.metadata) if hasattr(row, 'metadata') else {})


class ChunkTestUtils:
  """Helper functions for generating test Chunks."""
  @staticmethod
  def from_seed(seed: int, content_prefix: str, seed_multiplier: int) -> Chunk:
    """Creates a deterministic Chunk from a seed value."""
    return Chunk(
        id=f"id_{seed}",
        content=Content(text=f"{content_prefix}{seed}"),
        embedding=Embedding(
            dense_embedding=[
                float(seed + i * seed_multiplier) / 100
                for i in range(VECTOR_SIZE)
            ]),
        metadata={"seed": str(seed)})

  @staticmethod
  def get_expected_values(
      range_start: int,
      range_end: int,
      content_prefix: str = "Testval",
      seed_multiplier: int = 1) -> List[Chunk]:
    """Returns a range of test Chunks."""
    return [
        ChunkTestUtils.from_seed(i, content_prefix, seed_multiplier)
        for i in range(range_start, range_end)
    ]


class HashingFn(beam.CombineFn):
  """Hashing function for verification."""
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    # Hash based on content like TestRow's SelectNameFn
    accumulator.append(input.content.text if input.content.text else "")
    return accumulator

  def merge_accumulators(self, accumulators):
    merged = []
    for acc in accumulators:
      merged.extend(acc)
    return merged

  def extract_output(self, accumulator):
    sorted_values = sorted(accumulator)
    return hashlib.md5(''.join(sorted_values).encode()).hexdigest()


def generate_expected_hash(num_records: int) -> str:
  chunks = ChunkTestUtils.get_expected_values(0, num_records)
  values = sorted(
      chunk.content.text if chunk.content.text else "" for chunk in chunks)
  return hashlib.md5(''.join(values).encode()).hexdigest()


def key_on_id(chunk):
  return (int(chunk.id.split('_')[1]), chunk)


@unittest.skip("Temporarily skipping all AlloyDB tests")
class AlloyDBVectorWriterConfigTest(unittest.TestCase):
  ALLOYDB_TABLE_PREFIX = 'python_rag_alloydb_'

  @classmethod
  def setUpClass(cls):
    # TODO(claudevdm) Pass database args to test
    # cls.host =
    # cls.private_host =
    # cls.port = os.environ.get('ALLOYDB_PORT', '5432')
    # cls.database = os.environ.get('ALLOYDB_DATABASE', 'postgres')
    # cls.username = os.environ.get('ALLOYDB_USERNAME', 'postgres')
    # cls.password = os.environ.get('ALLOYDB_USERNAME')

    # Create unique table name suffix
    cls.table_suffix = '%d%s' % (int(time.time()), secrets.token_hex(3))

    # Setup database connection
    cls.conn = psycopg2.connect(
        host=cls.host,
        port=cls.port,
        database=cls.database,
        user=cls.username,
        password=cls.password)
    cls.conn.autocommit = True

  def setUp(self):
    self.write_test_pipeline = TestPipeline(is_integration_test=True)
    self.read_test_pipeline = TestPipeline(is_integration_test=True)
    self.write_test_pipeline2 = TestPipeline(is_integration_test=True)
    self.read_test_pipeline2 = TestPipeline(is_integration_test=True)
    self._runner = type(self.read_test_pipeline.runner).__name__

    self.default_table_name = f"{self.ALLOYDB_TABLE_PREFIX}" \
      f"{self.table_suffix}"
    self.default_table_name = f"{self.ALLOYDB_TABLE_PREFIX}" \
      f"{self.table_suffix}"
    self.custom_table_name = f"{self.ALLOYDB_TABLE_PREFIX}" \
      f"_custom_{self.table_suffix}"
    self.metadata_conflicts_table = f"{self.ALLOYDB_TABLE_PREFIX}" \
      f"_meta_conf_{self.table_suffix}"

    self.jdbc_url = f'jdbc:postgresql://{self.host}:{self.port}/{self.database}'

    # Create test table
    with self.conn.cursor() as cursor:
      cursor.execute(
          f"""
                CREATE TABLE {self.default_table_name} (
                    id TEXT PRIMARY KEY,
                    embedding VECTOR({VECTOR_SIZE}),
                    content TEXT,
                    metadata JSONB
                )
            """)
      cursor.execute(
          f"""
                CREATE TABLE {self.custom_table_name} (
                    custom_id TEXT PRIMARY KEY,
                    embedding_vec VECTOR(2),
                    content_col TEXT,
                    metadata JSONB
                )
            """)
      cursor.execute(
          f"""
            CREATE TABLE {self.metadata_conflicts_table} (
                    id TEXT,
                    source TEXT,
                    timestamp TIMESTAMP,
                    content TEXT,
                    embedding VECTOR(2),
                    PRIMARY KEY (id),
                    UNIQUE (source, timestamp)
                )
            """)
    _LOGGER = logging.getLogger(__name__)
    _LOGGER.info("Created table %s", self.default_table_name)

  def tearDown(self):
    # Drop test table
    with self.conn.cursor() as cursor:
      cursor.execute(f"DROP TABLE IF EXISTS {self.default_table_name}")
      cursor.execute(f"DROP TABLE IF EXISTS {self.custom_table_name}")
      cursor.execute(f"DROP TABLE IF EXISTS {self.metadata_conflicts_table}")
    _LOGGER = logging.getLogger(__name__)
    _LOGGER.info("Dropped table %s", self.default_table_name)

  @classmethod
  def tearDownClass(cls):
    if hasattr(cls, 'conn'):
      cls.conn.close()

  def test_default_schema(self):
    """Test basic write with default schema."""
    jdbc_url = f'jdbc:postgresql://{self.host}:{self.port}/{self.database}'
    connection_config = AlloyDBConnectionConfig(
        jdbc_url=jdbc_url, username=self.username, password=self.password)

    config = AlloyDBVectorWriterConfig(
        connection_config=connection_config, table_name=self.default_table_name)

    # Create test chunks
    num_records = 1500
    sample_size = min(500, num_records // 2)
    # Generate test chunks
    chunks = ChunkTestUtils.get_expected_values(0, num_records)

    # Run pipeline and verify
    self.write_test_pipeline.not_use_test_runner_api = True

    with self.write_test_pipeline as p:
      _ = (p | beam.Create(chunks) | config.create_write_transform())

    self.read_test_pipeline.not_use_test_runner_api = True
    # Read pipeline to verify
    read_query = f"""
          SELECT 
              CAST(id AS VARCHAR(255)),
              CAST(content AS VARCHAR(255)),
              CAST(embedding AS text),
              CAST(metadata AS text)
          FROM {self.default_table_name}
          """

    # Read and verify pipeline
    with self.read_test_pipeline as p:
      rows = (
          p
          | ReadFromJdbc(
              table_name=self.default_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      chunks = (rows | "To Chunks" >> beam.Map(row_to_chunk))
      chunk_hashes = chunks | "Hash Chunks" >> beam.CombineGlobally(HashingFn())
      assert_that(
          chunk_hashes,
          equal_to([generate_expected_hash(num_records)]),
          label='hash_check')

      # Sample validation
      first_n = (
          chunks
          | "Key on Index" >> beam.Map(key_on_id)
          | f"Get First {sample_size}" >> beam.transforms.combiners.Top.Of(
              sample_size, key=lambda x: x[0], reverse=True)
          | "Remove Keys 1" >> beam.Map(lambda xs: [x[1] for x in xs]))
      expected_first_n = ChunkTestUtils.get_expected_values(0, sample_size)
      assert_that(
          first_n,
          equal_to([expected_first_n]),
          label=f"first_{sample_size}_check")

      last_n = (
          chunks
          | "Key on Index 2" >> beam.Map(key_on_id)
          | f"Get Last {sample_size}" >> beam.transforms.combiners.Top.Of(
              sample_size, key=lambda x: x[0])
          | "Remove Keys 2" >> beam.Map(lambda xs: [x[1] for x in xs]))
      expected_last_n = ChunkTestUtils.get_expected_values(
          num_records - sample_size, num_records)[::-1]
      assert_that(
          last_n,
          equal_to([expected_last_n]),
          label=f"last_{sample_size}_check")

  def test_custom_specs(self):
    """Test custom specifications for ID, embedding, and content."""
    num_records = 20

    specs = (
        ColumnSpecsBuilder().add_custom_column_spec(
            ColumnSpec.text(
                column_name="custom_id",
                value_fn=lambda chunk:
                f"timestamp_{chunk.metadata.get('timestamp', '')}")
        ).add_custom_column_spec(
            ColumnSpec.vector(
                column_name="embedding_vec",
                value_fn=chunk_embedding_fn)).add_custom_column_spec(
                    ColumnSpec.text(
                        column_name="content_col",
                        value_fn=lambda chunk:
                        f"{len(chunk.content.text)}:{chunk.content.text}")).
        with_metadata_spec().build())

    connection_config = AlloyDBConnectionConfig(
        jdbc_url=self.jdbc_url, username=self.username, password=self.password)

    writer_config = AlloyDBVectorWriterConfig(
        connection_config=connection_config,
        table_name=self.custom_table_name,
        column_specs=specs)

    # Generate test chunks
    test_chunks = [
        Chunk(
            id=str(i),
            content=Content(text=f"content_{i}"),
            embedding=Embedding(dense_embedding=[float(i), float(i + 1)]),
            metadata={"timestamp": f"2024-02-02T{i:02d}:00:00"})
        for i in range(num_records)
    ]

    # Write pipeline
    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | beam.Create(test_chunks) | writer_config.create_write_transform())

    # Read and verify
    read_query = f"""
          SELECT 
              CAST(custom_id AS VARCHAR(255)),
              CAST(embedding_vec AS text),
              CAST(content_col AS VARCHAR(255)),
              CAST(metadata AS text)
          FROM {self.custom_table_name}
          ORDER BY custom_id
      """

    # Convert BeamRow back to Chunk
    def custom_row_to_chunk(row):
      # Extract timestamp from custom_id
      timestamp = row.custom_id.split('timestamp_')[1]
      # Extract index from timestamp
      i = int(timestamp.split('T')[1][:2])

      # Parse embedding vector
      embedding_list = [
          float(x) for x in row.embedding_vec.strip('[]').split(',')
      ]

      # Extract content from length-prefixed format
      content = row.content_col.split(':', 1)[1]

      return Chunk(
          id=str(i),
          content=Content(text=content),
          embedding=Embedding(dense_embedding=embedding_list),
          metadata=json.loads(row.metadata))

    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (
          p
          | ReadFromJdbc(
              table_name=self.custom_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      # Verify count
      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      chunks = rows | "To Chunks" >> beam.Map(custom_row_to_chunk)
      assert_that(chunks, equal_to(test_chunks), label='chunks_check')

  def test_defaults_with_args_specs(self):
    """Test custom specifications for ID, embedding, and content."""
    num_records = 20

    specs = (
        ColumnSpecsBuilder().with_id_spec(
            column_name="custom_id",
            python_type=int,
            convert_fn=lambda x: int(x),
            sql_typecast="::text").with_content_spec(
                column_name="content_col",
                convert_fn=lambda x: f"{len(x)}:{x}",
            ).with_embedding_spec(
                column_name="embedding_vec").with_metadata_spec().build())

    connection_config = AlloyDBConnectionConfig(
        jdbc_url=self.jdbc_url, username=self.username, password=self.password)

    writer_config = AlloyDBVectorWriterConfig(
        connection_config=connection_config,
        table_name=self.custom_table_name,
        column_specs=specs)

    # Generate test chunks
    test_chunks = [
        Chunk(
            id=str(i),
            content=Content(text=f"content_{i}"),
            embedding=Embedding(dense_embedding=[float(i), float(i + 1)]),
            metadata={"timestamp": f"2024-02-02T{i:02d}:00:00"})
        for i in range(num_records)
    ]

    # Write pipeline
    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | beam.Create(test_chunks) | writer_config.create_write_transform())

    # Read and verify
    read_query = f"""
          SELECT 
              CAST(custom_id AS VARCHAR(255)),
              CAST(embedding_vec AS text),
              CAST(content_col AS VARCHAR(255)),
              CAST(metadata AS text)
          FROM {self.custom_table_name}
          ORDER BY custom_id
      """

    # Convert BeamRow back to Chunk
    def custom_row_to_chunk(row):
      # Parse embedding vector
      embedding_list = [
          float(x) for x in row.embedding_vec.strip('[]').split(',')
      ]

      # Extract content from length-prefixed format
      content = row.content_col.split(':', 1)[1]

      return Chunk(
          id=row.custom_id,
          content=Content(text=content),
          embedding=Embedding(dense_embedding=embedding_list),
          metadata=json.loads(row.metadata))

    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (
          p
          | ReadFromJdbc(
              table_name=self.custom_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      # Verify count
      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      chunks = rows | "To Chunks" >> beam.Map(custom_row_to_chunk)
      assert_that(chunks, equal_to(test_chunks), label='chunks_check')

  def test_default_id_embedding_specs(self):
    """Test with only default id and embedding specs, others set to None."""
    num_records = 20
    connection_config = AlloyDBConnectionConfig(
        jdbc_url=self.jdbc_url, username=self.username, password=self.password)
    specs = (
        ColumnSpecsBuilder().with_id_spec()  # Use default id spec
        .with_embedding_spec()  # Use default embedding spec
        .build())

    writer_config = AlloyDBVectorWriterConfig(
        connection_config=connection_config,
        table_name=self.default_table_name,
        column_specs=specs)

    # Generate test chunks
    test_chunks = ChunkTestUtils.get_expected_values(0, num_records)

    # Write pipeline
    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | beam.Create(test_chunks) | writer_config.create_write_transform())

    # Read and verify only id and embedding
    read_query = f"""
          SELECT 
              CAST(id AS VARCHAR(255)),
              CAST(embedding AS text)
          FROM {self.default_table_name}
          ORDER BY id
      """

    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (
          p
          | ReadFromJdbc(
              table_name=self.default_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      chunks = rows | "To Chunks" >> beam.Map(row_to_chunk)

      # Create expected chunks with None values
      expected_chunks = ChunkTestUtils.get_expected_values(0, num_records)
      for chunk in expected_chunks:
        chunk.content.text = None
        chunk.metadata = {}

      assert_that(chunks, equal_to(expected_chunks), label='chunks_check')

  def test_metadata_spec_and_conflicts(self):
    """Test metadata specification and conflict resolution."""
    num_records = 20

    specs = (
        ColumnSpecsBuilder().with_id_spec().with_embedding_spec().
        with_content_spec().add_metadata_field(
            field="source",
            column_name="source",
            python_type=str,
            sql_typecast=None  # Plain text field
        ).add_metadata_field(
            field="timestamp", python_type=str,
            sql_typecast="::timestamp").build())

    # Conflict resolution on source+timestamp
    conflict_resolution = ConflictResolution(
        on_conflict_fields=["source", "timestamp"],
        action="UPDATE",
        update_fields=["embedding", "content"])
    connection_config = AlloyDBConnectionConfig(
        jdbc_url=self.jdbc_url, username=self.username, password=self.password)
    writer_config = AlloyDBVectorWriterConfig(
        connection_config=connection_config,
        table_name=self.metadata_conflicts_table,
        column_specs=specs,
        conflict_resolution=conflict_resolution)

    # Generate initial test chunks
    initial_chunks = [
        Chunk(
            id=str(i),
            content=Content(text=f"content_{i}"),
            embedding=Embedding(dense_embedding=[float(i), float(i + 1)]),
            metadata={
                "source": "source_A", "timestamp": f"2024-02-02T{i:02d}:00:00"
            }) for i in range(num_records)
    ]

    # Write initial chunks
    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | "Write Initial" >> beam.Create(initial_chunks)
          | writer_config.create_write_transform())

    # Generate conflicting chunks (same source+timestamp, different content)
    conflicting_chunks = [
        Chunk(
            id=f"new_{i}",
            content=Content(text=f"updated_content_{i}"),
            embedding=Embedding(
                dense_embedding=[float(i) * 2, float(i + 1) * 2]),
            metadata={
                "source": "source_A", "timestamp": f"2024-02-02T{i:02d}:00:00"
            }) for i in range(num_records)
    ]

    # Write conflicting chunks
    self.write_test_pipeline2.not_use_test_runner_api = True
    with self.write_test_pipeline2 as p:
      _ = (
          p | "Write Conflicts" >> beam.Create(conflicting_chunks)
          | writer_config.create_write_transform())

    # Read and verify
    read_query = f"""
            SELECT 
                CAST(id AS VARCHAR(255)),
                CAST(embedding AS text),
                CAST(content AS VARCHAR(255)),
                CAST(source AS VARCHAR(255)),
                CAST(timestamp AS VARCHAR(255))
            FROM {self.metadata_conflicts_table}
            ORDER BY timestamp, id
        """

    # Expected chunks after conflict resolution
    expected_chunks = [
        Chunk(
            id=str(i),
            content=Content(text=f"updated_content_{i}"),
            embedding=Embedding(
                dense_embedding=[float(i) * 2, float(i + 1) * 2]),
            metadata={
                "source": "source_A", "timestamp": f"2024-02-02T{i:02d}:00:00"
            }) for i in range(num_records)
    ]

    def metadata_row_to_chunk(row):
      return Chunk(
          id=row.id,
          content=Content(text=row.content),
          embedding=Embedding(
              dense_embedding=[
                  float(x) for x in row.embedding.strip('[]').split(',')
              ]),
          metadata={
              "source": row.source,
              "timestamp": row.timestamp.replace(' ', 'T')
          })

    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (
          p
          | ReadFromJdbc(
              table_name=self.metadata_conflicts_table,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      chunks = rows | "To Chunks" >> beam.Map(metadata_row_to_chunk)
      assert_that(chunks, equal_to(expected_chunks), label='chunks_check')

  def test_conflict_resolution_update(self):
    """Test conflict resolution with UPDATE action."""
    num_records = 20

    connection_config = AlloyDBConnectionConfig(
        jdbc_url=self.jdbc_url, username=self.username, password=self.password)

    conflict_resolution = ConflictResolution(
        on_conflict_fields="id",
        action="UPDATE",
        update_fields=["embedding", "content"])

    config = AlloyDBVectorWriterConfig(
        connection_config=connection_config,
        table_name=self.default_table_name,
        conflict_resolution=conflict_resolution)

    # Generate initial test chunks
    test_chunks = ChunkTestUtils.get_expected_values(0, num_records)
    self.write_test_pipeline.not_use_test_runner_api = True
    # Insert initial test chunks
    with self.write_test_pipeline as p:
      _ = (
          p
          | "Create initial chunks" >> beam.Create(test_chunks)
          | "Write initial chunks" >> config.create_write_transform())

    read_query = f"""
          SELECT 
              CAST(id AS VARCHAR(255)),
              CAST(content AS VARCHAR(255)),
              CAST(embedding AS text),
              CAST(metadata AS text)
          FROM {self.default_table_name}
            ORDER BY id desc
          """
    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (
          p
          | ReadFromJdbc(
              table_name=self.default_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      chunks = (
          rows
          | "To Chunks" >> beam.Map(row_to_chunk)
          | "Key on Index" >> beam.Map(key_on_id)
          | "Get First 500" >> beam.transforms.combiners.Top.Of(
              num_records, key=lambda x: x[0], reverse=True)
          | "Remove Keys 1" >> beam.Map(lambda xs: [x[1] for x in xs]))
      assert_that(
          chunks, equal_to([test_chunks]), label='original_chunks_check')

    updated_chunks = ChunkTestUtils.get_expected_values(
        0, num_records, content_prefix="Newcontent", seed_multiplier=2)
    self.write_test_pipeline2.not_use_test_runner_api = True
    with self.write_test_pipeline2 as p:
      _ = (
          p
          | "Create updated Chunks" >> beam.Create(updated_chunks)
          | "Write updated Chunks" >> config.create_write_transform())
    self.read_test_pipeline2.not_use_test_runner_api = True
    with self.read_test_pipeline2 as p:
      rows = (
          p
          | "Read Updated chunks" >> ReadFromJdbc(
              table_name=self.default_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      chunks = (
          rows
          | "To Chunks 2" >> beam.Map(row_to_chunk)
          | "Key on Index 2" >> beam.Map(key_on_id)
          | "Get First 500 2" >> beam.transforms.combiners.Top.Of(
              num_records, key=lambda x: x[0], reverse=True)
          | "Remove Keys 2" >> beam.Map(lambda xs: [x[1] for x in xs]))
      assert_that(
          chunks, equal_to([updated_chunks]), label='updated_chunks_check')

  def test_conflict_resolution_default_ignore(self):
    """Test conflict resolution with default."""
    num_records = 20

    connection_config = AlloyDBConnectionConfig(
        jdbc_url=self.jdbc_url, username=self.username, password=self.password)

    config = AlloyDBVectorWriterConfig(
        connection_config=connection_config, table_name=self.default_table_name)

    # Generate initial test chunks
    test_chunks = ChunkTestUtils.get_expected_values(0, num_records)
    self.write_test_pipeline.not_use_test_runner_api = True
    # Insert initial test chunks
    with self.write_test_pipeline as p:
      _ = (
          p
          | "Create initial chunks" >> beam.Create(test_chunks)
          | "Write initial chunks" >> config.create_write_transform())

    read_query = f"""
          SELECT 
              CAST(id AS VARCHAR(255)),
              CAST(content AS VARCHAR(255)),
              CAST(embedding AS text),
              CAST(metadata AS text)
          FROM {self.default_table_name}
            ORDER BY id desc
          """
    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (
          p
          | ReadFromJdbc(
              table_name=self.default_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      chunks = (
          rows
          | "To Chunks" >> beam.Map(row_to_chunk)
          | "Key on Index" >> beam.Map(key_on_id)
          | "Get First 500" >> beam.transforms.combiners.Top.Of(
              num_records, key=lambda x: x[0], reverse=True)
          | "Remove Keys 1" >> beam.Map(lambda xs: [x[1] for x in xs]))
      assert_that(
          chunks, equal_to([test_chunks]), label='original_chunks_check')

    updated_chunks = ChunkTestUtils.get_expected_values(
        0, num_records, content_prefix="Newcontent", seed_multiplier=2)
    self.write_test_pipeline2.not_use_test_runner_api = True
    with self.write_test_pipeline2 as p:
      _ = (
          p
          | "Create updated Chunks" >> beam.Create(updated_chunks)
          | "Write updated Chunks" >> config.create_write_transform())
    self.read_test_pipeline2.not_use_test_runner_api = True
    with self.read_test_pipeline2 as p:
      rows = (
          p
          | "Read Updated chunks" >> ReadFromJdbc(
              table_name=self.default_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query))

      chunks = (
          rows
          | "To Chunks 2" >> beam.Map(row_to_chunk)
          | "Key on Index 2" >> beam.Map(key_on_id)
          | "Get First 500 2" >> beam.transforms.combiners.Top.Of(
              num_records, key=lambda x: x[0], reverse=True)
          | "Remove Keys 2" >> beam.Map(lambda xs: [x[1] for x in xs]))
      assert_that(chunks, equal_to([test_chunks]), label='updated_chunks_check')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
