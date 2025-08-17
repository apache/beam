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

import json
import logging
import os
import secrets
import time
import unittest
from dataclasses import dataclass
from typing import Any
from typing import List
from typing import Literal
from typing import Optional

import pytest
import sqlalchemy
from google.cloud.sql.connector import Connector
from parameterized import parameterized
from sqlalchemy import text

import apache_beam as beam
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.ml.rag.ingestion import mysql_common
from apache_beam.ml.rag.ingestion import postgres_common
from apache_beam.ml.rag.ingestion import test_utils
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteTransform
from apache_beam.ml.rag.ingestion.cloudsql import CloudSQLMySQLVectorWriterConfig
from apache_beam.ml.rag.ingestion.cloudsql import CloudSQLPostgresVectorWriterConfig
from apache_beam.ml.rag.ingestion.cloudsql import LanguageConnectorConfig
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)


@dataclass
class DatabaseTestConfig:
  """Database-specific test configuration."""
  database_type: Literal["postgresql", "mysql"]
  writer_config_class: type
  jdbc_driver: str
  connector_module: Literal["pg8000", "pymysql"]
  table_prefix: str

  password_env_var: str
  username: str
  database: str
  instance_uri: str

  vector_column_type: str
  metadata_column_type: str
  common_module: Any
  id_column_type: str = "VARCHAR(255)"


class DatabaseTestHelper:
  """Helper class to manage database setup, connections, and operations."""
  def __init__(self, db_config: DatabaseTestConfig, table_suffix: str):
    self.db_config = db_config
    self.table_suffix = table_suffix
    self.connector = None
    self.engine = None
    self.connection_config = None

    self.default_table_name = f"{db_config.table_prefix}{table_suffix}"
    self.custom_table_name = f"{db_config.table_prefix}_custom_{table_suffix}"
    self.metadata_conflicts_table = f"{db_config.table_prefix}_meta_conf_" \
      f"{table_suffix}"

    self._setup_read_queries()

  def _setup_read_queries(self):
    if self.db_config.database_type == "postgresql":
      self.read_queries = {
          self.default_table_name: f"""
                    SELECT 
                        CAST(id AS VARCHAR(255)),
                        CAST(content AS VARCHAR(255)),
                        CAST(embedding AS text),
                        CAST(metadata AS text)
                    FROM {self.default_table_name}
                """,
          self.custom_table_name: f"""
                    SELECT 
                        CAST(custom_id AS VARCHAR(255)),
                        CAST(embedding_vec AS text),
                        CAST(content_col AS VARCHAR(255)),
                        CAST(metadata AS text)
                    FROM {self.custom_table_name}
                    ORDER BY custom_id
                """,
          self.metadata_conflicts_table: f"""
                    SELECT 
                        CAST(id AS VARCHAR(255)),
                        CAST(embedding AS text),
                        CAST(content AS VARCHAR(255)),
                        CAST(source AS VARCHAR(255)),
                        CAST(timestamp AS VARCHAR(255))
                    FROM {self.metadata_conflicts_table}
                    ORDER BY timestamp, id
                """
      }
    elif self.db_config.database_type == "mysql":
      self.read_queries = {
          self.default_table_name: f"""
                    SELECT 
                        CAST(id AS CHAR(255)) as id,
                        CAST(content AS CHAR(255)) as content,
                        vector_to_string(embedding) as embedding,
                        CAST(metadata AS CHAR(10000)) as metadata
                    FROM {self.default_table_name}
                """,
          self.custom_table_name: f"""
                    SELECT 
                        CAST(custom_id AS CHAR(255)) as custom_id,
                        vector_to_string(embedding_vec) as embedding_vec,
                        CAST(content_col AS CHAR(255)) as content_col,
                        CAST(metadata AS CHAR(10000)) as metadata
                    FROM {self.custom_table_name}
                    ORDER BY custom_id
                """,
          self.metadata_conflicts_table: f"""
                    SELECT 
                        CAST(id AS CHAR(255)) as id,
                        vector_to_string(embedding) as embedding,
                        CAST(content AS CHAR(255)) as content,
                        CAST(source AS CHAR(255)) as source,
                        CAST(timestamp AS CHAR(255)) as timestamp
                    FROM {self.metadata_conflicts_table}
                    ORDER BY timestamp, id
                """
      }

  def get_read_query(self, table_name: str) -> str:
    if table_name not in self.read_queries:
      raise ValueError(f"No read query defined for table: {table_name}")
    return self.read_queries[table_name]

  def setup_connection(self):
    """Set up database connection and engine."""
    if not os.environ.get(self.db_config.password_env_var):
      raise ValueError("Password environment variable not set.")
    password = os.environ.get(self.db_config.password_env_var)

    self.connection_config = LanguageConnectorConfig(
        username=self.db_config.username,
        password=password,
        database_name=self.db_config.database,
        instance_name=self.db_config.instance_uri)

    self.connector = Connector(refresh_strategy="LAZY")

    def getconn():
      return self.connector.connect(
          self.db_config.instance_uri,
          self.db_config.connector_module,
          user=self.db_config.username,
          password=password,
          db=self.db_config.database,
      )

    dialect = "postgresql+pg8000" \
      if self.db_config.database_type == "postgresql" else "mysql+pymysql"
    self.engine = sqlalchemy.create_engine(f"{dialect}://", creator=getconn)

  def create_all_tables(self):
    if not self.engine:
      raise ValueError("Engine not initialized. Call setup_connection() first.")

    vector_type_large = self.db_config.vector_column_type.format(
        size=test_utils.VECTOR_SIZE)
    vector_type_small = self.db_config.vector_column_type.format(size=2)
    metadata_type = self.db_config.metadata_column_type
    id_type = self.db_config.id_column_type

    with self.engine.connect() as connection:
      default_table_sql = f"""
                CREATE TABLE {self.default_table_name} (
                    id {id_type} PRIMARY KEY,
                    embedding {vector_type_large},
                    content TEXT,
                    metadata {metadata_type}
                )
            """
      connection.execute(text(default_table_sql))

      custom_table_sql = f"""
                CREATE TABLE {self.custom_table_name} (
                    custom_id {id_type} PRIMARY KEY,
                    embedding_vec {vector_type_small},
                    content_col TEXT,
                    metadata {metadata_type}
                )
            """
      connection.execute(text(custom_table_sql))

      if self.db_config.database_type == "postgresql":
        metadata_conflicts_sql = f"""
                    CREATE TABLE {self.metadata_conflicts_table} (
                        id {id_type},
                        source TEXT,
                        timestamp TIMESTAMP,
                        content TEXT,
                        embedding {vector_type_small},
                        PRIMARY KEY (id),
                        UNIQUE (source, timestamp)
                    )
                """
      elif self.db_config.database_type == "mysql":
        metadata_conflicts_sql = f"""
                    CREATE TABLE {self.metadata_conflicts_table} (
                        id {id_type},
                        source TEXT,
                        timestamp TIMESTAMP,
                        content TEXT,
                        embedding {vector_type_small},
                        PRIMARY KEY (id),
                        UNIQUE KEY unique_source_timestamp (source(255), timestamp)
                    )
                """
      connection.execute(text(metadata_conflicts_sql))
      connection.commit()

  def create_writer_config(
      self,
      table_name: Optional[str] = None,
      column_specs=None,
      conflict_resolution=None):
    if not self.connection_config:
      raise ValueError(
          "Connection not initialized. Call setup_connection() first.")

    table_name = table_name or self.default_table_name

    kwargs = {
        'connection_config': self.connection_config,
        'table_name': table_name,
    }

    if column_specs is not None:
      kwargs['column_specs'] = column_specs
    if conflict_resolution is not None:
      kwargs['conflict_resolution'] = conflict_resolution

    return self.db_config.writer_config_class(**kwargs)

  def cleanup(self):
    if self.engine:
      table_names = [
          self.default_table_name,
          self.custom_table_name,
          self.metadata_conflicts_table
      ]

      try:
        with self.engine.connect() as connection:
          for table_name in table_names:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
          connection.commit()
        _LOGGER.info(
            "Dropped %s tables: %s",
            self.db_config.database_type,
            ', '.join(table_names))
      except Exception as e:
        _LOGGER.warning(
            "Error dropping %s tables: %s", self.db_config.database_type, e)

    if self.connector:
      try:
        self.connector.close()
      except Exception as e:
        _LOGGER.warning("Error closing connector: %s", e)

    if self.engine:
      try:
        self.engine.dispose()
      except Exception as e:
        _LOGGER.warning("Error disposing engine: %s", e)


class PipelineVerificationHelper:
  """Helper class for common pipeline verification patterns."""
  @staticmethod
  def build_jdbc_params(helper: DatabaseTestHelper, table_name: str) -> dict:
    """Build JDBC parameters dictionary for ReadFromJdbc."""
    writer_config = helper.create_writer_config(table_name)

    return {
        'table_name': table_name,
        'driver_class_name': helper.db_config.jdbc_driver,
        'jdbc_url': writer_config.connector_config.to_connection_config().
        jdbc_url,
        'username': helper.db_config.username,
        'password': helper.connection_config.password,
        'query': helper.get_read_query(table_name),
        'classpath': writer_config.connector_config.additional_jdbc_args()
        ['classpath']
    }

  @staticmethod
  def verify_standard_operations(
      pipeline, jdbc_params: dict, expected_chunks: List[Chunk]):
    num_records = len(expected_chunks)
    sample_size = min(500, num_records // 2)

    with pipeline as p:
      rows = (p | ReadFromJdbc(**jdbc_params))

      # Count verification
      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      # Hash verification
      chunks = (rows | "To Chunks" >> beam.Map(test_utils.row_to_chunk))
      chunk_hashes = chunks | "Hash Chunks" >> beam.CombineGlobally(
          test_utils.HashingFn())
      expected_hash = test_utils.generate_expected_hash(num_records)
      assert_that(chunk_hashes, equal_to([expected_hash]), label='hash_check')

      # Sample validation - first N
      first_n = (
          chunks
          | "Key on Index" >> beam.Map(test_utils.key_on_id)
          | f"Get First {sample_size}" >> beam.transforms.combiners.Top.Of(
              sample_size, key=lambda x: x[0], reverse=True)
          | "Remove Keys 1" >> beam.Map(lambda xs: [x[1] for x in xs]))
      expected_first_n = expected_chunks[:sample_size]
      assert_that(
          first_n,
          equal_to([expected_first_n]),
          label=f"first_{sample_size}_check")

      # Sample validation - last N
      last_n = (
          chunks
          | "Key on Index 2" >> beam.Map(test_utils.key_on_id)
          | f"Get Last {sample_size}" >> beam.transforms.combiners.Top.Of(
              sample_size, key=lambda x: x[0])
          | "Remove Keys 2" >> beam.Map(lambda xs: [x[1] for x in xs]))
      expected_last_n = expected_chunks[-sample_size:][::-1]
      assert_that(
          last_n,
          equal_to([expected_last_n]),
          label=f"last_{sample_size}_check")


# Database configurations
POSTGRES_CONFIG = DatabaseTestConfig(
    database_type="postgresql",
    writer_config_class=CloudSQLPostgresVectorWriterConfig,
    jdbc_driver="org.postgresql.Driver",
    connector_module="pg8000",
    table_prefix="python_rag_postgres_",
    password_env_var="ALLOYDB_PASSWORD",
    username="postgres",
    database="postgres",
    instance_uri="apache-beam-testing:us-central1:beam-integration-tests",
    vector_column_type="VECTOR({size})",
    metadata_column_type="JSONB",
    common_module=postgres_common)

MYSQL_CONFIG = DatabaseTestConfig(
    database_type="mysql",
    writer_config_class=CloudSQLMySQLVectorWriterConfig,
    jdbc_driver="com.mysql.cj.jdbc.Driver",
    connector_module="pymysql",
    table_prefix="python_rag_mysql_",
    password_env_var="ALLOYDB_PASSWORD",
    username="mysql",
    database="embeddings",
    instance_uri="apache-beam-testing:us-central1:beam-integration-tests-mysql",
    vector_column_type="VECTOR({size}) USING VARBINARY",
    metadata_column_type="JSON",
    common_module=mysql_common)


@pytest.mark.uses_gcp_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
class CloudSQLVectorWriterConfigTest(unittest.TestCase):
  def setUp(self):
    self.write_test_pipeline = TestPipeline(is_integration_test=True)
    self.read_test_pipeline = TestPipeline(is_integration_test=True)
    self.write_test_pipeline2 = TestPipeline(is_integration_test=True)
    self.read_test_pipeline2 = TestPipeline(is_integration_test=True)

    self.write_test_pipeline.not_use_test_runner_api = True
    self.read_test_pipeline.not_use_test_runner_api = True
    self.write_test_pipeline2.not_use_test_runner_api = True
    self.read_test_pipeline2.not_use_test_runner_api = True
    self._runner = type(self.read_test_pipeline.runner).__name__

    self.db_helpers = {}
    self.table_suffix = '%d%s' % (int(time.time()), secrets.token_hex(3))

    # Set up database helpers
    for config in [POSTGRES_CONFIG, MYSQL_CONFIG]:
      helper = DatabaseTestHelper(config, self.table_suffix)
      helper.setup_connection()
      helper.create_all_tables()
      self.db_helpers[config.database_type] = helper
      _LOGGER.info("Successfully set up %s database", config.database_type)

  def tearDown(self):
    for helper in self.db_helpers.values():
      helper.cleanup()

  def skip_if_dataflow_runner(self):
    if self._runner and "dataflowrunner" in self._runner.lower():
      self.skipTest(
          "Skipping some tests on Dataflow Runner to avoid bloat and timeouts")

  @parameterized.expand([(POSTGRES_CONFIG), (MYSQL_CONFIG)])
  def test_default_config(self, db_config):
    """Test basic write and read operations with default configuration.
      
      This test validates the most basic CloudSQL vector database functionality:
      - Default table schema: id (VARCHAR), content (TEXT), embedding (VECTOR),
        metadata (JSON/JSONB) 
      - Default column specifications (no customization)
      - Default conflict resolution (IGNORE on primary key conflicts)
      - Write chunks to database and read them back
      - Verify data integrity through count, hash, and sample validation
      """
    self.skip_if_dataflow_runner()

    helper = self.db_helpers[db_config.database_type]
    num_records = 150

    # Create test data
    test_chunks = test_utils.ChunkTestUtils.get_expected_values(0, num_records)

    # Write test
    writer_config = helper.create_writer_config()
    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | beam.Create(test_chunks)
          | VectorDatabaseWriteTransform(writer_config))

    # Read and verify
    self.read_test_pipeline.not_use_test_runner_api = True
    jdbc_params = PipelineVerificationHelper.build_jdbc_params(
        helper, helper.default_table_name)
    PipelineVerificationHelper.verify_standard_operations(
        self.read_test_pipeline, jdbc_params, test_chunks)

  @parameterized.expand([
      (POSTGRES_CONFIG, "UPDATE", ["embedding", "content"]),
      (MYSQL_CONFIG, "UPDATE", ["embedding", "content"]),
      (POSTGRES_CONFIG, "IGNORE", None),
      (MYSQL_CONFIG, "IGNORE", None),
      (POSTGRES_CONFIG, "UPDATE_ALL", None),  # Default update fields
      (MYSQL_CONFIG, "UPDATE_ALL", None),
  ])
  def test_conflict_resolution(self, db_config, action, update_fields):
    """Test conflict resolution strategies when primary key conflicts occur.
    
      This test validates different approaches to handling duplicate primary
      keys:
      
      UPDATE with specific fields:
      - When duplicate ID encountered, update only specified fields (embedding,
        content)
      - Other fields (metadata) remain unchanged from original record
      
      IGNORE:
      - When duplicate ID encountered, keep original record unchanged
      
      UPDATE_ALL (default update fields):
      - When duplicate ID encountered, update ALL non-key fields
      - This includes content, embedding, AND metadata
      
      Scenario for all strategies:
      1. Insert initial records
      2. Insert records with same IDs but different content/embeddings  
      3. Verify final state matches expected conflict resolution behavior
      """
    self.skip_if_dataflow_runner()

    helper = self.db_helpers[db_config.database_type]
    num_records = 20

    common_module = db_config.common_module
    if action == "IGNORE":
      if db_config.database_type == "mysql":
        conflict_resolution = common_module.ConflictResolution(
            action="IGNORE", primary_key_field="id")
      else:
        conflict_resolution = None  # Default behavior for PostgreSQL
    elif action == "UPDATE":
      if db_config.database_type == "postgresql":
        conflict_resolution = common_module.ConflictResolution(
            on_conflict_fields="id",
            action="UPDATE",
            update_fields=update_fields)
      else:
        conflict_resolution = common_module.ConflictResolution(
            action="UPDATE", update_fields=update_fields)
    else:  # UPDATE_ALL
      if db_config.database_type == "postgresql":
        conflict_resolution = common_module.ConflictResolution(
            on_conflict_fields="id", action="UPDATE")
      else:
        conflict_resolution = common_module.ConflictResolution(action="UPDATE")

    initial_chunks = test_utils.ChunkTestUtils.get_expected_values(
        0, num_records)
    writer_config = helper.create_writer_config(
        conflict_resolution=conflict_resolution)

    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | "Write Initial" >> beam.Create(initial_chunks)
          | VectorDatabaseWriteTransform(writer_config))

    # Write conflicting data
    updated_chunks = test_utils.ChunkTestUtils.get_expected_values(
        0, num_records, content_prefix="Updated", seed_multiplier=2)

    self.write_test_pipeline2.not_use_test_runner_api = True
    with self.write_test_pipeline2 as p:
      _ = (
          p | "Write Conflicts" >> beam.Create(updated_chunks)
          | VectorDatabaseWriteTransform(writer_config))

    jdbc_params = PipelineVerificationHelper.build_jdbc_params(
        helper, helper.default_table_name)
    expected_chunks = updated_chunks if action != "IGNORE" else initial_chunks

    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (p | ReadFromJdbc(**jdbc_params))

      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      chunks = rows | "To Chunks" >> beam.Map(test_utils.row_to_chunk)
      assert_that(chunks, equal_to(expected_chunks), label='chunks_check')

  @parameterized.expand([(POSTGRES_CONFIG), (MYSQL_CONFIG)])
  def test_custom_column_names_and_value_functions(self, db_config):
    """Test completely custom column specifications with custom value
      extraction.
    
      This test validates advanced customization of how chunk data is stored:
      
      Custom column names:
      - custom_id (instead of 'id')
      - embedding_vec (instead of 'embedding') 
      - content_col (instead of 'content')
      
      Custom value extraction functions:
      - ID: Extract timestamp from metadata and prefix with "timestamp_"
      - Content: Prefix content with its character length "10:actual_content"
      - Embedding: Use custom embedding extraction function
      
      This tests the flexibility to completely reshape how chunk data maps 
      to database columns, useful for integrating with existing database schemas
      or applying business-specific transformations.
      """
    self.skip_if_dataflow_runner()

    helper = self.db_helpers[db_config.database_type]
    num_records = 20
    common_module = db_config.common_module

    test_chunks = [
        Chunk(
            id=str(i),
            content=Content(text=f"content_{i}"),
            embedding=Embedding(dense_embedding=[float(i), float(i + 1)]),
            metadata={"timestamp": f"2024-02-02T{i:02d}:00:00"})
        for i in range(num_records)
    ]

    chunk_embedding_fn = common_module.chunk_embedding_fn
    specs = (
        common_module.ColumnSpecsBuilder().add_custom_column_spec(
            common_module.ColumnSpec.text(
                column_name="custom_id",
                value_fn=lambda chunk:
                f"timestamp_{chunk.metadata.get('timestamp', '')}")
        ).add_custom_column_spec(
            common_module.ColumnSpec.vector(
                column_name="embedding_vec",
                value_fn=chunk_embedding_fn)).add_custom_column_spec(
                    common_module.ColumnSpec.text(
                        column_name="content_col",
                        value_fn=lambda chunk:
                        f"{len(chunk.content.text)}:{chunk.content.text}")).
        with_metadata_spec().build())

    def custom_row_to_chunk(row):
      timestamp = row.custom_id.split('timestamp_')[1]
      i = int(timestamp.split('T')[1][:2])

      embedding_list = [
          float(x) for x in row.embedding_vec.strip('[]').split(',')
      ]

      content = row.content_col.split(':', 1)[1]

      return Chunk(
          id=str(i),
          content=Content(text=content),
          embedding=Embedding(dense_embedding=embedding_list),
          metadata=json.loads(row.metadata))

    writer_config = helper.create_writer_config(helper.custom_table_name, specs)
    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | beam.Create(test_chunks)
          | VectorDatabaseWriteTransform(writer_config))

    jdbc_params = PipelineVerificationHelper.build_jdbc_params(
        helper, helper.custom_table_name)

    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (p | ReadFromJdbc(**jdbc_params))

      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      chunks = rows | "To Chunks" >> beam.Map(custom_row_to_chunk)
      assert_that(chunks, equal_to(test_chunks), label='chunks_check')

  @parameterized.expand([(POSTGRES_CONFIG), (MYSQL_CONFIG)])
  def test_custom_type_conversion_with_default_columns(self, db_config):
    """Test custom type conversion and SQL typecasting with modified column
      names.
    
      This test validates data type handling and database-specific SQL features:
      
      Type conversion:
      - Convert string IDs to integers before storage
      - Apply length-prefix transformation to content
      
      SQL typecasting (database-specific):
      - PostgreSQL: Use ::text typecast for converted integers
      - MySQL: Rely on automatic type conversion (no explicit typecast)
      
      Column name customization:
      - Use custom names but with standard spec builders (not completely custom
        functions)
      
      This tests the ability to adapt data types for database constraints
      while maintaining the standard chunk-to-database mapping logic.
    """
    self.skip_if_dataflow_runner()

    helper = self.db_helpers[db_config.database_type]
    num_records = 20
    common_module = db_config.common_module

    test_chunks = [
        Chunk(
            id=str(i),
            content=Content(text=f"content_{i}"),
            embedding=Embedding(dense_embedding=[float(i), float(i + 1)]),
            metadata={"timestamp": f"2024-02-02T{i:02d}:00:00"})
        for i in range(num_records)
    ]

    if db_config.database_type == "postgresql":
      specs = (
          common_module.ColumnSpecsBuilder().with_id_spec(
              column_name="custom_id",
              python_type=int,
              convert_fn=lambda x: int(x),
              sql_typecast="::text").with_content_spec(
                  column_name="content_col",
                  convert_fn=lambda x: f"{len(x)}:{x}"  # Add length prefix
              ).with_embedding_spec(
                  column_name="embedding_vec").with_metadata_spec().build())
    else:  # MySQL
      specs = (
          common_module.ColumnSpecsBuilder().with_id_spec(
              column_name="custom_id",
              python_type=int,
              convert_fn=lambda x: int(x)).with_content_spec(
                  column_name="content_col",
                  convert_fn=lambda x: f"{len(x)}:{x}").with_embedding_spec(
                      column_name="embedding_vec").with_metadata_spec().build())

    def type_conversion_row_to_chunk(row):
      embedding_list = [
          float(x) for x in row.embedding_vec.strip('[]').split(',')
      ]

      content = row.content_col.split(':', 1)[1]

      return Chunk(
          id=row.custom_id,  # custom_id is the converted ID field
          content=Content(text=content),
          embedding=Embedding(dense_embedding=embedding_list),
          metadata=json.loads(row.metadata))

    writer_config = helper.create_writer_config(helper.custom_table_name, specs)
    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | beam.Create(test_chunks)
          | VectorDatabaseWriteTransform(writer_config))

    jdbc_params = PipelineVerificationHelper.build_jdbc_params(
        helper, helper.custom_table_name)

    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (p | ReadFromJdbc(**jdbc_params))

      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      chunks = rows | "To Chunks" >> beam.Map(type_conversion_row_to_chunk)
      assert_that(chunks, equal_to(test_chunks), label='chunks_check')

  @parameterized.expand([(POSTGRES_CONFIG), (MYSQL_CONFIG)])
  def test_default_id_embedding_specs(self, db_config):
    """Test minimal schema with only ID and embedding columns.
  
      This test validates the ability to create a minimal vector database
      schema:
      - Only stores id and embedding fields
      - content and metadata columns are excluded from the table
      - Tests that the system correctly handles missing/null fields
      
      Use case: When you only need vector similarity search without storing 
      the original content or metadata (perhaps stored elsewhere).
      
      Validation:
      - Chunks written with content/metadata are stored with those fields as
        null
      - Reading back produces chunks with null content and empty metadata
      - Vector similarity operations still work normally
      """
    self.skip_if_dataflow_runner()

    helper = self.db_helpers[db_config.database_type]
    num_records = 20
    common_module = db_config.common_module

    specs = (
        common_module.ColumnSpecsBuilder().with_id_spec().with_embedding_spec().
        build())

    writer_config = helper.create_writer_config(column_specs=specs)

    test_chunks = test_utils.ChunkTestUtils.get_expected_values(0, num_records)

    with self.write_test_pipeline as p:
      _ = (
          p | beam.Create(test_chunks)
          | VectorDatabaseWriteTransform(writer_config))

    expected_chunks = test_utils.ChunkTestUtils.get_expected_values(
        0, num_records)
    for chunk in expected_chunks:
      chunk.content.text = None  # Content column not included in schema
      chunk.metadata = {}  # Metadata column not included in schema

    jdbc_params = PipelineVerificationHelper.build_jdbc_params(
        helper, helper.default_table_name)
    if db_config.database_type == "postgresql":
      jdbc_params['query'] = f"""
            SELECT 
                CAST(id AS VARCHAR(255)),
                CAST(embedding AS text)
            FROM {helper.default_table_name}
            ORDER BY id
        """
    elif db_config.database_type == "mysql":
      jdbc_params['query'] = f"""
            SELECT 
                CAST(id AS CHAR(255)) as id,
                vector_to_string(embedding) as embedding
            FROM {helper.default_table_name}
        """

    with self.read_test_pipeline as p:
      rows = (p | ReadFromJdbc(**jdbc_params))
      chunks = rows | "To Chunks" >> beam.Map(test_utils.row_to_chunk)
      assert_that(chunks, equal_to(expected_chunks), label='chunks_check')

  @parameterized.expand([(POSTGRES_CONFIG), (MYSQL_CONFIG)])
  def test_metadata_field_extraction(self, db_config):
    """Test extracting specific metadata fields into separate database columns.
      
      This test validates the ability to:
      - Extract specific fields from the JSON metadata object 
      - Map them to dedicated database columns (e.g., metadata.source -> source
        column)
      - Apply database-specific SQL typecasts (PostgreSQL ::timestamp vs MySQL
        default)
      - Store and retrieve the extracted fields correctly
      
      This is different from default metadata handling which stores the entire 
      metadata object as JSON in a single column.
      """
    self.skip_if_dataflow_runner()

    helper = self.db_helpers[db_config.database_type]
    num_records = 20
    common_module = db_config.common_module

    if db_config.database_type == "postgresql":
      specs = (
          common_module.ColumnSpecsBuilder().with_id_spec().with_embedding_spec(
          ).with_content_spec().add_metadata_field(
              field="source",
              column_name="source",
              python_type=str,
              sql_typecast=None).add_metadata_field(
                  field="timestamp",
                  python_type=str,
                  sql_typecast="::timestamp").build())
    else:
      specs = (
          common_module.ColumnSpecsBuilder().with_id_spec().with_embedding_spec(
          ).with_content_spec().add_metadata_field(
              field="source", column_name="source",
              python_type=str).add_metadata_field(
                  field="timestamp", python_type=str).build())

    writer_config = helper.create_writer_config(
        helper.metadata_conflicts_table, specs, conflict_resolution=None)

    test_chunks = [
        Chunk(
            id=str(i),
            content=Content(text=f"content_{i}"),
            embedding=Embedding(dense_embedding=[float(i), float(i + 1)]),
            metadata={
                "source": f"source_{i % 3}",
                "timestamp": f"2024-02-02T{i:02d}:00:00"
            }) for i in range(num_records)
    ]

    self.write_test_pipeline.not_use_test_runner_api = True
    with self.write_test_pipeline as p:
      _ = (
          p | beam.Create(test_chunks)
          | VectorDatabaseWriteTransform(writer_config))

    def metadata_row_to_chunk(row):
      embedding_list = [float(x) for x in row.embedding.strip('[]').split(',')]
      timestamp = row.timestamp.replace(
          ' ', 'T') if ' ' in row.timestamp else row.timestamp
      return Chunk(
          id=row.id,
          content=Content(text=row.content),
          embedding=Embedding(dense_embedding=embedding_list),
          metadata={
              "source": row.source, "timestamp": timestamp
          })

    jdbc_params = PipelineVerificationHelper.build_jdbc_params(
        helper, helper.metadata_conflicts_table)

    self.read_test_pipeline.not_use_test_runner_api = True
    with self.read_test_pipeline as p:
      rows = (p | ReadFromJdbc(**jdbc_params))
      chunks = rows | "To Chunks" >> beam.Map(metadata_row_to_chunk)
      assert_that(chunks, equal_to(test_chunks), label='chunks_check')

  @parameterized.expand([(POSTGRES_CONFIG), (MYSQL_CONFIG)])
  def test_composite_unique_constraint_conflicts(self, db_config):
    """Test conflict resolution when unique constraints span multiple columns.
      
      This test validates conflict resolution when the unique constraint is NOT 
      on the primary key, but on a combination of other columns (source +
      timestamp).
      
      Scenario:
      1. Insert records with unique (source, timestamp) combinations
      2. Attempt to insert records with same (source, timestamp) but different
         IDs and content
      3. Verify that conflict resolution (UPDATE) works correctly based on
         composite key
      
      This is different from test_conflict_resolution which tests conflicts on 
      the primary key field only.
      """
    self.skip_if_dataflow_runner()

    helper = self.db_helpers[db_config.database_type]
    num_records = 5
    common_module = db_config.common_module

    if db_config.database_type == "postgresql":
      specs = (
          common_module.ColumnSpecsBuilder().with_id_spec().with_embedding_spec(
          ).with_content_spec().add_metadata_field(
              field="source",
              column_name="source",
              python_type=str,
              sql_typecast=None).add_metadata_field(
                  field="timestamp",
                  python_type=str,
                  sql_typecast="::timestamp").build())

      conflict_resolution = common_module.ConflictResolution(
          on_conflict_fields=["source", "timestamp"],
          action="UPDATE",
          update_fields=["embedding", "content"])
    elif db_config.database_type == "mysql":
      specs = (
          common_module.ColumnSpecsBuilder().with_id_spec().with_embedding_spec(
          ).with_content_spec().add_metadata_field(
              field="source", column_name="source",
              python_type=str).add_metadata_field(
                  field="timestamp", python_type=str).build())

      # MySQL conflict resolution - detects unique constraint automatically
      conflict_resolution = common_module.ConflictResolution(
          action="UPDATE", update_fields=["embedding", "content"])

    writer_config = helper.create_writer_config(
        helper.metadata_conflicts_table, specs, conflict_resolution)

    initial_chunks = [
        Chunk(
            id=str(i),
            content=Content(text=f"content_{i}"),
            embedding=Embedding(dense_embedding=[float(i), float(i + 1)]),
            metadata={
                "source": "source_A", "timestamp": f"2024-02-02T{i:02d}:00:00"
            }) for i in range(num_records)
    ]

    with self.write_test_pipeline as p:
      _ = (
          p | "Write Initial" >> beam.Create(initial_chunks)
          | VectorDatabaseWriteTransform(writer_config))

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

    with self.write_test_pipeline2 as p:
      _ = (
          p | "Write Conflicts" >> beam.Create(conflicting_chunks)
          | VectorDatabaseWriteTransform(writer_config))

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
      embedding_list = [float(x) for x in row.embedding.strip('[]').split(',')]
      timestamp = row.timestamp.replace(
          ' ', 'T') if ' ' in row.timestamp else row.timestamp
      return Chunk(
          id=row.id,
          content=Content(text=row.content),
          embedding=Embedding(dense_embedding=embedding_list),
          metadata={
              "source": row.source, "timestamp": timestamp
          })

    jdbc_params = PipelineVerificationHelper.build_jdbc_params(
        helper, helper.metadata_conflicts_table)

    with self.read_test_pipeline as p:
      rows = (p | ReadFromJdbc(**jdbc_params))

      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      chunks = rows | "To Chunks" >> beam.Map(metadata_row_to_chunk)
      assert_that(chunks, equal_to(expected_chunks), label='chunks_check')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
