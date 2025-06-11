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
import os
import secrets
import time
import unittest

import pytest
import sqlalchemy
from google.cloud.sql.connector import Connector
from sqlalchemy import text

import apache_beam as beam
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.ml.rag.ingestion import test_utils
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteTransform
from apache_beam.ml.rag.ingestion.cloudsql import CloudSQLPostgresVectorWriterConfig
from apache_beam.ml.rag.ingestion.cloudsql import LanguageConnectorConfig
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@pytest.mark.uses_gcp_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
@unittest.skipUnless(
    os.environ.get('ALLOYDB_PASSWORD'),
    "ALLOYDB_PASSWORD environment var is not provided")
class CloudSQLPostgresVectorWriterConfigTest(unittest.TestCase):
  POSTGRES_TABLE_PREFIX = 'python_rag_postgres_'

  @classmethod
  def _create_engine(cls):
    """Create SQLAlchemy engine using Cloud SQL connector."""
    def getconn():
      conn = cls.connector.connect(
          cls.instance_uri,
          "pg8000",
          user=cls.username,
          password=cls.password,
          db=cls.database,
      )
      return conn

    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    return engine

  @classmethod
  def setUpClass(cls):
    cls.database = os.environ.get('POSTGRES_DATABASE', 'postgres')
    cls.username = os.environ.get('POSTGRES_USERNAME', 'postgres')
    if not os.environ.get('ALLOYDB_PASSWORD'):
      raise ValueError('ALLOYDB_PASSWORD env not set')
    cls.password = os.environ.get('ALLOYDB_PASSWORD')
    cls.instance_uri = os.environ.get(
        'POSTGRES_INSTANCE_URI',
        'apache-beam-testing:us-central1:beam-integration-tests')

    # Create unique table name suffix
    cls.table_suffix = '%d%s' % (int(time.time()), secrets.token_hex(3))

    # Setup database connection
    cls.connector = Connector(refresh_strategy="LAZY")
    cls.engine = cls._create_engine()

  def skip_if_dataflow_runner(self):
    if self._runner and "dataflowrunner" in self._runner.lower():
      self.skipTest(
          "Skipping some tests on Dataflow Runner to avoid bloat and timeouts")

  def setUp(self):
    self.write_test_pipeline = TestPipeline(is_integration_test=True)
    self.read_test_pipeline = TestPipeline(is_integration_test=True)
    self._runner = type(self.read_test_pipeline.runner).__name__

    self.default_table_name = f"{self.POSTGRES_TABLE_PREFIX}" \
      f"{self.table_suffix}"

    # Create test table
    with self.engine.connect() as connection:
      connection.execute(
          text(
              f"""
                CREATE TABLE {self.default_table_name} (
                    id TEXT PRIMARY KEY,
                    embedding VECTOR({test_utils.VECTOR_SIZE}),
                    content TEXT,
                    metadata JSONB
                )
            """))
      connection.commit()
    _LOGGER = logging.getLogger(__name__)
    _LOGGER.info("Created table %s", self.default_table_name)

  def tearDown(self):
    # Drop test table
    with self.engine.connect() as connection:
      connection.execute(
          text(f"DROP TABLE IF EXISTS {self.default_table_name}"))
      connection.commit()
    _LOGGER = logging.getLogger(__name__)
    _LOGGER.info("Dropped table %s", self.default_table_name)

  @classmethod
  def tearDownClass(cls):
    if hasattr(cls, 'connector'):
      cls.connector.close()
    if hasattr(cls, 'engine'):
      cls.engine.dispose()

  def test_language_connector(self):
    """Test language connector."""
    self.skip_if_dataflow_runner()

    connection_config = LanguageConnectorConfig(
        username=self.username,
        password=self.password,
        database_name=self.database,
        instance_name=self.instance_uri)
    writer_config = CloudSQLPostgresVectorWriterConfig(
        connection_config=connection_config, table_name=self.default_table_name)

    # Create test chunks
    num_records = 150
    sample_size = min(500, num_records // 2)
    chunks = test_utils.ChunkTestUtils.get_expected_values(0, num_records)

    self.write_test_pipeline.not_use_test_runner_api = True

    with self.write_test_pipeline as p:
      _ = (
          p
          | beam.Create(chunks)
          | VectorDatabaseWriteTransform(writer_config))

    self.read_test_pipeline.not_use_test_runner_api = True
    read_query = f"""
          SELECT 
              CAST(id AS VARCHAR(255)),
              CAST(content AS VARCHAR(255)),
              CAST(embedding AS text),
              CAST(metadata AS text)
          FROM {self.default_table_name}
          """

    with self.read_test_pipeline as p:
      rows = (
          p
          | ReadFromJdbc(
              table_name=self.default_table_name,
              driver_class_name="org.postgresql.Driver",
              jdbc_url=writer_config.connector_config.to_connection_config(
              ).jdbc_url,
              username=self.username,
              password=self.password,
              query=read_query,
              classpath=writer_config.connector_config.additional_jdbc_args()
              ['classpath']))

      count_result = rows | "Count All" >> beam.combiners.Count.Globally()
      assert_that(count_result, equal_to([num_records]), label='count_check')

      chunks = (rows | "To Chunks" >> beam.Map(test_utils.row_to_chunk))
      chunk_hashes = chunks | "Hash Chunks" >> beam.CombineGlobally(
          test_utils.HashingFn())
      assert_that(
          chunk_hashes,
          equal_to([test_utils.generate_expected_hash(num_records)]),
          label='hash_check')

      # Sample validation
      first_n = (
          chunks
          | "Key on Index" >> beam.Map(test_utils.key_on_id)
          | f"Get First {sample_size}" >> beam.transforms.combiners.Top.Of(
              sample_size, key=lambda x: x[0], reverse=True)
          | "Remove Keys 1" >> beam.Map(lambda xs: [x[1] for x in xs]))
      expected_first_n = test_utils.ChunkTestUtils.get_expected_values(
          0, sample_size)
      assert_that(
          first_n,
          equal_to([expected_first_n]),
          label=f"first_{sample_size}_check")

      last_n = (
          chunks
          | "Key on Index 2" >> beam.Map(test_utils.key_on_id)
          | f"Get Last {sample_size}" >> beam.transforms.combiners.Top.Of(
              sample_size, key=lambda x: x[0])
          | "Remove Keys 2" >> beam.Map(lambda xs: [x[1] for x in xs]))
      expected_last_n = test_utils.ChunkTestUtils.get_expected_values(
          num_records - sample_size, num_records)[::-1]
      assert_that(
          last_n,
          equal_to([expected_last_n]),
          label=f"last_{sample_size}_check")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
