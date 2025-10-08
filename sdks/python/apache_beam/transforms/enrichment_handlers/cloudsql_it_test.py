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
import functools
import logging
import os
import unittest
import uuid
from dataclasses import dataclass
from typing import Optional
from unittest.mock import MagicMock

import pytest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  from testcontainers.core.generic import DbContainer
  from testcontainers.postgres import PostgresContainer
  from testcontainers.mysql import MySqlContainer
  from testcontainers.mssql import SqlServerContainer
  from testcontainers.redis import RedisContainer
  from sqlalchemy import (
      create_engine, MetaData, Table, Column, Integer, VARCHAR, Engine)
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.cloudsql import (
      CloudSQLEnrichmentHandler,
      DatabaseTypeAdapter,
      CustomQueryConfig,
      TableFieldsQueryConfig,
      TableFunctionQueryConfig,
      CloudSQLConnectionConfig,
      ExternalSQLDBConnectionConfig,
      ConnectionConfig)
except ImportError as e:
  raise unittest.SkipTest(f'CloudSQL dependencies not installed: {str(e)}')

_LOGGER = logging.getLogger(__name__)


def where_clause_value_fn(row: beam.Row):
  return [row.id]  # type: ignore[attr-defined]


def query_fn(table, row: beam.Row):
  return f"SELECT * FROM {table} WHERE id = {row.id}"  # type: ignore[attr-defined]


@dataclass
class SQLDBContainerInfo:
  adapter: DatabaseTypeAdapter
  container: DbContainer
  host: str
  port: int
  user: str
  password: str
  id: str

  @property
  def address(self) -> str:
    return f"{self.host}:{self.port}"

  @property
  def url(self) -> str:
    return self.adapter.to_sqlalchemy_dialect() + "://"


class SQLEnrichmentTestHelper:
  @staticmethod
  def start_sql_db_container(
      database_type: DatabaseTypeAdapter,
      sql_client_retries=3) -> Optional[SQLDBContainerInfo]:
    info = None
    for i in range(sql_client_retries):
      sql_db_container = DbContainer("")
      try:
        if database_type == DatabaseTypeAdapter.POSTGRESQL:
          user, password, db_id = "test", "test", "test"
          sql_db_container = PostgresContainer(
              image="postgres:16",
              username=user,
              password=password,
              dbname=db_id,
              driver=database_type.value)
          sql_db_container.start()
          host = sql_db_container.get_container_host_ip()
          port = int(sql_db_container.get_exposed_port(5432))
        elif database_type == DatabaseTypeAdapter.MYSQL:
          user, password, db_id = "test", "test", "test"
          sql_db_container = MySqlContainer(
              image="mysql:8.0",
              username=user,
              root_password=password,
              password=password,
              dbname=db_id)
          sql_db_container.start()
          host = sql_db_container.get_container_host_ip()
          port = int(sql_db_container.get_exposed_port(3306))
        elif database_type == DatabaseTypeAdapter.SQLSERVER:
          user, password, db_id = "SA", "A_Str0ng_Required_Password", "tempdb"
          sql_db_container = SqlServerContainer(
              image="mcr.microsoft.com/mssql/server:2022-latest",
              username=user,
              password=password,
              dbname=db_id,
              dialect=database_type.to_sqlalchemy_dialect())
          sql_db_container.start()
          host = sql_db_container.get_container_host_ip()
          port = int(sql_db_container.get_exposed_port(1433))
        else:
          raise ValueError(f"Unsupported database type: {database_type}")

        info = SQLDBContainerInfo(
            adapter=database_type,
            container=sql_db_container,
            host=host,
            port=port,
            user=user,
            password=password,
            id=db_id)
        _LOGGER.info(
            "%s container started successfully on %s.",
            database_type.name,
            info.address)
        break
      except Exception as e:
        stdout_logs, stderr_logs = sql_db_container.get_logs()
        stdout_logs = stdout_logs.decode("utf-8")
        stderr_logs = stderr_logs.decode("utf-8")
        _LOGGER.warning(
            "Retry %d/%d: Failed to start %s container. Reason: %s. "
            "STDOUT logs:\n%s\nSTDERR logs:\n%s",
            i + 1,
            sql_client_retries,
            database_type.name,
            e,
            stdout_logs,
            stderr_logs)
        if i == sql_client_retries - 1:
          _LOGGER.error(
              "Unable to start %s container for I/O tests after %d "
              "retries. Tests cannot proceed. STDOUT logs:\n%s\n"
              "STDERR logs:\n%s",
              database_type.name,
              sql_client_retries,
              stdout_logs,
              stderr_logs)
          raise e

    return info

  @staticmethod
  def stop_sql_db_container(db_info: SQLDBContainerInfo):
    try:
      _LOGGER.debug("Stopping %s container.", db_info.adapter.name)
      db_info.container.stop()
      _LOGGER.info("%s container stopped successfully.", db_info.adapter.name)
    except Exception as e:
      _LOGGER.warning(
          "Error encountered while stopping %s container: %s",
          db_info.adapter.name,
          e)

  @staticmethod
  def create_table(
      table_id: str,
      engine: Engine,
      columns: list[Column],
      table_data: list[dict],
      metadata: MetaData):
    # Create table metadata.
    table = Table(table_id, metadata, *columns)

    # Create contextual connection for schema creation.
    with engine.connect() as schema_connection:
      try:
        metadata.create_all(schema_connection)
        schema_connection.commit()
      except Exception as e:
        schema_connection.rollback()
        raise RuntimeError(f"Failed to create table schema: {e}")

    # Now create a separate contextual connection for data insertion.
    with engine.connect() as connection:
      try:
        connection.execute(table.insert(), table_data)
        connection.commit()
      except Exception as e:
        connection.rollback()
        raise Exception(f"Failed to insert table data: {e}")


class BaseTestSQLEnrichment(unittest.TestCase):
  _cache_client_retries = 3
  _table_data = [
      {
          "id": 1, "name": "A", 'quantity': 2, 'distribution_center_id': 3
      },
      {
          "id": 2, "name": "B", 'quantity': 3, 'distribution_center_id': 1
      },
      {
          "id": 3, "name": "C", 'quantity': 10, 'distribution_center_id': 4
      },
      {
          "id": 4, "name": "D", 'quantity': 1, 'distribution_center_id': 3
      },
      {
          "id": 5, "name": "C", 'quantity': 100, 'distribution_center_id': 4
      },
      {
          "id": 6, "name": "D", 'quantity': 11, 'distribution_center_id': 3
      },
      {
          "id": 7, "name": "C", 'quantity': 7, 'distribution_center_id': 1
      },
      {
          "id": 8, "name": "D", 'quantity': 4, 'distribution_center_id': 1
      },
  ]

  @classmethod
  def setUpClass(cls):
    if not hasattr(cls, '_connection_config') or not hasattr(cls, '_metadata'):
      # Skip setup for the base class.
      raise unittest.SkipTest(
          "Base class - no connection_config or metadata defined")

    # Type hint data from subclasses.
    cls._table_id: str
    cls._connection_config: ConnectionConfig
    cls._metadata: MetaData

    connector = cls._connection_config.get_connector_handler()
    cls._engine = create_engine(
        url=cls._connection_config.get_db_url(), creator=connector)

    SQLEnrichmentTestHelper.create_table(
        table_id=cls._table_id,
        engine=cls._engine,
        columns=cls.get_columns(),
        table_data=cls._table_data,
        metadata=cls._metadata)

  @classmethod
  def get_columns(cls):
    """Returns fresh column objects each time it's called."""
    return [
        Column("id", Integer, nullable=False),
        Column("name", VARCHAR(255), nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("distribution_center_id", Integer, nullable=False),
    ]

  @pytest.fixture
  def cache_container(self):
    self._start_cache_container()

    # Hand control to the test.
    yield

    self._cache_container.stop()
    self._cache_container = None

  def _start_cache_container(self):
    for i in range(self._cache_client_retries):
      try:
        self._cache_container = RedisContainer(image="redis:7.2.4")
        self._cache_container.start()
        host = self._cache_container.get_container_host_ip()
        port = self._cache_container.get_exposed_port(6379)
        self._cache_container_host = host
        self._cache_container_port = port
        self._cache_client = self._cache_container.get_client()
        break
      except Exception as e:
        if i == self._cache_client_retries - 1:
          _LOGGER.error(
              "Unable to start redis container for RRIO tests after "
              "%d retries.",
              self._cache_client_retries)
          raise e

  @classmethod
  def tearDownClass(cls):
    # Drop all tables using metadata as the primary approach.
    cls._metadata.drop_all(cls._engine)

    # Fallback to raw SQL drop if needed.
    try:
      with cls._engine.connect() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {cls._table_id}")
        conn.commit()
        _LOGGER.info("Dropped table %s", cls._table_id)
    except Exception as e:
      _LOGGER.warning("Failed to drop table %s: %s", cls._table_id, e)

    cls._engine.dispose(close=True)
    cls._engine = None

  def test_sql_enrichment(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    fields = ['id']
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]

    query_config = TableFieldsQueryConfig(
        table_id=self._table_id,
        where_clause_template="id = :id_param",
        where_clause_fields=fields)

    handler = CloudSQLEnrichmentHandler(
        connection_config=self._connection_config,
        query_config=query_config,
        min_batch_size=1,
        max_batch_size=100,
    )

    with TestPipeline() as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_sql_enrichment_batched(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    fields = ['id']
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]

    query_config = TableFieldsQueryConfig(
        table_id=self._table_id,
        where_clause_template="id = :id",
        where_clause_fields=fields)

    handler = CloudSQLEnrichmentHandler(
        connection_config=self._connection_config,
        query_config=query_config,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline() as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_sql_enrichment_batched_multiple_fields(self):
    expected_rows = [
        beam.Row(id=1, distribution_center_id=3, name="A", quantity=2),
        beam.Row(id=2, distribution_center_id=1, name="B", quantity=3)
    ]
    fields = ['id', 'distribution_center_id']
    requests = [
        beam.Row(id=1, distribution_center_id=3),
        beam.Row(id=2, distribution_center_id=1),
    ]

    query_config = TableFieldsQueryConfig(
        table_id=self._table_id,
        where_clause_template="id = :id AND distribution_center_id = :param_1",
        where_clause_fields=fields)

    handler = CloudSQLEnrichmentHandler(
        connection_config=self._connection_config,
        query_config=query_config,
        min_batch_size=8,
        max_batch_size=100,
    )
    with TestPipeline() as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_sql_enrichment_with_query_fn(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    fn = functools.partial(query_fn, self._table_id)

    query_config = CustomQueryConfig(query_fn=fn)

    handler = CloudSQLEnrichmentHandler(
        connection_config=self._connection_config, query_config=query_config)
    with TestPipeline() as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_sql_enrichment_with_condition_value_fn(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]

    query_config = TableFunctionQueryConfig(
        table_id=self._table_id,
        where_clause_template="id = :param_0",
        where_clause_value_fn=where_clause_value_fn)

    handler = CloudSQLEnrichmentHandler(
        connection_config=self._connection_config,
        query_config=query_config,
        min_batch_size=2,
        max_batch_size=100)
    with TestPipeline() as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_sql_enrichment_on_non_existent_table(self):
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]

    query_config = TableFunctionQueryConfig(
        table_id=self._table_id,
        where_clause_template="id = :id",
        where_clause_value_fn=where_clause_value_fn)

    handler = CloudSQLEnrichmentHandler(
        connection_config=self._connection_config,
        query_config=query_config,
        column_names=["wrong_column"],
    )

    with self.assertRaises(Exception) as context:
      with TestPipeline() as p:
        _ = (p | beam.Create(requests) | Enrichment(handler))

    expect_err_msg_contains = (
        "Could not execute the query. Please check if the query is properly "
        "formatted and the table exists.")
    self.assertIn(expect_err_msg_contains, str(context.exception))

  @pytest.mark.usefixtures("cache_container")
  def test_sql_enrichment_with_redis(self):
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]

    query_config = TableFunctionQueryConfig(
        table_id=self._table_id,
        where_clause_template="id = :param_0",
        where_clause_value_fn=where_clause_value_fn)

    handler = CloudSQLEnrichmentHandler(
        connection_config=self._connection_config,
        query_config=query_config,
        min_batch_size=2,
        max_batch_size=100)
    with TestPipeline() as test_pipeline:
      pcoll_populate_cache = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(
              self._cache_container_host, self._cache_container_port))

      assert_that(pcoll_populate_cache, equal_to(expected_rows))

    # Manually check cache entry to verify entries were correctly stored.
    c = coders.StrUtf8Coder()
    for req in requests:
      key = handler.get_cache_key(req)
      response = self._cache_client.get(c.encode(key))
      if not response:
        raise ValueError("No cache entry found for %s" % key)

    # Mocks the CloudSQL enrichment handler to prevent actual database calls.
    # This ensures that a cache hit scenario does not trigger any database
    # interaction, raising an exception if an unexpected call occurs.
    actual = CloudSQLEnrichmentHandler.__call__
    CloudSQLEnrichmentHandler.__call__ = MagicMock(
        side_effect=Exception("Database should not be called on a cache hit."))

    # Run a second pipeline to verify cache is being used.
    with TestPipeline() as test_pipeline:
      pcoll_cached = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(
              self._cache_container_host, self._cache_container_port))

      assert_that(pcoll_cached, equal_to(expected_rows))

    # Restore the original CloudSQL enrichment handler implementation.
    CloudSQLEnrichmentHandler.__call__ = actual


class BaseCloudSQLDBEnrichment(BaseTestSQLEnrichment):
  @classmethod
  def setUpClass(cls):
    if not hasattr(cls, '_db_adapter'):
      # Skip setup for the base class.
      raise unittest.SkipTest("Base class - no db_adapter defined")

    # Type hint data from subclasses.
    cls._db_adapter: DatabaseTypeAdapter
    cls._instance_connection_uri: str
    cls._user: str
    cls._password: str
    cls._db_id: str

    cls._connection_config = CloudSQLConnectionConfig(
        db_adapter=cls._db_adapter,
        instance_connection_uri=cls._instance_connection_uri,
        user=cls._user,
        password=cls._password,
        db_id=cls._db_id)
    super().setUpClass()

  @classmethod
  def tearDownClass(cls):
    super().tearDownClass()


@unittest.skipUnless(
    os.environ.get('ALLOYDB_PASSWORD'),
    "ALLOYDB_PASSWORD environment var is not provided")
class TestCloudSQLPostgresEnrichment(BaseCloudSQLDBEnrichment):
  _db_adapter = DatabaseTypeAdapter.POSTGRESQL

  # Configuration required for locating the CloudSQL instance.
  _unique_suffix = str(uuid.uuid4())[:8]
  _table_id = f"product_details_cloudsql_pg_enrichment_{_unique_suffix}"
  _gcp_project_id = "apache-beam-testing"
  _region = "us-central1"
  _instance_name = "beam-integration-tests"
  _instance_connection_uri = f"{_gcp_project_id}:{_region}:{_instance_name}"

  # Configuration required for authenticating to the CloudSQL instance.
  _user = "postgres"
  _password = os.getenv("ALLOYDB_PASSWORD")
  _db_id = "postgres"

  _metadata = MetaData()


class BaseExternalSQLDBEnrichment(BaseTestSQLEnrichment):
  @classmethod
  def setUpClass(cls):
    if not hasattr(cls, '_db_adapter'):
      # Skip setup for the base class.
      raise unittest.SkipTest("Base class - no db_adapter defined")

    # Type hint data from subclasses.
    cls._db_adapter: DatabaseTypeAdapter

    cls._db = SQLEnrichmentTestHelper.start_sql_db_container(cls._db_adapter)
    cls._connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=cls._db_adapter,
        host=cls._db.host,
        user=cls._db.user,
        password=cls._db.password,
        db_id=cls._db.id,
        port=cls._db.port)
    super().setUpClass()

  @classmethod
  def tearDownClass(cls):
    super().tearDownClass()
    SQLEnrichmentTestHelper.stop_sql_db_container(cls._db)
    cls._db = None


class TestExternalPostgresEnrichment(BaseExternalSQLDBEnrichment):
  _db_adapter = DatabaseTypeAdapter.POSTGRESQL
  _unique_suffix = str(uuid.uuid4())[:8]
  _table_id = f"product_details_external_pg_enrichment_{_unique_suffix}"
  _metadata = MetaData()


class TestExternalMySQLEnrichment(BaseExternalSQLDBEnrichment):
  _db_adapter = DatabaseTypeAdapter.MYSQL
  _unique_suffix = str(uuid.uuid4())[:8]
  _table_id = f"product_details_external_mysql_enrichment_{_unique_suffix}"
  _metadata = MetaData()


class TestExternalSQLServerEnrichment(BaseExternalSQLDBEnrichment):
  _db_adapter = DatabaseTypeAdapter.SQLSERVER
  _unique_suffix = str(uuid.uuid4())[:8]
  _table_id = f"product_details_external_mssql_enrichment_{_unique_suffix}"
  _metadata = MetaData()


if __name__ == "__main__":
  unittest.main()
