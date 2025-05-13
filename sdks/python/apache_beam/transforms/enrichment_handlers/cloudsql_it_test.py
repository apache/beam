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
import unittest
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
  from testcontainers.redis import RedisContainer
  from sqlalchemy import (
      create_engine, MetaData, Table, Column, Integer, String, Engine)
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.cloudsql import (
      CloudSQLEnrichmentHandler,
      DatabaseTypeAdapter,
  )
except ImportError:
  raise unittest.SkipTest('Google Cloud SQL dependencies are not installed.')

_LOGGER = logging.getLogger(__name__)


def where_clause_value_fn(row: beam.Row):
  return [row.id]  # type: ignore[attr-defined]


def query_fn(table, row: beam.Row):
  return f"SELECT * FROM `{table}` WHERE id = {row.id}"  # type: ignore[attr-defined]


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
    dialect = self.adapter.to_sqlalchemy_dialect()
    return f"{dialect}://{self.user}:{self.password}@{self.address}/{self.id}"


class CloudSQLEnrichmentTestHelper:
  @staticmethod
  def start_sql_db_container(
      sql_client_retries=3) -> Optional[SQLDBContainerInfo]:
    info = None
    for i in range(sql_client_retries):
      try:
        database_type_adapter = DatabaseTypeAdapter.POSTGRESQL
        sql_db_container = PostgresContainer(image="postgres:16")
        sql_db_container.start()
        host = sql_db_container.get_container_host_ip()
        port = sql_db_container.get_exposed_port(5432)
        user, password, db_id = "test", "test", "test"
        info = SQLDBContainerInfo(
            adapter=database_type_adapter,
            container=sql_db_container,
            host=host,
            port=port,
            user=user,
            password=password,
            id=db_id)
        _LOGGER.info(
            "PostgreSQL container started successfully on %s.", info.address)
        break
      except Exception as e:
        _LOGGER.warning(
            "Retry %d/%d: Failed to start PostgreSQL container. Reason: %s",
            i + 1,
            sql_client_retries,
            e)
        if i == sql_client_retries - 1:
          _LOGGER.error(
              "Unable to start PostgreSQL container for IO tests after %d "
              "retries. Tests cannot proceed.",
              sql_client_retries)
          raise e

    return info

  @staticmethod
  def stop_sql_db_container(sql_db: DbContainer):
    try:
      _LOGGER.debug("Stopping PostgreSQL container.")
      sql_db.stop()
      _LOGGER.info("PostgreSQL container stopped successfully.")
    except Exception as e:
      _LOGGER.warning(
          "Error encountered while stopping PostgreSQL container: %s", e)

  @staticmethod
  def create_table(
      table_id: str,
      db_url: str,
      columns: list[Column],
      table_data: list[dict],
      metadata: MetaData = MetaData()) -> Engine:
    engine = create_engine(db_url)
    table = Table(table_id, metadata, *columns)
    metadata.create_all(engine)

    # Insert data into the table.
    with engine.connect() as connection:
      transaction = connection.begin()
      try:
        connection.execute(table.insert(), table_data)
        transaction.commit()
        return engine
      except Exception as e:
        transaction.rollback()
        raise e


@pytest.mark.uses_testcontainer
class TestCloudSQLEnrichment(unittest.TestCase):
  _table_id = "product_details"
  _columns = [
      Column("id", Integer, primary_key=True),
      Column("name", String, nullable=False),
      Column("quantity", Integer, nullable=False),
      Column("distribution_center_id", Integer, nullable=False),
  ]
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
    cls.db = CloudSQLEnrichmentTestHelper.start_sql_db_container()
    cls._engine = CloudSQLEnrichmentTestHelper.create_table(
        cls._table_id, cls.db.url, cls._columns, cls._table_data)
    cls._cache_client_retries = 3

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
    cls._engine.dispose(close=True)
    CloudSQLEnrichmentTestHelper.stop_sql_db_container(cls.db.container)
    cls._engine = None

  def test_cloudsql_enrichment(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    fields = ['id']
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    handler = CloudSQLEnrichmentHandler(
        database_type_adapter=self.db.adapter,
        database_address=self.db.address,
        database_user=self.db.user,
        database_password=self.db.id,
        database_id=self.db.id,
        table_id=self._table_id,
        where_clause_template="id = {}",
        where_clause_fields=fields,
        min_batch_size=1,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_cloudsql_enrichment_batched(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    fields = ['id']
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    handler = CloudSQLEnrichmentHandler(
        database_type_adapter=self.db.adapter,
        database_address=self.db.address,
        database_user=self.db.user,
        database_password=self.db.password,
        database_id=self.db.id,
        table_id=self._table_id,
        where_clause_template="id = {}",
        where_clause_fields=fields,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_cloudsql_enrichment_batched_multiple_fields(self):
    expected_rows = [
        beam.Row(id=1, distribution_center_id=3, name="A", quantity=2),
        beam.Row(id=2, distribution_center_id=1, name="B", quantity=3)
    ]
    fields = ['id', 'distribution_center_id']
    requests = [
        beam.Row(id=1, distribution_center_id=3),
        beam.Row(id=2, distribution_center_id=1),
    ]
    handler = CloudSQLEnrichmentHandler(
        database_type_adapter=self.db.adapter,
        database_address=self.db.address,
        database_user=self.db.user,
        database_password=self.db.password,
        database_id=self.db.id,
        table_id=self._table_id,
        where_clause_template="id = {} AND distribution_center_id = {}",
        where_clause_fields=fields,
        min_batch_size=8,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_cloudsql_enrichment_with_query_fn(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    fn = functools.partial(query_fn, self._table_id)
    handler = CloudSQLEnrichmentHandler(
        database_type_adapter=self.db.adapter,
        database_address=self.db.address,
        database_user=self.db.user,
        database_password=self.db.password,
        database_id=self.db.id,
        query_fn=fn)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_cloudsql_enrichment_with_condition_value_fn(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    handler = CloudSQLEnrichmentHandler(
        database_type_adapter=self.db.adapter,
        database_address=self.db.address,
        database_user=self.db.user,
        database_password=self.db.password,
        database_id=self.db.id,
        table_id=self._table_id,
        where_clause_template="id = {}",
        where_clause_value_fn=where_clause_value_fn,
        min_batch_size=2,
        max_batch_size=100)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_cloudsql_enrichment_table_nonexistent_runtime_error_raised(self):
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    handler = CloudSQLEnrichmentHandler(
        database_type_adapter=self.db.adapter,
        database_address=self.db.address,
        database_user=self.db.user,
        database_password=self.db.password,
        database_id=self.db.id,
        table_id=self._table_id,
        where_clause_template="id = {}",
        where_clause_value_fn=where_clause_value_fn,
        column_names=["wrong_column"],
    )
    with self.assertRaises(RuntimeError):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(requests)
          | "Enrichment" >> Enrichment(handler))
      res = test_pipeline.run()
      res.wait_until_finish()

  @pytest.mark.usefixtures("cache_container")
  def test_cloudsql_enrichment_with_redis(self):
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    handler = CloudSQLEnrichmentHandler(
        database_type_adapter=self.db.adapter,
        database_address=self.db.address,
        database_user=self.db.user,
        database_password=self.db.password,
        database_id=self.db.id,
        table_id=self._table_id,
        where_clause_template="id = {}",
        where_clause_value_fn=where_clause_value_fn,
        min_batch_size=2,
        max_batch_size=100)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll_populate_cache = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(self.host, self.port))

      assert_that(pcoll_populate_cache, equal_to(expected_rows))

    # Manually check cache entry to verify entries were correctly stored.
    c = coders.StrUtf8Coder()
    for req in requests:
      key = handler.get_cache_key(req)
      response = self._cache_client.get(c.encode(key))
      if not response:
        raise ValueError("No cache entry found for %s" % key)

    # Mock the CloudSQL enrichment handler to avoid actual database calls.
    # This simulates a cache hit scenario by returning predefined data.
    actual = CloudSQLEnrichmentHandler.__call__
    CloudSQLEnrichmentHandler.__call__ = MagicMock(return_value=(beam.Row()))

    # Run a second pipeline to verify cache is being used.
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll_cached = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(self.host, self.port))

      assert_that(pcoll_cached, equal_to(expected_rows))

    # Restore the original CloudSQL enrichment handler implementation.
    CloudSQLEnrichmentHandler.__call__ = actual


if __name__ == "__main__":
  unittest.main()
