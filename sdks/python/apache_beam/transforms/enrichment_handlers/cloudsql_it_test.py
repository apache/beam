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
from unittest.mock import MagicMock

import pytest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  from testcontainers.postgres import PostgresContainer
  from testcontainers.redis import RedisContainer
  from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.cloudsql import (
      CloudSQLEnrichmentHandler, DatabaseTypeAdapter, DatabaseTypeAdapter)
except ImportError:
  raise unittest.SkipTest('Google Cloud SQL dependencies are not installed.')

_LOGGER = logging.getLogger(__name__)


def where_clause_value_fn(row: beam.Row):
  return [row.id]  # type: ignore[attr-defined]


def query_fn(table, row: beam.Row):
  return f"SELECT * FROM `{table}` WHERE id = {row.id}"  # type: ignore[attr-defined]


@pytest.mark.uses_testcontainer
class CloudSQLEnrichmentIT(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls._sql_client_retries = 3
    cls._start_sql_db_container()

  @classmethod
  def tearDownClass(cls):
    cls._stop_sql_db_container()

  @classmethod
  def _start_sql_db_container(cls):
    for i in range(cls._sql_client_retries):
      try:
        cls._sql_db_container = PostgresContainer(image="postgres:16")
        cls._sql_db_container.start()
        cls.sql_db_container_host = cls._sql_db_container.get_container_host_ip(
        )
        cls.sql_db_container_port = cls._sql_db_container.get_exposed_port(5432)
        cls.database_type_adapter = DatabaseTypeAdapter.POSTGRESQL
        cls.sql_db_user, cls.sql_db_password, cls.sql_db_id = "test", "test", "test"
        _LOGGER.info(
            f"PostgreSQL container started successfully on {cls.get_db_address()}."
        )
        break
      except Exception as e:
        _LOGGER.warning(
            f"Retry {i + 1}/{cls._sql_client_retries}: Failed to start PostgreSQL container. Reason: {e}"
        )
        if i == cls._sql_client_retries - 1:
          _LOGGER.error(
              f"Unable to start PostgreSQL container for IO tests after {cls._sql_client_retries} retries. Tests cannot proceed."
          )
          raise e

  @classmethod
  def _stop_sql_db_container(cls):
    try:
      _LOGGER.info("Stopping PostgreSQL container.")
      cls._sql_db_container.stop()
      cls._sql_db_container = None
      _LOGGER.info("PostgreSQL container stopped successfully.")
    except Exception as e:
      _LOGGER.warning(
          f"Error encountered while stopping PostgreSQL container: {e}")

  @classmethod
  def get_db_address(cls):
    return f"{cls.sql_db_container_host}:{cls.sql_db_container_port}"


@pytest.mark.uses_testcontainer
class TestCloudSQLEnrichment(CloudSQLEnrichmentIT):
  _table_id = "product_details"
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
    super(TestCloudSQLEnrichment, cls).setUpClass()
    cls.create_table(cls._table_id)
    cls._cache_client_retries = 3

  @classmethod
  def create_table(cls, table_id):
    cls._engine = create_engine(cls._get_db_url())

    # Define the table schema.
    metadata = MetaData()
    table = Table(
        table_id,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("distribution_center_id", Integer, nullable=False),
    )

    # Create the table in the database.
    metadata.create_all(cls._engine)

    # Insert data into the table.
    with cls._engine.connect() as connection:
      transaction = connection.begin()
      try:
        connection.execute(table.insert(), cls._table_data)
        transaction.commit()
      except Exception as e:
        transaction.rollback()
        raise e

  @classmethod
  def _get_db_url(cls):
    dialect = cls.database_type_adapter.to_sqlalchemy_dialect()
    db_url = f"{dialect}://{cls.sql_db_user}:{cls.sql_db_password}@{cls.get_db_address()}/{cls.sql_db_id}"
    return db_url

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
        self._cache_container_host = self._cache_container.get_container_host_ip(
        )
        self._cache_container_port = self._cache_container.get_exposed_port(
            6379)
        self._cache_client = self._cache_container.get_client()
        break
      except Exception as e:
        if i == self._cache_client_retries - 1:
          _LOGGER.error(
              f"Unable to start redis container for RRIO tests after {self._cache_client_retries} retries."
          )
          raise e

  @classmethod
  def tearDownClass(cls):
    cls._engine.dispose(close=True)
    super(TestCloudSQLEnrichment, cls).tearDownClass()
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
        database_type_adapter=self.database_type_adapter,
        database_address=self.get_db_address(),
        database_user=self.sql_db_user,
        database_password=self.sql_db_password,
        database_id=self.sql_db_id,
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
        database_type_adapter=self.database_type_adapter,
        database_address=self.get_db_address(),
        database_user=self.sql_db_user,
        database_password=self.sql_db_password,
        database_id=self.sql_db_id,
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
        database_type_adapter=self.database_type_adapter,
        database_address=self.get_db_address(),
        database_user=self.sql_db_user,
        database_password=self.sql_db_password,
        database_id=self.sql_db_id,
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
        database_type_adapter=self.database_type_adapter,
        database_address=self.get_db_address(),
        database_user=self.sql_db_user,
        database_password=self.sql_db_password,
        database_id=self.sql_db_id,
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
        database_type_adapter=self.database_type_adapter,
        database_address=self.get_db_address(),
        database_user=self.sql_db_user,
        database_password=self.sql_db_password,
        database_id=self.sql_db_id,
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
        database_type_adapter=self.database_type_adapter,
        database_address=self.get_db_address(),
        database_user=self.sql_db_user,
        database_password=self.sql_db_password,
        database_id=self.sql_db_id,
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
        database_type_adapter=self.database_type_adapter,
        database_address=self.get_db_address(),
        database_user=self.sql_db_user,
        database_password=self.sql_db_password,
        database_id=self.sql_db_id,
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
