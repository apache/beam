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
import unittest
from unittest.mock import MagicMock
import pytest
import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.cloudsql import (
    CloudSQLEnrichmentHandler,
    DatabaseTypeAdapter,
    ExceptionLevel,
)
from testcontainers.redis import RedisContainer
from google.cloud.sql.connector import Connector
import os

_LOGGER = logging.getLogger(__name__)


def _row_key_fn(request: beam.Row, key_id="product_id") -> tuple[str]:
  key_value = str(getattr(request, key_id))
  return (key_id, key_value)


class ValidateResponse(beam.DoFn):
  """ValidateResponse validates if a PCollection of `beam.Row`
    has the required fields."""
  def __init__(
      self,
      n_fields: int,
      fields: list[str],
      enriched_fields: dict[str, list[str]],
  ):
    self.n_fields = n_fields
    self._fields = fields
    self._enriched_fields = enriched_fields

  def process(self, element: beam.Row, *args, **kwargs):
    element_dict = element.as_dict()
    if len(element_dict.keys()) != self.n_fields:
      raise BeamAssertException(
          "Expected %d fields in enriched PCollection:" % self.n_fields)

    for field in self._fields:
      if field not in element_dict or element_dict[field] is None:
        raise BeamAssertException(f"Expected a not None field: {field}")

    for key in self._enriched_fields:
      if key not in element_dict:
        raise BeamAssertException(
            f"Response from Cloud SQL should contain {key} column.")


def create_rows(cursor):
  """Insert test rows into the Cloud SQL database table."""
  cursor.execute(
      """
        CREATE TABLE IF NOT EXISTS products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(255),
            product_stock INT
        )
        """)
  cursor.execute(
      """
        INSERT INTO products (product_name, product_stock)
        VALUES
        ('pixel 5', 2),
        ('pixel 6', 4),
        ('pixel 7', 20),
        ('pixel 8', 10),
        ('iphone 11', 3),
        ('iphone 12', 7),
        ('iphone 13', 8),
        ('iphone 14', 3)
        ON CONFLICT DO NOTHING
        """)


@pytest.mark.uses_testcontainer
class TestCloudSQLEnrichment(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.project_id = "apache-beam-testing"
    cls.region_id = "us-central1"
    cls.instance_id = "beam-test"
    cls.database_id = "postgres"
    cls.database_user = os.getenv("BEAM_TEST_CLOUDSQL_PG_USER")
    cls.database_password = os.getenv("BEAM_TEST_CLOUDSQL_PG_PASSWORD")
    cls.table_id = "products"
    cls.row_key = "product_id"
    cls.database_type_adapter = DatabaseTypeAdapter.POSTGRESQL
    cls.req = [
        beam.Row(sale_id=1, customer_id=1, product_id=1, quantity=1),
        beam.Row(sale_id=3, customer_id=3, product_id=2, quantity=3),
        beam.Row(sale_id=5, customer_id=5, product_id=3, quantity=2),
        beam.Row(sale_id=7, customer_id=7, product_id=4, quantity=1),
    ]
    cls.connector = Connector()
    cls.client = cls.connector.connect(
        f"{cls.project_id}:{cls.region_id}:{cls.instance_id}",
        driver=cls.database_type_adapter.value,
        db=cls.database_id,
        user=cls.database_user,
        password=cls.database_password,
    )
    cls.cursor = cls.client.cursor()
    create_rows(cls.cursor)
    cls.cache_client_retries = 3

  def _start_cache_container(self):
    for i in range(self.cache_client_retries):
      try:
        self.container = RedisContainer(image="redis:7.2.4")
        self.container.start()
        self.host = self.container.get_container_host_ip()
        self.port = self.container.get_exposed_port(6379)
        self.cache_client = self.container.get_client()
        break
      except Exception as e:
        if i == self.cache_client_retries - 1:
          _LOGGER.error(
              f"Unable to start redis container for RRIO tests after {self.cache_client_retries} retries."
          )
          raise e

  @classmethod
  def tearDownClass(cls):
    cls.cursor.close()
    cls.client.close()
    cls.connector.close()
    cls.cursor, cls.client, cls.connector = None, None, None

  def test_enrichment_with_cloudsql(self):
    expected_fields = [
        "sale_id",
        "customer_id",
        "product_id",
        "quantity",
        "product_name",
        "product_stock",
    ]
    expected_enriched_fields = ["product_id", "product_name", "product_stock"]
    cloudsql = CloudSQLEnrichmentHandler(
        region_id=self.region_id,
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_type_adapter=self.database_type_adapter,
        database_id=self.database_id,
        database_user=self.database_user,
        database_password=self.database_password,
        table_id=self.table_id,
        row_key=self.row_key,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ CloudSQL" >> Enrichment(cloudsql)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields,
              )))

  def test_enrichment_with_cloudsql_no_enrichment(self):
    expected_fields = ["sale_id", "customer_id", "product_id", "quantity"]
    expected_enriched_fields = {}
    cloudsql = CloudSQLEnrichmentHandler(
        region_id=self.region_id,
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_type_adapter=self.database_type_adapter,
        database_id=self.database_id,
        database_user=self.database_user,
        database_password=self.database_password,
        table_id=self.table_id,
        row_key=self.row_key,
    )
    req = [beam.Row(sale_id=1, customer_id=1, product_id=99, quantity=1)]
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create(req)
          | "Enrich W/ CloudSQL" >> Enrichment(cloudsql)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields,
              )))

  def test_enrichment_with_cloudsql_raises_key_error(self):
    cloudsql = CloudSQLEnrichmentHandler(
        region_id=self.region_id,
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_type_adapter=self.database_type_adapter,
        database_id=self.database_id,
        database_user=self.database_user,
        database_password=self.database_password,
        table_id=self.table_id,
        row_key="car_name",
    )
    with self.assertRaises(KeyError):
      test_pipeline = TestPipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ CloudSQL" >> Enrichment(cloudsql))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_enrichment_with_cloudsql_raises_not_found(self):
    """Raises a database error when the GCP Cloud SQL table doesn't exist."""
    table_id = "invalid_table"
    cloudsql = CloudSQLEnrichmentHandler(
        region_id=self.region_id,
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_type_adapter=self.database_type_adapter,
        database_id=self.database_id,
        database_user=self.database_user,
        database_password=self.database_password,
        table_id=table_id,
        row_key=self.row_key,
    )
    try:
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ CloudSQL" >> Enrichment(cloudsql))
      res = test_pipeline.run()
      res.wait_until_finish()
    except (PgDatabaseError, RuntimeError) as e:
      self.assertIn(f'relation "{table_id}" does not exist', str(e))

  def test_enrichment_with_cloudsql_exception_level(self):
    """raises a `ValueError` exception when the GCP Cloud SQL query returns
        an empty row."""
    cloudsql = CloudSQLEnrichmentHandler(
        region_id=self.region_id,
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_type_adapter=self.database_type_adapter,
        database_id=self.database_id,
        database_user=self.database_user,
        database_password=self.database_password,
        table_id=self.table_id,
        row_key=self.row_key,
        exception_level=ExceptionLevel.RAISE,
    )
    req = [beam.Row(sale_id=1, customer_id=1, product_id=11, quantity=1)]
    with self.assertRaises(ValueError):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(req)
          | "Enrich W/ CloudSQL" >> Enrichment(cloudsql))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_cloudsql_enrichment_with_lambda(self):
    expected_fields = [
        "sale_id",
        "customer_id",
        "product_id",
        "quantity",
        "product_name",
        "product_stock",
    ]
    expected_enriched_fields = ["product_id", "product_name", "product_stock"]
    cloudsql = CloudSQLEnrichmentHandler(
        region_id=self.region_id,
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_type_adapter=self.database_type_adapter,
        database_id=self.database_id,
        database_user=self.database_user,
        database_password=self.database_password,
        table_id=self.table_id,
        row_key_fn=_row_key_fn,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ CloudSQL" >> Enrichment(cloudsql)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields)))

  @pytest.fixture
  def cache_container(self):
    # Setup phase: start the container.
    self._start_cache_container()

    # Hand control to the test.
    yield

    # Cleanup phase: stop the container. It runs after the test completion
    # even if it failed.
    self.container.stop()
    self.container = None

  @pytest.mark.usefixtures("cache_container")
  def test_cloudsql_enrichment_with_redis(self):
    expected_fields = [
        "sale_id",
        "customer_id",
        "product_id",
        "quantity",
        "product_name",
        "product_stock",
    ]
    expected_enriched_fields = ["product_id", "product_name", "product_stock"]
    cloudsql = CloudSQLEnrichmentHandler(
        region_id=self.region_id,
        project_id=self.project_id,
        instance_id=self.instance_id,
        database_type_adapter=self.database_type_adapter,
        database_id=self.database_id,
        database_user=self.database_user,
        database_password=self.database_password,
        table_id=self.table_id,
        row_key_fn=_row_key_fn,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create1" >> beam.Create(self.req)
          | "Enrich W/ CloudSQL1" >> Enrichment(cloudsql).with_redis_cache(
              self.host, self.port, 300)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields,
              )))

    # Manually check cache entry to verify entries were correctly stored.
    c = coders.StrUtf8Coder()
    for req in self.req:
      key = cloudsql.get_cache_key(req)
      response = self.cache_client.get(c.encode(key))
      if not response:
        raise ValueError("No cache entry found for %s" % key)

    # Mock the CloudSQL handler to avoid actual database calls.
    # This simulates a cache hit scenario by returning predefined data.
    actual = CloudSQLEnrichmentHandler.__call__
    CloudSQLEnrichmentHandler.__call__ = MagicMock(
        return_value=(
            beam.Row(sale_id=1, customer_id=1, product_id=1, quantity=1),
            beam.Row(),
        ))

    # Run a second pipeline to verify cache is being used.
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create2" >> beam.Create(self.req)
          | "Enrich W/ CloudSQL2" >> Enrichment(cloudsql).with_redis_cache(
              self.host, self.port)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields)))
    CloudSQLEnrichmentHandler.__call__ = actual


if __name__ == "__main__":
  unittest.main()
