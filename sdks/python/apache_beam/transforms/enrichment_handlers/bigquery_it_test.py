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
import secrets
import time
import unittest
from unittest.mock import MagicMock

import pytest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  from testcontainers.redis import RedisContainer
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery import \
    BigQueryEnrichmentHandler
  from apitools.base.py.exceptions import HttpError
except ImportError:
  raise unittest.SkipTest(
      'Google Cloud BigQuery dependencies are not installed.')

_LOGGER = logging.getLogger(__name__)


def condition_value_fn(row: beam.Row):
  return [row.id]  # type: ignore[attr-defined]


def query_fn(table, row: beam.Row):
  return f"SELECT * FROM `{table}` WHERE id = {row.id}"  # type: ignore[attr-defined]


@pytest.mark.uses_testcontainer
class BigQueryEnrichmentIT(unittest.TestCase):
  bigquery_dataset_id = 'python_enrichment_transform_read_table_'
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
      _LOGGER.debug(
          "Deleting dataset %s in project %s", cls.dataset_id, cls.project)
      cls.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      _LOGGER.warning(
          'Failed to clean up dataset %s in project %s',
          cls.dataset_id,
          cls.project)


@pytest.mark.uses_testcontainer
class TestBigQueryEnrichmentIT(BigQueryEnrichmentIT):
  table_data = [
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
  def create_table(cls, table_name):
    fields = [('id', 'INTEGER'), ('name', 'STRING'), ('quantity', 'INTEGER'),
              ('distribution_center_id', 'INTEGER')]
    table_schema = bigquery.TableSchema()
    for name, field_type in fields:
      table_field = bigquery.TableFieldSchema()
      table_field.name = name
      table_field.type = field_type
      table_schema.fields.append(table_field)
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
    super(TestBigQueryEnrichmentIT, cls).setUpClass()
    cls.create_table('product_details')

  def setUp(self) -> None:
    self.condition_template = "id = {}"
    self.retries = 5
    self._start_container()

  def _start_container(self):
    for i in range(self.retries):
      try:
        self.container = RedisContainer(image='redis:7.2.4')
        self.container.start()
        self.host = self.container.get_container_host_ip()
        self.port = self.container.get_exposed_port(6379)
        self.client = self.container.get_client()
        break
      except Exception as e:
        if i == self.retries - 1:
          _LOGGER.error(
              'Unable to start redis container for BigQuery '
              ' enrichment tests.')
          raise e
        # Add a small delay between retries to avoid rapid successive failures
        time.sleep(2)

  def tearDown(self) -> None:
    self.container.stop()
    self.client = None

  def test_bigquery_enrichment(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    fields = ['id']
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template="id = {}",
        table_name=self.table_name,
        fields=fields,
        min_batch_size=1,
        max_batch_size=100,
    )

    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_bigquery_enrichment_batched(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    fields = ['id']
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template="id = {}",
        table_name=self.table_name,
        fields=fields,
        min_batch_size=2,
        max_batch_size=100,
    )

    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_bigquery_enrichment_batched_multiple_fields(self):
    expected_rows = [
        beam.Row(id=1, distribution_center_id=3, name="A", quantity=2),
        beam.Row(id=2, distribution_center_id=1, name="B", quantity=3)
    ]
    fields = ['id', 'distribution_center_id']
    requests = [
        beam.Row(id=1, distribution_center_id=3),
        beam.Row(id=2, distribution_center_id=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template="id = {} AND distribution_center_id = {}",
        table_name=self.table_name,
        fields=fields,
        min_batch_size=8,
        max_batch_size=100,
    )

    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_bigquery_enrichment_with_query_fn(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    fn = functools.partial(query_fn, self.table_name)
    handler = BigQueryEnrichmentHandler(project=self.project, query_fn=fn)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_bigquery_enrichment_with_condition_value_fn(self):
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        condition_value_fn=condition_value_fn,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll = (test_pipeline | beam.Create(requests) | Enrichment(handler))

      assert_that(pcoll, equal_to(expected_rows))

  def test_bigquery_enrichment_bad_request(self):
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        column_names=['wrong_column'],
        condition_value_fn=condition_value_fn,
    )
    with self.assertRaises(Exception):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(requests)
          | "Enrichment" >> Enrichment(handler))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_bigquery_enrichment_with_redis(self):
    """
    In this test, we run two pipelines back to back.

    In the first pipeline, we run a simple BigQuery enrichment pipeline
    with zero cache records. Therefore, it makes call to the source
    and ultimately writes to the cache with a TTL of 300 seconds.

    For the second pipeline, we mock the
    `BigQueryEnrichmentHandler`'s `__call__` method to always
    return a `None` response. However, this change won't impact the second
    pipeline because the Enrichment transform first checks the cache to fulfill
    requests. Since all requests are cached, it will return from there without
    making calls to the BigQuery service.
    """
    requests = [
        beam.Row(id=1, name='A'),
        beam.Row(id=2, name='B'),
    ]
    expected_rows = [
        beam.Row(id=1, name="A", quantity=2, distribution_center_id=3),
        beam.Row(id=2, name="B", quantity=3, distribution_center_id=1)
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        condition_value_fn=condition_value_fn,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll_populate_cache = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(self.host, self.port))

      assert_that(pcoll_populate_cache, equal_to(expected_rows))

    # manually check cache entry
    c = coders.StrUtf8Coder()
    for req in requests:
      key = handler.get_cache_key(req)
      response = self.client.get(c.encode(key))
      if not response:
        raise ValueError("No cache entry found for %s" % key)

    actual = BigQueryEnrichmentHandler.__call__
    BigQueryEnrichmentHandler.__call__ = MagicMock(return_value=(beam.Row()))

    with TestPipeline(is_integration_test=True) as test_pipeline:
      pcoll_cached = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler).with_redis_cache(self.host, self.port))

      assert_that(pcoll_cached, equal_to(expected_rows))
    BigQueryEnrichmentHandler.__call__ = actual


if __name__ == '__main__':
  unittest.main()
