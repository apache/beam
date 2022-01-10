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
import random
import sys
import unittest
import uuid

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where spanner library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
# pylint: disable=unused-import
try:
  from google.cloud import spanner
  from apache_beam.io.gcp import resource_identifiers
  from apache_beam.io.gcp.experimental.spannerio import create_transaction
  from apache_beam.io.gcp.experimental.spannerio import ReadOperation
  from apache_beam.io.gcp.experimental.spannerio import ReadFromSpanner
  from apache_beam.metrics import monitoring_infos
  from apache_beam.metrics.execution import MetricsEnvironment
  from apache_beam.metrics.metricbase import MetricName
except ImportError:
  spanner = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports
# pylint: enable=unused-import

_LOGGER = logging.getLogger(__name__)
_TEST_INSTANCE_ID = 'beam-test'


@unittest.skipIf(spanner is None, 'GCP dependencies are not installed.')
class SpannerReadIntegrationTest(unittest.TestCase):
  TEST_DATABASE = None
  _database_prefix = "pybeam-read-{}"
  _data = None
  _SPANNER_CLIENT = None
  _SPANNER_INSTANCE = None

  @classmethod
  def _generate_table_name(cls):
    cls.TEST_DATABASE = cls._database_prefix.format(
        ''.join(random.sample(uuid.uuid4().hex, 15)))
    return cls.TEST_DATABASE

  @classmethod
  def _create_database(cls):
    _LOGGER.info("Creating test database: %s" % cls.TEST_DATABASE)
    instance = cls._SPANNER_INSTANCE
    database = instance.database(
        cls.TEST_DATABASE,
        ddl_statements=[
            """CREATE TABLE Users (
            UserId    INT64 NOT NULL,
            Key       STRING(1024)
        ) PRIMARY KEY (UserId)""",
        ])
    operation = database.create()
    _LOGGER.info("Creating database: Done! %s" % str(operation.result()))

  @classmethod
  def _add_dummy_entries(cls):
    _LOGGER.info("Dummy Data: Adding dummy data...")
    instance = cls._SPANNER_INSTANCE
    database = instance.database(cls.TEST_DATABASE)
    data = cls._data = [(x + 1, uuid.uuid4().hex) for x in range(200)]
    with database.batch() as batch:
      batch.insert(table='Users', columns=('UserId', 'Key'), values=data)

  @classmethod
  def setUpClass(cls):
    _LOGGER.info(".... PyVersion ---> %s" % str(sys.version))
    _LOGGER.info(".... Setting up!")
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.args = cls.test_pipeline.get_full_options_as_args()
    cls.runner_name = type(cls.test_pipeline.runner).__name__
    cls.project = cls.test_pipeline.get_option('project')
    cls.instance = (
        cls.test_pipeline.get_option('instance') or _TEST_INSTANCE_ID)
    _ = cls._generate_table_name()
    spanner_client = cls._SPANNER_CLIENT = spanner.Client()
    _LOGGER.info(".... Spanner Client created!")
    cls._SPANNER_INSTANCE = spanner_client.instance(cls.instance)
    cls._create_database()
    cls._add_dummy_entries()
    _LOGGER.info("Spanner Read IT Setup Complete...")

  @pytest.mark.spannerio_it
  def test_read_via_table(self):
    _LOGGER.info("Spanner Read via table")
    with beam.Pipeline(argv=self.args) as p:
      r = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          table="Users",
          columns=["UserId", "Key"])
    assert_that(r, equal_to(self._data))

  @pytest.mark.spannerio_it
  def test_read_via_sql(self):
    _LOGGER.info("Running Spanner via sql")
    with beam.Pipeline(argv=self.args) as p:
      r = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          sql="select * from Users")
    assert_that(r, equal_to(self._data))

  @pytest.mark.spannerio_it
  def test_transaction_table_metrics_ok_call(self):
    if 'DirectRunner' not in self.runner_name:
      raise unittest.SkipTest('This test only runs with DirectRunner.')

    MetricsEnvironment.process_wide_container().reset()

    with beam.Pipeline(argv=self.args) as p:
      transaction = (
          p
          | create_transaction(self.project, self.instance, self.TEST_DATABASE))
      r = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          table="Users",
          columns=["UserId", "Key"],
          transaction=transaction)

    assert_that(r, equal_to(self._data))
    self.verify_table_read_call_metric(
        self.project, self.TEST_DATABASE, 'Users', 'ok', 1)

  @pytest.mark.spannerio_it
  def test_transaction_table_metrics_error_call(self):
    if 'DirectRunner' not in self.runner_name:
      raise unittest.SkipTest('This test only runs with DirectRunner.')

    MetricsEnvironment.process_wide_container().reset()

    with self.assertRaises(Exception):
      p = beam.Pipeline(argv=self.args)
      transaction = (
          p
          | create_transaction(self.project, self.instance, self.TEST_DATABASE))
      _ = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          table="INVALID_TABLE",
          columns=["UserId", "Key"],
          transaction=transaction)

      res = p.run()
      res.wait_until_finish()

    self.verify_table_read_call_metric(
        self.project, self.TEST_DATABASE, 'INVALID_TABLE', '404', 1)

  @pytest.mark.spannerio_it
  def test_transaction_sql_metrics_ok_call(self):
    if 'DirectRunner' not in self.runner_name:
      raise unittest.SkipTest('This test only runs with DirectRunner.')

    MetricsEnvironment.process_wide_container().reset()

    with beam.Pipeline(argv=self.args) as p:
      transaction = (
          p
          | create_transaction(self.project, self.instance, self.TEST_DATABASE))
      r = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          sql="select * from Users",
          query_name='query-1',
          transaction=transaction)

    assert_that(r, equal_to(self._data))
    self.verify_sql_read_call_metric(
        self.project, self.TEST_DATABASE, 'query-1', 'ok', 1)

  @pytest.mark.spannerio_it
  def test_transaction_sql_metrics_error_call(self):
    if 'DirectRunner' not in self.runner_name:
      raise unittest.SkipTest('This test only runs with DirectRunner.')

    MetricsEnvironment.process_wide_container().reset()

    with self.assertRaises(Exception):
      p = beam.Pipeline(argv=self.args)
      transaction = (
          p
          | create_transaction(self.project, self.instance, self.TEST_DATABASE))
      _ = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          sql="select * from NonExistent",
          query_name="query-2",
          transaction=transaction)

      res = p.run()
      res.wait_until_finish()

    self.verify_sql_read_call_metric(
        self.project, self.TEST_DATABASE, 'query-2', '400', 1)

  @pytest.mark.spannerio_it
  def test_table_metrics_ok_call(self):
    if 'DirectRunner' not in self.runner_name:
      raise unittest.SkipTest('This test only runs with DirectRunner.')

    MetricsEnvironment.process_wide_container().reset()

    with beam.Pipeline(argv=self.args) as p:
      r = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          table="Users",
          columns=["UserId", "Key"])

    assert_that(r, equal_to(self._data))
    self.verify_table_read_call_metric(
        self.project, self.TEST_DATABASE, 'Users', 'ok', 1)

  @pytest.mark.spannerio_it
  def test_table_metrics_error_call(self):
    if 'DirectRunner' not in self.runner_name:
      raise unittest.SkipTest('This test only runs with DirectRunner.')

    MetricsEnvironment.process_wide_container().reset()

    with self.assertRaises(Exception):
      p = beam.Pipeline(argv=self.args)
      _ = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          table="INVALID_TABLE",
          columns=["UserId", "Key"])

      res = p.run()
      res.wait_until_finish()

      self.verify_table_read_call_metric(
          self.project, self.TEST_DATABASE, 'INVALID_TABLE', '404', 1)

  @pytest.mark.spannerio_it
  def test_sql_metrics_ok_call(self):
    if 'DirectRunner' not in self.runner_name:
      raise unittest.SkipTest('This test only runs with DirectRunner.')

    MetricsEnvironment.process_wide_container().reset()

    with beam.Pipeline(argv=self.args) as p:
      r = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          sql="select * from Users",
          query_name='query-1')

    assert_that(r, equal_to(self._data))
    self.verify_sql_read_call_metric(
        self.project, self.TEST_DATABASE, 'query-1', 'ok', 1)

  @pytest.mark.spannerio_it
  def test_sql_metrics_error_call(self):
    if 'DirectRunner' not in self.runner_name:
      raise unittest.SkipTest('This test only runs with DirectRunner.')

    MetricsEnvironment.process_wide_container().reset()

    with self.assertRaises(Exception):
      p = beam.Pipeline(argv=self.args)
      _ = p | ReadFromSpanner(
          self.project,
          self.instance,
          self.TEST_DATABASE,
          sql="select * from NonExistent",
          query_name='query-2')

      res = p.run()
      res.wait_until_finish()

      self.verify_sql_read_call_metric(
          self.project, self.TEST_DATABASE, 'query-2', '400', 1)

  def verify_table_read_call_metric(
      self, project, database, table, status, count):
    resource = resource_identifiers.SpannerTable(project, database, table)
    labels = {
        monitoring_infos.SERVICE_LABEL: 'Spanner',
        monitoring_infos.METHOD_LABEL: 'Read',
        monitoring_infos.SPANNER_PROJECT_ID: project,
        monitoring_infos.SPANNER_DATABASE_ID: database,
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.SPANNER_TABLE_ID: table,
        monitoring_infos.STATUS_LABEL: status
    }
    metric_name = MetricName(
        None, None, urn=monitoring_infos.API_REQUEST_COUNT_URN, labels=labels)
    metric_value = MetricsEnvironment.process_wide_container().get_counter(
        metric_name).get_cumulative()

    self.assertEqual(metric_value, count)

  def verify_sql_read_call_metric(
      self, project, database, query_name, status, count):
    resource = resource_identifiers.SpannerSqlQuery(project, query_name)
    labels = {
        monitoring_infos.SERVICE_LABEL: 'Spanner',
        monitoring_infos.METHOD_LABEL: 'Read',
        monitoring_infos.SPANNER_PROJECT_ID: project,
        monitoring_infos.SPANNER_DATABASE_ID: database,
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.SPANNER_QUERY_NAME: query_name,
        monitoring_infos.STATUS_LABEL: status
    }

    metric_name = MetricName(
        None, None, urn=monitoring_infos.API_REQUEST_COUNT_URN, labels=labels)
    metric_value = MetricsEnvironment.process_wide_container().get_counter(
        metric_name).get_cumulative()

    self.assertEqual(metric_value, count)

  @classmethod
  def tearDownClass(cls):
    # drop the testing database after the tests
    database = cls._SPANNER_INSTANCE.database(cls.TEST_DATABASE)
    database.drop()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
