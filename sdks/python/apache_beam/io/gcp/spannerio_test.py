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

from __future__ import absolute_import

import mock
import random
import string
import unittest

import apache_beam as beam
from apache_beam.io.gcp.spannerio import ReadFromSpanner
from apache_beam.io.gcp.spannerio import WriteToSpanner
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from google.cloud.spanner_v1.database import SnapshotCheckout

MAX_DB_NAME_LENGTH = 30

TEST_PROJECT_ID = 'apache-beam-testing'

TEST_INSTANCE_ID = 'beam-test'

TEST_DATABASE_PREFIX = 'raheel-testdb-'

FAKE_ROWS = [[1, 'Alice'], [2, 'Bob'], [3, 'Carl'], [4, 'Dan'], [5, 'Evan'],
             [6, 'Floyd']]

TEST_TABLE = 'users'

TEST_COLUMNS = ['Key', 'Value']


def _generate_database_name():
  mask = string.ascii_lowercase + string.digits
  length = MAX_DB_NAME_LENGTH - 1 - len(TEST_DATABASE_PREFIX)
  return TEST_DATABASE_PREFIX + ''.join(random.choice(mask) for i in range(
      length))


def _create_database(database_id):
  from google.cloud.spanner import Client
  spanner_client = Client(TEST_PROJECT_ID)
  instance = spanner_client.instance(TEST_INSTANCE_ID)
  database = instance.database(database_id, ddl_statements=[
      """CREATE TABLE users (
          Key     INT64 NOT NULL,
          Value    STRING(1024) NOT NULL,
      ) PRIMARY KEY (Key)"""
  ])
  database.create()


def _generate_test_data():
  mask = string.ascii_lowercase + string.digits
  length = 100
  return [
    ('users', ['Key', 'Value'], [(x, ''.join(random.choice(mask) for _ in range(
      length))) for x in range(1, 5)])
  ]


class ReadFromSpannerTest(unittest.TestCase):

  @mock.patch('apache_beam.io.gcp.spannerio.Client')
  @mock.patch('apache_beam.io.gcp.spannerio.BatchSnapshot')
  def test_read_with_query_batch(self, mock_batch_snapshot_class,
                                 mock_client_class):
    mock_client = mock.MagicMock()
    mock_instance = mock.MagicMock()
    mock_database = mock.MagicMock()
    mock_snapshot = mock.MagicMock()

    mock_client_class.return_value = mock_client
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.batch_snapshot.return_value = mock_snapshot
    mock_batch_snapshot_class.return_value = mock_snapshot
    mock_batch_snapshot_class.from_dict.return_value = mock_snapshot

    mock_snapshot.to_dict.return_value = dict()
    mock_snapshot.generate_query_batches.return_value = \
      [{'query': {'sql': 'SELECT * FROM users'},
        'partition': 'test_partition'} for x in range(3)]
    mock_snapshot.process_query_batch.side_effect = \
      [FAKE_ROWS[0:2], FAKE_ROWS[2:4], FAKE_ROWS[4:]]

    pipeline = TestPipeline()
    records = pipeline \
              | ReadFromSpanner(
                  TEST_PROJECT_ID,
                  TEST_INSTANCE_ID,
                  _generate_database_name()).with_query('SELECT * FROM users')
    assert_that(records, equal_to(FAKE_ROWS))
    pipeline.run()

  @mock.patch('apache_beam.io.gcp.spannerio.Client')
  @mock.patch('apache_beam.io.gcp.spannerio.BatchSnapshot')
  def test_read_with_table_batch(self, mock_batch_snapshot_class,
                                 mock_client_class):
    mock_client = mock.MagicMock()
    mock_instance = mock.MagicMock()
    mock_database = mock.MagicMock()
    mock_snapshot = mock.MagicMock()

    mock_client_class.return_value = mock_client
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.batch_snapshot.return_value = mock_snapshot
    mock_batch_snapshot_class.return_value = mock_snapshot
    mock_batch_snapshot_class.from_dict.return_value = mock_snapshot

    mock_snapshot.to_dict.return_value = dict()
    mock_snapshot.generate_read_batches.return_value = \
      [{
          'read': {'table': 'users', 'keyset': {'all': True},
                   'columns': ['Key', 'Value'], 'index': ''},
          'partition': 'test_partition'} for x in range(3)]
    mock_snapshot.process_read_batch.side_effect = \
      [FAKE_ROWS[0:2], FAKE_ROWS[2:4], FAKE_ROWS[4:]]

    pipeline = TestPipeline()
    records = pipeline | ReadFromSpanner(
        TEST_PROJECT_ID,
        TEST_INSTANCE_ID,
        _generate_database_name()).with_table('users', ['Key', 'Value'])
    assert_that(records, equal_to(FAKE_ROWS))
    pipeline.run()

  @mock.patch('apache_beam.io.gcp.spannerio.Client')
  def test_read_with_query_transaction(self, mock_client_class):
    mock_client = mock.MagicMock()
    mock_instance = mock.MagicMock()
    mock_database = mock.MagicMock()
    mock_snapshot_context = mock.MagicMock()
    mock_snapshot = mock.MagicMock()
    # because we have an explicit check on the instance type in
    # with_transaction
    mock_snapshot_context.mock_add_spec(SnapshotCheckout)
    mock_snapshot_context.__enter__.return_value = mock_snapshot

    mock_client_class.return_value = mock_client
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.snapshot.return_value = mock_snapshot_context
    mock_snapshot.execute_sql.return_value = FAKE_ROWS

    pipeline = TestPipeline()
    transaction = ReadFromSpanner.create_transaction(
        TEST_PROJECT_ID, TEST_INSTANCE_ID, _generate_database_name())
    records = pipeline \
              | ReadFromSpanner(TEST_PROJECT_ID,
                                TEST_INSTANCE_ID,
                                _generate_database_name()).with_transaction(
                                    transaction).with_query(
                                        'SELECT * FROM users')
    assert_that(records, equal_to(FAKE_ROWS))
    pipeline.run()

  @mock.patch('apache_beam.io.gcp.spannerio.Client')
  def test_read_with_table_transaction(self, mock_client_class):
    mock_client = mock.MagicMock()
    mock_instance = mock.MagicMock()
    mock_database = mock.MagicMock()
    mock_snapshot_context = mock.MagicMock()
    mock_snapshot = mock.MagicMock()
    # because we have an explicit check on the instance type in
    # with_transaction
    mock_snapshot_context.mock_add_spec(SnapshotCheckout)
    mock_snapshot_context.__enter__.return_value = mock_snapshot

    mock_client_class.return_value = mock_client
    mock_client.instance.return_value = mock_instance
    mock_instance.database.return_value = mock_database
    mock_database.snapshot.return_value = mock_snapshot_context
    mock_snapshot.read.return_value = FAKE_ROWS

    pipeline = TestPipeline()
    transaction = ReadFromSpanner.create_transaction(
        TEST_PROJECT_ID, TEST_INSTANCE_ID, _generate_database_name())
    records = pipeline \
              | ReadFromSpanner(TEST_PROJECT_ID,
                                TEST_INSTANCE_ID,
                                _generate_database_name()).with_transaction(
                                    transaction).with_table('users',
                                                            ['Key', 'Value'])
    assert_that(records, equal_to(FAKE_ROWS))
    pipeline.run()


@unittest.skip
class WriteToSpannerTest(unittest.TestCase):

  def setUp(self):
    self._database_id = _generate_database_name()
    # _create_database(self._database_id)

  def test_write_to_spanner(self):
    pipeline = TestPipeline()
    pipeline | beam.Create(_generate_test_data()) | \
    WriteToSpanner(TEST_PROJECT_ID, TEST_INSTANCE_ID,
                   self._database_id).insert()
    pipeline.run()
