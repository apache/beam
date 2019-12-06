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

import datetime
import logging
import random
import string
import unittest

import mock

import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where spanner library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import spanner
  from apache_beam.io.gcp.spannerio import create_transaction  # pylint: disable=unused-import
  from apache_beam.io.gcp.spannerio import ReadOperation  # pylint: disable=unused-import
  from apache_beam.io.gcp.spannerio import ReadFromSpanner  # pylint: disable=unused-import
  from apache_beam.io.gcp.spannerio import WriteMutation  # pylint: disable=unused-import
  from apache_beam.io.gcp.spannerio import MutationGroup  # pylint: disable=unused-import
  from apache_beam.io.gcp.spannerio import WriteToSpanner  # pylint: disable=unused-import
  from apache_beam.io.gcp.spannerio import _BatchFn  # pylint: disable=unused-import
except ImportError:
  spanner = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

MAX_DB_NAME_LENGTH = 30
TEST_PROJECT_ID = 'apache-beam-testing'
TEST_INSTANCE_ID = 'beam-test'
TEST_DATABASE_PREFIX = 'spanner-testdb-'
# TEST_TABLE = 'users'
# TEST_COLUMNS = ['Key', 'Value']
FAKE_ROWS = [[1, 'Alice'], [2, 'Bob'], [3, 'Carl'], [4, 'Dan'], [5, 'Evan'],
             [6, 'Floyd']]


def _generate_database_name():
  mask = string.ascii_lowercase + string.digits
  length = MAX_DB_NAME_LENGTH - 1 - len(TEST_DATABASE_PREFIX)
  return TEST_DATABASE_PREFIX + ''.join(random.choice(mask) for i in range(
      length))


def _generate_test_data():
  mask = string.ascii_lowercase + string.digits
  length = 100
  return [('users', ['Key', 'Value'], [(x, ''.join(
      random.choice(mask) for _ in range(length))) for x in range(1, 5)])]


@unittest.skipIf(spanner is None, 'GCP dependencies are not installed.')
@mock.patch('apache_beam.io.gcp.spannerio.Client')
@mock.patch('apache_beam.io.gcp.spannerio.BatchSnapshot')
class SpannerReadTest(unittest.TestCase):

  def test_read_with_query_batch(self, mock_batch_snapshot_class,
                                 mock_client_class):
    mock_snapshot = mock.MagicMock()

    mock_snapshot.generate_query_batches.return_value = [
        {'query': {'sql': 'SELECT * FROM users'},
         'partition': 'test_partition'} for _ in range(3)]
    mock_snapshot.process_query_batch.side_effect = [
        FAKE_ROWS[0:2], FAKE_ROWS[2:4], FAKE_ROWS[4:]]

    ro = [ReadOperation.query("Select * from users")]
    pipeline = TestPipeline()

    read = (pipeline
            | 'read' >> ReadFromSpanner(TEST_PROJECT_ID, TEST_INSTANCE_ID,
                                        _generate_database_name(),
                                        sql="SELECT * FROM users"))

    readall = (pipeline
               | 'read all' >> ReadFromSpanner(TEST_PROJECT_ID,
                                               TEST_INSTANCE_ID,
                                               _generate_database_name(),
                                               read_operations=ro))

    readpipeline = (pipeline
                    | 'create reads' >> beam.Create(ro)
                    | 'reads' >> ReadFromSpanner(TEST_PROJECT_ID,
                                                 TEST_INSTANCE_ID,
                                                 _generate_database_name()))

    pipeline.run()
    assert_that(read, equal_to(FAKE_ROWS), label='checkRead')
    assert_that(readall, equal_to(FAKE_ROWS), label='checkReadAll')
    assert_that(readpipeline, equal_to(FAKE_ROWS), label='checkReadPipeline')

  def test_read_with_table_batch(self, mock_batch_snapshot_class,
                                 mock_client_class):
    mock_snapshot = mock.MagicMock()
    mock_snapshot.generate_read_batches.return_value = [{
        'read': {'table': 'users', 'keyset': {'all': True},
                 'columns': ['Key', 'Value'], 'index': ''},
        'partition': 'test_partition'} for _ in range(3)]
    mock_snapshot.process_read_batch.side_effect = [
        FAKE_ROWS[0:2], FAKE_ROWS[2:4], FAKE_ROWS[4:]]

    ro = [ReadOperation.table("users", ["Key", "Value"])]
    pipeline = TestPipeline()

    read = (pipeline
            | 'read' >> ReadFromSpanner(TEST_PROJECT_ID, TEST_INSTANCE_ID,
                                        _generate_database_name(),
                                        table="users",
                                        columns=["Key", "Value"]))

    readall = (pipeline
               | 'read all' >> ReadFromSpanner(TEST_PROJECT_ID,
                                               TEST_INSTANCE_ID,
                                               _generate_database_name(),
                                               read_operations=ro))

    readpipeline = (pipeline
                    | 'create reads' >> beam.Create(ro)
                    | 'reads' >> ReadFromSpanner(TEST_PROJECT_ID,
                                                 TEST_INSTANCE_ID,
                                                 _generate_database_name()))

    pipeline.run()
    assert_that(read, equal_to(FAKE_ROWS), label='checkRead')
    assert_that(readall, equal_to(FAKE_ROWS), label='checkReadAll')
    assert_that(readpipeline, equal_to(FAKE_ROWS), label='checkReadPipeline')

    with self.assertRaises(ValueError):
      # Test the exception raised when user passes the read operations in the
      # constructor and also in the pipeline.
      _ = (pipeline | 'reads error' >> ReadFromSpanner(
          project_id=TEST_PROJECT_ID,
          instance_id=TEST_INSTANCE_ID,
          database_id=_generate_database_name(),
          table="users"
      ))
      pipeline.run()

  def test_read_with_transaction(self, mock_batch_snapshot_class,
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

    mock_session = mock.MagicMock()
    mock_transaction_ctx = mock.MagicMock()
    mock_transaction = mock.MagicMock()

    mock_snapshot._get_session.return_value = mock_session
    mock_session.transaction.return_value = mock_transaction
    mock_transaction.__enter__.return_value = mock_transaction_ctx
    mock_transaction_ctx.execute_sql.return_value = FAKE_ROWS

    ro = [ReadOperation.query("Select * from users")]
    p = TestPipeline()

    transaction = (p | create_transaction(
        project_id=TEST_PROJECT_ID, instance_id=TEST_INSTANCE_ID,
        database_id=_generate_database_name(),
        exact_staleness=datetime.timedelta(seconds=10)))

    read_query = (p | 'with query' >> ReadFromSpanner(
        project_id=TEST_PROJECT_ID,
        instance_id=TEST_INSTANCE_ID,
        database_id=_generate_database_name(),
        transaction=transaction,
        sql="Select * from users"
    ))

    read_table = (p | 'with table' >> ReadFromSpanner(
        project_id=TEST_PROJECT_ID,
        instance_id=TEST_INSTANCE_ID,
        database_id=_generate_database_name(),
        transaction=transaction,
        table="users",
        columns=["Key", "Value"]
    ))

    read_all = (p | 'read all' >> ReadFromSpanner(TEST_PROJECT_ID,
                                                  TEST_INSTANCE_ID,
                                                  _generate_database_name(),
                                                  transaction=transaction,
                                                  read_operations=ro))

    read_all_pipeline = (p
                         | 'create read operations' >> beam.Create(ro)
                         | 'reads' >> ReadFromSpanner(TEST_PROJECT_ID,
                                                      TEST_INSTANCE_ID,
                                                      _generate_database_name(),
                                                      transaction=transaction))

    p.run()

    assert_that(read_query, equal_to(FAKE_ROWS), label='checkQuery')
    assert_that(read_table, equal_to(FAKE_ROWS), label='checkTable')
    assert_that(read_all, equal_to(FAKE_ROWS), label='checkReadAll')
    assert_that(read_all_pipeline, equal_to(FAKE_ROWS),
                label='checkReadPipeline')

    with self.assertRaises(ValueError):
      # Test the exception raised when user passes the read operations in the
      # constructor and also in the pipeline.
      _ = (p
           | 'create read operations2' >> beam.Create(ro)
           | 'reads with error' >> ReadFromSpanner(
               TEST_PROJECT_ID, TEST_INSTANCE_ID, _generate_database_name(),
               transaction=transaction, read_operations=ro))
      p.run()

  def test_display_data(self, *args):
    dd_sql = ReadFromSpanner(
        project_id=TEST_PROJECT_ID,
        instance_id=TEST_INSTANCE_ID,
        database_id=_generate_database_name(),
        sql="Select * from users"
    ).display_data()

    dd_table = ReadFromSpanner(
        project_id=TEST_PROJECT_ID,
        instance_id=TEST_INSTANCE_ID,
        database_id=_generate_database_name(),
        table="users",
        columns=['id', 'name']
    ).display_data()

    dd_transaction = ReadFromSpanner(
        project_id=TEST_PROJECT_ID,
        instance_id=TEST_INSTANCE_ID,
        database_id=_generate_database_name(),
        table="users",
        columns=['id', 'name'],
        transaction={"transaction_id": "test123", "session_id": "test456"}
    ).display_data()

    self.assertTrue("sql" in dd_sql)
    self.assertTrue("table" in dd_table)
    self.assertTrue("table" in dd_transaction)
    self.assertTrue("transaction" in dd_transaction)


@unittest.skipIf(spanner is None, 'GCP dependencies are not installed.')
@mock.patch('apache_beam.io.gcp.spannerio.Client')
@mock.patch('google.cloud.spanner_v1.database.BatchCheckout')
class SpannerWriteTest(unittest.TestCase):

  def test_spanner_write(self, mock_batch_snapshot_class, mock_batch_checkout):
    ks = spanner.KeySet(keys=[[1233], [1234]])

    mutations = [
        WriteMutation.delete("roles", ks),
        WriteMutation.insert("roles", ("key", "rolename"),
                             [('1233', "mutations-inset-1233")]),
        WriteMutation.insert("roles", ("key", "rolename"),
                             [('1234', "mutations-inset-1234")]),
        WriteMutation.update("roles", ("key", "rolename"),
                             [('1234', "mutations-inset-1233-updated")]),
    ]

    p = TestPipeline()
    _ = (
        p
        | beam.Create(mutations)
        | WriteToSpanner(
            project_id=TEST_PROJECT_ID,
            instance_id=TEST_INSTANCE_ID,
            database_id=_generate_database_name(),
            max_batch_size_bytes=1024,
        ))
    res = p.run()
    res.wait_until_finish()

    metric_results = res.metrics().query(
        MetricsFilter().with_name("SpannerBatches"))
    batches_counter = metric_results['counters'][0]

    self.assertEqual(batches_counter.committed, 2)
    self.assertEqual(batches_counter.attempted, 2)

  def test_spanner_bundles_size(self, mock_batch_snapshot_class,
                                mock_batch_checkout):
    ks = spanner.KeySet(keys=[[1233], [1234]])
    mutations = [
        WriteMutation.delete("roles", ks),
        WriteMutation.insert("roles", ("key", "rolename"),
                             [('1234', "mutations-inset-1234")])
    ] * 50
    p = TestPipeline()
    _ = (
        p
        | beam.Create(mutations)
        | WriteToSpanner(
            project_id=TEST_PROJECT_ID,
            instance_id=TEST_INSTANCE_ID,
            database_id=_generate_database_name(),
            max_batch_size_bytes=1024,
        ))
    res = p.run()
    res.wait_until_finish()

    metric_results = res.metrics().query(
        MetricsFilter().with_name('SpannerBatches'))
    batches_counter = metric_results['counters'][0]

    self.assertEqual(batches_counter.committed, 53)
    self.assertEqual(batches_counter.attempted, 53)

  def test_spanner_write_mutation_groups(self, mock_batch_snapshot_class,
                                         mock_batch_checkout):
    ks = spanner.KeySet(keys=[[1233], [1234]])
    mutation_groups = [
        MutationGroup([
            WriteMutation.insert("roles", ("key", "rolename"),
                                 [('9001233', "mutations-inset-1233")]),
            WriteMutation.insert("roles", ("key", "rolename"),
                                 [('9001234', "mutations-inset-1234")])
        ]),
        MutationGroup([
            WriteMutation.update(
                "roles", ("key", "rolename"),
                [('9001234', "mutations-inset-9001233-updated")])
        ]),
        MutationGroup([WriteMutation.delete("roles", ks)])
    ]

    p = TestPipeline()
    _ = (
        p
        | beam.Create(mutation_groups)
        | WriteToSpanner(
            project_id=TEST_PROJECT_ID,
            instance_id=TEST_INSTANCE_ID,
            database_id=_generate_database_name(),
            max_batch_size_bytes=100,
        ))
    res = p.run()
    res.wait_until_finish()

    metric_results = res.metrics().query(
        MetricsFilter().with_name('SpannerBatches'))
    batches_counter = metric_results['counters'][0]

    self.assertEqual(batches_counter.committed, 3)
    self.assertEqual(batches_counter.attempted, 3)

  def test_mutation_group_batching(self, mock_batch_snapshot_class,
                                   mock_batch_checkout):
    # each mutation group byte size is 58
    mutation_group = [
        MutationGroup([
            WriteMutation.insert("roles", ("key", "rolename"),
                                 [('1234', "mutations-inset-1234")])
        ])
    ] * 50

    with TestPipeline() as p:
      res = (
          p | beam.Create(mutation_group) | beam.ParDo(_BatchFn(1450))
          | beam.Map(lambda x: len(x))
      )
      assert_that(res, equal_to([25, 25]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
