#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import logging
import random
import unittest
import uuid

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

# Protect against environments where spanner library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
# pylint: disable=unused-import
try:
  from google.cloud import spanner
  from google.api_core.exceptions import NotFound
  from apache_beam.io.gcp.experimental.spannerio import WriteMutation
  from apache_beam.io.gcp.experimental.spannerio import MutationGroup
  from apache_beam.io.gcp.experimental.spannerio import WriteToSpanner
except ImportError:
  spanner = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports
# pylint: enable=unused-import

_LOGGER = logging.getLogger(__name__)
_TEST_INSTANCE_ID = 'beam-test'


@unittest.skipIf(spanner is None, 'GCP dependencies are not installed.')
class SpannerWriteIntegrationTest(unittest.TestCase):
  TEST_DATABASE = None
  _database_prefix = 'pybeam-write-{}'
  _SPANNER_CLIENT = None
  _SPANNER_INSTANCE = None

  @classmethod
  def _generate_table_name(cls):
    cls.TEST_DATABASE = cls._database_prefix.format(
        ''.join(random.sample(uuid.uuid4().hex, 15)))
    return cls.TEST_DATABASE

  @classmethod
  def _create_database(cls):
    _LOGGER.info('Creating test database: %s' % cls.TEST_DATABASE)
    instance = cls._SPANNER_INSTANCE
    database = instance.database(
        cls.TEST_DATABASE,
        ddl_statements=[
            '''CREATE TABLE Users (
            UserId    STRING(256) NOT NULL,
            Key       STRING(1024)
        ) PRIMARY KEY (UserId)''',
        ])
    operation = database.create()
    _LOGGER.info('Creating database: Done! %s' % str(operation.result()))

  @classmethod
  def _count_data(cls, prefix):
    instance = cls._SPANNER_INSTANCE
    database = instance.database(cls.TEST_DATABASE)
    count = None
    with database.snapshot() as snapshot:
      results = snapshot.execute_sql(
          'SELECT COUNT(*) FROM Users WHERE UserId '
          'LIKE "{}%"'.format(prefix))
      try:
        count = list(results)[0][0]
      except IndexError:
        raise ValueError(
            "Spanner Count rows results not found for %s." % prefix)
    return count

  @classmethod
  def setUpClass(cls):
    _LOGGER.info('.... Setting up!')
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.args = cls.test_pipeline.get_full_options_as_args()
    cls.runner_name = type(cls.test_pipeline.runner).__name__
    cls.project = cls.test_pipeline.get_option('project')
    cls.instance = (
        cls.test_pipeline.get_option('instance') or _TEST_INSTANCE_ID)
    _ = cls._generate_table_name()
    spanner_client = cls._SPANNER_CLIENT = spanner.Client()
    _LOGGER.info('.... Spanner Client created!')
    cls._SPANNER_INSTANCE = spanner_client.instance(cls.instance)
    cls._create_database()
    _LOGGER.info('Spanner Write IT Setup Complete...')

  @attr('IT')
  def test_write_batches(self):
    _prefex = 'test_write_batches'
    mutations = [
        WriteMutation.insert(
            'Users', ('UserId', 'Key'), [(_prefex + '1', _prefex + 'inset-1')]),
        WriteMutation.insert(
            'Users', ('UserId', 'Key'), [(_prefex + '2', _prefex + 'inset-2')]),
        WriteMutation.insert(
            'Users', ('UserId', 'Key'), [(_prefex + '3', _prefex + 'inset-3')]),
        WriteMutation.insert(
            'Users', ('UserId', 'Key'), [(_prefex + '4', _prefex + 'inset-4')])
    ]

    p = beam.Pipeline(argv=self.args)
    _ = (
        p | beam.Create(mutations) | WriteToSpanner(
            project_id=self.project,
            instance_id=self.instance,
            database_id=self.TEST_DATABASE,
            max_batch_size_bytes=250))

    res = p.run()
    res.wait_until_finish()
    self.assertEqual(self._count_data(_prefex), len(mutations))

  @attr('IT')
  def test_spanner_update(self):
    _prefex = 'test_update'

    # adding data to perform test
    instance = self._SPANNER_INSTANCE
    database = instance.database(self.TEST_DATABASE)
    data = [
        (_prefex + '1', _prefex + 'inset-1'),
        (_prefex + '2', _prefex + 'inset-2'),
        (_prefex + '3', _prefex + 'inset-3'),
    ]
    with database.batch() as batch:
      batch.insert(table='Users', columns=('UserId', 'Key'), values=data)

    mutations_update = [
        WriteMutation.update(
            'Users', ('UserId', 'Key'),
            [(_prefex + '1', _prefex + 'update-1')]),
        WriteMutation.update(
            'Users', ('UserId', 'Key'),
            [(_prefex + '2', _prefex + 'update-2')]),
        WriteMutation.delete('Users', spanner.KeySet(keys=[[_prefex + '3']]))
    ]

    p = beam.Pipeline(argv=self.args)
    _ = (
        p | beam.Create(mutations_update) | WriteToSpanner(
            project_id=self.project,
            instance_id=self.instance,
            database_id=self.TEST_DATABASE))

    res = p.run()
    res.wait_until_finish()
    self.assertEqual(self._count_data(_prefex), 2)

  @attr('IT')
  def test_spanner_error(self):
    mutations_update = [
        WriteMutation.update(
            'Users', ('UserId', 'Key'), [('INVALD_ID', 'Error-error')]),
    ]

    with self.assertRaises(Exception):
      with beam.Pipeline(argv=self.args) as p:
        _ = (
            p | beam.Create(mutations_update) | WriteToSpanner(
                project_id=self.project,
                instance_id=self.instance,
                database_id=self.TEST_DATABASE))

  @classmethod
  def tearDownClass(cls):
    # drop the testing database after the tests
    database = cls._SPANNER_INSTANCE.database(cls.TEST_DATABASE)
    database.drop()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
