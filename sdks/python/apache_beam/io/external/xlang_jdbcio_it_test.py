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

# pytype: skip-file

from __future__ import absolute_import

import logging
import typing
import unittest

from past.builtins import unicode

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import sqlalchemy
except ImportError:
  sqlalchemy = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from testcontainers.postgres import PostgresContainer
except ImportError:
  PostgresContainer = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

ROW_COUNT = 10

JdbcReadTestRow = typing.NamedTuple(
    "JdbcReadTestRow",
    [
        ("f_int", int),
    ],
)
coders.registry.register_coder(JdbcReadTestRow, coders.RowCoder)

JdbcWriteTestRow = typing.NamedTuple(
    "JdbcWriteTestRow",
    [
        ("f_id", int),
        ("f_real", float),
        ("f_string", unicode),
    ],
)
coders.registry.register_coder(JdbcWriteTestRow, coders.RowCoder)


@unittest.skipIf(sqlalchemy is None, 'sql alchemy package is not installed.')
@unittest.skipIf(
    PostgresContainer is None, 'testcontainers package is not installed')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner is
    None,
    'Do not run this test on precommit suites.')
class CrossLanguageJdbcIOTest(unittest.TestCase):
  def setUp(self):
    self.start_postgres_container(retries=3)
    self.engine = sqlalchemy.create_engine(self.postgres.get_connection_url())
    self.username = 'test'
    self.password = 'test'
    self.host = self.postgres.get_container_host_ip()
    self.port = self.postgres.get_exposed_port(5432)
    self.database_name = 'test'
    self.driver_class_name = 'org.postgresql.Driver'
    self.jdbc_url = 'jdbc:postgresql://{}:{}/{}'.format(
        self.host, self.port, self.database_name)

  def tearDown(self):
    # Sometimes stopping the container raises ReadTimeout. We can ignore it
    # here to avoid the test failure.
    try:
      self.postgres.stop()
    except:  # pylint: disable=bare-except
      logging.error('Could not stop the postgreSQL container.')

  def test_xlang_jdbc_write(self):
    table_name = 'jdbc_external_test_write'
    self.engine.execute(
        "CREATE TABLE {}(f_id INTEGER, f_real REAL, f_string VARCHAR)".format(
            table_name))
    inserted_rows = [
        JdbcWriteTestRow(i, i + 0.1, 'Test{}'.format(i))
        for i in range(ROW_COUNT)
    ]

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | beam.Create(inserted_rows).with_output_types(JdbcWriteTestRow)
          | 'Write to jdbc' >> WriteToJdbc(
              driver_class_name=self.driver_class_name,
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              statement='INSERT INTO {} VALUES(?, ?, ?)'.format(table_name),
          ))

    fetched_data = self.engine.execute("SELECT * FROM {}".format(table_name))
    fetched_rows = [
        JdbcWriteTestRow(int(row[0]), float(row[1]), str(row[2]))
        for row in fetched_data
    ]

    self.assertEqual(
        set(fetched_rows),
        set(inserted_rows),
        'Inserted data does not fit data fetched from table',
    )

  def test_xlang_jdbc_read(self):
    table_name = 'jdbc_external_test_read'
    self.engine.execute("CREATE TABLE {}(f_int INTEGER)".format(table_name))

    for i in range(ROW_COUNT):
      self.engine.execute("INSERT INTO {} VALUES({})".format(table_name, i))

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      result = (
          p
          | 'Read from jdbc' >> ReadFromJdbc(
              driver_class_name=self.driver_class_name,
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              query='SELECT f_int FROM {}'.format(table_name),
          ))

      assert_that(
          result, equal_to([JdbcReadTestRow(i) for i in range(ROW_COUNT)]))

  # Creating a container with testcontainers sometimes raises ReadTimeout
  # error. In java there are 2 retries set by default.
  def start_postgres_container(self, retries):
    for i in range(retries):
      try:
        self.postgres = PostgresContainer('postgres:12.3')
        self.postgres.start()
        break
      except Exception as e:  # pylint: disable=bare-except
        if i == retries - 1:
          logging.error('Unable to initialize postgreSQL container.')
          raise e


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
