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

import logging
import typing
import unittest
from typing import Callable
from typing import Union

from parameterized import parameterized

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
  from testcontainers.mysql import MySqlContainer
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
        ("f_string", str),
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
  DbData = typing.NamedTuple(
      'DbData',
      [('container_fn', typing.Any), ('classpath', typing.List[str]),
       ('db_string', str), ('connector', str)])
  DB_CONTAINER_CLASSPATH_STRING = {
      'postgres': DbData(
          lambda: PostgresContainer('postgres:12.3'),
          None,
          'postgresql',
          'org.postgresql.Driver'),
      'mysql': DbData(
          lambda: MySqlContainer(), ['mysql:mysql-connector-java:8.0.28'],
          'mysql',
          'com.mysql.cj.jdbc.Driver')
  }

  def _setUpTestCase(
      self,
      container_init: Callable[[], Union[PostgresContainer, MySqlContainer]],
      db_string: str,
      driver: str):
    # This method is not the normal setUp from unittest, because the test has
    # beem parameterized. The setup then needs extra parameters to initialize.
    self.start_db_container(retries=3, container_init=container_init)
    self.engine = sqlalchemy.create_engine(self.db.get_connection_url())
    self.username = 'test'
    self.password = 'test'
    self.host = self.db.get_container_host_ip()
    self.port = self.db.get_exposed_port(self.db.port_to_expose)
    self.database_name = 'test'
    self.driver_class_name = driver
    self.jdbc_url = 'jdbc:{}://{}:{}/{}'.format(
        db_string, self.host, self.port, self.database_name)

  def tearDown(self):
    # Sometimes stopping the container raises ReadTimeout. We can ignore it
    # here to avoid the test failure.
    try:
      self.db.stop()
    except:  # pylint: disable=bare-except
      logging.error('Could not stop the postgreSQL container.')

  @parameterized.expand(['postgres', 'mysql'])
  def test_xlang_jdbc_write(self, database):
    container_init, classpath, db_string, driver = (
        CrossLanguageJdbcIOTest.DB_CONTAINER_CLASSPATH_STRING[database])
    self._setUpTestCase(container_init, db_string, driver)
    table_name = 'jdbc_external_test_write'
    self.engine.execute(
        "CREATE TABLE {}(f_id INTEGER, f_real FLOAT, f_string VARCHAR(100))".
        format(table_name))
    inserted_rows = [
        JdbcWriteTestRow(i, i + 0.1, 'Test{}'.format(i))
        for i in range(ROW_COUNT)
    ]

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | beam.Create(inserted_rows).with_output_types(JdbcWriteTestRow)
          # TODO(BEAM-10750) Add test with overridden write_statement
          | 'Write to jdbc' >> WriteToJdbc(
              table_name=table_name,
              driver_class_name=self.driver_class_name,
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              classpath=classpath,
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

  @parameterized.expand(['postgres', 'mysql'])
  def test_xlang_jdbc_read(self, database):
    container_init, classpath, db_string, driver = (
        CrossLanguageJdbcIOTest.DB_CONTAINER_CLASSPATH_STRING[database])
    self._setUpTestCase(container_init, db_string, driver)
    table_name = 'jdbc_external_test_read'
    self.engine.execute("CREATE TABLE {}(f_int INTEGER)".format(table_name))

    for i in range(ROW_COUNT):
      self.engine.execute("INSERT INTO {} VALUES({})".format(table_name, i))

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      result = (
          p
          # TODO(BEAM-10750) Add test with overridden read_query
          | 'Read from jdbc' >> ReadFromJdbc(
              table_name=table_name,
              driver_class_name=self.driver_class_name,
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              classpath=classpath))

      assert_that(
          result, equal_to([JdbcReadTestRow(i) for i in range(ROW_COUNT)]))

  # Creating a container with testcontainers sometimes raises ReadTimeout
  # error. In java there are 2 retries set by default.
  def start_db_container(self, retries, container_init):
    for i in range(retries):
      try:
        self.db = container_init()
        self.db.start()
        break
      except Exception as e:  # pylint: disable=bare-except
        if i == retries - 1:
          logging.error('Unable to initialize database container.')
          raise e


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
