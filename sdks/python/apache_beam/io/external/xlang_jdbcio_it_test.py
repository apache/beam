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

import datetime
import logging
import os
import time
import typing
import unittest
from decimal import Decimal

import pytest
from parameterized import parameterized

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp

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
POSTGRES_BINARY_TYPE = ('BYTEA', 'BYTEA')
MYSQL_BINARY_TYPE = ('BINARY(10)', 'VARBINARY(10)')

JdbcTestRow = typing.NamedTuple(
    "JdbcTestRow",
    [("f_id", int), ("f_id_long", int), ("f_float", float), ("f_char", str),
     ("f_varchar", str), ("f_bytes", bytes), ("f_varbytes", bytes),
     ("f_timestamp", Timestamp), ("f_decimal", Decimal),
     ("f_date", datetime.date), ("f_time", datetime.time)],
)
coders.registry.register_coder(JdbcTestRow, coders.RowCoder)

CustomSchemaRow = typing.NamedTuple(
    "CustomSchemaRow",
    [
        ("renamed_id", int),
        ("renamed_id_long", int),
        ("renamed_float", float),
        ("renamed_char", str),
        ("renamed_varchar", str),
        ("renamed_bytes", bytes),
        ("renamed_varbytes", bytes),
        ("renamed_timestamp", Timestamp),
        ("renamed_decimal", Decimal),
        ("renamed_date", datetime.date),
        ("renamed_time", datetime.time),
    ],
)
coders.registry.register_coder(CustomSchemaRow, coders.RowCoder)

SimpleRow = typing.NamedTuple(
    "SimpleRow", [("id", int), ("name", str), ("value", float)])
coders.registry.register_coder(SimpleRow, coders.RowCoder)


@pytest.mark.uses_gcp_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
@unittest.skipIf(sqlalchemy is None, 'sql alchemy package is not installed.')
@unittest.skipIf(
    PostgresContainer is None, 'testcontainers package is not installed')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
    is None,
    'Do not run this test on precommit suites.')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
    is not None and
    "dataflowrunner" in TestPipeline().get_pipeline_options().view_as(
        StandardOptions).runner.lower(),
    'Do not run this test on dataflow runner.')
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
          lambda: MySqlContainer(dialect='pymysql'),
          ['mysql:mysql-connector-java:8.0.28'],
          'mysql',
          'com.mysql.cj.jdbc.Driver')
  }

  @classmethod
  def setUpClass(cls):
    cls.containers = {}
    cls.engines = {}
    cls.jdbc_configs = {}

    for db_type, db_data in cls.DB_CONTAINER_CLASSPATH_STRING.items():
      container = cls.start_container(db_data.container_fn)
      cls.containers[db_type] = container

      cls.engines[db_type] = sqlalchemy.create_engine(
          container.get_connection_url())

      cls.jdbc_configs[db_type] = {
          'username': 'test',
          'password': 'test',
          'host': container.get_container_host_ip(),
          'port': container.get_exposed_port(container.port),
          'database_name': 'test',
          'driver_class_name': db_data.connector,
          'classpath': db_data.classpath,
          'jdbc_url': (
              f'jdbc:{db_data.db_string}://{container.get_container_host_ip()}:'
              f'{container.get_exposed_port(container.port)}/test'),
          'binary_type': POSTGRES_BINARY_TYPE
          if db_type == 'postgres' else MYSQL_BINARY_TYPE
      }

  @classmethod
  def tearDownClass(cls):
    for db_type, container in cls.containers.items():
      if container:
        # Sometimes stopping the container raises ReadTimeout. We can ignore it
        # here to avoid the test failure.
        try:
          container.stop()
        except Exception:  # pylint: disable=broad-except
          logging.warning("Could not stop %s container", db_type)

  @classmethod
  def start_container(cls, container_init, max_retries=3):
    # Creating a container with testcontainers sometimes raises ReadTimeout
    # error. In java there are 2 retries set by default.
    for attempt in range(max_retries):
      try:
        container = container_init()
        container.start()
        return container
      except Exception:  # pylint: disable=broad-except
        if attempt == max_retries - 1:
          logging.error(
              'Failed to initialize container after %s attempts', max_retries)
          raise

  def create_test_table(self, connection, table_name, database):
    binary_type = self.jdbc_configs[database]['binary_type']
    connection.execute(
        sqlalchemy.text(
            f"CREATE TABLE IF NOT EXISTS {table_name}" +
            "(f_id INTEGER, f_id_long BIGINT, f_float DOUBLE PRECISION, " +
            "f_char CHAR(10), f_varchar VARCHAR(10), " +
            f"f_bytes {binary_type[0]}, f_varbytes {binary_type[1]}, " +
            "f_timestamp TIMESTAMP(3), f_decimal DECIMAL(10, 2), " +
            "f_date DATE, f_time TIME(3))"))

  def generate_test_data(self, count):
    return [
        JdbcTestRow(
            i - 3,
            i - 3,
            i + 0.1,
            f'Test{i}',
            f'Test{i}',
            f'Test{i}'.encode(),
            f'Test{i}'.encode(),
            Timestamp.of(seconds=round(time.time(), 3)),
            Decimal(f'{i-1}.23'),
            datetime.date(1969 + i, i % 12 + 1, i % 31 + 1),
            datetime.time(i % 24, i % 60, i % 60, (i * 1000) % 1_000_000))
        for i in range(count)
    ]

  @parameterized.expand(['postgres', 'mysql'])
  def test_xlang_jdbc_write_read(self, database):
    table_name = f"jdbc_write_read_test_{database}"

    with self.engines[database].begin() as connection:
      self.create_test_table(connection, table_name, database)

    test_rows = self.generate_test_data(ROW_COUNT)

    expected_rows = []
    for row in test_rows:
      f_char = row.f_char + ' ' * (10 - len(row.f_char))
      f_bytes = row.f_bytes

      if database != 'postgres':
        f_bytes = row.f_bytes + b'\0' * (10 - len(row.f_bytes))

      expected_rows.append(
          JdbcTestRow(
              row.f_id,
              row.f_id,
              row.f_float,
              f_char,
              row.f_varchar,
              f_bytes,
              row.f_bytes,
              row.f_timestamp,
              row.f_decimal,
              row.f_date,
              row.f_time))

    config = self.jdbc_configs[database]

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | beam.Create(test_rows).with_output_types(JdbcTestRow)
          | 'Write to jdbc' >> WriteToJdbc(
              table_name=table_name,
              driver_class_name=config['driver_class_name'],
              jdbc_url=config['jdbc_url'],
              username=config['username'],
              password=config['password'],
              classpath=config['classpath'],
          ))

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      result = (
          p
          | 'Read from jdbc' >> ReadFromJdbc(
              table_name=table_name,
              driver_class_name=config['driver_class_name'],
              jdbc_url=config['jdbc_url'],
              username=config['username'],
              password=config['password'],
              classpath=config['classpath']))

      assert_that(result, equal_to(expected_rows))

    # Try the same read using the partitioned reader code path.
    # Outputs should be the same.
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      result = (
          p
          | 'Partitioned read from jdbc' >> ReadFromJdbc(
              table_name=table_name,
              partition_column='f_id',
              partitions=3,
              driver_class_name=config['driver_class_name'],
              jdbc_url=config['jdbc_url'],
              username=config['username'],
              password=config['password'],
              classpath=config['classpath']))

      assert_that(result, equal_to(expected_rows))

  @parameterized.expand(['postgres', 'mysql'])
  def test_xlang_jdbc_read_with_explicit_schema(self, database):
    if self.containers[database] is None:
      self.skipTest(f"{database} container could not be initialized")

    table_name = f"jdbc_schema_test_{database}"

    with self.engines[database].begin() as connection:
      self.create_test_table(connection, table_name, database)

    test_rows = self.generate_test_data(ROW_COUNT)

    expected_rows = []
    for row in test_rows:
      f_char = row.f_char
      f_bytes = row.f_bytes

      if database != 'postgres':
        f_bytes = row.f_bytes + b'\0' * (10 - len(row.f_bytes))

      expected_rows.append(
          CustomSchemaRow(
              row.f_id,
              row.f_id,
              row.f_float,
              f_char,
              row.f_varchar,
              f_bytes,
              row.f_bytes,
              row.f_timestamp,
              row.f_decimal,
              row.f_date,
              row.f_time))

    def custom_row_equals(expected, actual):
      return (
          expected.renamed_id == actual.renamed_id and
          expected.renamed_id_long == actual.renamed_id_long and
          expected.renamed_float == actual.renamed_float and
          expected.renamed_char.rstrip() == actual.renamed_char.rstrip() and
          expected.renamed_varchar == actual.renamed_varchar and
          expected.renamed_bytes == actual.renamed_bytes and
          expected.renamed_timestamp == actual.renamed_timestamp and
          expected.renamed_decimal == actual.renamed_decimal and
          expected.renamed_date == actual.renamed_date and
          expected.renamed_time == actual.renamed_time)

    config = self.jdbc_configs[database]

    # Run write pipeline
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | beam.Create(test_rows).with_output_types(JdbcTestRow)
          | 'Write to jdbc' >> WriteToJdbc(
              table_name=table_name,
              driver_class_name=config['driver_class_name'],
              jdbc_url=config['jdbc_url'],
              username=config['username'],
              password=config['password'],
              classpath=config['classpath'],
          ))

    # Run read pipeline with custom schema
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      result = (
          p
          | 'Read from jdbc with schema' >> ReadFromJdbc(
              table_name=table_name,
              driver_class_name=config['driver_class_name'],
              jdbc_url=config['jdbc_url'],
              username=config['username'],
              password=config['password'],
              classpath=config['classpath'],
              schema=CustomSchemaRow))

      assert_that(result, equal_to(expected_rows, equals_fn=custom_row_equals))

  @parameterized.expand(['postgres', 'mysql'])
  def test_xlang_jdbc_custom_statements(self, database):
    # Skip if container wasn't initialized
    if self.containers[database] is None:
      self.skipTest(f"{database} container could not be initialized")

    # Create a simple table for this test
    table_name = f"jdbc_custom_statements_{database}"

    with self.engines[database].begin() as connection:
      connection.execute(
          sqlalchemy.text(
              f"CREATE TABLE IF NOT EXISTS {table_name}" +
              "(id INTEGER, name VARCHAR(50), value DOUBLE PRECISION)"))

    test_rows = [
        SimpleRow(1, "Item1", 10.5),
        SimpleRow(2, "Item2", 20.75),
        SimpleRow(3, "Item3", 30.25),
        SimpleRow(4, "Item4", 40.0),
        SimpleRow(-5, "Item5", 50.5)
    ]

    config = self.jdbc_configs[database]

    write_statement = f"INSERT INTO {table_name} (id, name, value) VALUES \
        (?, ?, ?)"

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | beam.Create(test_rows).with_output_types(SimpleRow)
          | 'Write with custom statement' >> WriteToJdbc(
              table_name="",
              driver_class_name=config['driver_class_name'],
              jdbc_url=config['jdbc_url'],
              username=config['username'],
              password=config['password'],
              classpath=config['classpath'],
              statement=write_statement))

    # Schema inference fails when there is a WHERE clause, so we pass explicit
    # schema.
    read_query = f"SELECT id, name, value FROM {table_name} WHERE value > 25.0"
    expected_filtered_rows = [row for row in test_rows if row.value > 25.0]

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      result = (
          p
          | 'Read with custom query' >> ReadFromJdbc(
              table_name="",
              driver_class_name=config['driver_class_name'],
              jdbc_url=config['jdbc_url'],
              username=config['username'],
              password=config['password'],
              classpath=config['classpath'],
              query=read_query,
              schema=SimpleRow))

      assert_that(result, equal_to(expected_filtered_rows))

    # JdbcIO#readWithPartitions requires custom queries to be passed as a
    # wrapped subquery to table_name.
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      result = (
          p
          | 'Read with custom query' >> ReadFromJdbc(
              table_name=f"({read_query}) as subq",
              driver_class_name=config['driver_class_name'],
              jdbc_url=config['jdbc_url'],
              username=config['username'],
              password=config['password'],
              classpath=config['classpath'],
              partition_column="id",
              schema=SimpleRow))

      assert_that(result, equal_to(expected_filtered_rows))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
