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

from nose.plugins.attrib import attr
from past.builtins import unicode

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.external.jdbc import WriteToJdbc
from apache_beam.testing.test_pipeline import TestPipeline

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

JdbcTestRow = typing.NamedTuple(
    "JdbcTestRow",
    [
        ("f_id", int),
        ("f_real", float),
        ("f_string", unicode),
    ],
)

coders.registry.register_coder(JdbcTestRow, coders.RowCoder)

@attr('UsesCrossLanguageTransforms')
@unittest.skipIf(sqlalchemy is None, "sql alchemy package is not installed.")
@unittest.skipIf(
    PostgresContainer is None, "testcontainers package is not installed")
class JdbcExternalTransformTest(unittest.TestCase):
  """Tests that exercise the cross-language JdbcIO Transform
   (implemented in java).

  It is required to have testcontainers package installed.
  You can do that e.g. via:
    pip install testcontainers

  To run with the local expansion service, flink job server you need to
  build it, e.g. via command:
    ./gradlew :sdks:java:io:expansion-service:shadowJar
    ./gradlew :runners:flink:1.10:job-server:shadowJar

  Also, beam java sdk container is necessary. You can build it via:
    ./gradlew :sdks:java:container:docker
  """
  ROW_COUNT = 10

  pipeline_options = {
      'runner': 'FlinkRunner',
      'flink_version': '1.10',
      'experiment': 'beam_fn_api',
      'environment_type': 'LOOPBACK'
  }

  def test_xlang_jdbc_write(self):
    table_name = 'jdbc_external_test_write'
    self.engine.execute(
        "CREATE TABLE {}(f_id INTEGER, f_real REAL, f_string VARCHAR)".format(
            table_name))
    inserted_rows = [
        JdbcTestRow(i, float(i + 0.1), str(i)) for i in range(self.ROW_COUNT)
    ]
    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | beam.Create(inserted_rows).with_output_types(JdbcTestRow)
          | 'Write to jdbc' >> WriteToJdbc(
              driver_class_name=self.driver_class_name,
              jdbc_url=self.jdbc_url,
              username=self.username,
              password=self.password,
              statement='insert into {}(f_id, f_real, f_string) values(?, ?, ?)'
              .format(table_name),
          ))
    fetched_data = self.engine.execute("select * from {}".format(table_name))
    fetched_rows = [
        JdbcTestRow(int(row[0]), float(row[1]), str(row[2]))
        for row in fetched_data
    ]

    self.assertEqual(
        set(fetched_rows),
        set(inserted_rows),
        'Inserted data does not fit data fetched from table')

  def setUp(self):
    self.postgres = PostgresContainer('postgres:latest')
    self.postgres.start()
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
    self.postgres.stop()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
