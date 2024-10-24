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

from apache_beam.io.debezium import DriverClassName
from apache_beam.io.debezium import ReadFromDebezium
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from testcontainers.postgres import PostgresContainer
except ImportError:
  PostgresContainer = None

NUM_RECORDS = 1


@unittest.skipIf(
    PostgresContainer is None, 'testcontainers package is not installed')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner is
    None,
    'Do not run this test on precommit suites.')
class CrossLanguageDebeziumIOTest(unittest.TestCase):
  def setUp(self):
    self.username = 'debezium'
    self.password = 'dbz'
    self.database = 'inventory'
    self.start_db_container(retries=1)
    self.host = self.db.get_container_host_ip()
    self.port = self.db.get_exposed_port(5432)
    self.connector_class = DriverClassName.POSTGRESQL
    self.connection_properties = [
        "database.dbname=inventory",
        "database.server.name=dbserver1",
        "database.include.list=inventory",
        "include.schema.changes=false"
    ]

  def tearDown(self):
    # Sometimes stopping the container raises ReadTimeout. We can ignore it
    # here to avoid the test failure.
    try:
      self.db.stop()
    except:  # pylint: disable=bare-except
      logging.error('Could not stop the DB container.')

  def test_xlang_debezium_read(self):
    expected_response = [{
        "metadata": {
            "connector": "postgresql",
            "version": "1.3.1.Final",
            "name": "dbserver1",
            "database": "inventory",
            "schema": "inventory",
            "table": "customers"
        },
        "before": None,
        "after": {
            "fields": {
                "last_name": "Thomas",
                "id": 1001,
                "first_name": "Sally",
                "email": "sally.thomas@acme.com"
            }
        }
    }]

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      results = (
          p
          | 'Read from debezium' >> ReadFromDebezium(
              username=self.username,
              password=self.password,
              host=self.host,
              port=self.port,
              max_number_of_records=NUM_RECORDS,
              connector_class=self.connector_class,
              connection_properties=self.connection_properties))
      assert_that(results, equal_to(expected_response))


# Creating a container with testcontainers sometimes raises ReadTimeout
# error. In java there are 2 retries set by default.
# TODO(https://github.com/apache/beam/issues/32937): use latest tag once a
# container exists again

  def start_db_container(self, retries):
    for i in range(retries):
      try:
        self.db = PostgresContainer(
            'debezium/example-postgres:3.0.0.Final',
            user=self.username,
            password=self.password,
            dbname=self.database)
        self.db.start()
        break
      except Exception as e:  # pylint: disable=bare-except
        if i == retries - 1:
          logging.error('Unable to initialize DB container.')
          raise e

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
