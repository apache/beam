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
from apache_beam.io.gcp.spannerio import *
# from apache_beam.io.gcp.spannerio import WriteToSpanner
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


def pp(*args, **kwargs):
  print({
      "args": args,
      "kw": kwargs
  })


import datetime

class SpannerReadTest(unittest.TestCase):
  TEST_INSTANCE_ID = 'testingdb-shoaib-vd'
  DB_NAME = "testdb1"  #from gcp https://console.cloud.google.com/spanner/instances/testingdb-shoaib-vd/details/databases?project=apache-beam-testing

  def test_read_actual(self):
    pipeline = TestPipeline()
    records = pipeline \
              | NewReadFromSpanner(
        TEST_PROJECT_ID,
        self.TEST_INSTANCE_ID,
        self.DB_NAME).with_query('SELECT * FROM users where Key < 10') | beam.Map(pp)
    pipeline.run()

  def test_read_all_actual(self):
    pipeline = TestPipeline()

    reads = [
        ReadOperation.with_query('SELECT * FROM users'),
        ReadOperation.with_table("roles", ['key', 'rolename'])
    ]
    snapshot_options = {
        "exact_staleness": datetime.timedelta(seconds=10)
    }
    records = pipeline \
              | NewReadFromSpanner(
        TEST_PROJECT_ID,
        self.TEST_INSTANCE_ID,
        self.DB_NAME, snapshot_options).read_all(reads) | beam.Map(pp)
    pipeline.run()

  def test_read_all_actual_transaction(self):
    pipeline = TestPipeline()

    reads = [
        ReadOperation.with_query('SELECT * FROM users'),
        ReadOperation.with_table("roles", ['rolename'])
    ]

    snapshot_options = {
        "exact_staleness": datetime.timedelta(seconds=10)
    }
    transaction = NewReadFromSpanner.create_transaction(
        TEST_PROJECT_ID,
        self.TEST_INSTANCE_ID,
        self.DB_NAME,
        snapshot_options=snapshot_options
    )
    records = pipeline \
              | NewReadFromSpanner(
        TEST_PROJECT_ID,
        self.TEST_INSTANCE_ID,
        self.DB_NAME).with_transaction(transaction).read_all(reads) | beam.Map(pp)
    pipeline.run()


