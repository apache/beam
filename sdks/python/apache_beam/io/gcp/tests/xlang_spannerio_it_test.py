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

import argparse
import logging
import os
import time
import unittest
import uuid
from typing import NamedTuple
from typing import Optional

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.gcp.spanner import ReadFromSpanner
from apache_beam.io.gcp.spanner import SpannerDelete
from apache_beam.io.gcp.spanner import SpannerInsert
from apache_beam.io.gcp.spanner import SpannerInsertOrUpdate
from apache_beam.io.gcp.spanner import SpannerReplace
from apache_beam.io.gcp.spanner import SpannerUpdate
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import spanner
except ImportError:
  spanner = None

try:
  from testcontainers.core.container import DockerContainer
except ImportError:
  DockerContainer = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports


class SpannerTestKey(NamedTuple):
  f_string: str


class SpannerTestRow(NamedTuple):
  f_string: str
  f_int64: Optional[int]
  f_boolean: Optional[bool]


class SpannerPartTestRow(NamedTuple):
  f_string: str
  f_int64: Optional[int]


@unittest.skipIf(spanner is None, 'GCP dependencies are not installed.')
@unittest.skipIf(
    DockerContainer is None, 'testcontainers package is not installed.')
class CrossLanguageSpannerIOTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--spanner_instance_id',
        default='beam-test',
        help='Spanner instance id',
    )
    parser.add_argument(
        '--spanner_project_id',
        default='beam-testing',
        help='GCP project with spanner instance',
    )
    parser.add_argument(
        '--use_real_spanner',
        action='store_true',
        default=False,
        help='Whether to use emulator or real spanner instance',
    )

    pipeline = TestPipeline(is_integration_test=True)
    argv = pipeline.get_full_options_as_args()

    known_args, _ = parser.parse_known_args(argv)
    cls.project_id = known_args.spanner_project_id
    cls.instance_id = known_args.spanner_instance_id
    use_spanner_emulator = not known_args.use_real_spanner
    cls.table = 'xlang_beam_spanner'
    cls.spanner_helper = SpannerHelper(
        cls.project_id, cls.instance_id, cls.table, use_spanner_emulator)

    coders.registry.register_coder(SpannerTestRow, coders.RowCoder)
    coders.registry.register_coder(SpannerPartTestRow, coders.RowCoder)
    coders.registry.register_coder(SpannerTestKey, coders.RowCoder)

  @classmethod
  def tearDownClass(cls):
    cls.spanner_helper.shutdown()

  def setUp(self):
    self.database_id = f'xlang_beam{uuid.uuid4()}'.replace('-', '')[:30]
    self.spanner_helper.create_database(self.database_id)

  def tearDown(self):
    self.spanner_helper.drop_database(self.database_id)

  def test_spanner_insert_or_update(self):
    self.spanner_helper.insert_values(
        self.database_id, [('or_update0', 5, False), ('or_update1', 9, False)])

    def to_row_fn(i):
      return SpannerTestRow(
          f_int64=i, f_string=f'or_update{i}', f_boolean=i % 2 == 0)

    self.run_write_pipeline(3, to_row_fn, SpannerTestRow, SpannerInsertOrUpdate)

    self.assertEqual(
        self.spanner_helper.read_data(self.database_id, prefix='or_update'),
        [[f'or_update{i}', i, i % 2 == 0] for i in range(3)])

  def test_spanner_insert(self):
    def to_row_fn(num):
      return SpannerTestRow(
          f_string=f'insert{num}', f_int64=num, f_boolean=None)

    self.run_write_pipeline(1000, to_row_fn, SpannerTestRow, SpannerInsert)

    def compare_row(row):
      return row[1]

    self.assertEqual(
        sorted(
            self.spanner_helper.read_data(self.database_id, 'insert'),
            key=compare_row), [[f'insert{i}', i, None] for i in range(1000)])

  def test_spanner_replace(self):
    self.spanner_helper.insert_values(
        self.database_id, [('replace0', 0, True), ('replace1', 1, False)])

    def to_row_fn(num):
      return SpannerPartTestRow(f_string=f'replace{num}', f_int64=num + 10)

    self.run_write_pipeline(2, to_row_fn, SpannerPartTestRow, SpannerReplace)

    self.assertEqual(
        self.spanner_helper.read_data(self.database_id, prefix='replace'),
        [['replace0', 10, None], ['replace1', 11, None]])

  def test_spanner_update(self):
    self.spanner_helper.insert_values(
        self.database_id, [('update0', 5, False), ('update1', 9, False)])

    def to_row_fn(num):
      return SpannerPartTestRow(f_string=f'update{num}', f_int64=num + 10)

    self.run_write_pipeline(2, to_row_fn, SpannerPartTestRow, SpannerUpdate)

    self.assertEqual(
        self.spanner_helper.read_data(self.database_id, 'update'),
        [['update0', 10, False], ['update1', 11, False]])

  def test_spanner_delete(self):
    self.spanner_helper.insert_values(
        self.database_id,
        values=[
            ('delete0', 0, None),
            ('delete6', 6, False),
            ('delete20', 20, True),
        ])

    def to_row_fn(num):
      return SpannerTestKey(f_string=f'delete{num}')

    self.run_write_pipeline(10, to_row_fn, SpannerTestKey, SpannerDelete)

    self.assertEqual(
        self.spanner_helper.read_data(self.database_id, prefix='delete'),
        [['delete20', 20, True]])

  def test_spanner_read_query(self):
    self.insert_read_values('query_read')
    self.run_read_pipeline('query_read', query=f'SELECT * FROM {self.table}')

  def test_spanner_read_table(self):
    self.insert_read_values('table_read')
    self.run_read_pipeline('table_read', table=self.table)

  def run_read_pipeline(self, prefix, table=None, query=None):
    with TestPipeline(is_integration_test=True) as p:
      p.not_use_test_runner_api = True
      result = (
          p
          | ReadFromSpanner(
              instance_id=self.instance_id,
              database_id=self.database_id,
              project_id=self.project_id,
              row_type=SpannerTestRow,
              sql=query,
              table=table,
              emulator_host=self.spanner_helper.get_emulator_host(),
          ))

      assert_that(
          result,
          equal_to([
              SpannerTestRow(f_int64=0, f_string=f'{prefix}0', f_boolean=None),
              SpannerTestRow(f_int64=1, f_string=f'{prefix}1', f_boolean=True),
              SpannerTestRow(f_int64=2, f_string=f'{prefix}2', f_boolean=False),
          ]))

  def run_write_pipeline(
      self, num_rows, to_row_fn, row_type, spanner_transform=None):
    with TestPipeline(is_integration_test=True) as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | 'Impulse' >> beam.Impulse()
          | 'Generate' >> beam.FlatMap(lambda x: range(num_rows))  # pylint: disable=bad-option-value
          | 'Map to row' >> beam.Map(to_row_fn).with_output_types(row_type)
          | 'Write to Spanner' >> spanner_transform(
              instance_id=self.instance_id,
              database_id=self.database_id,
              project_id=self.project_id,
              table=self.table,
              emulator_host=self.spanner_helper.get_emulator_host(),
          ))

  def insert_read_values(self, prefix):
    self.spanner_helper.insert_values(
        self.database_id,
        values=[
            (f'{prefix}0', 0, None),
            (f'{prefix}1', 1, True),
            (f'{prefix}2', 2, False),
        ])


def retry(fn, retries, err_msg, *args, **kwargs):
  for _ in range(retries):
    try:
      return fn(*args, **kwargs)
    except:  # pylint: disable=bare-except
      pass
  logging.error(err_msg)
  raise RuntimeError(err_msg)


class SpannerHelper(object):
  def __init__(self, project_id, instance_id, table, use_emulator):
    self.use_emulator = use_emulator
    self.table = table
    self.host = None
    if use_emulator:
      self.emulator = DockerContainer(
          'gcr.io/cloud-spanner-emulator/emulator:latest').with_exposed_ports(
              9010, 9020)
      retry(self.emulator.start, 3, 'Could not start spanner emulator.')
      time.sleep(3)
      self.host = f'{self.emulator.get_container_host_ip()}:' \
                  f'{self.emulator.get_exposed_port(9010)}'
      os.environ['SPANNER_EMULATOR_HOST'] = self.host
    self.client = spanner.Client(project_id)
    self.instance = self.client.instance(instance_id)
    if use_emulator:
      self.create_instance()

  def create_instance(self):
    self.instance.create().result(120)

  def create_database(self, database_id):
    database = self.instance.database(
        database_id,
        ddl_statements=[
            f'''
          CREATE TABLE {self.table} (
              f_string  STRING(1024) NOT NULL,
              f_int64   INT64,
              f_boolean BOOL
          ) PRIMARY KEY (f_string)'''
        ])
    database.create().result(120)

  def insert_values(self, database_id, values, columns=None):
    values = values or []
    columns = columns or ('f_string', 'f_int64', 'f_boolean')
    with self.instance.database(database_id).batch() as batch:
      batch.insert(
          table=self.table,
          columns=columns,
          values=values,
      )

  def get_emulator_host(self):
    return f'http://{self.host}'

  def read_data(self, database_id, prefix):
    database = self.instance.database(database_id)
    with database.snapshot() as snapshot:
      results = snapshot.execute_sql(
          f'''SELECT * FROM {self.table}
              WHERE f_string LIKE "{prefix}%"
              ORDER BY f_int64''')
      try:
        rows = list(results) if results else None
      except IndexError:
        raise ValueError(f"Spanner results not found for {prefix}.")
    return rows

  def drop_database(self, database_id):
    database = self.instance.database(database_id)
    database.drop()

  def shutdown(self):
    if self.use_emulator:
      try:
        self.emulator.stop()
      except:  # pylint: disable=bare-except
        logging.error('Could not stop Spanner Cloud emulator.')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
