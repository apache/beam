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

"""Integration tests for DeltaIO and Delta CDC using Managed Transforms."""

import os
import shutil
import sys
import tempfile
import unittest

import pyarrow as pa
import pytest

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from deltalake import write_deltalake
except ImportError:
  write_deltalake = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@pytest.mark.uses_io_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
@unittest.skipIf(write_deltalake is None, 'deltalake is not installed.')
class ManagedDeltaIT(unittest.TestCase):
  def setUp(self):
    if any('DataflowRunner' in arg for arg in sys.argv):
      self.skipTest(
          'ManagedDeltaIT only supports direct runner execution with '
          'local file paths.')

    self.temp_dir = tempfile.mkdtemp()

    # Version 0 commit
    table_data_0 = pa.table({"name": ["a", "b"]})
    write_deltalake(
        self.temp_dir,
        table_data_0,
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"})

    # Version 1 commit
    table_data_1 = pa.table({"name": ["c"]})
    write_deltalake(self.temp_dir, table_data_1, mode="append")

  def tearDown(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_read_delta(self):
    with TestPipeline() as p:
      output = (
          p
          | beam.managed.Read(
              beam.managed.DELTA, config={"table": self.temp_dir})
          | beam.Map(lambda row: row.name))
      assert_that(output, equal_to(["a", "b", "c"]))

  def test_read_delta_cdc_all_versions(self):
    with TestPipeline() as p:
      output = (
          p
          | beam.managed.Read(
              beam.managed.DELTA_CDC,
              config={
                  "table": self.temp_dir, "start_version": 0
              })
          | beam.Map(lambda row: row.name))
      assert_that(output, equal_to(["a", "b", "c"]))

  def test_read_delta_cdc_from_version_1(self):
    with TestPipeline() as p:
      output = (
          p
          | beam.managed.Read(
              beam.managed.DELTA_CDC,
              config={
                  "table": self.temp_dir, "start_version": 1
              })
          | beam.Map(lambda row: row.name))
      assert_that(output, equal_to(["c"]))

  def test_read_delta_cdc_version_range(self):
    with TestPipeline() as p:
      output = (
          p
          | beam.managed.Read(
              beam.managed.DELTA_CDC,
              config={
                  "table": self.temp_dir, "start_version": 0, "end_version": 0
              })
          | beam.Map(lambda row: row.name))
      assert_that(output, equal_to(["a", "b"]))


if __name__ == '__main__':
  unittest.main()
