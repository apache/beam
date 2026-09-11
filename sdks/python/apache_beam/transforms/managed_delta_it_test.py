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

import logging
import os
import shutil
import tempfile
import unittest
import uuid

import pyarrow as pa
import pytest

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from deltalake import write_deltalake
except ImportError:
  write_deltalake = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

import apache_beam as beam
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)

# GCS location used to stage the Delta table when the pipeline runs on a remote
# runner. Beam's integration test suites have read/write access to this bucket.
_REMOTE_TABLE_ROOT = 'gs://temp-storage-for-end-to-end-tests/managed_delta_it'


@pytest.mark.uses_io_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
@unittest.skipIf(write_deltalake is None, 'deltalake is not installed.')
class ManagedDeltaIT(unittest.TestCase):
  # The table is written once and shared by all test methods, since every test
  # only reads from it.
  test_pipeline = None
  args = None
  # Path handed to the Managed transform; either `local_dir` or `staged_table`.
  table = None
  local_dir = None
  staged_table = None

  @classmethod
  def setUpClass(cls):
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.args = cls.test_pipeline.get_full_options_as_args()

    cls.local_dir = tempfile.mkdtemp()
    cls._write_table(cls.local_dir)

    # The Delta table is read by the runner's workers, so a local path only
    # works when those workers run on this machine (e.g. DirectRunner). For
    # remote runners the table has to be staged somewhere they can reach.
    if cls._is_remote_runner():
      cls.staged_table = FileSystems.join(_REMOTE_TABLE_ROOT, uuid.uuid4().hex)
      _LOGGER.info('Staging Delta table at %s', cls.staged_table)
      cls._upload_table(cls.local_dir, cls.staged_table)
      cls.table = cls.staged_table
    else:
      cls.table = cls.local_dir

  @classmethod
  def tearDownClass(cls):
    if cls.local_dir:
      shutil.rmtree(cls.local_dir, ignore_errors=True)
    if cls.staged_table:
      try:
        paths = [
            metadata.path
            for match_result in FileSystems.match([cls.staged_table + '/**'])
            for metadata in match_result.metadata_list
        ]
        if paths:
          FileSystems.delete(paths)
      except Exception:  # pylint: disable=broad-except
        # Not fatal: the staged table is a handful of small files under a
        # unique prefix in a shared test bucket.
        _LOGGER.warning(
            'Failed to clean up staged Delta table at %s',
            cls.staged_table,
            exc_info=True)

  @classmethod
  def _is_remote_runner(cls):
    runner = cls.test_pipeline.get_pipeline_options().view_as(
        StandardOptions).runner or ''
    return 'dataflow' in runner.lower()

  @staticmethod
  def _write_table(path):
    """Writes a two-version Delta table with change data feed enabled."""
    # Version 0 commit
    write_deltalake(
        path,
        pa.table({"name": ["a", "b"]}),
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"})

    # Version 1 commit
    write_deltalake(path, pa.table({"name": ["c"]}), mode="append")

  @staticmethod
  def _upload_table(local_dir, dest_dir):
    """Copies a local Delta table directory to ``dest_dir``.

    Delta transaction logs reference data files using paths relative to the
    table root, so a verbatim copy of the directory is itself a valid table.
    """
    for root, _, files in os.walk(local_dir):
      for name in files:
        local_path = os.path.join(root, name)
        relative_path = os.path.relpath(local_path, local_dir)
        dest_path = FileSystems.join(dest_dir, *relative_path.split(os.sep))
        with open(local_path, 'rb') as src, FileSystems.create(
            dest_path, compression_type=CompressionTypes.UNCOMPRESSED) as dest:
          shutil.copyfileobj(src, dest)

  def test_read_delta(self):
    with beam.Pipeline(argv=self.args) as p:
      output = (
          p
          | beam.managed.Read(beam.managed.DELTA, config={"table": self.table})
          | beam.Map(lambda row: row.name))
      assert_that(output, equal_to(["a", "b", "c"]))

  def test_read_delta_cdc_all_versions(self):
    with beam.Pipeline(argv=self.args) as p:
      output = (
          p
          | beam.managed.Read(
              beam.managed.DELTA_CDC,
              config={
                  "table": self.table, "start_version": 0
              })
          | beam.Map(lambda row: row.name))
      assert_that(output, equal_to(["a", "b", "c"]))

  def test_read_delta_cdc_from_version_1(self):
    with beam.Pipeline(argv=self.args) as p:
      output = (
          p
          | beam.managed.Read(
              beam.managed.DELTA_CDC,
              config={
                  "table": self.table, "start_version": 1
              })
          | beam.Map(lambda row: row.name))
      assert_that(output, equal_to(["c"]))

  def test_read_delta_cdc_version_range(self):
    with beam.Pipeline(argv=self.args) as p:
      output = (
          p
          | beam.managed.Read(
              beam.managed.DELTA_CDC,
              config={
                  "table": self.table, "start_version": 0, "end_version": 0
              })
          | beam.Map(lambda row: row.name))
      assert_that(output, equal_to(["a", "b"]))


if __name__ == '__main__':
  unittest.main()
