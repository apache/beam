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

"""Integration tests for gcsio module.

Runs tests against Google Cloud Storage service.
Instantiates a TestPipeline to get options such as GCP project name, but
doesn't actually start a Beam pipeline or test any specific runner.

Run the following in 'sdks/python' directory to run these tests manually:
    scripts/run_integration_test.sh \
      --test_opts apache_beam/io/gcp/gcsio_integration_test.py
"""

# pytype: skip-file

import logging
import unittest
import uuid

import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

try:
  from apache_beam.io.gcp import gcsio
except ImportError:
  gcsio = None  # type: ignore


@unittest.skipIf(gcsio is None, 'GCP dependencies are not installed')
class GcsIOIntegrationTest(unittest.TestCase):

  INPUT_FILE = 'gs://dataflow-samples/shakespeare/kinglear.txt'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    if self.runner_name != 'TestDataflowRunner':
      # This test doesn't run a pipeline, so it doesn't make sense to try it on
      # different runners. Running with TestDataflowRunner makes sense since
      # it uses GoogleCloudOptions such as 'project'.
      raise unittest.SkipTest('This test only runs with TestDataflowRunner.')
    self.project = self.test_pipeline.get_option('project')
    self.gcs_tempdir = (
        self.test_pipeline.get_option('temp_location') + '/gcs_it-' +
        str(uuid.uuid4()))
    self.gcsio = gcsio.GcsIO()

  def tearDown(self):
    FileSystems.delete([self.gcs_tempdir + '/'])

  def _verify_copy(self, src, dest, dest_kms_key_name=None):
    self.assertTrue(
        FileSystems.exists(src), 'source file does not exist: %s' % src)
    self.assertTrue(
        FileSystems.exists(dest),
        'copied file not present in destination: %s' % dest)
    src_checksum = self.gcsio.checksum(src)
    dest_checksum = self.gcsio.checksum(dest)
    self.assertEqual(src_checksum, dest_checksum)
    actual_dest_kms_key = self.gcsio.kms_key(dest)
    if actual_dest_kms_key is None:
      self.assertEqual(actual_dest_kms_key, dest_kms_key_name)
    else:
      self.assertTrue(
          actual_dest_kms_key.startswith(dest_kms_key_name),
          "got: %s, wanted startswith: %s" %
          (actual_dest_kms_key, dest_kms_key_name))

  @pytest.mark.it_postcommit
  def test_copy(self):
    src = self.INPUT_FILE
    dest = self.gcs_tempdir + '/test_copy'

    self.gcsio.copy(src, dest)
    self._verify_copy(src, dest)

  @pytest.mark.it_postcommit
  def test_batch_copy_and_delete(self):
    num_copies = 10
    srcs = [self.INPUT_FILE] * num_copies
    dests = [
        self.gcs_tempdir + '/test_copy_batch_%d' % i for i in range(num_copies)
    ]
    src_dest_pairs = list(zip(srcs, dests))

    copy_results = self.gcsio.copy_batch(src_dest_pairs)

    self.assertEqual(len(copy_results), len(src_dest_pairs))

    for pair, result in list(zip(src_dest_pairs, copy_results)):
      self._verify_copy(pair[0], pair[1])
      self.assertEqual(
          pair[0],
          result[0],
          'copy source %s does not match %s' % (pair[0], str(result)))
      self.assertEqual(
          pair[1],
          result[1],
          'copy destination %s does not match %s' % (pair[1], result[1]))
      self.assertFalse(
          result[2],
          'response code %s indicates that copy operation did not succeed' %
          result[2])

    delete_results = self.gcsio.delete_batch(dests)

    self.assertEqual(len(delete_results), len(dests))

    for dest, result in list(zip(dests, delete_results)):
      self.assertFalse(
          FileSystems.exists(dest), 'deleted file still exists: %s' % dest)
      self.assertEqual(
          dest,
          result[0],
          'delete path %s does not match %s' % (dest, result[0]))
      self.assertFalse(
          result[1],
          'response code %s indicates that delete operation did not succeed' %
          result[1])

    redelete_results = self.gcsio.delete_batch(dests)

    for dest, result in list(zip(dests, redelete_results)):
      self.assertFalse(
          result[1], 're-delete should not throw error: %s' % result[1])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
