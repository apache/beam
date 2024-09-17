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

import mock
from parameterized import parameterized_class
import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.testing.test_pipeline import TestPipeline

try:
  from apache_beam.io.gcp import gcsio
except ImportError:
  gcsio = None  # type: ignore

try:
  from google.api_core.exceptions import NotFound
except ImportError:
  NotFound = None


@unittest.skipIf(gcsio is None, 'GCP dependencies are not installed')
@parameterized_class(('no_gcsio_throttling_counter', 'enable_gcsio_blob_generation'),
  [(False, False), (False, True), (True, False), (True, True)])
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
    self.gcsio = gcsio.GcsIO(pipeline_options={
      "no_gcsio_throttling_counter": self.no_gcsio_throttling_counter,
      "enable_gcsio_blob_generation": self.enable_gcsio_blob_generation})
    src = self.INPUT_FILE
    dest = self.gcs_tempdir + '/test_copy'

    self.gcsio.copy(src, dest)
    self._verify_copy(src, dest)

    unknown_src = self.test_pipeline.get_option('temp_location') + \
        '/gcs_it-' + str(uuid.uuid4())
    with self.assertRaises(NotFound):
      self.gcsio.copy(unknown_src, dest)

  @pytest.mark.it_postcommit
  def test_copy_and_delete(self):
    self.gcsio = gcsio.GcsIO(pipeline_options={
      "no_gcsio_throttling_counter": self.no_gcsio_throttling_counter,
      "enable_gcsio_blob_generation": self.enable_gcsio_blob_generation})
    src = self.INPUT_FILE
    dest = self.gcs_tempdir + '/test_copy'

    self.gcsio.copy(src, dest)
    self._verify_copy(src, dest)

    self.gcsio.delete(dest)

    # no exception if we delete an nonexistent file.
    self.gcsio.delete(dest)

  @pytest.mark.it_postcommit
  def test_batch_copy_and_delete(self):
    self.gcsio = gcsio.GcsIO(pipeline_options={
    "no_gcsio_throttling_counter": self.no_gcsio_throttling_counter,
    "enable_gcsio_blob_generation": self.enable_gcsio_blob_generation})
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

  @pytest.mark.it_postcommit
  @mock.patch('apache_beam.io.gcp.gcsio.default_gcs_bucket_name')
  @unittest.skipIf(NotFound is None, 'GCP dependencies are not installed')
  def test_create_default_bucket(self, mock_default_gcs_bucket_name):
    google_cloud_options = self.test_pipeline.options.view_as(
        GoogleCloudOptions)
    # overwrite kms option here, because get_or_create_default_gcs_bucket()
    # requires this option unset.
    google_cloud_options.dataflow_kms_key = None

    import random
    from hashlib import md5
    # Add a random number to avoid collision if multiple test instances
    # are run at the same time. To avoid too many dangling buckets if bucket
    # removal fails, we limit the max number of possible bucket names in this
    # test to 1000.
    overridden_bucket_name = 'gcsio-it-%d-%s-%s' % (
        random.randint(0, 999),
        google_cloud_options.region,
        md5(google_cloud_options.project.encode('utf8')).hexdigest())

    mock_default_gcs_bucket_name.return_value = overridden_bucket_name

    # remove the existing bucket with the same name as the default bucket
    existing_bucket = self.gcsio.get_bucket(overridden_bucket_name)
    if existing_bucket:
      try:
        existing_bucket.delete()
      except NotFound:
        # Bucket existence check from get_bucket may be inaccurate due to gcs
        # cache or delay
        pass

    bucket = gcsio.get_or_create_default_gcs_bucket(google_cloud_options)
    self.assertIsNotNone(bucket)
    self.assertEqual(bucket.name, overridden_bucket_name)

    # verify soft delete policy is disabled by default in the default bucket
    # after creation
    self.assertEqual(bucket.soft_delete_policy.retention_duration_seconds, 0)
    bucket.delete()

    self.assertIsNone(self.gcsio.get_bucket(overridden_bucket_name))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
