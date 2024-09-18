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

"""Tests for Google Cloud Storage client."""
# pytype: skip-file

import logging
import os
import random
import unittest
from datetime import datetime

import mock

from apache_beam import version as beam_version
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.worker import statesampler
from apache_beam.utils import counters

# pylint: disable=wrong-import-order, wrong-import-position

try:
  from apache_beam.io.gcp import gcsio
  from apache_beam.io.gcp.gcsio_retry import DEFAULT_RETRY_WITH_THROTTLING_COUNTER
  from google.cloud.exceptions import BadRequest, NotFound
except ImportError:
  NotFound = None
# pylint: enable=wrong-import-order, wrong-import-position

DEFAULT_GCP_PROJECT = 'apache-beam-testing'


class FakeGcsClient(object):
  # Fake storage client.

  def __init__(self):
    self.buckets = {}

  def _add_bucket(self, bucket):
    self.buckets[bucket.name] = bucket
    return self.buckets[bucket.name]

  def bucket(self, name):
    return FakeBucket(self, name)

  def create_bucket(self, name):
    return self._add_bucket(self.bucket(name))

  def get_bucket(self, name):
    if name in self.buckets:
      return self.buckets[name]
    else:
      raise NotFound("Bucket not found")

  def lookup_bucket(self, name):
    if name in self.buckets:
      return self.buckets[name]
    else:
      return self.create_bucket(name)

  def batch(self):
    pass

  def add_file(self, bucket, blob, contents):
    folder = self.lookup_bucket(bucket)
    holder = folder.lookup_blob(blob)
    holder.contents = contents

  def get_file(self, bucket, blob):
    folder = self.get_bucket(bucket.name)
    holder = folder.get_blob(blob.name)
    return holder

  def list_blobs(self, bucket_or_path, prefix=None, **unused_kwargs):
    bucket = self.get_bucket(bucket_or_path.name)
    if not prefix:
      return list(bucket.blobs.values())
    else:
      output = []
      for name in list(bucket.blobs):
        if name[0:len(prefix)] == prefix:
          output.append(bucket.blobs[name])
      return output


class FakeBucket(object):
  #Fake bucket for storing test blobs locally.

  def __init__(self, client, name):
    self.client = client
    self.name = name
    self.blobs = {}
    self.default_kms_key_name = None

  def _get_canonical_bucket(self):
    return self.client.get_bucket(self.name)

  def _create_blob(self, name):
    return FakeBlob(name, self)

  def add_blob(self, blob):
    bucket = self._get_canonical_bucket()
    bucket.blobs[blob.name] = blob
    return bucket.blobs[blob.name]

  def blob(self, name):
    return self._create_blob(name)

  def copy_blob(self, blob, dest, new_name=None, **kwargs):
    if self.get_blob(blob.name) is None:
      raise NotFound("source blob not found")
    if not new_name:
      new_name = blob.name
    new_blob = FakeBlob(new_name, dest)
    dest.add_blob(new_blob)
    return new_blob

  def get_blob(self, blob_name, **unused_kwargs):
    bucket = self._get_canonical_bucket()
    if blob_name in bucket.blobs:
      return bucket.blobs[blob_name]
    else:
      return None

  def lookup_blob(self, name):
    bucket = self._get_canonical_bucket()
    if name in bucket.blobs:
      return bucket.blobs[name]
    else:
      return bucket.create_blob(name)

  def set_default_kms_key_name(self, name):
    self.default_kms_key_name = name

  def delete_blob(self, name, **kwargs):
    bucket = self._get_canonical_bucket()
    if name in bucket.blobs:
      del bucket.blobs[name]


class FakeBlob(object):
  def __init__(
      self,
      name,
      bucket,
      size=0,
      contents=None,
      generation=1,
      crc32c=None,
      kms_key_name=None,
      updated=None,
      fail_when_getting_metadata=False,
      fail_when_reading=False):
    self.name = name
    self.bucket = bucket
    self.size = size
    self.contents = contents
    self._generation = generation
    self.crc32c = crc32c
    self.kms_key_name = kms_key_name
    self.updated = updated
    self._fail_when_getting_metadata = fail_when_getting_metadata
    self._fail_when_reading = fail_when_reading
    self.generation = random.randint(0, (1 << 63) - 1)

  def delete(self):
    self.bucket.delete_blob(self.name)

  def download_as_bytes(self, **kwargs):
    blob = self.bucket.get_blob(self.name)
    if blob is None:
      raise NotFound("blob not found")
    return blob.contents

  def __eq__(self, other):
    return self.bucket.get_blob(self.name) is other.bucket.get_blob(other.name)


@unittest.skipIf(NotFound is None, 'GCP dependencies are not installed')
class TestGCSPathParser(unittest.TestCase):

  BAD_GCS_PATHS = [
      'gs://',
      'gs://bucket',
      'gs:///name',
      'gs:///',
      'gs:/blah/bucket/name',
  ]

  def test_gcs_path(self):
    self.assertEqual(
        gcsio.parse_gcs_path('gs://bucket/name'), ('bucket', 'name'))
    self.assertEqual(
        gcsio.parse_gcs_path('gs://bucket/name/sub'), ('bucket', 'name/sub'))

  def test_bad_gcs_path(self):
    for path in self.BAD_GCS_PATHS:
      self.assertRaises(ValueError, gcsio.parse_gcs_path, path)
    self.assertRaises(ValueError, gcsio.parse_gcs_path, 'gs://bucket/')

  def test_gcs_path_object_optional(self):
    self.assertEqual(
        gcsio.parse_gcs_path('gs://bucket/name', object_optional=True),
        ('bucket', 'name'))
    self.assertEqual(
        gcsio.parse_gcs_path('gs://bucket/', object_optional=True),
        ('bucket', ''))

  def test_bad_gcs_path_object_optional(self):
    for path in self.BAD_GCS_PATHS:
      self.assertRaises(ValueError, gcsio.parse_gcs_path, path, True)


class SampleOptions(object):
  def __init__(self, project, region, kms_key=None):
    self.project = DEFAULT_GCP_PROJECT
    self.region = region
    self.dataflow_kms_key = kms_key


_DEFAULT_UNIVERSE_DOMAIN = "googleapis.com"


def _make_credentials(project=None, universe_domain=_DEFAULT_UNIVERSE_DOMAIN):
  import google.auth.credentials

  if project is not None:
    return mock.Mock(
        spec=google.auth.credentials.Credentials,
        project_id=project,
        universe_domain=universe_domain,
    )

  return mock.Mock(
      spec=google.auth.credentials.Credentials, universe_domain=universe_domain)


@unittest.skipIf(NotFound is None, 'GCP dependencies are not installed')
class TestGCSIO(unittest.TestCase):
  def _insert_random_file(
      self,
      client,
      path,
      size=0,
      crc32c=None,
      kms_key_name=None,
      updated=None,
      fail_when_getting_metadata=False,
      fail_when_reading=False):
    bucket_name, blob_name = gcsio.parse_gcs_path(path)
    bucket = client.lookup_bucket(bucket_name)
    blob = FakeBlob(
        blob_name,
        bucket,
        size=size,
        contents=os.urandom(size),
        crc32c=crc32c,
        kms_key_name=kms_key_name,
        updated=updated,
        fail_when_getting_metadata=fail_when_getting_metadata,
        fail_when_reading=fail_when_reading)
    bucket.add_blob(blob)
    return blob

  def setUp(self):
    self.client = FakeGcsClient()
    self.gcs = gcsio.GcsIO(self.client)
    self.client.create_bucket("gcsio-test")

  def test_read_bucket_metric(self):
    sampler = statesampler.StateSampler('', counters.CounterFactory())
    statesampler.set_current_tracker(sampler)
    state1 = sampler.scoped_state(
        'mystep', 'myState', metrics_container=MetricsContainer('mystep'))

    try:
      sampler.start()
      with state1:
        client = FakeGcsClient()
        gcs = gcsio.GcsIO(client, {"enable_bucket_read_metric_counter": True})
        client.create_bucket("gcsio-test")
        file_name = 'gs://gcsio-test/dummy_file'
        file_size = 1234
        self._insert_random_file(client, file_name, file_size)
        reader = gcs.open(file_name, 'r')
        reader.read()

        container = MetricsEnvironment.current_container()
        self.assertEqual(
            container.get_counter(
                MetricName(
                    "apache_beam.io.gcp.gcsio.BeamBlobReader",
                    "GCS_read_bytes_counter_gcsio-test")).get_cumulative(),
            file_size)
    finally:
      sampler.stop()

  def test_write_bucket_metric(self):
    sampler = statesampler.StateSampler('', counters.CounterFactory())
    statesampler.set_current_tracker(sampler)
    state1 = sampler.scoped_state(
        'mystep', 'myState', metrics_container=MetricsContainer('mystep'))

    try:
      sampler.start()
      with state1:
        client = FakeGcsClient()
        gcs = gcsio.GcsIO(client, {"enable_bucket_write_metric_counter": True})
        client.create_bucket("gcsio-test")
        file_name = 'gs://gcsio-test/dummy_file'
        gcsFile = gcs.open(file_name, 'w')
        gcsFile.write(str.encode("some text"))

        container = MetricsEnvironment.current_container()
        self.assertEqual(
            container.get_counter(
                MetricName(
                    "apache_beam.io.gcp.gcsio.BeamBlobWriter",
                    "GCS_write_bytes_counter_gcsio-test")).get_cumulative(),
            9)
    finally:
      sampler.stop()

  def test_default_bucket_name(self):
    self.assertEqual(
        gcsio.default_gcs_bucket_name(DEFAULT_GCP_PROJECT, "us-central1"),
        'dataflow-staging-us-central1-77b801c0838aee13391c0d1885860494')

  def test_default_bucket_name_failure(self):
    self.assertEqual(
        gcsio.get_or_create_default_gcs_bucket(
            SampleOptions(
                DEFAULT_GCP_PROJECT, "us-central1", kms_key="kmskey!")),
        None)

  def test_exists(self):
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234
    self._insert_random_file(self.client, file_name, file_size)
    self.assertFalse(self.gcs.exists(file_name + 'xyz'))
    self.assertTrue(self.gcs.exists(file_name))

  @mock.patch.object(FakeBucket, 'get_blob')
  def test_exists_failure(self, mock_get):
    # Raising an error other than 404. Raising 404 is a valid failure for
    # exists() call.
    mock_get.side_effect = BadRequest("Try again")
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234
    self._insert_random_file(self.client, file_name, file_size)
    with self.assertRaises(BadRequest):
      self.gcs.exists(file_name)

  def test_checksum(self):
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234
    checksum = 'deadbeef'
    self._insert_random_file(self.client, file_name, file_size, crc32c=checksum)
    self.assertTrue(self.gcs.exists(file_name))
    self.assertEqual(checksum, self.gcs.checksum(file_name))

  def test_size(self):
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234

    self._insert_random_file(self.client, file_name, file_size)
    self.assertTrue(self.gcs.exists(file_name))
    self.assertEqual(1234, self.gcs.size(file_name))

  def test_kms_key(self):
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234
    kms_key_name = "dummy"

    self._insert_random_file(
        self.client, file_name, file_size, kms_key_name=kms_key_name)
    self.assertTrue(self.gcs.exists(file_name))
    self.assertEqual(kms_key_name, self.gcs.kms_key(file_name))

  def test_last_updated(self):
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234
    updated = datetime.fromtimestamp(123456.78)

    self._insert_random_file(self.client, file_name, file_size, updated=updated)
    self.assertTrue(self.gcs.exists(file_name))
    self.assertEqual(
        gcsio.GcsIO._updated_to_seconds(updated),
        self.gcs.last_updated(file_name))

  def test_file_status(self):
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234
    updated = datetime.fromtimestamp(123456.78)
    checksum = 'deadbeef'

    self._insert_random_file(
        self.client, file_name, file_size, updated=updated, crc32c=checksum)
    file_checksum = self.gcs.checksum(file_name)

    file_status = self.gcs._status(file_name)

    self.assertEqual(file_status['size'], file_size)
    self.assertEqual(file_status['checksum'], file_checksum)
    self.assertEqual(
        file_status['updated'], gcsio.GcsIO._updated_to_seconds(updated))

  def test_file_mode_calls(self):
    file_name = 'gs://gcsio-test/dummy_mode_file'
    self._insert_random_file(self.client, file_name)
    with mock.patch('apache_beam.io.gcp.gcsio.BeamBlobWriter') as writer:
      self.gcs.open(file_name, 'wb')
      writer.assert_called()
    with mock.patch('apache_beam.io.gcp.gcsio.BeamBlobReader') as reader:
      self.gcs.open(file_name, 'rb')
      reader.assert_called()

  def test_bad_file_modes(self):
    file_name = 'gs://gcsio-test/dummy_mode_file'
    self._insert_random_file(self.client, file_name)
    with self.assertRaises(ValueError):
      self.gcs.open(file_name, 'w+')
    with self.assertRaises(ValueError):
      self.gcs.open(file_name, 'r+b')

  def test_delete(self):
    file_name = 'gs://gcsio-test/delete_me'
    file_size = 1024
    bucket_name, blob_name = gcsio.parse_gcs_path(file_name)
    # Test deletion of non-existent file.
    bucket = self.client.get_bucket(bucket_name)
    self.gcs.delete(file_name)

    self._insert_random_file(self.client, file_name, file_size)
    self.assertTrue(blob_name in bucket.blobs)

    self.gcs.delete(file_name)

    self.assertFalse(blob_name in bucket.blobs)

  def test_copy(self):
    src_file_name = 'gs://gcsio-test/source'
    dest_file_name = 'gs://gcsio-test/dest'
    src_bucket_name, src_blob_name = gcsio.parse_gcs_path(src_file_name)
    dest_bucket_name, dest_blob_name = gcsio.parse_gcs_path(dest_file_name)
    src_bucket = self.client.lookup_bucket(src_bucket_name)
    dest_bucket = self.client.lookup_bucket(dest_bucket_name)
    file_size = 1024
    self._insert_random_file(self.client, src_file_name, file_size)
    self.assertTrue(src_blob_name in src_bucket.blobs)
    self.assertFalse(dest_blob_name in dest_bucket.blobs)

    self.gcs.copy(src_file_name, dest_file_name)

    self.assertTrue(src_blob_name in src_bucket.blobs)
    self.assertTrue(dest_blob_name in dest_bucket.blobs)

    # Test copy of non-existent files.
    with self.assertRaises(NotFound):
      self.gcs.copy(
          'gs://gcsio-test/non-existent',
          'gs://gcsio-test/non-existent-destination')

  def test_copytree(self):
    src_dir_name = 'gs://gcsio-test/source/'
    dest_dir_name = 'gs://gcsio-test/dest/'
    file_size = 1024
    paths = ['a', 'b/c', 'b/d']
    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      src_bucket_name, src_blob_name = gcsio.parse_gcs_path(src_file_name)
      dest_bucket_name, dest_blob_name = gcsio.parse_gcs_path(dest_file_name)
      src_bucket = self.client.lookup_bucket(src_bucket_name)
      dest_bucket = self.client.lookup_bucket(dest_bucket_name)
      file_size = 1024
      self._insert_random_file(self.client, src_file_name, file_size)
      self.assertTrue(src_blob_name in src_bucket.blobs)
      self.assertFalse(dest_blob_name in dest_bucket.blobs)

    self.gcs.copytree(src_dir_name, dest_dir_name)

    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      src_bucket_name, src_blob_name = gcsio.parse_gcs_path(src_file_name)
      dest_bucket_name, dest_blob_name = gcsio.parse_gcs_path(dest_file_name)
      src_bucket = self.client.lookup_bucket(src_bucket_name)
      dest_bucket = self.client.lookup_bucket(dest_bucket_name)
      self.assertTrue(src_blob_name in src_bucket.blobs)
      self.assertTrue(dest_blob_name in dest_bucket.blobs)

  def test_rename(self):
    src_file_name = 'gs://gcsio-test/source'
    dest_file_name = 'gs://gcsio-test/dest'
    src_bucket_name, src_blob_name = gcsio.parse_gcs_path(src_file_name)
    dest_bucket_name, dest_blob_name = gcsio.parse_gcs_path(dest_file_name)
    file_size = 1024
    self._insert_random_file(self.client, src_file_name, file_size)
    src_bucket = self.client.lookup_bucket(src_bucket_name)
    dest_bucket = self.client.lookup_bucket(dest_bucket_name)
    self.assertTrue(src_blob_name in src_bucket.blobs)
    self.assertFalse(dest_blob_name in dest_bucket.blobs)

    self.gcs.rename(src_file_name, dest_file_name)

    self.assertFalse(src_blob_name in src_bucket.blobs)
    self.assertTrue(dest_blob_name in dest_bucket.blobs)

  def test_file_buffered_read_call(self):
    file_name = 'gs://gcsio-test/read_line_file'
    read_buffer_size = 1024
    self._insert_random_file(self.client, file_name, 10240)

    bucket_name, blob_name = gcsio.parse_gcs_path(file_name)
    bucket = self.client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    with mock.patch('apache_beam.io.gcp.gcsio.BeamBlobReader') as reader:
      self.gcs.open(file_name, read_buffer_size=read_buffer_size)
      reader.assert_called_with(
          blob,
          chunk_size=read_buffer_size,
          enable_read_bucket_metric=False,
          retry=DEFAULT_RETRY_WITH_THROTTLING_COUNTER)

  def test_file_write_call(self):
    file_name = 'gs://gcsio-test/write_file'
    with mock.patch('apache_beam.io.gcp.gcsio.BeamBlobWriter') as writer:
      self.gcs.open(file_name, 'w')
      writer.assert_called()

  def test_list_prefix(self):
    bucket_name = 'gcsio-test'
    objects = [
        ('cow/cat/fish', 2),
        ('cow/cat/blubber', 3),
        ('cow/dog/blubber', 4),
    ]
    for (object_name, size) in objects:
      file_name = 'gs://%s/%s' % (bucket_name, object_name)
      self._insert_random_file(self.client, file_name, size)
    test_cases = [
        (
            'gs://gcsio-test/c',
            [
                ('cow/cat/fish', 2),
                ('cow/cat/blubber', 3),
                ('cow/dog/blubber', 4),
            ]),
        (
            'gs://gcsio-test/cow/',
            [
                ('cow/cat/fish', 2),
                ('cow/cat/blubber', 3),
                ('cow/dog/blubber', 4),
            ]),
        ('gs://gcsio-test/cow/cat/fish', [
            ('cow/cat/fish', 2),
        ]),
    ]
    for file_pattern, expected_object_names in test_cases:
      expected_file_names = [('gs://%s/%s' % (bucket_name, object_name), size)
                             for (object_name, size) in expected_object_names]
      self.assertEqual(
          set(self.gcs.list_prefix(file_pattern).items()),
          set(expected_file_names))

  def test_downloader_fail_non_existent_object(self):
    file_name = 'gs://gcsio-metrics-test/dummy_mode_file'
    with self.assertRaises(NotFound):
      with self.gcs.open(file_name, 'r') as f:
        f.read(1)

  def test_blob_delete(self):
    file_name = 'gs://gcsio-test/delete_me'
    file_size = 1024
    bucket_name, blob_name = gcsio.parse_gcs_path(file_name)
    # Test deletion of non-existent file.
    bucket = self.client.get_bucket(bucket_name)
    self.gcs.delete(file_name)

    self._insert_random_file(self.client, file_name, file_size)
    self.assertTrue(blob_name in bucket.blobs)

    blob = bucket.get_blob(blob_name)
    self.assertIsNotNone(blob)

    blob.delete()
    self.assertFalse(blob_name in bucket.blobs)

  @mock.patch('google.cloud._http.JSONConnection._do_request')
  @mock.patch('apache_beam.internal.gcp.auth.get_service_credentials')
  def test_headers(self, mock_get_service_credentials, mock_do_request):
    from apache_beam.internal.gcp.auth import _ApitoolsCredentialsAdapter
    mock_get_service_credentials.return_value = _ApitoolsCredentialsAdapter(
        _make_credentials("test-project"))

    gcs = gcsio.GcsIO(pipeline_options={"job_name": "test-job-name"})
    # no HTTP request when initializing GcsIO
    mock_do_request.assert_not_called()

    import requests
    response = requests.Response()
    response.status_code = 200
    mock_do_request.return_value = response

    # The function of get_bucket() is supposed to send only one HTTP request
    gcs.get_bucket("test-bucket")
    mock_do_request.assert_called_once()
    call_args = mock_do_request.call_args[0]

    # Headers are specified as the third argument of
    # google.cloud._http.JSONConnection._do_request
    actual_headers = call_args[2]
    beam_user_agent = "apache-beam/%s (GPN:Beam)" % beam_version.__version__
    self.assertIn(beam_user_agent, actual_headers['User-Agent'])
    self.assertEqual(actual_headers['x-goog-custom-audit-job'], 'test-job-name')

  @mock.patch('google.cloud._http.JSONConnection._do_request')
  @mock.patch('apache_beam.internal.gcp.auth.get_service_credentials')
  def test_create_default_bucket(
      self, mock_get_service_credentials, mock_do_request):
    from apache_beam.internal.gcp.auth import _ApitoolsCredentialsAdapter
    mock_get_service_credentials.return_value = _ApitoolsCredentialsAdapter(
        _make_credentials("test-project"))

    gcs = gcsio.GcsIO(pipeline_options={"job_name": "test-job-name"})
    # no HTTP request when initializing GcsIO
    mock_do_request.assert_not_called()

    import requests
    response = requests.Response()
    response.status_code = 200
    mock_do_request.return_value = response

    # The function of create_bucket() is supposed to send only one HTTP request
    gcs.create_bucket("test-bucket", "test-project")
    mock_do_request.assert_called_once()
    call_args = mock_do_request.call_args[0]

    # Request data is specified as the fourth argument of
    # google.cloud._http.JSONConnection._do_request
    actual_request_data = call_args[3]

    import json
    request_data_json = json.loads(actual_request_data)
    # verify soft delete policy is disabled by default in the bucket creation
    # request
    self.assertEqual(
        request_data_json['softDeletePolicy']['retentionDurationSeconds'], 0)

  @mock.patch("apache_beam.io.gcp.gcsio.GcsIO.get_bucket")
  def test_is_soft_delete_enabled(self, mock_get_bucket):
    bucket = mock.MagicMock()
    mock_get_bucket.return_value = bucket

    # soft delete policy enabled
    bucket.soft_delete_policy.retention_duration_seconds = 1024
    self.assertTrue(
        self.gcs.is_soft_delete_enabled("gs://beam_with_soft_delete/tmp"))

    # soft delete policy disabled
    bucket.soft_delete_policy.retention_duration_seconds = 0
    self.assertFalse(
        self.gcs.is_soft_delete_enabled("gs://beam_without_soft_delete/tmp"))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
