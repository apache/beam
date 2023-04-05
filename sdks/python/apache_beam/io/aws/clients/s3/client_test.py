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

import logging
import os
import unittest

from apache_beam.io.aws import s3io
from apache_beam.io.aws.clients.s3 import fake_client
from apache_beam.io.aws.clients.s3 import messages
from apache_beam.options import pipeline_options


class ClientErrorTest(unittest.TestCase):
  def setUp(self):

    # These tests can be run locally against a mock S3 client, or as integration
    # tests against the real S3 client.
    self.USE_MOCK = True

    # If you're running integration tests with S3, set this variable to be an
    # s3 path that you have access to where test data can be written. If you're
    # just running tests against the mock, this can be any s3 path. It should
    # end with a '/'.
    self.TEST_DATA_PATH = 's3://random-data-sets/beam_tests/'

    self.test_bucket, self.test_path = s3io.parse_s3_path(self.TEST_DATA_PATH)

    if self.USE_MOCK:
      self.client = fake_client.FakeS3Client()
      test_data_bucket, _ = s3io.parse_s3_path(self.TEST_DATA_PATH)
      self.client.known_buckets.add(test_data_bucket)
      self.aws = s3io.S3IO(self.client)
    else:
      self.aws = s3io.S3IO(options=pipeline_options.S3Options())

  def test_get_object_metadata(self):

    # Test nonexistent object
    object = self.test_path + 'nonexistent_file_doesnt_exist'
    request = messages.GetRequest(self.test_bucket, object)
    self.assertRaises(
        messages.S3ClientError, self.client.get_object_metadata, request)

    try:
      self.client.get_object_metadata(request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 404)

  def test_get_range_nonexistent(self):

    # Test nonexistent object
    object = self.test_path + 'nonexistent_file_doesnt_exist'
    request = messages.GetRequest(self.test_bucket, object)
    self.assertRaises(
        messages.S3ClientError, self.client.get_range, request, 0, 10)

    try:
      self.client.get_range(request, 0, 10)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 404)

  def test_get_range_bad_start_end(self):

    file_name = self.TEST_DATA_PATH + 'get_range'
    contents = os.urandom(1024)

    with self.aws.open(file_name, 'w') as f:
      f.write(contents)
    bucket, object = s3io.parse_s3_path(file_name)

    response = self.client.get_range(
        messages.GetRequest(bucket, object), -10, 20)
    self.assertEqual(response, contents)

    response = self.client.get_range(
        messages.GetRequest(bucket, object), 20, 10)
    self.assertEqual(response, contents)

    # Clean up
    self.aws.delete(file_name)

  def test_upload_part_nonexistent_upload_id(self):

    object = self.test_path + 'upload_part'
    upload_id = 'not-an-id-12345'
    part_number = 1
    contents = os.urandom(1024)

    request = messages.UploadPartRequest(
        self.test_bucket, object, upload_id, part_number, contents)

    self.assertRaises(messages.S3ClientError, self.client.upload_part, request)

    try:
      self.client.upload_part(request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 404)

  def test_copy_nonexistent(self):

    src_key = self.test_path + 'not_a_real_file_does_not_exist'
    dest_key = self.test_path + 'destination_file_location'

    request = messages.CopyRequest(
        self.test_bucket, src_key, self.test_bucket, dest_key)

    with self.assertRaises(messages.S3ClientError) as e:
      self.client.copy(request)

    self.assertEqual(e.exception.code, 404)

  def test_upload_part_bad_number(self):

    object = self.test_path + 'upload_part'
    contents = os.urandom(1024)

    request = messages.UploadRequest(self.test_bucket, object, None)
    response = self.client.create_multipart_upload(request)
    upload_id = response.upload_id

    part_number = 0.5
    request = messages.UploadPartRequest(
        self.test_bucket, object, upload_id, part_number, contents)

    self.assertRaises(messages.S3ClientError, self.client.upload_part, request)

    try:
      response = self.client.upload_part(request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 400)

  def test_complete_multipart_upload_too_small(self):

    object = self.test_path + 'upload_part'
    request = messages.UploadRequest(self.test_bucket, object, None)
    response = self.client.create_multipart_upload(request)
    upload_id = response.upload_id

    part_number = 1
    contents_1 = os.urandom(1024)
    request_1 = messages.UploadPartRequest(
        self.test_bucket, object, upload_id, part_number, contents_1)
    response_1 = self.client.upload_part(request_1)

    part_number = 2
    contents_2 = os.urandom(1024)
    request_2 = messages.UploadPartRequest(
        self.test_bucket, object, upload_id, part_number, contents_2)
    response_2 = self.client.upload_part(request_2)

    parts = [{
        'PartNumber': 1, 'ETag': response_1.etag
    }, {
        'PartNumber': 2, 'ETag': response_2.etag
    }]
    complete_request = messages.CompleteMultipartUploadRequest(
        self.test_bucket, object, upload_id, parts)

    try:
      self.client.complete_multipart_upload(complete_request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 400)

  def test_complete_multipart_upload_too_many(self):

    object = self.test_path + 'upload_part'
    request = messages.UploadRequest(self.test_bucket, object, None)
    response = self.client.create_multipart_upload(request)
    upload_id = response.upload_id

    part_number = 1
    contents_1 = os.urandom(5 * 1024)
    request_1 = messages.UploadPartRequest(
        self.test_bucket, object, upload_id, part_number, contents_1)
    response_1 = self.client.upload_part(request_1)

    part_number = 2
    contents_2 = os.urandom(1024)
    request_2 = messages.UploadPartRequest(
        self.test_bucket, object, upload_id, part_number, contents_2)
    response_2 = self.client.upload_part(request_2)

    parts = [
        {
            'PartNumber': 1, 'ETag': response_1.etag
        },
        {
            'PartNumber': 2, 'ETag': response_2.etag
        },
        {
            'PartNumber': 3, 'ETag': 'fake-etag'
        },
    ]
    complete_request = messages.CompleteMultipartUploadRequest(
        self.test_bucket, object, upload_id, parts)

    try:
      self.client.complete_multipart_upload(complete_request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 400)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
