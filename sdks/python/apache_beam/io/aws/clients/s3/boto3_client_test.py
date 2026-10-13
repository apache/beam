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

"""Tests for option handling in the boto3 S3 client."""

import unittest
from unittest import mock

from apache_beam.options.pipeline_options import PipelineOptions

try:
  import boto3  # pylint: disable=unused-import

  from apache_beam.io.aws.clients.s3 import boto3_client
except ImportError:
  boto3_client = None


@unittest.skipIf(boto3_client is None, 'boto3 is not installed')
class Boto3ClientOptionsTest(unittest.TestCase):
  def setUp(self):
    patcher = mock.patch('boto3.session.Session')
    self.mock_session = patcher.start().return_value
    self.addCleanup(patcher.stop)

  def _client_kwargs(self):
    return self.mock_session.client.call_args.kwargs

  def test_aws_options_passed_to_client(self):
    boto3_client.Client(
        PipelineOptions([
            '--aws_access_key_id=AKIDEXAMPLE',
            '--aws_secret_access_key=SECRETEXAMPLE',
            '--aws_session_token=TOKEN',
            '--aws_endpoint_url=http://localhost:4566',
            '--aws_region_name=us-east-1',
            '--aws_api_version=2006-03-01',
            '--aws_verify=False',
            '--aws_disable_ssl',
        ]))
    self.assertEqual(
        self._client_kwargs(),
        {
            'service_name': 's3',
            'region_name': 'us-east-1',
            'api_version': '2006-03-01',
            'use_ssl': False,
            'verify': 'False',
            'endpoint_url': 'http://localhost:4566',
            'aws_access_key_id': 'AKIDEXAMPLE',
            'aws_secret_access_key': 'SECRETEXAMPLE',
            'aws_session_token': 'TOKEN',
        })

  def test_deprecated_s3_options_still_work(self):
    boto3_client.Client(
        PipelineOptions([
            '--s3_access_key_id=AKIDEXAMPLE',
            '--s3_secret_access_key=SECRETEXAMPLE',
            '--s3_region_name=us-west-2',
            '--s3_disable_ssl',
        ]))
    kwargs = self._client_kwargs()
    self.assertEqual(kwargs['aws_access_key_id'], 'AKIDEXAMPLE')
    self.assertEqual(kwargs['aws_secret_access_key'], 'SECRETEXAMPLE')
    self.assertEqual(kwargs['region_name'], 'us-west-2')
    self.assertFalse(kwargs['use_ssl'])

  def test_aws_options_take_precedence_over_s3_options(self):
    boto3_client.Client(
        PipelineOptions([
            '--s3_region_name=us-west-2',
            '--aws_region_name=us-east-1',
        ]))
    self.assertEqual(self._client_kwargs()['region_name'], 'us-east-1')

  def test_dict_options(self):
    boto3_client.Client({
        'aws_access_key_id': 'AKIDEXAMPLE',
        's3_secret_access_key': 'SECRETEXAMPLE',
        'aws_disable_ssl': True,
    })
    kwargs = self._client_kwargs()
    self.assertEqual(kwargs['aws_access_key_id'], 'AKIDEXAMPLE')
    self.assertEqual(kwargs['aws_secret_access_key'], 'SECRETEXAMPLE')
    self.assertFalse(kwargs['use_ssl'])

  def test_no_options_uses_boto3_defaults(self):
    boto3_client.Client(PipelineOptions([]))
    kwargs = self._client_kwargs()
    self.assertIsNone(kwargs['aws_access_key_id'])
    self.assertIsNone(kwargs['aws_secret_access_key'])
    self.assertTrue(kwargs['use_ssl'])


if __name__ == '__main__':
  unittest.main()
