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

import json
import unittest
from unittest import mock

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.secret import RawSecret

try:
  import boto3  # pylint: disable=unused-import

  from apache_beam.io.aws.clients import client_options
  from apache_beam.io.aws.clients.s3 import boto3_client
except ImportError:
  boto3_client = None

try:
  from google.cloud import secretmanager  # pylint: disable=unused-import
except ImportError:
  secretmanager = None


@unittest.skipIf(boto3_client is None, 'boto3 is not installed')
class Boto3ClientOptionsTest(unittest.TestCase):
  def setUp(self):
    client_options._fetch_secret.cache_clear()
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
            '--s3_api_version=2006-03-01',
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

  def test_s3_options_without_aws_options(self):
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

  def test_s3_options_override_aws_options(self):
    boto3_client.Client(
        PipelineOptions([
            '--aws_access_key_id=AKIDEXAMPLE',
            '--aws_region_name=us-east-1',
            '--aws_endpoint_url=https://vpce.example.com',
            '--s3_region_name=us-west-2',
            '--s3_endpoint_url=http://localhost:9000',
        ]))
    kwargs = self._client_kwargs()
    self.assertEqual(kwargs['region_name'], 'us-west-2')
    self.assertEqual(kwargs['endpoint_url'], 'http://localhost:9000')
    # Settings without an S3 override use the --aws_* default.
    self.assertEqual(kwargs['aws_access_key_id'], 'AKIDEXAMPLE')

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
    self.assertIsNone(kwargs['aws_session_token'])
    self.assertTrue(kwargs['use_ssl'])

  @mock.patch('apache_beam.utils.secret.Secret.from_json')
  def test_credentials_parsed_by_secret_manager(self, mock_from_json):
    key_id_spec = json.dumps({'name': 'key-id', 'project': 'p'})
    secret_key_spec = json.dumps({'name': 'secret-key', 'project': 'p'})
    token_spec = json.dumps({'name': 'token', 'project': 'p'})
    resolved = {
        key_id_spec: 'AKIDEXAMPLE',
        secret_key_spec: 'SECRETEXAMPLE',
        token_spec: 'TOKEN',
    }
    mock_from_json.side_effect = lambda spec, manager: RawSecret(resolved[spec])

    boto3_client.Client(
        PipelineOptions([
            '--aws_credentials_secret_manager=GoogleCloudSecretManager',
            f'--aws_access_key_id={key_id_spec}',
            f'--aws_secret_access_key={secret_key_spec}',
            f'--aws_session_token={token_spec}',
        ]))

    kwargs = self._client_kwargs()
    self.assertEqual(kwargs['aws_access_key_id'], 'AKIDEXAMPLE')
    self.assertEqual(kwargs['aws_secret_access_key'], 'SECRETEXAMPLE')
    self.assertEqual(kwargs['aws_session_token'], 'TOKEN')
    mock_from_json.assert_has_calls([
        mock.call(key_id_spec, 'GoogleCloudSecretManager'),
        mock.call(secret_key_spec, 'GoogleCloudSecretManager'),
        mock.call(token_spec, 'GoogleCloudSecretManager'),
    ])

  @mock.patch('apache_beam.utils.secret.Secret.from_json')
  def test_secret_manager_applies_to_s3_credential_overrides(
      self, mock_from_json):
    resolved = {
        'projects/p/secrets/key-id/versions/1': 'AKIDEXAMPLE',
        'projects/p/secrets/secret-key/versions/1': 'SECRETEXAMPLE',
    }
    mock_from_json.side_effect = lambda spec, manager: RawSecret(resolved[spec])

    boto3_client.Client({
        'aws_credentials_secret_manager': 'GoogleCloudSecretManager',
        's3_access_key_id': 'projects/p/secrets/key-id/versions/1',
        's3_secret_access_key': 'projects/p/secrets/secret-key/versions/1',
    })

    kwargs = self._client_kwargs()
    self.assertEqual(kwargs['aws_access_key_id'], 'AKIDEXAMPLE')
    self.assertEqual(kwargs['aws_secret_access_key'], 'SECRETEXAMPLE')

  @unittest.skipIf(secretmanager is None, 'GCP dependencies are not installed')
  @mock.patch('google.cloud.secretmanager.SecretManagerServiceClient')
  def test_gcp_secret_manager_end_to_end(self, mock_gcp_client):
    payloads = {
        'projects/p/secrets/key-id/versions/latest': b'AKIDEXAMPLE',
        'projects/p/secrets/secret-key/versions/latest': b'SECRETEXAMPLE',
    }

    def access(request):
      response = mock.MagicMock()
      response.payload.data = payloads[request['name']]
      return response

    mock_gcp_client.return_value.access_secret_version.side_effect = access

    boto3_client.Client(
        PipelineOptions([
            '--aws_credentials_secret_manager=GoogleCloudSecretManager',
            '--aws_access_key_id={"name": "key-id", "project": "p"}',
            '--aws_secret_access_key={"name": "secret-key", "project": "p"}',
        ]))

    kwargs = self._client_kwargs()
    self.assertEqual(kwargs['aws_access_key_id'], 'AKIDEXAMPLE')
    self.assertEqual(kwargs['aws_secret_access_key'], 'SECRETEXAMPLE')

  @mock.patch('apache_beam.utils.secret.Secret.from_json')
  def test_parsed_secrets_are_cached(self, mock_from_json):
    mock_from_json.side_effect = lambda spec, manager: RawSecret('value')
    options = PipelineOptions([
        '--aws_credentials_secret_manager=GoogleCloudSecretManager',
        '--aws_access_key_id=projects/p/secrets/key-id/versions/1',
        '--aws_secret_access_key=projects/p/secrets/secret-key/versions/1',
    ])

    boto3_client.Client(options)
    boto3_client.Client(options)

    self.assertEqual(mock_from_json.call_count, 2)

  def test_connection_setting_overrides(self):
    kwargs = client_options.get_client_kwargs(
        PipelineOptions([
            '--aws_access_key_id=AKIDEXAMPLE',
            '--aws_region_name=us-east-1',
            '--s3_region_name=us-east-2',
        ]),
        overrides={
            'region_name': 'us-west-2',
            'endpoint_url': 'http://localhost:4566',
            'disable_ssl': 'true',
            'verify': None,
        })
    self.assertEqual(kwargs['aws_access_key_id'], 'AKIDEXAMPLE')
    self.assertEqual(kwargs['region_name'], 'us-west-2')
    self.assertEqual(kwargs['endpoint_url'], 'http://localhost:4566')
    self.assertFalse(kwargs['use_ssl'])

    for bad in (
        {'access_key_id': 'x'},
        {'unknown_setting': 'x'},
        {'disable_ssl': 'maybe'},
    ):
      with self.assertRaises(ValueError):
        client_options.parse_connection_settings(bad)


if __name__ == '__main__':
  unittest.main()
