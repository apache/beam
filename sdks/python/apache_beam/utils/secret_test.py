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

import unittest
from unittest import mock

from parameterized import param
from parameterized import parameterized

from apache_beam.utils.secret import GcpHsmGeneratedSecret
from apache_beam.utils.secret import GcpSecret
from apache_beam.utils.secret import Secret

try:
  from google.api_core import exceptions as api_exceptions
  from google.cloud import secretmanager
except ImportError:
  secretmanager = None  # type: ignore[assignment]

class SecretTest(unittest.TestCase):
  @parameterized.expand([
      param(
          secret_string='type:GcpSecret;version_name:my_secret/versions/latest',
          secret=GcpSecret('my_secret/versions/latest')),
      param(
          secret_string='type:GcpSecret;version_name:foo',
          secret=GcpSecret('foo')),
      param(
          secret_string='type:gcpsecreT;version_name:my_secret/versions/latest',
          secret=GcpSecret('my_secret/versions/latest')),
  ])
  def test_secret_manager_parses_correctly(self, secret_string, secret):
    self.assertEqual(secret, Secret.parse_secret_option(secret_string))

  @parameterized.expand([
      param(
          secret_string='version_name:foo',
          exception_str='must contain a valid type parameter'),
      param(
          secret_string='type:gcpsecreT',
          exception_str='missing 1 required positional argument'),
      param(
          secret_string='type:gcpsecreT;version_name:foo;extra:val',
          exception_str='Invalid secret parameter extra'),
  ])
  def test_secret_manager_throws_on_invalid(self, secret_string, exception_str):
    with self.assertRaisesRegex(Exception, exception_str):
      Secret.parse_secret_option(secret_string)


@unittest.skipIf(secretmanager is None, 'GCP dependencies are not installed')
class GcpHsmGeneratedSecretTest(unittest.TestCase):
  def setUp(self):
    self.mock_secret_manager_client = mock.MagicMock()
    self.mock_kms_client = mock.MagicMock()

    # Patch the clients
    self.secretmanager_patcher = mock.patch(
        'google.cloud.secretmanager.SecretManagerServiceClient',
        return_value=self.mock_secret_manager_client)
    self.kms_patcher = mock.patch(
        'google.cloud.kms.KeyManagementServiceClient',
        return_value=self.mock_kms_client)
    self.os_urandom_patcher = mock.patch('os.urandom', return_value=b'0' * 32)
    self.hkdf_patcher = mock.patch(
        'cryptography.hazmat.primitives.kdf.hkdf.HKDF.derive',
        return_value=b'derived_key')

    self.secretmanager_patcher.start()
    self.kms_patcher.start()
    self.os_urandom_patcher.start()
    self.hkdf_patcher.start()

  def tearDown(self):
    self.secretmanager_patcher.stop()
    self.kms_patcher.stop()
    self.os_urandom_patcher.stop()
    self.hkdf_patcher.stop()

  def test_happy_path_secret_creation(self):
    project_id = 'test-project'
    location_id = 'global'
    key_ring_id = 'test-key-ring'
    key_id = 'test-key'
    job_name = 'test-job'

    secret = GcpHsmGeneratedSecret(
        project_id, location_id, key_ring_id, key_id, job_name)

    # Mock responses for secret creation path
    self.mock_secret_manager_client.access_secret_version.side_effect = [
        api_exceptions.NotFound('not found'),  # first check
        api_exceptions.NotFound('not found'),  # second check
        mock.MagicMock(payload=mock.MagicMock(data=b'derived_key'))
    ]
    self.mock_kms_client.encrypt.return_value = mock.MagicMock(
        ciphertext=b'encrypted_nonce')

    secret_bytes = secret.get_secret_bytes()
    self.assertEqual(secret_bytes, b'derived_key')

    # Assertions on mocks
    secret_version_path = (
        f'projects/{project_id}/secrets/{secret._secret_version_name}'
        '/versions/1')
    self.mock_secret_manager_client.access_secret_version.assert_any_call(
        request={'name': secret_version_path})
    self.assertEqual(
        self.mock_secret_manager_client.access_secret_version.call_count, 3)
    self.mock_secret_manager_client.create_secret.assert_called_once()
    self.mock_kms_client.encrypt.assert_called_once()
    self.mock_secret_manager_client.add_secret_version.assert_called_once()

  def test_secret_already_exists(self):
    project_id = 'test-project'
    location_id = 'global'
    key_ring_id = 'test-key-ring'
    key_id = 'test-key'
    job_name = 'test-job'

    secret = GcpHsmGeneratedSecret(
        project_id, location_id, key_ring_id, key_id, job_name)

    # Mock responses for secret creation path
    self.mock_secret_manager_client.access_secret_version.side_effect = [
        api_exceptions.NotFound('not found'),
        api_exceptions.NotFound('not found'),
        mock.MagicMock(payload=mock.MagicMock(data=b'derived_key'))
    ]
    self.mock_secret_manager_client.create_secret.side_effect = (
        api_exceptions.AlreadyExists('exists'))
    self.mock_kms_client.encrypt.return_value = mock.MagicMock(
        ciphertext=b'encrypted_nonce')

    secret_bytes = secret.get_secret_bytes()
    self.assertEqual(secret_bytes, b'derived_key')

    # Assertions on mocks
    self.mock_secret_manager_client.create_secret.assert_called_once()
    self.mock_secret_manager_client.add_secret_version.assert_called_once()

  def test_secret_version_already_exists(self):
    project_id = 'test-project'
    location_id = 'global'
    key_ring_id = 'test-key-ring'
    key_id = 'test-key'
    job_name = 'test-job'

    secret = GcpHsmGeneratedSecret(
        project_id, location_id, key_ring_id, key_id, job_name)

    self.mock_secret_manager_client.access_secret_version.return_value = (
        mock.MagicMock(payload=mock.MagicMock(data=b'existing_dek')))

    secret_bytes = secret.get_secret_bytes()
    self.assertEqual(secret_bytes, b'existing_dek')

    # Assertions
    self.mock_secret_manager_client.access_secret_version.assert_called_once()
    self.mock_secret_manager_client.create_secret.assert_not_called()
    self.mock_secret_manager_client.add_secret_version.assert_not_called()
    self.mock_kms_client.encrypt.assert_not_called()


if __name__ == "__main__":
  unittest.main()
