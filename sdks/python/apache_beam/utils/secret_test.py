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

import json
import unittest
from unittest import mock

from parameterized import param
from parameterized import parameterized

from apache_beam.utils.annotations import BeamDeprecationWarning
from apache_beam.utils.secret import GcpHsmGeneratedSecret
from apache_beam.utils.secret import GcpSecret
from apache_beam.utils.secret import RawSecret
from apache_beam.utils.secret import Secret

try:
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
          exception_str='Secret name must be specified in secret spec'),
      param(
          secret_string='type:gcpsecreT;version_name:foo;extra:val',
          exception_str='Invalid secret parameter extra'),
  ])
  def test_secret_manager_throws_on_invalid(self, secret_string, exception_str):
    with self.assertRaisesRegex(Exception, exception_str):
      Secret.parse_secret_option(secret_string)


@unittest.skipIf(secretmanager is None, 'GCP dependencies are not installed')
class GcpSecretTest(unittest.TestCase):
  @mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
  def test_gcp_secret_success(self, mock_client_cls):
    mock_client = mock.MagicMock()
    mock_client_cls.return_value = mock_client
    mock_response = mock.MagicMock()
    mock_response.payload.data = b"secret-payload-value"
    mock_client.access_secret_version.return_value = mock_response

    spec_dict = {"name": "my-secret", "version": "1", "project": "my-project"}
    secret = GcpSecret.from_dict(spec_dict)

    secret_val = secret.get_str(cacheSecret=True)
    self.assertEqual(secret_val, "secret-payload-value")
    secret_bytes = secret.get_bytes(cacheSecret=True)
    self.assertEqual(secret_bytes, b"secret-payload-value")
    mock_client.access_secret_version.assert_called_once_with(
        request={"name": "projects/my-project/secrets/my-secret/versions/1"})

    # Second call with cacheSecret=True should return cached value without calling client again
    mock_client.reset_mock()
    secret_val_cached = secret.get_str(cacheSecret=True)
    self.assertEqual(secret_val_cached, "secret-payload-value")
    mock_client.access_secret_version.assert_not_called()

  @mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
  def test_gcp_secret_get_bytes_uncached(self, mock_client_cls):
    mock_client = mock.MagicMock()
    mock_client_cls.return_value = mock_client
    mock_response = mock.MagicMock()
    mock_response.payload.data = b"secret-payload-value"
    mock_client.access_secret_version.return_value = mock_response

    spec_dict = {"name": "my-secret", "project": "my-project"}
    secret = GcpSecret.from_dict(spec_dict)

    secret_bytes = secret.get_bytes()
    self.assertEqual(secret_bytes, b"secret-payload-value")
    self.assertIsNone(secret._cached_secret_bytes)

  @mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
  def test_gcp_secret_getstate_clears_cached_secret(self, mock_client_cls):
    mock_client = mock.MagicMock()
    mock_client_cls.return_value = mock_client
    mock_response = mock.MagicMock()
    mock_response.payload.data = b"secret-payload-value"
    mock_client.access_secret_version.return_value = mock_response

    spec_dict = {"name": "my-secret", "project": "my-project"}
    secret = GcpSecret.from_dict(spec_dict)

    # Cache the secret in memory
    secret.get_str(cacheSecret=True)
    self.assertEqual(secret._cached_secret_bytes, b"secret-payload-value")

    # When pickled / getstate is called during pipeline submission
    state = secret.__getstate__()
    self.assertIsNone(state["_cached_secret_bytes"])

  @mock.patch.dict("os.environ", {"GOOGLE_CLOUD_PROJECT": "env-project-123"})
  @mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
  def test_gcp_secret_env_project_fallback(self, mock_client_cls):
    mock_client = mock.MagicMock()
    mock_client_cls.return_value = mock_client
    mock_response = mock.MagicMock()
    mock_response.payload.data = b"env-secret-val"
    mock_client.access_secret_version.return_value = mock_response

    # Project omitted from spec
    spec_dict = {"name": "env-secret", "version": "latest"}
    secret = GcpSecret.from_dict(spec_dict)

    secret_val = secret.get_str(cacheSecret=False)
    self.assertEqual(secret_val, "env-secret-val")
    self.assertEqual(secret.get_bytes(cacheSecret=False), b"env-secret-val")
    mock_client.access_secret_version.assert_called_with(
        request={
            "name": "projects/env-project-123/secrets/env-secret/versions/latest"
        })

  @mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
  def test_gcp_secret_failure_raises_exception(self, mock_client_cls):
    mock_client = mock.MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.access_secret_version.side_effect = RuntimeError(
        "Permission denied or secret not found")

    spec_dict = {"name": "non-existent-secret", "project": "my-project"}
    secret = GcpSecret.from_dict(spec_dict)

    with self.assertRaises(RuntimeError) as ctx:
      secret.get_str(cacheSecret=False)
    self.assertIn("Permission denied or secret not found", str(ctx.exception))

  @mock.patch.dict("os.environ", {}, clear=True)
  @mock.patch("google.auth.default", side_effect=Exception("No ADC"))
  def test_ill_formed_missing_project_raises_value_error(
      self, mock_auth_default):
    spec_dict = {"name": "my-secret"}
    with self.assertRaises(ValueError) as ctx:
      GcpSecret.from_dict(spec_dict)
    self.assertIn("Could not resolve GCP project ID", str(ctx.exception))

  def test_ill_formed_missing_secret_name_raises_value_error(self):
    spec_dict = {"project": "my-project"}
    with self.assertRaises(ValueError) as ctx:
      GcpSecret.from_dict(spec_dict)
    self.assertIn("Secret name must be specified", str(ctx.exception))


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
    from google.api_core import exceptions as api_exceptions

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
    from google.api_core import exceptions as api_exceptions

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

  def test_from_dict_success(self):
    spec_dict = {
        "project_id": "test-proj",
        "location_id": "global",
        "key_ring_id": "ring",
        "key_id": "key",
        "job_name": "my-job"
    }
    secret = GcpHsmGeneratedSecret.from_dict(spec_dict)
    self.assertEqual(secret._project_id, "test-proj")
    self.assertEqual(secret._location_id, "global")
    self.assertEqual(secret._key_ring_id, "ring")
    self.assertEqual(secret._key_id, "key")
    self.assertEqual(secret._job_name, "my-job")
    self.assertEqual(secret._secret_version_name, "HsmGeneratedSecret_my-job")

  def test_from_dict_missing_params_raises_value_error(self):
    spec_dict = {"project_id": "test-proj", "location_id": "global"}
    with self.assertRaises(ValueError) as ctx:
      GcpHsmGeneratedSecret.from_dict(spec_dict)
    self.assertIn("Missing required parameter(s)", str(ctx.exception))

  @mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
  def test_get_bytes_cached(self, mock_sm_client_cls):
    mock_client = mock.MagicMock()
    mock_sm_client_cls.return_value = mock_client
    mock_response = mock.MagicMock()
    mock_response.payload.data = b"hsm-derived-key"
    mock_client.access_secret_version.return_value = mock_response

    secret = GcpHsmGeneratedSecret("p", "l", "r", "k", "j")
    secret_bytes = secret.get_bytes(cacheSecret=True)
    self.assertEqual(secret_bytes, b"hsm-derived-key")

    # Second call uses cache
    mock_client.reset_mock()
    self.assertEqual(secret.get_bytes(cacheSecret=True), b"hsm-derived-key")
    mock_client.access_secret_version.assert_not_called()

  @mock.patch("google.cloud.secretmanager.SecretManagerServiceClient")
  def test_getstate_clears_cached_secret(self, mock_sm_client_cls):
    mock_client = mock.MagicMock()
    mock_sm_client_cls.return_value = mock_client
    mock_response = mock.MagicMock()
    mock_response.payload.data = b"hsm-derived-key"
    mock_client.access_secret_version.return_value = mock_response

    secret = GcpHsmGeneratedSecret("p", "l", "r", "k", "j")
    secret.get_bytes(cacheSecret=True)
    self.assertEqual(secret._cached_secret_bytes, b"hsm-derived-key")

    state = secret.__getstate__()
    self.assertIsNone(state["_cached_secret_bytes"])


class RawSecretTest(unittest.TestCase):
  def test_raw_secret_str(self):
    secret = RawSecret("STATIC_SECRET_")
    self.assertEqual(secret.get_str(cacheSecret=True), "STATIC_SECRET_")
    self.assertEqual(secret.get_bytes(cacheSecret=True), b"STATIC_SECRET_")

  def test_raw_secret_bytes(self):
    secret = RawSecret(b"STATIC_BYTES_")
    self.assertEqual(secret.get_str(cacheSecret=True), "STATIC_BYTES_")
    self.assertEqual(secret.get_bytes(cacheSecret=True), b"STATIC_BYTES_")


class SecretFactoryTest(unittest.TestCase):
  def test_secret_factory(self):
    spec = json.dumps({"name": "test-secret", "project": "proj"})

    # When provider is set to 'GoogleCloudSecretManager'
    secret_gcp = Secret.from_json(
        spec=spec, secret_manager="GoogleCloudSecretManager")
    self.assertIsInstance(secret_gcp, GcpSecret)

    # When spec is a valid JSON string
    single_quoted_spec = "{\"name\": \"test-secret\", \"project\": \"proj\"}"
    secret_single_quoted = Secret.from_json(
        spec=single_quoted_spec, secret_manager="GoogleCloudSecretManager")
    self.assertIsInstance(secret_single_quoted, GcpSecret)
    self.assertEqual(
        secret_single_quoted._version_name,
        "projects/proj/secrets/test-secret/versions/latest")

    # When spec is a single-quoted JSON string, we still allow it for convienence
    # though it is not a valid JSON string.
    single_quoted_spec = "{'name': 'test-secret', 'project': 'proj'}"
    secret_single_quoted = Secret.from_json(
        spec=single_quoted_spec, secret_manager="GoogleCloudSecretManager")
    self.assertIsInstance(secret_single_quoted, GcpSecret)
    self.assertEqual(
        secret_single_quoted._version_name,
        "projects/proj/secrets/test-secret/versions/latest")

    # When provider is None or empty with plain string
    secret_raw = Secret.from_json(spec="STATIC_SECRET_", secret_manager=None)
    self.assertIsInstance(secret_raw, RawSecret)

    # Unsupported provider raises ValueError
    with self.assertRaises(ValueError):
      Secret.from_json(spec="spec", secret_manager="unsupported_provider")

    # Non-string spec raises TypeError
    spec_dict = {"name": "test-secret"}
    with self.assertRaises(TypeError):
      Secret.from_json(
          spec=spec_dict,  # type: ignore[arg-type]
          secret_manager="GoogleCloudSecretManager")

  def test_secret_factory_hsm(self):
    hsm_spec = json.dumps({
        "project_id": "p",
        "location_id": "l",
        "key_ring_id": "r",
        "key_id": "k",
        "job_name": "j"
    })
    secret_hsm = Secret.from_json(
        spec=hsm_spec, secret_manager="GoogleCloudHsmGeneratedSecretManager")
    self.assertIsInstance(secret_hsm, GcpHsmGeneratedSecret)
    self.assertEqual(secret_hsm._project_id, "p")

  def test_json_secret_without_secret_manager_warning(self):
    json_spec = json.dumps({"name": "my-secret", "project": "my-proj"})
    with self.assertWarns(UserWarning):
      secret = Secret.from_json(spec=json_spec, secret_manager=None)
    self.assertIsInstance(secret, RawSecret)

  def test_generate_secret_bytes(self):
    key = Secret.generate_secret_bytes()
    self.assertIsInstance(key, bytes)
    self.assertTrue(len(key) > 0)

  def test_equality(self):
    raw1 = RawSecret("secret_value")
    raw2 = RawSecret("secret_value")
    raw3 = RawSecret("other_value")
    self.assertEqual(raw1, raw2)
    self.assertNotEqual(raw1, raw3)
    self.assertNotEqual(raw1, "secret_value")

    gcp1 = GcpSecret.from_dict({"name": "sec", "project": "proj"})
    gcp2 = GcpSecret.from_dict({"name": "sec", "project": "proj"})
    gcp3 = GcpSecret.from_dict({"name": "other", "project": "proj"})
    self.assertEqual(gcp1, gcp2)
    self.assertNotEqual(gcp1, gcp3)
    self.assertNotEqual(gcp1, raw1)

    hsm1 = GcpHsmGeneratedSecret("p", "l", "r", "k", "j")
    hsm2 = GcpHsmGeneratedSecret("p", "l", "r", "k", "j")
    hsm3 = GcpHsmGeneratedSecret("p", "l", "r", "k", "other")
    self.assertEqual(hsm1, hsm2)
    self.assertNotEqual(hsm1, hsm3)
    self.assertNotEqual(hsm1, gcp1)


if __name__ == "__main__":
  unittest.main()
