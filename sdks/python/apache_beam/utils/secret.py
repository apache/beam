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

"""Interface and implementations for Secret providers in Apache Beam."""

import abc
import logging

_LOGGER = logging.getLogger(__name__)

class Secret(abc.ABC):
  """A secret management class used for handling sensitive data.

  This class provides a generic interface for secret management. Implementations
  of this class should handle fetching secrets from a secret management system.
  """
  def __init__(self):
    self._cached_secret_bytes: Optional[bytes] = None

  def get(self, cacheSecret: bool = False) -> str:
    """Retrieve secret value as string.

    Args:
      cacheSecret: If True, caches secret value in memory after first fetch.

    Returns:
      The retrieved secret value as string.
    """
    return self.get_bytes(cacheSecret=cacheSecret).decode("utf-8")

  def get_bytes(self, cacheSecret: bool = False) -> bytes:
    """Retrieve secret value as bytes.

    Args:
      cacheSecret: If True, caches secret value in memory after first fetch.

    Returns:
      The retrieved secret value as bytes.
    """
    if cacheSecret and getattr(self, '_cached_secret_bytes', None) is not None:
      return self._cached_secret_bytes

    secret_val_bytes = self.get_secret_bytes()

    if cacheSecret:
      self._cached_secret_bytes = secret_val_bytes

    return secret_val_bytes

  @abc.abstractmethod
  def get_secret_bytes(self) -> bytes:
    """Returns the secret as a byte string."""
    raise NotImplementedError()

  @staticmethod
  def generate_secret_bytes() -> bytes:
    """Generates a new secret key."""
    from cryptography.fernet import Fernet
    return Fernet.generate_key()

  def __getstate__(self):
    """Strip cached secrets before pickling for pipeline submission/transmission."""
    state = self.__dict__.copy()
    state['_cached_secret_bytes'] = None
    return state

  @staticmethod
  def parse_secret_option(secret) -> 'Secret':
    """Parses a secret string and returns the appropriate secret type.

    The secret string should be formatted like:
    'type:<secret_type>;<secret_param>:<value>'

    For example, 'type:GcpSecret;version_name:my_secret/versions/latest'
    would return a GcpSecret initialized with 'my_secret/versions/latest'.
    """
    param_map = {}
    for param in secret.split(';'):
      parts = param.split(':')
      param_map[parts[0]] = parts[1]

    if 'type' not in param_map:
      raise ValueError('Secret string must contain a valid type parameter')

    secret_type = param_map['type'].lower()
    del param_map['type']
    secret_class = Secret
    secret_params = None
    if secret_type == 'gcpsecret':
      secret_class = GcpSecret  # type: ignore[assignment]
      secret_params = ['version_name']
    elif secret_type == 'gcphsmgeneratedsecret':
      secret_class = GcpHsmGeneratedSecret  # type: ignore[assignment]
      secret_params = [
          'project_id', 'location_id', 'key_ring_id', 'key_id', 'job_name'
      ]
    else:
      raise ValueError(
          f'Invalid secret type {secret_type}, currently only '
          'GcpSecret and GcpHsmGeneratedSecret are supported')

    for param_name in param_map.keys():
      if param_name not in secret_params:
        raise ValueError(
            f'Invalid secret parameter {param_name}, '
            f'{secret_type} only supports the following '
            f'parameters: {secret_params}')
    return secret_class(**param_map)


class GcpSecret(Secret):
  """A secret manager implementation that retrieves secrets from Google Cloud
  Secret Manager.
  """
  def __init__(self, version_name: str):
    """Initializes a GcpSecret object.

    Args:
      version_name: The full version name of the secret in Google Cloud Secret
        Manager. For example:
        projects/<id>/secrets/<secret_name>/versions/1.
        For more info, see
        https://cloud.google.com/python/docs/reference/secretmanager/latest/google.cloud.secretmanager_v1beta1.services.secret_manager_service.SecretManagerServiceClient#google_cloud_secretmanager_v1beta1_services_secret_manager_service_SecretManagerServiceClient_access_secret_version
    """
    self._version_name = version_name

  def get_secret_bytes(self) -> bytes:
    try:
      from google.cloud import secretmanager
      client = secretmanager.SecretManagerServiceClient()
      response = client.access_secret_version(
          request={"name": self._version_name})
      secret = response.payload.data
      return secret
    except Exception as e:
      raise RuntimeError(
          'Failed to retrieve secret bytes for secret '
          f'{self._version_name} with exception {e}')

  def __eq__(self, secret):
    return self._version_name == getattr(secret, '_version_name', None)


class GcpHsmGeneratedSecret(Secret):
  """A secret manager implementation that generates a secret using a GCP HSM key
  and stores it in Google Cloud Secret Manager. If the secret already exists,
  it will be retrieved.
  """
  def __init__(
      self,
      project_id: str,
      location_id: str,
      key_ring_id: str,
      key_id: str,
      job_name: str):
    """Initializes a GcpHsmGeneratedSecret object.

    Args:
      project_id: The GCP project ID.
      location_id: The GCP location ID for the HSM key.
      key_ring_id: The ID of the KMS key ring.
      key_id: The ID of the KMS key.
      job_name: The name of the job, used to generate a unique secret name.
    """
    self._project_id = project_id
    self._location_id = location_id
    self._key_ring_id = key_ring_id
    self._key_id = key_id
    self._secret_version_name = f'HsmGeneratedSecret_{job_name}'

  def get_secret_bytes(self) -> bytes:
    """Retrieves the secret bytes.

    If the secret version already exists in Secret Manager, it is retrieved.
    Otherwise, a new secret and version are created. The new secret is
    generated using the HSM key.

    Returns:
      The secret as a byte string.
    """
    try:
      from google.api_core import exceptions as api_exceptions
      from google.cloud import secretmanager
      client = secretmanager.SecretManagerServiceClient()

      project_path = f"projects/{self._project_id}"
      secret_path = f"{project_path}/secrets/{self._secret_version_name}"
      # Since we may generate multiple versions when doing this on workers,
      # just always take the first version added to maintain consistency.
      secret_version_path = f"{secret_path}/versions/1"

      try:
        response = client.access_secret_version(
            request={"name": secret_version_path})
        return response.payload.data
      except api_exceptions.NotFound:
        # Don't bother logging yet, we'll only log if we actually add the
        # secret version below
        pass

      try:
        client.create_secret(
            request={
                "parent": project_path,
                "secret_id": self._secret_version_name,
                "secret": {
                    "replication": {
                        "automatic": {}
                    }
                },
            })
      except api_exceptions.AlreadyExists:
        # Don't bother logging yet, we'll only log if we actually add the
        # secret version below
        pass

      new_key = self.generate_dek()
      try:
        # Try one more time in case it was created while we were generating the
        # DEK.
        response = client.access_secret_version(
            request={"name": secret_version_path})
        return response.payload.data
      except api_exceptions.NotFound:
        _LOGGER.info(
            "Secret version %s not found. "
            "Creating new secret and version.",
            secret_version_path)
      client.add_secret_version(
          request={
              "parent": secret_path, "payload": {
                  "data": new_key
              }
          })
      response = client.access_secret_version(
          request={"name": secret_version_path})
      return response.payload.data

    except Exception as e:
      raise RuntimeError(
          f'Failed to retrieve or create secret bytes for secret '
          f'{self._secret_version_name} with exception {e}')

  def generate_dek(self, dek_size: int = 32) -> bytes:
    """Generates a new Data Encryption Key (DEK) using an HSM-backed key.

    This function follows a key derivation process that incorporates entropy
    from the HSM-backed key into the nonce used for key derivation.

    Args:
      dek_size: The size of the DEK to generate.

    Returns:
        A new DEK of the specified size, url-safe base64-encoded.
    """
    try:
      import base64
      import os

      from cryptography.hazmat.primitives import hashes
      from cryptography.hazmat.primitives.kdf.hkdf import HKDF
      from google.cloud import kms

      # 1. Generate a random nonce (nonce_one)
      nonce_one = os.urandom(dek_size)

      # 2. Use the HSM-backed key to encrypt nonce_one to create nonce_two
      kms_client = kms.KeyManagementServiceClient()
      key_path = kms_client.crypto_key_path(
          self._project_id, self._location_id, self._key_ring_id, self._key_id)
      response = kms_client.encrypt(
          request={
              'name': key_path, 'plaintext': nonce_one
          })
      nonce_two = response.ciphertext

      # 3. Generate a Derivation Key (DK)
      dk = os.urandom(dek_size)

      # 4. Use a KDF to derive the DEK using DK and nonce_two
      hkdf = HKDF(
          algorithm=hashes.SHA256(),
          length=dek_size,
          salt=nonce_two,
          info=None,
      )
      dek = hkdf.derive(dk)
      return base64.urlsafe_b64encode(dek)
    except Exception as e:
      raise RuntimeError(f'Failed to generate DEK with exception {e}')