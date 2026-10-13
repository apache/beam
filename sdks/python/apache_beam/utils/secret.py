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
import json
import logging
import os
import warnings
from typing import Any
from typing import Optional
from typing import Union

_LOGGER = logging.getLogger(__name__)


class Secret(abc.ABC):
  """A secret management class used for handling sensitive data.

  This class provides a generic interface for secret management. Implementations
  of this class should handle fetching secrets from a secret management system.
  """
  def __init__(self):
    self._cached_secret_bytes: Optional[bytes] = None

  def get_str(self, cacheSecret: bool = False) -> str:
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

  def __getstate__(self):
    """Strip cached secrets before pickling for pipeline submission/transmission."""
    state = self.__dict__.copy()
    state['_cached_secret_bytes'] = None
    return state

  @staticmethod
  def generate_secret_bytes() -> bytes:
    """Generates a new secret key using Fernet."""
    from cryptography.fernet import Fernet
    return Fernet.generate_key()

  def apply_pipeline_options(self, options: Any) -> None:
    """Configures this secret from pipeline options.

    Called by :meth:`from_json` and :meth:`parse_secret_option` when pipeline
    options are given. The default implementation does nothing; secrets that
    need settings from pipeline options (e.g. :class:`AwsSecret`) override it.

    Args:
      options: PipelineOptions, or a dict of option names to values.
    """
    pass

  @classmethod
  def parse_secret_option(
      cls, secret: str, options: Optional[Any] = None) -> 'Secret':
    """Parses a secret string and returns the appropriate secret type.

    The secret string should be formatted like:
    'type:<secret_type>;<secret_param>:<value>'

    For example, 'type:GcpSecret;version_name:my_secret/versions/latest'
    would return a GcpSecret initialized with 'my_secret/versions/latest'.

    Args:
      secret: The secret string.
      options: Optional pipeline options, passed to
        :meth:`apply_pipeline_options` of the returned secret.
    """
    param_map = {}
    for param in secret.split(';'):
      parts = param.split(':')
      if len(parts) == 2:
        param_map[parts[0]] = parts[1]

    if 'type' not in param_map:
      raise ValueError('Secret string must contain a valid type parameter')

    raw_type = param_map.pop('type')
    secret_type = raw_type.lower()
    secret_manager = _SECRET_TYPE_TO_SECRET_MANAGER.get(secret_type)
    if not secret_manager:
      raise ValueError(
          f'Invalid secret type {secret_type}, currently only '
          'GcpSecret, GcpHsmGeneratedSecret, and AwsSecret are supported')

    return cls.from_json(json.dumps(param_map), secret_manager, options)

  @classmethod
  def from_json(
      cls,
      spec: str,
      secret_manager: Optional[str] = None,
      options: Optional[Any] = None) -> 'Secret':
    """Return a Secret instance based on secret_manager provider and secret specification.

    Args:
      spec: Secret string (raw secret or JSON specification string).
      secret_manager: Secret manager string (e.g. 'GoogleCloudSecretManager').
      options: Optional pipeline options (PipelineOptions or a dict), passed to
        :meth:`apply_pipeline_options` of the returned secret. For example,
        AwsSecret uses them to get AWS credentials from AwsOptions.

    Returns:
      An instance of Secret.
    """
    secret = cls._from_json(spec, secret_manager)
    if options is not None:
      secret.apply_pipeline_options(options)
    return secret

  @classmethod
  def _from_json(cls, spec: str, secret_manager: Optional[str]) -> 'Secret':
    if not isinstance(spec, str):
      raise TypeError(
          f"Secret 'spec' must be a string, got {type(spec).__name__}")

    secret_manager_name = (
        secret_manager.strip()
        if secret_manager and secret_manager.strip() else None)

    spec_dict = None
    try:
      spec_dict = json.loads(spec)
      if not isinstance(spec_dict, dict):
        spec_dict = None
    except Exception:
      try:
        import ast
        spec_dict = ast.literal_eval(spec)
        if not isinstance(spec_dict, dict):
          spec_dict = None
      except Exception:
        pass

    if secret_manager_name:
      secret_cls_entry = _SECRET_CLASSES.get(secret_manager_name.lower())
      if secret_cls_entry:
        if isinstance(secret_cls_entry, str):
          secret_cls = globals().get(secret_cls_entry, secret_cls_entry)
        else:
          secret_cls = secret_cls_entry
        if isinstance(spec_dict, dict) and hasattr(secret_cls, 'from_dict'):
          return secret_cls.from_dict(spec_dict)
        elif isinstance(spec_dict, dict):
          return secret_cls(**spec_dict)
        else:
          return secret_cls(spec)
      else:
        raise ValueError(
            f"Unsupported secret manager: '{secret_manager_name}'. Currently supported options: 'GoogleCloudSecretManager', 'GoogleCloudHsmGeneratedSecretManager', 'AwsSecretsManager'."
        )

    # If secret_manager is not set or empty, check if spec is a JSON specification dict
    if spec_dict is not None:
      msg = (
          "The 'spec' parameter appears to be a JSON specification, but "
          "'secret_manager' is not set. Defaulting to Raw.")
      _LOGGER.warning(msg)
      warnings.warn(msg, UserWarning)

    return RawSecret(spec)


class RawSecret(Secret):
  """Secret implementation wrapping a raw secret string or bytes directly."""
  def __init__(self, secret: Union[str, bytes]):
    super().__init__()
    if isinstance(secret, str):
      self._secret = secret.encode("utf-8")
    else:
      self._secret = secret

  def get_secret_bytes(self) -> bytes:
    return self._secret

  def __eq__(self, other: Any) -> bool:
    if not isinstance(other, RawSecret):
      return False
    return self._secret == other._secret


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
    super().__init__()
    self._version_name = version_name

  @classmethod
  def from_dict(cls, spec_dict: dict[str, str]) -> 'GcpSecret':
    """Initialize GcpSecret from a dictionary specification."""
    allowed_keys = {'version_name', 'name', 'project', 'version'}
    invalid_keys = set(spec_dict.keys()) - allowed_keys
    if invalid_keys:
      raise ValueError(
          f"Invalid secret parameter {', '.join(sorted(invalid_keys))}")
    version_name = cls._parse_version_name(spec_dict)
    return cls(version_name)

  @classmethod
  def _parse_version_name(cls, spec_dict: dict[str, str]) -> str:
    if "version_name" in spec_dict:
      return spec_dict["version_name"]

    secret_id = spec_dict.get("name")
    if not secret_id:
      raise ValueError("Secret name must be specified in secret spec.")

    # Resolve project ID from spec, environment variables, or Application Default Credentials
    project_id = (
        spec_dict.get("project") or os.environ.get("GOOGLE_CLOUD_PROJECT") or
        os.environ.get("GCP_PROJECT"))

    if not project_id:
      try:
        import google.auth
        _, project_id = google.auth.default()
      except Exception:
        pass

    version_id = spec_dict.get("version", "latest")

    if not project_id:
      raise ValueError(
          f"Could not resolve GCP project ID for secret '{secret_id}'. "
          "Please specify 'project' in the secret spec, set GOOGLE_CLOUD_PROJECT environment variable, "
          "or configure Application Default Credentials.")

    return f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

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


class AwsSecret(Secret):
  """A secret manager implementation that retrieves secrets from AWS Secrets
  Manager.

  AWS credentials and connection settings (region, endpoint, SSL) come from the
  ``--aws_*`` defaults in :class:`AwsOptions` when pipeline options are given
  (see :meth:`apply_pipeline_options`), and otherwise from boto3's defaults
  (e.g. environment variables, ~/.aws, or an IAM role). Connection settings
  can be overridden per secret with keyword arguments, e.g.
  ``AwsSecret('my-secret', region_name='us-west-2')``, or in the secret spec,
  e.g. ``type:awssecret;name:my-secret;region_name:us-west-2``.
  """
  # Spec keys that identify the secret. Other spec keys are client settings.
  _SECRET_SPEC_KEYS = frozenset({
      'secret_id',
      'name',
      'arn',
      'version_id',
      'version',
      'version_stage',
      'stage',
  })

  def __init__(
      self,
      secret_id: str,
      version_id: Optional[str] = None,
      version_stage: Optional[str] = None,
      **client_settings: Any):
    """Initializes an AwsSecret object.

    Args:
      secret_id: The ARN or name of the secret in AWS Secrets Manager.
      version_id: The unique identifier of the version of the secret.
      version_stage: The staging label of the version of the secret (e.g. 'AWSCURRENT').
      **client_settings: AWS client connection settings that override the
        ``--aws_*`` pipeline options for this secret. Names are AwsOptions
        setting names without the ``aws_`` prefix (e.g. ``region_name``,
        ``endpoint_url``, ``verify``, ``disable_ssl``). Credentials cannot be
        set here; use the ``--aws_*`` pipeline options instead.
    """
    super().__init__()
    self._secret_id = secret_id
    self._version_id = version_id
    self._version_stage = version_stage
    # Set by apply_pipeline_options.
    self._aws_options: Optional[dict[str, Any]] = None
    self._client_settings: Optional[dict[str, Any]] = None
    if client_settings:
      from apache_beam.io.aws.clients import client_options
      self._client_settings = client_options.parse_connection_settings(
          client_settings)

  @classmethod
  def from_dict(cls, spec_dict: dict[str, str]) -> 'AwsSecret':
    """Initialize AwsSecret from a dictionary specification.

    Keys other than the secret identifiers are AWS client connection settings
    (see the ``**client_settings`` keyword arguments of :meth:`__init__`).
    """
    secret_id = (
        spec_dict.get("secret_id") or spec_dict.get("name") or
        spec_dict.get("arn"))
    if not secret_id:
      raise ValueError("Secret ID or name must be specified in secret spec.")

    version_id = spec_dict.get("version_id") or spec_dict.get("version")
    version_stage = spec_dict.get("version_stage") or spec_dict.get("stage")
    client_settings = {
        k: v
        for k, v in spec_dict.items() if k not in cls._SECRET_SPEC_KEYS
    }

    return cls(
        secret_id=secret_id,
        version_id=version_id,
        version_stage=version_stage,
        **client_settings,
    )

  def apply_pipeline_options(self, options: Any) -> None:
    """Uses AWS credentials and connection settings from AwsOptions.

    Only the (unresolved) option values are stored; credential secret specs
    are resolved when the secret is fetched, e.g. on workers. Settings in
    ``client_settings`` still take precedence.

    Args:
      options: PipelineOptions, or a dict of option names to values.
    """
    from apache_beam.io.aws.clients import client_options
    from apache_beam.options.pipeline_options import AwsOptions
    aws_options = client_options.view_options(options, AwsOptions)
    self._aws_options = aws_options.get_all_options(
        drop_default=True, current_only=True) or None

  def _get_client_kwargs(self) -> dict[str, Any]:
    if not self._aws_options and not self._client_settings:
      return {}
    from apache_beam.io.aws.clients import client_options
    # Unset settings are dropped so boto3 falls back to its own defaults.
    return {
        k: v
        for k, v in client_options.get_client_kwargs(
            self._aws_options or {}, overrides=self._client_settings).items()
        if v is not None
    }

  def get_secret_bytes(self) -> bytes:
    try:
      import base64
      import boto3

      client = boto3.client('secretsmanager', **self._get_client_kwargs())

      request_kwargs = {'SecretId': self._secret_id}
      if self._version_id:
        request_kwargs['VersionId'] = self._version_id
      if self._version_stage:
        request_kwargs['VersionStage'] = self._version_stage

      response = client.get_secret_value(**request_kwargs)
      if 'SecretString' in response:
        return response['SecretString'].encode('utf-8')
      elif 'SecretBinary' in response:
        binary_data = response['SecretBinary']
        if isinstance(binary_data, str):
          return base64.b64decode(binary_data)
        return binary_data
      else:
        raise ValueError(
            f"Secret '{self._secret_id}' contains neither SecretString nor SecretBinary."
        )
    except Exception as e:
      raise RuntimeError(
          'Failed to retrieve secret bytes for secret '
          f'{self._secret_id} with exception {e}')

  def __eq__(self, other: Any) -> bool:
    if not isinstance(other, AwsSecret):
      return False
    return (
        self._secret_id == other._secret_id and
        self._version_id == other._version_id and
        self._version_stage == other._version_stage and
        self._aws_options == other._aws_options and
        self._client_settings == other._client_settings)


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
    super().__init__()
    self._project_id = project_id
    self._location_id = location_id
    self._key_ring_id = key_ring_id
    self._key_id = key_id
    self._job_name = job_name
    self._secret_version_name = f'HsmGeneratedSecret_{job_name}'

  def __eq__(self, other: Any) -> bool:
    if not isinstance(other, GcpHsmGeneratedSecret):
      return False
    return (
        self._project_id == other._project_id and
        self._location_id == other._location_id and
        self._key_ring_id == other._key_ring_id and
        self._key_id == other._key_id and
        getattr(self, '_job_name', None) == getattr(other, '_job_name', None))

  @classmethod
  def from_dict(cls, spec_dict: dict[str, str]) -> 'GcpHsmGeneratedSecret':
    """Initialize GcpHsmGeneratedSecret from a dictionary specification."""
    allowed_keys = {
        'project_id', 'location_id', 'key_ring_id', 'key_id', 'job_name'
    }
    missing = allowed_keys - set(spec_dict.keys())
    if missing:
      raise ValueError(
          f"Missing required parameter(s) for GcpHsmGeneratedSecret: {sorted(list(missing))}"
      )
    invalid_keys = set(spec_dict.keys()) - allowed_keys
    if invalid_keys:
      raise ValueError(
          f"Invalid secret parameter {', '.join(sorted(invalid_keys))}")
    return cls(
        project_id=spec_dict['project_id'],
        location_id=spec_dict['location_id'],
        key_ring_id=spec_dict['key_ring_id'],
        key_id=spec_dict['key_id'],
        job_name=spec_dict['job_name'],
    )

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


_SECRET_TYPE_TO_SECRET_MANAGER: dict[str, str] = {
    "gcpsecret": "GoogleCloudSecretManager",
    "gcphsmgeneratedsecret": "GoogleCloudHsmGeneratedSecretManager",
    "awssecret": "AwsSecretsManager",
}

_SECRET_CLASSES: dict[str, Any] = {
    "googlecloudsecretmanager": "GcpSecret",
    "googlecloudhsmgeneratedsecretmanager": "GcpHsmGeneratedSecret",
    "awssecretsmanager": "AwsSecret",
}