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

"""Builds boto3 client arguments from :class:`AwsOptions`.

Shared across AWS service clients (e.g. the S3 filesystem client) so that every
boto3 client created by Beam uses the same credentials and connection settings.
"""

# pytype: skip-file

import functools
import types
from collections.abc import Mapping
from typing import Any
from typing import Optional
from typing import Union

from apache_beam.options.pipeline_options import AwsOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.secret import _SECRET_CLASSES
from apache_beam.utils.secret import Secret

# Secret manager names that resolve to AwsSecret. AWS credentials cannot be
# stored there, because the credentials are needed to access it.
_AWS_SECRET_MANAGERS = frozenset(
    name for name, secret_cls in _SECRET_CLASSES.items()
    if secret_cls == 'AwsSecret')

# Client settings (see AwsOptions.get_client_settings) that hold credentials.
CREDENTIAL_SETTINGS = frozenset(
    {'access_key_id', 'secret_access_key', 'session_token'})

_TRUE_STRINGS = frozenset({'true', '1', 'yes', 'y', 'on'})
_FALSE_STRINGS = frozenset({'false', '0', 'no', 'n', 'off', ''})


@functools.lru_cache(maxsize=None)
def get_connection_setting_defaults() -> Mapping[str, Any]:
  """Returns the non-credential client settings and their default values.

  The names (e.g. ``region_name``, ``endpoint_url``) come from
  AwsOptions.get_client_settings, so new settings added to AwsOptions are
  picked up automatically. The result is computed once and is read-only.
  """
  return types.MappingProxyType({
      name: value
      for name, value in AwsOptions(flags=[]).get_client_settings().items()
      if name not in CREDENTIAL_SETTINGS
  })


def parse_connection_settings(settings: Mapping[str, Any]) -> dict[str, Any]:
  """Validates connection settings, e.g. from a secret spec.

  Values for boolean settings (e.g. ``disable_ssl``) may be strings such as
  'true' or 'false'.

  Args:
    settings: A dict of client setting names (without the ``aws_`` prefix) to
      values.

  Returns:
    A dict of the settings with values converted to the setting types.

  Raises:
    ValueError: If a setting is a credential or not a known setting, or a
      boolean value is invalid.
  """
  credentials = sorted(CREDENTIAL_SETTINGS.intersection(settings))
  if credentials:
    raise ValueError(
        f"Invalid secret parameter {', '.join(credentials)}: AWS credentials "
        "cannot be set here. Use the "
        f"{', '.join('--aws_' + name for name in credentials)} pipeline "
        "option instead.")
  defaults = get_connection_setting_defaults()
  unknown = sorted(set(settings) - set(defaults))
  if unknown:
    raise ValueError(f"Invalid secret parameter {', '.join(unknown)}")

  parsed = {}
  for name, value in settings.items():
    if isinstance(defaults[name], bool) and isinstance(value, str):
      lowered = value.strip().lower()
      if lowered in _TRUE_STRINGS:
        value = True
      elif lowered in _FALSE_STRINGS:
        value = False
      else:
        raise ValueError(f'Invalid boolean value for {name}: {value}')
    parsed[name] = value
  return parsed


def view_options(
    options: Union[PipelineOptions, dict],
    options_cls: type[PipelineOptions]) -> PipelineOptions:
  """Returns an ``options_cls`` view of PipelineOptions or a dict of values."""
  if isinstance(options, PipelineOptions):
    return options.view_as(options_cls)
  return options_cls(flags=[], **options)


# S3IO (and therefore the S3 client) is created for almost every filesystem
# operation, so cache parsed secrets per process to avoid calling the secret
# manager each time. Note that a rotated secret is only picked up by new
# worker processes.
@functools.lru_cache(maxsize=32)
def _fetch_secret(spec: str, secret_manager: Optional[str]) -> str:
  return Secret.from_json(spec, secret_manager).get_str()


def _parse_credential(value: Optional[str],
                      secret_manager: Optional[str]) -> Optional[str]:
  """Parses a credential option with Secret.from_json.

  With no secret_manager, Secret.from_json returns the value itself (as a
  RawSecret), so plain credentials keep working.
  """
  if not value:
    return value
  if (secret_manager and
      secret_manager.strip().lower() in _AWS_SECRET_MANAGERS):
    raise ValueError(
        '--aws_credentials_secret_manager cannot be AWS Secrets Manager, '
        'because the AWS credentials are needed to access it. Store the '
        'credentials in another secret manager, e.g. GoogleCloudSecretManager.')
  return _fetch_secret(value, secret_manager)


def get_client_kwargs(
    options: Union[PipelineOptions, dict],
    service_options_cls: Optional[type[PipelineOptions]] = None,
    overrides: Optional[dict[str, Any]] = None) -> dict[str, Any]:
  """Returns common boto3 client keyword arguments.

  Settings come from the --aws_* defaults in AwsOptions, with the overrides
  from ``service_options_cls`` applied (e.g. --s3_* in S3Options; see
  AwsOptions.get_client_settings), and then ``overrides``. Credentials are then
  resolved through --aws_credentials_secret_manager if it is set.
  Service-specific settings that are not shared (e.g. --s3_api_version) are not
  included.

  Args:
    options: PipelineOptions, or a dict of option names to values.
    service_options_cls: Optional options class whose overrides apply, e.g.
      S3Options. If None, only the --aws_* defaults are used.
    overrides: Optional dict of connection settings (setting names without the
      ``aws_`` prefix, e.g. ``region_name``) that take precedence over the
      options. Values that are None are ignored. Credentials cannot be
      overridden; see parse_connection_settings.
  """
  aws_options = view_options(options, AwsOptions)
  service_options = (
      view_options(options, service_options_cls)
      if service_options_cls else None)
  settings = aws_options.get_client_settings(service_options)
  if overrides:
    settings.update(
        (name, value)
        for name, value in parse_connection_settings(overrides).items()
        if value is not None)
  secret_manager = aws_options.aws_credentials_secret_manager
  return {
      'aws_access_key_id': _parse_credential(
          settings['access_key_id'], secret_manager),
      'aws_secret_access_key': _parse_credential(
          settings['secret_access_key'], secret_manager),
      'aws_session_token': _parse_credential(
          settings['session_token'], secret_manager),
      'region_name': settings['region_name'],
      'endpoint_url': settings['endpoint_url'],
      'use_ssl': not settings['disable_ssl'],
      'verify': settings['verify'],
  }
