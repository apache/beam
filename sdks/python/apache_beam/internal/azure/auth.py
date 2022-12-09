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

"""Azure credentials and authentication."""

# pytype: skip-file

import logging
import threading

from apache_beam.options.pipeline_options import AzureOptions

try:
  from azure.identity import DefaultAzureCredential
  _AZURE_AUTH_AVAILABLE = True
except ImportError:
  _AZURE_AUTH_AVAILABLE = False

_LOGGER = logging.getLogger(__name__)


def get_service_credentials(pipeline_options):
  """For internal use only; no backwards-compatibility guarantees.

  Get credentials to access Azure services.
  Args:
    pipeline_options: Pipeline options, used in creating credentials
      like managed identity credentials.

  Returns:
    A ``azure.identity.*Credential`` object or None if credentials
    not found. Returned object is thread-safe.
  """
  return _Credentials.get_service_credentials(pipeline_options)


class _Credentials(object):
  _credentials_lock = threading.Lock()
  _credentials_init = False
  _credentials = None

  @classmethod
  def get_service_credentials(cls, pipeline_options):
    with cls._credentials_lock:
      if cls._credentials_init:
        return cls._credentials
      cls._credentials = cls._get_service_credentials(pipeline_options)
      cls._credentials_init = True

    return cls._credentials

  @staticmethod
  def _get_service_credentials(pipeline_options):
    if not _AZURE_AUTH_AVAILABLE:
      _LOGGER.warning(
          'Unable to find default credentials because the azure.identity '
          'library is not available. Install the azure.identity library to use '
          'Azure default credentials.')
      return None

    try:
      credentials = DefaultAzureCredential(
        managed_identity_client_id=pipeline_options.view_as(AzureOptions)\
          .azure_managed_identity_client_id)
      _LOGGER.debug('Connecting using Azure Default Credentials.')
      return credentials
    except Exception as e:
      _LOGGER.warning('Unable to find Azure credentials to use: %s\n', e)
      return None
