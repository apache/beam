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

"""Dataflow credentials and authentication."""

from __future__ import absolute_import

import logging
import socket
import threading

from oauth2client.client import GoogleCredentials

from apache_beam.utils import retry

# Protect against environments where apitools library is not available.
try:
  from apitools.base.py.credentials_lib import GceAssertionCredentials
except ImportError:
  GceAssertionCredentials = None

# When we are running in GCE, we can authenticate with VM credentials.
is_running_in_gce = False

# When we are running in GCE, this value is set based on worker startup
# information.
executing_project = None


_LOGGER = logging.getLogger(__name__)


if GceAssertionCredentials is not None:
  class _GceAssertionCredentials(GceAssertionCredentials):
    """GceAssertionCredentials with retry wrapper.

    For internal use only; no backwards-compatibility guarantees.
    """

    @retry.with_exponential_backoff(
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _do_refresh_request(self, http_request):
      return super(_GceAssertionCredentials, self)._do_refresh_request(
          http_request)


def set_running_in_gce(worker_executing_project):
  """For internal use only; no backwards-compatibility guarantees.

  Informs the authentication library that we are running in GCE.

  When we are running in GCE, we have the option of using the VM metadata
  credentials for authentication to Google services.

  Args:
    worker_executing_project: The project running the workflow. This information
      comes from worker startup information.
  """
  global is_running_in_gce
  global executing_project
  is_running_in_gce = True
  executing_project = worker_executing_project


def get_service_credentials():
  """For internal use only; no backwards-compatibility guarantees.

  Get credentials to access Google services.

  Returns:
    A ``oauth2client.client.OAuth2Credentials`` object or None if credentials
    not found. Returned object is thread-safe.
  """
  return _Credentials.get_service_credentials()


class _Credentials(object):
  _credentials_lock = threading.Lock()
  _credentials_init = False
  _credentials = None

  @classmethod
  def get_service_credentials(cls):
    if cls._credentials_init:
      return cls._credentials

    with cls._credentials_lock:
      if cls._credentials_init:
        return cls._credentials

      # apitools use urllib with the global timeout. Set it to 60 seconds
      # to prevent network related stuckness issues.
      if not socket.getdefaulttimeout():
        _LOGGER.info("Setting socket default timeout to 60 seconds.")
        socket.setdefaulttimeout(60)
      _LOGGER.info(
          "socket default timeout is %s seconds.", socket.getdefaulttimeout())

      cls._credentials = cls._get_service_credentials()
      cls._credentials_init = True

    return cls._credentials

  @staticmethod
  def _get_service_credentials():
    if is_running_in_gce:
      # We are currently running as a GCE taskrunner worker.
      return _GceAssertionCredentials(user_agent='beam-python-sdk/1.0')
    else:
      client_scopes = [
          'https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/devstorage.full_control',
          'https://www.googleapis.com/auth/userinfo.email',
          'https://www.googleapis.com/auth/datastore',
          'https://www.googleapis.com/auth/spanner'
      ]
      try:
        credentials = GoogleCredentials.get_application_default()
        credentials = credentials.create_scoped(client_scopes)
        logging.debug('Connecting using Google Application Default '
                      'Credentials.')
        return credentials
      except Exception as e:
        _LOGGER.warning(
            'Unable to find default credentials to use: %s\n'
            'Connecting anonymously.', e)
        return None
