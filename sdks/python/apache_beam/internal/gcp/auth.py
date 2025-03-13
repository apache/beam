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

# pytype: skip-file

import logging
import socket
import threading
from typing import Optional

from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils import retry

# google.auth is only available when Beam is installed with the gcp extra.
try:
  from google.auth import impersonated_credentials
  import google.auth
  import google_auth_httplib2
  _GOOGLE_AUTH_AVAILABLE = True
except ImportError:
  _GOOGLE_AUTH_AVAILABLE = False

# When we are running in GCE, we can authenticate with VM credentials.
is_running_in_gce = False

# When we are running in GCE, this value is set based on worker startup
# information.
executing_project = None

_LOGGER = logging.getLogger(__name__)


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


def get_service_credentials(pipeline_options):
  # type: (PipelineOptions) -> Optional[_ApitoolsCredentialsAdapter]

  """For internal use only; no backwards-compatibility guarantees.

  Get credentials to access Google services.
  Args:
    pipeline_options: Pipeline options, used in creating credentials
      like impersonated credentials.

  Returns:
    A ``_ApitoolsCredentialsAdapter`` object or None if credentials
    not found. Returned object is thread-safe.
  """
  return _Credentials.get_service_credentials(pipeline_options)


if _GOOGLE_AUTH_AVAILABLE:

  class _ApitoolsCredentialsAdapter:
    """For internal use only; no backwards-compatibility guarantees.

    Adapter allowing use of google-auth credentials with apitools, which
    normally expects credentials from the oauth2client library. This allows
    upgrading the auth library used by Beam without simultaneously upgrading
    all the GCP client libraries (a much larger change).
    """
    def __init__(self, google_auth_credentials):
      self._google_auth_credentials = google_auth_credentials

    def authorize(self, http):
      """Return an http client authorized with the google-auth credentials.

      Args:
        http: httplib2.Http, an http object to be used to make the refresh
          request.

      Returns:
        google_auth_httplib2.AuthorizedHttp: An authorized http client.
      """
      return google_auth_httplib2.AuthorizedHttp(
          self._google_auth_credentials, http=http)

    def __getattr__(self, attr):
      """Delegate attribute access to underlying google-auth credentials."""
      return getattr(self._google_auth_credentials, attr)

    def get_google_auth_credentials(self):
      return self._google_auth_credentials


class _Credentials(object):
  _credentials_lock = threading.Lock()
  _credentials_init = False
  _credentials = None

  @classmethod
  def get_service_credentials(cls, pipeline_options):
    # type: (PipelineOptions) -> Optional[_ApitoolsCredentialsAdapter]
    with cls._credentials_lock:
      if cls._credentials_init:
        return cls._credentials

      # apitools use urllib with the global timeout. Set it to 60 seconds
      # to prevent network related stuckness issues.
      if not socket.getdefaulttimeout():
        _LOGGER.debug("Setting socket default timeout to 60 seconds.")
        socket.setdefaulttimeout(60)
      _LOGGER.debug(
          "socket default timeout is %s seconds.", socket.getdefaulttimeout())

      cls._credentials = cls._get_service_credentials(pipeline_options)
      cls._credentials_init = True

    return cls._credentials

  @staticmethod
  def _get_service_credentials(pipeline_options):
    # type: (PipelineOptions) -> Optional[_ApitoolsCredentialsAdapter]
    if not _GOOGLE_AUTH_AVAILABLE:
      _LOGGER.warning(
          'Unable to find default credentials because the google-auth library '
          'is not available. Install the gcp extra (apache_beam[gcp]) to use '
          'Google default credentials. Connecting anonymously.')
      return None

    try:
      # pylint: disable=c-extension-no-member
      credentials = _Credentials._get_credentials_with_retrys(pipeline_options)
      credentials = _Credentials._add_impersonation_credentials(
          credentials, pipeline_options)
      credentials = _ApitoolsCredentialsAdapter(credentials)
      logging.debug(
          'Connecting using Google Application Default '
          'Credentials.')
      return credentials
    except Exception as e:
      _LOGGER.warning(
          'Unable to find default credentials to use: %s\n'
          'Connecting anonymously. This is expected if no '
          'credentials are needed to access GCP resources.',
          e)
      return None

  @staticmethod
  @retry.with_exponential_backoff(num_retries=4, initial_delay_secs=2)
  def _get_credentials_with_retrys(pipeline_options):
    credentials, _ = google.auth.default(
      scopes=pipeline_options.view_as(GoogleCloudOptions).gcp_oauth_scopes)
    return credentials

  @staticmethod
  def _add_impersonation_credentials(credentials, pipeline_options):
    gcs_options = pipeline_options.view_as(GoogleCloudOptions)
    impersonate_service_account = gcs_options.impersonate_service_account
    scopes = gcs_options.gcp_oauth_scopes
    if impersonate_service_account:
      _LOGGER.info('Impersonating: %s', impersonate_service_account)
      impersonate_accounts = impersonate_service_account.split(',')
      target_principal = impersonate_accounts[-1]
      delegate_to = impersonate_accounts[0:-1]
      credentials = impersonated_credentials.Credentials(
          source_credentials=credentials,
          target_principal=target_principal,
          delegates=delegate_to,
          target_scopes=scopes,
      )
    return credentials
