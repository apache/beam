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

import traceback # DO_NOT_SUBMIT

# google.auth is only available when Beam is installed with the gcp extra.
try:
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

CLIENT_SCOPES = [
  'https://www.googleapis.com/auth/bigquery',
  'https://www.googleapis.com/auth/cloud-platform',
  'https://www.googleapis.com/auth/devstorage.full_control',
  'https://www.googleapis.com/auth/userinfo.email',
  'https://www.googleapis.com/auth/datastore',
  'https://www.googleapis.com/auth/spanner.admin',
  'https://www.googleapis.com/auth/spanner.data'
]


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

def set_impersonation_accounts(target_principal = None, delegate_to = None):
  _Credentials.set_impersonation_accounts(target_principal, delegate_to)


def get_service_credentials():
  """For internal use only; no backwards-compatibility guarantees.

  Get credentials to access Google services.

  Returns:
    A ``google.auth.credentials.Credentials`` object or None if credentials
    not found. Returned object is thread-safe.
  """
  return _Credentials.get_service_credentials()


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


class _Credentials(object):
  _credentials_lock = threading.Lock()
  _credentials_init = False
  _credentials = None

  _delegate_accounts = None
  _target_principal = None
  _impersonation_parameters_set = False

  @classmethod
  def get_service_credentials(cls):
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
      cls._credentials = cls._add_impersonation_credentials()
      cls._credentials_init = True

    return cls._credentials

  @staticmethod
  def _get_service_credentials():
    if not _GOOGLE_AUTH_AVAILABLE:
      _LOGGER.warning(
          'Unable to find default credentials because the google-auth library '
          'is not available. Install the gcp extra (apache_beam[gcp]) to use '
          'Google default credentials. Connecting anonymously.')
      return None

    try:
      credentials, _ = google.auth.default(scopes=CLIENT_SCOPES)  # pylint: disable=c-extension-no-member
      credentials = _ApitoolsCredentialsAdapter(credentials)
      logging.debug(
          'Connecting using Google Application Default '
          'Credentials.')
      return credentials
    except Exception as e:
      _LOGGER.warning(
          'Unable to find default credentials to use: %s\n'
          'Connecting anonymously.',
          e)
      return None

  @classmethod
  def set_impersonation_accounts(cls, target_principal, delegate_to):
    cls._target_principal = target_principal
    cls._delegate_accounts = delegate_to
    cls._impersonation_parameters_set = True

  @classmethod
  def _add_impersonation_credentials(cls):
    if not cls._impersonation_parameters_set:
      traceback_str = traceback.format_stack()
      raise Exception('Impersonation credentials not yet set. \n' + str(traceback_str))
    """Adds impersonation credentials if the client species them."""
    credentials = cls._credentials
    if cls._target_principal:
      credentials = google.auth.impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=cls._target_principal,
        delegates=cls._delegate_accounts,
        target_scopes=CLIENT_SCOPES,
      )
    return credentials
