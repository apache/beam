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

# google.auth is only available when Beam is installed with the gcp extra.
try:
  import google.auth
  _GOOGLE_AUTH_AVAILABLE = True
except ImportError:
  _GOOGLE_AUTH_AVAILABLE = False

# When we are running in GCE, we can authenticate with VM credentials.
is_running_in_gce = False

# When we are running in GCE, this value is set based on worker startup
# information.
executing_project = None

_DEFAULT_MAX_REDIRECTS = 5
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


def get_service_credentials():
  """For internal use only; no backwards-compatibility guarantees.

  Get credentials to access Google services.

  Returns:
    A ``google.auth.credentials.Credentials`` object or None if credentials
    not found. Returned object is thread-safe.
  """
  return _Credentials.get_service_credentials()


if _GOOGLE_AUTH_AVAILABLE:

  class _Httplib2Response(google.auth.transport.Response):  # pylint: disable=c-extension-no-member
    """For internal use only; no backwards-compatibility guarantees.

    Wrapper for httplib2 response so that it can be used with google-auth.
    """
    def __init__(self, httplib2_response, httplib2_content):
      self._httplib2_response = httplib2_response
      self._httplib2_content = httplib2_content

    @property
    def status(self):
      return self._httplib2_response.status

    @property
    def headers(self):
      return self._httplib2_response

    @property
    def data(self):
      return self._httplib2_content

  class _GoogleAuthRequestAdapter(google.auth.transport.Request):  # pylint: disable=c-extension-no-member
    """For internal use only; no backwards-compatibility guarantees.

    Adapter allowing use of the httplib2.Http.request method for refreshing
    google-auth credentials.
    """
    def __init__(self, httplib2_request):
      self._httplib2_request = httplib2_request

    def __call__(
        self,
        url,
        method="GET",
        body=None,
        headers=None,
        timeout=None,
        **kwargs):
      resp, content = self._httplib2_request(
          url, method=method, body=body, headers=headers, **kwargs)
      return _Httplib2Response(resp, content)

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
      """Take an httplib2.Http instance (or equivalent) and authorizes it.

      Authorizes it for the set of credentials, usually by replacing
      http.request() with a method that adds in the appropriate headers and
      then delegates to the original Http.request() method.

      Args:
        http: httplib2.Http, an http object to be used to make the refresh
          request.
      """
      orig_request_method = http.request

      # The closure that will replace 'httplib2.Http.request'.
      def new_request(
          uri,
          method="GET",
          body=None,
          headers=None,
          redirections=_DEFAULT_MAX_REDIRECTS,
          connection_type=None):
        # Clone headers or create an empty headers dict.
        headers = {} if not headers else dict(headers)

        # Add auth headers.
        self._google_auth_credentials.before_request(
            _GoogleAuthRequestAdapter(orig_request_method),
            method=method,
            url=uri,
            headers=headers)

        # Make the request.
        return orig_request_method(
            uri, method, body, headers, redirections, connection_type)

      http.request = new_request
      return http

    def __getattr__(self, attr):
      """Delegate attribute access to underlying google-auth credentials."""
      return getattr(self._google_auth_credentials, attr)


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
    if not _GOOGLE_AUTH_AVAILABLE:
      _LOGGER.warning(
          'Unable to find default credentials because the google-auth library '
          'is not available. Install the gcp extra (apache_beam[gcp]) to use '
          'Google default credentials. Connecting anonymously.')
      return None

    client_scopes = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/devstorage.full_control',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/datastore',
        'https://www.googleapis.com/auth/spanner.admin',
        'https://www.googleapis.com/auth/spanner.data'
    ]
    try:
      credentials, _ = google.auth.default(scopes=client_scopes)  # pylint: disable=c-extension-no-member
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
