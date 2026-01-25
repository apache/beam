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
import logging
import unittest

import mock

from apache_beam.internal.gcp import auth
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions

try:
  import google.auth as gauth
  import google_auth_httplib2  # pylint: disable=unused-import
except ImportError:
  gauth = None  # type: ignore


class MockLoggingHandler(logging.Handler):
  """Mock logging handler to check for expected logs."""
  def __init__(self, *args, **kwargs):
    self.reset()
    logging.Handler.__init__(self, *args, **kwargs)

  def emit(self, record):
    self.messages[record.levelname.lower()].append(record.getMessage())

  def reset(self):
    self.messages = {
        'debug': [],
        'info': [],
        'warning': [],
        'error': [],
        'critical': [],
    }


@unittest.skipIf(gauth is None, 'Google Auth dependencies are not installed')
class AuthTest(unittest.TestCase):
  @mock.patch('google.auth.default')
  def test_auth_with_retrys(self, unused_mock_arg):
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(
        GoogleCloudOptions).impersonate_service_account = False

    credentials = ('creds', 1)

    self.is_called = False

    def side_effect(scopes=None):
      if self.is_called:
        return credentials
      else:
        self.is_called = True
        raise IOError('Failed')

    google_auth_mock = mock.MagicMock()
    gauth.default = google_auth_mock
    google_auth_mock.side_effect = side_effect

    # _Credentials caches the actual credentials.
    # This resets it for idempotent tests.
    if auth._Credentials._credentials_init:
      auth._Credentials._credentials_init = False
      auth._Credentials._credentials = None

    returned_credentials = auth.get_service_credentials(pipeline_options)

    # _Credentials caches the actual credentials.
    # This resets it for idempotent tests.
    if auth._Credentials._credentials_init:
      auth._Credentials._credentials_init = False
      auth._Credentials._credentials = None

    self.assertEqual('creds', returned_credentials._google_auth_credentials)

  @mock.patch(
      'apache_beam.internal.gcp.auth._Credentials._get_credentials_with_retrys')
  def test_auth_with_retrys_always_fail(self, unused_mock_arg):
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(
        GoogleCloudOptions).impersonate_service_account = False

    loggerHandler = MockLoggingHandler()

    auth._LOGGER.addHandler(loggerHandler)

    #Remove call to retrying method, as otherwise test takes ~10 minutes to run
    def raise_(scopes=None):
      raise IOError('Failed')

    retry_auth_mock = mock.MagicMock()
    auth._Credentials._get_credentials_with_retrys = retry_auth_mock
    retry_auth_mock.side_effect = raise_

    # _Credentials caches the actual credentials.
    # This resets it for idempotent tests.
    if auth._Credentials._credentials_init:
      auth._Credentials._credentials_init = False
      auth._Credentials._credentials = None

    returned_credentials = auth.get_service_credentials(pipeline_options)

    self.assertEqual(None, returned_credentials)
    self.assertEqual([
        'Unable to find default credentials to use: Failed\n'
        'Connecting anonymously. This is expected if no credentials are '
        'needed to access GCP resources.'
    ],
                     loggerHandler.messages.get('warning'))

    # _Credentials caches the actual credentials.
    # This resets it for idempotent tests.
    if auth._Credentials._credentials_init:
      auth._Credentials._credentials_init = False
      auth._Credentials._credentials = None

    auth._LOGGER.removeHandler(loggerHandler)


@unittest.skipIf(gauth is None, 'Google Auth dependencies are not installed')
class WithQuotaProjectTest(unittest.TestCase):
  """Tests for with_quota_project function."""
  def test_with_quota_project_returns_credentials_unchanged_when_none(self):
    """Test that None credentials are returned unchanged."""
    result = auth.with_quota_project(None, 'my-project')
    self.assertIsNone(result)

  def test_with_quota_project_returns_credentials_unchanged_when_no_quota(self):
    """Test that credentials are returned unchanged when
    quota_project_id is None."""
    mock_creds = mock.MagicMock()
    result = auth.with_quota_project(mock_creds, None)
    self.assertEqual(result, mock_creds)
    mock_creds.with_quota_project.assert_not_called()

  @mock.patch('apache_beam.internal.gcp.auth._ApitoolsCredentialsAdapter')
  def test_with_quota_project_applies_quota_to_wrapped_credentials(
      self, mock_adapter_class):
    """Test that quota project is applied to wrapped credentials."""
    mock_inner_creds = mock.MagicMock()
    mock_new_creds = mock.MagicMock()
    mock_inner_creds.with_quota_project.return_value = mock_new_creds

    mock_adapter = mock.MagicMock()
    mock_adapter.get_google_auth_credentials.return_value = mock_inner_creds

    mock_adapter_instance = mock.MagicMock()
    mock_adapter_class.return_value = mock_adapter_instance

    result = auth.with_quota_project(mock_adapter, 'my-billing-project')

    mock_inner_creds.with_quota_project.assert_called_once_with(
        'my-billing-project')
    # Result should be a new adapter wrapping the new credentials
    mock_adapter_class.assert_called_once_with(mock_new_creds)
    self.assertEqual(result, mock_adapter_instance)

  def test_with_quota_project_applies_quota_to_direct_credentials(self):
    """Test that quota project is applied to direct credentials."""
    mock_creds = mock.MagicMock(spec=['with_quota_project'])
    mock_new_creds = mock.MagicMock()
    mock_creds.with_quota_project.return_value = mock_new_creds

    result = auth.with_quota_project(mock_creds, 'my-billing-project')

    mock_creds.with_quota_project.assert_called_once_with('my-billing-project')
    self.assertEqual(result, mock_new_creds)

  def test_with_quota_project_returns_original_when_not_supported(self):
    """Test that original credentials are returned when
    with_quota_project is not supported."""
    # Create a mock without with_quota_project method
    mock_creds = mock.MagicMock(spec=[])

    result = auth.with_quota_project(mock_creds, 'my-billing-project')

    self.assertEqual(result, mock_creds)


if __name__ == '__main__':
  unittest.main()
