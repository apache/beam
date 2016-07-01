#
# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for oauth2l."""

import json
import os
import sys

import mock
import oauth2client.client
import six
from six.moves import http_client
import unittest2

import apitools.base.py as apitools_base
from apitools.scripts import oauth2l

_OAUTH2L_MAIN_RUN = False


class _FakeResponse(object):

    def __init__(self, status_code, scopes=None):
        self.status_code = status_code
        if self.status_code == http_client.OK:
            self.content = json.dumps({'scope': ' '.join(scopes or [])})
        else:
            self.content = 'Error'
            self.info = str(http_client.responses[self.status_code])
            self.request_url = 'some-url'


def _GetCommandOutput(command_name, command_argv):
    orig_stdout = sys.stdout
    orig_stderr = sys.stderr
    new_stdout = six.StringIO()
    new_stderr = six.StringIO()
    try:
        sys.stdout = new_stdout
        sys.stderr = new_stderr
        oauth2l.main(['oauth2l', command_name] + command_argv)
    finally:
        sys.stdout = orig_stdout
        sys.stderr = orig_stderr
    new_stdout.seek(0)
    return new_stdout.getvalue().rstrip()


class InvalidCommandTest(unittest2.TestCase):

    def testOutput(self):
        self.assertRaises(SystemExit,
                          _GetCommandOutput, 'foo', [])


class Oauth2lFormattingTest(unittest2.TestCase):

    def setUp(self):
        # Set up an access token to use
        self.access_token = 'ya29.abdefghijklmnopqrstuvwxyz'
        self.user_agent = 'oauth2l/1.0'
        self.credentials = oauth2client.client.AccessTokenCredentials(
            self.access_token, self.user_agent)

    def _Args(self, credentials_format):
        return ['--credentials_format=' + credentials_format, 'userinfo.email']

    def testFormatBare(self):
        with mock.patch.object(oauth2l, '_FetchCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_credentials:
            output = _GetCommandOutput('fetch', self._Args('bare'))
            self.assertEqual(self.access_token, output)
            self.assertEqual(1, mock_credentials.call_count)

    def testFormatHeader(self):
        with mock.patch.object(oauth2l, '_FetchCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_credentials:
            output = _GetCommandOutput('fetch', self._Args('header'))
            header = 'Authorization: Bearer %s' % self.access_token
            self.assertEqual(header, output)
            self.assertEqual(1, mock_credentials.call_count)

    def testHeaderCommand(self):
        with mock.patch.object(oauth2l, '_FetchCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_credentials:
            output = _GetCommandOutput('header', ['userinfo.email'])
            header = 'Authorization: Bearer %s' % self.access_token
            self.assertEqual(header, output)
            self.assertEqual(1, mock_credentials.call_count)

    def testFormatJson(self):
        with mock.patch.object(oauth2l, '_FetchCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_credentials:
            output = _GetCommandOutput('fetch', self._Args('json'))
            output_lines = [l.strip() for l in output.splitlines()]
            expected_lines = [
                '"_class": "AccessTokenCredentials",',
                '"access_token": "%s",' % self.access_token,
            ]
            for line in expected_lines:
                self.assertIn(line, output_lines)
            self.assertEqual(1, mock_credentials.call_count)

    def testFormatJsonCompact(self):
        with mock.patch.object(oauth2l, '_FetchCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_credentials:
            output = _GetCommandOutput('fetch', self._Args('json_compact'))
            expected_clauses = [
                '"_class":"AccessTokenCredentials",',
                '"access_token":"%s",' % self.access_token,
            ]
            for clause in expected_clauses:
                self.assertIn(clause, output)
            self.assertEqual(1, len(output.splitlines()))
            self.assertEqual(1, mock_credentials.call_count)

    def testFormatPretty(self):
        with mock.patch.object(oauth2l, '_FetchCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_credentials:
            output = _GetCommandOutput('fetch', self._Args('pretty'))
            expecteds = ['oauth2client.client.AccessTokenCredentials',
                         self.access_token]
            for expected in expecteds:
                self.assertIn(expected, output)
            self.assertEqual(1, mock_credentials.call_count)

    def testFakeFormat(self):
        self.assertRaises(ValueError,
                          oauth2l._Format, 'xml', self.credentials)


class TestFetch(unittest2.TestCase):

    def setUp(self):
        # Set up an access token to use
        self.access_token = 'ya29.abdefghijklmnopqrstuvwxyz'
        self.user_agent = 'oauth2l/1.0'
        self.credentials = oauth2client.client.AccessTokenCredentials(
            self.access_token, self.user_agent)

    def testNoScopes(self):
        output = _GetCommandOutput('fetch', [])
        self.assertEqual(
            'Error encountered in fetch operation: No scopes provided',
            output)

    def testScopes(self):
        expected_scopes = [
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/cloud-platform',
        ]
        with mock.patch.object(apitools_base, 'GetCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_fetch:
            with mock.patch.object(oauth2l, '_GetTokenScopes',
                                   return_value=expected_scopes,
                                   autospec=True) as mock_get_scopes:
                output = _GetCommandOutput(
                    'fetch', ['userinfo.email', 'cloud-platform'])
                self.assertIn(self.access_token, output)
                self.assertEqual(1, mock_fetch.call_count)
                args, _ = mock_fetch.call_args
                self.assertEqual(expected_scopes, args[-1])
                self.assertEqual(1, mock_get_scopes.call_count)
                self.assertEqual((self.access_token,),
                                 mock_get_scopes.call_args[0])

    def testCredentialsRefreshed(self):
        with mock.patch.object(apitools_base, 'GetCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_fetch:
            with mock.patch.object(oauth2l, '_ValidateToken',
                                   return_value=False,
                                   autospec=True) as mock_validate:
                with mock.patch.object(self.credentials, 'refresh',
                                       return_value=None,
                                       autospec=True) as mock_refresh:
                    output = _GetCommandOutput('fetch', ['userinfo.email'])
                    self.assertIn(self.access_token, output)
                    self.assertEqual(1, mock_fetch.call_count)
                    self.assertEqual(1, mock_validate.call_count)
                    self.assertEqual(1, mock_refresh.call_count)

    def testDefaultClientInfo(self):
        with mock.patch.object(apitools_base, 'GetCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_fetch:
            with mock.patch.object(oauth2l, '_ValidateToken',
                                   return_value=True,
                                   autospec=True) as mock_validate:
                output = _GetCommandOutput('fetch', ['userinfo.email'])
                self.assertIn(self.access_token, output)
                self.assertEqual(1, mock_fetch.call_count)
                _, kwargs = mock_fetch.call_args
                self.assertEqual(
                    '1042881264118.apps.googleusercontent.com',
                    kwargs['client_id'])
                self.assertEqual(1, mock_validate.call_count)

    def testMissingClientSecrets(self):
        self.assertRaises(
            ValueError,
            oauth2l.GetClientInfoFromFlags, '/non/existent/file')

    def testWrongClientSecretsFormat(self):
        client_secrets = os.path.join(
            os.path.dirname(__file__),
            'testdata/noninstalled_client_secrets.json')
        self.assertRaises(
            ValueError,
            oauth2l.GetClientInfoFromFlags, client_secrets)

    def testCustomClientInfo(self):
        client_secrets_path = os.path.join(
            os.path.dirname(__file__), 'testdata/fake_client_secrets.json')
        with mock.patch.object(apitools_base, 'GetCredentials',
                               return_value=self.credentials,
                               autospec=True) as mock_fetch:
            with mock.patch.object(oauth2l, '_ValidateToken',
                                   return_value=True,
                                   autospec=True) as mock_validate:
                fetch_args = [
                    '--client_secrets=' + client_secrets_path,
                    'userinfo.email']
                output = _GetCommandOutput('fetch', fetch_args)
                self.assertIn(self.access_token, output)
                self.assertEqual(1, mock_fetch.call_count)
                _, kwargs = mock_fetch.call_args
                self.assertEqual('144169.apps.googleusercontent.com',
                                 kwargs['client_id'])
                self.assertEqual('awesomesecret',
                                 kwargs['client_secret'])
                self.assertEqual(1, mock_validate.call_count)


class TestOtherCommands(unittest2.TestCase):

    def setUp(self):
        # Set up an access token to use
        self.access_token = 'ya29.abdefghijklmnopqrstuvwxyz'
        self.user_agent = 'oauth2l/1.0'
        self.credentials = oauth2client.client.AccessTokenCredentials(
            self.access_token, self.user_agent)

    def testEmail(self):
        user_info = {'email': 'foo@example.com'}
        with mock.patch.object(apitools_base, 'GetUserinfo',
                               return_value=user_info,
                               autospec=True) as mock_get_userinfo:
            output = _GetCommandOutput('email', [self.access_token])
            self.assertEqual(user_info['email'], output)
            self.assertEqual(1, mock_get_userinfo.call_count)
            self.assertEqual(self.access_token,
                             mock_get_userinfo.call_args[0][0].access_token)

    def testNoEmail(self):
        with mock.patch.object(apitools_base, 'GetUserinfo',
                               return_value={},
                               autospec=True) as mock_get_userinfo:
            output = _GetCommandOutput('email', [self.access_token])
            self.assertEqual('', output)
            self.assertEqual(1, mock_get_userinfo.call_count)

    def testUserinfo(self):
        user_info = {'email': 'foo@example.com'}
        with mock.patch.object(apitools_base, 'GetUserinfo',
                               return_value=user_info,
                               autospec=True) as mock_get_userinfo:
            output = _GetCommandOutput('userinfo', [self.access_token])
            self.assertEqual(json.dumps(user_info, indent=4), output)
            self.assertEqual(1, mock_get_userinfo.call_count)
            self.assertEqual(self.access_token,
                             mock_get_userinfo.call_args[0][0].access_token)

    def testUserinfoCompact(self):
        user_info = {'email': 'foo@example.com'}
        with mock.patch.object(apitools_base, 'GetUserinfo',
                               return_value=user_info,
                               autospec=True) as mock_get_userinfo:
            output = _GetCommandOutput(
                'userinfo', ['--format=json_compact', self.access_token])
            self.assertEqual(json.dumps(user_info, separators=(',', ':')),
                             output)
            self.assertEqual(1, mock_get_userinfo.call_count)
            self.assertEqual(self.access_token,
                             mock_get_userinfo.call_args[0][0].access_token)

    def testScopes(self):
        scopes = [u'https://www.googleapis.com/auth/userinfo.email',
                  u'https://www.googleapis.com/auth/cloud-platform']
        response = _FakeResponse(http_client.OK, scopes=scopes)
        with mock.patch.object(apitools_base, 'MakeRequest',
                               return_value=response,
                               autospec=True) as mock_make_request:
            output = _GetCommandOutput('scopes', [self.access_token])
            self.assertEqual(sorted(scopes), output.splitlines())
            self.assertEqual(1, mock_make_request.call_count)

    def testValidate(self):
        scopes = [u'https://www.googleapis.com/auth/userinfo.email',
                  u'https://www.googleapis.com/auth/cloud-platform']
        response = _FakeResponse(http_client.OK, scopes=scopes)
        with mock.patch.object(apitools_base, 'MakeRequest',
                               return_value=response,
                               autospec=True) as mock_make_request:
            output = _GetCommandOutput('validate', [self.access_token])
            self.assertEqual('', output)
            self.assertEqual(1, mock_make_request.call_count)

    def testBadResponseCode(self):
        response = _FakeResponse(http_client.BAD_REQUEST)
        with mock.patch.object(apitools_base, 'MakeRequest',
                               return_value=response,
                               autospec=True) as mock_make_request:
            output = _GetCommandOutput('scopes', [self.access_token])
            self.assertEqual('', output)
            self.assertEqual(1, mock_make_request.call_count)

    def testUnexpectedResponseCode(self):
        response = _FakeResponse(http_client.INTERNAL_SERVER_ERROR)
        with mock.patch.object(apitools_base, 'MakeRequest',
                               return_value=response,
                               autospec=True) as mock_make_request:
            output = _GetCommandOutput('scopes', [self.access_token])
            self.assertIn(str(http_client.responses[response.status_code]),
                          output)
            self.assertIn('Error encountered in scopes operation: HttpError',
                          output)
            self.assertEqual(1, mock_make_request.call_count)
