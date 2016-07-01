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

import mock
import six
import unittest2

from apitools.base.py import credentials_lib
from apitools.base.py import util


class CredentialsLibTest(unittest2.TestCase):

    def _GetServiceCreds(self, service_account_name=None, scopes=None):
        kwargs = {}
        if service_account_name is not None:
            kwargs['service_account_name'] = service_account_name
        service_account_name = service_account_name or 'default'

        def MockMetadataCalls(request_url):
            default_scopes = scopes or ['scope1']
            if request_url.endswith('scopes'):
                return six.StringIO(''.join(default_scopes))
            elif request_url.endswith('service-accounts'):
                return six.StringIO(service_account_name)
            elif request_url.endswith(
                    '/service-accounts/%s/token' % service_account_name):
                return six.StringIO('{"access_token": "token"}')
            self.fail('Unexpected HTTP request to %s' % request_url)

        with mock.patch.object(credentials_lib, '_GceMetadataRequest',
                               side_effect=MockMetadataCalls,
                               autospec=True) as opener_mock:
            with mock.patch.object(util, 'DetectGce',
                                   autospec=True) as mock_detect:
                mock_detect.return_value = True
                credentials = credentials_lib.GceAssertionCredentials(
                    scopes, **kwargs)
                self.assertIsNone(credentials._refresh(None))
            self.assertEqual(3, opener_mock.call_count)
        return credentials

    def testGceServiceAccounts(self):
        scopes = ['scope1']
        self._GetServiceCreds()
        self._GetServiceCreds(scopes=scopes)
        self._GetServiceCreds(service_account_name='my_service_account',
                              scopes=scopes)

    def testGetServiceAccount(self):
        # We'd also like to test the metadata calls, which requires
        # having some knowledge about how HTTP calls are made (so that
        # we can mock them). It's unfortunate, but there's no way
        # around it.
        creds = self._GetServiceCreds()
        opener = mock.MagicMock()
        opener.open = mock.MagicMock()
        opener.open.return_value = six.StringIO('default/\nanother')
        with mock.patch.object(six.moves.urllib.request, 'build_opener',
                               return_value=opener,
                               autospec=True) as build_opener:
            creds.GetServiceAccount('default')
            self.assertEqual(1, build_opener.call_count)
            self.assertEqual(1, opener.open.call_count)
            req = opener.open.call_args[0][0]
            self.assertTrue(req.get_full_url().startswith(
                'http://metadata.google.internal/'))
            # The urllib module does weird things with header case.
            self.assertEqual('Google', req.get_header('Metadata-flavor'))


class TestGetRunFlowFlags(unittest2.TestCase):

    def setUp(self):
        self._flags_actual = credentials_lib.FLAGS

    def tearDown(self):
        credentials_lib.FLAGS = self._flags_actual

    def test_with_gflags(self):
        HOST = 'myhostname'
        PORT = '144169'

        class MockFlags(object):
            auth_host_name = HOST
            auth_host_port = PORT
            auth_local_webserver = False

        credentials_lib.FLAGS = MockFlags
        flags = credentials_lib._GetRunFlowFlags([
            '--auth_host_name=%s' % HOST,
            '--auth_host_port=%s' % PORT,
            '--noauth_local_webserver',
        ])
        self.assertEqual(flags.auth_host_name, HOST)
        self.assertEqual(flags.auth_host_port, PORT)
        self.assertEqual(flags.logging_level, 'ERROR')
        self.assertEqual(flags.noauth_local_webserver, True)

    def test_without_gflags(self):
        credentials_lib.FLAGS = None
        flags = credentials_lib._GetRunFlowFlags([])
        self.assertEqual(flags.auth_host_name, 'localhost')
        self.assertEqual(flags.auth_host_port, [8080, 8090])
        self.assertEqual(flags.logging_level, 'ERROR')
        self.assertEqual(flags.noauth_local_webserver, False)
