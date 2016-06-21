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

"""Tests for apitools.base.py.batch."""

import textwrap

import mock
from six.moves import http_client
from six.moves.urllib import parse
import unittest2

from apitools.base.py import batch
from apitools.base.py import exceptions
from apitools.base.py import http_wrapper


class FakeCredentials(object):

    def __init__(self):
        self.num_refreshes = 0

    def refresh(self, _):
        self.num_refreshes += 1


class FakeHttp(object):

    class FakeRequest(object):

        def __init__(self, credentials=None):
            if credentials is not None:
                self.credentials = credentials

    def __init__(self, credentials=None):
        self.request = FakeHttp.FakeRequest(credentials=credentials)


class FakeService(object):

    """A service for testing."""

    def GetMethodConfig(self, _):
        return {}

    def GetUploadConfig(self, _):
        return {}

    # pylint: disable=unused-argument
    def PrepareHttpRequest(
            self, method_config, request, global_params, upload_config):
        return global_params['desired_request']
    # pylint: enable=unused-argument

    def ProcessHttpResponse(self, _, http_response):
        return http_response


class BatchTest(unittest2.TestCase):

    def assertUrlEqual(self, expected_url, provided_url):

        def parse_components(url):
            parsed = parse.urlsplit(url)
            query = parse.parse_qs(parsed.query)
            return parsed._replace(query=''), query

        expected_parse, expected_query = parse_components(expected_url)
        provided_parse, provided_query = parse_components(provided_url)

        self.assertEqual(expected_parse, provided_parse)
        self.assertEqual(expected_query, provided_query)

    def __ConfigureMock(self, mock_request, expected_request, response):

        if isinstance(response, list):
            response = list(response)

        def CheckRequest(_, request, **unused_kwds):
            self.assertUrlEqual(expected_request.url, request.url)
            self.assertEqual(expected_request.http_method, request.http_method)
            if isinstance(response, list):
                return response.pop(0)
            else:
                return response

        mock_request.side_effect = CheckRequest

    def testRequestServiceUnavailable(self):
        mock_service = FakeService()

        desired_url = 'https://www.example.com'
        batch_api_request = batch.BatchApiRequest(batch_url=desired_url,
                                                  retryable_codes=[])
        # The request to be added. The actual request sent will be somewhat
        # larger, as this is added to a batch.
        desired_request = http_wrapper.Request(desired_url, 'POST', {
            'content-type': 'multipart/mixed; boundary="None"',
            'content-length': 80,
        }, 'x' * 80)

        with mock.patch.object(http_wrapper, 'MakeRequest',
                               autospec=True) as mock_request:
            self.__ConfigureMock(
                mock_request,
                http_wrapper.Request(desired_url, 'POST', {
                    'content-type': 'multipart/mixed; boundary="None"',
                    'content-length': 419,
                }, 'x' * 419),
                http_wrapper.Response({
                    'status': '200',
                    'content-type': 'multipart/mixed; boundary="boundary"',
                }, textwrap.dedent("""\
                --boundary
                content-type: text/plain
                content-id: <id+0>

                HTTP/1.1 503 SERVICE UNAVAILABLE
                nope
                --boundary--"""), None))

            batch_api_request.Add(
                mock_service, 'unused', None,
                global_params={'desired_request': desired_request})

            api_request_responses = batch_api_request.Execute(
                FakeHttp(), sleep_between_polls=0)

            self.assertEqual(1, len(api_request_responses))

            # Make sure we didn't retry non-retryable code 503.
            self.assertEqual(1, mock_request.call_count)

            self.assertTrue(api_request_responses[0].is_error)
            self.assertIsNone(api_request_responses[0].response)
            self.assertIsInstance(api_request_responses[0].exception,
                                  exceptions.HttpError)

    def testSingleRequestInBatch(self):
        mock_service = FakeService()

        desired_url = 'https://www.example.com'
        batch_api_request = batch.BatchApiRequest(batch_url=desired_url)
        # The request to be added. The actual request sent will be somewhat
        # larger, as this is added to a batch.
        desired_request = http_wrapper.Request(desired_url, 'POST', {
            'content-type': 'multipart/mixed; boundary="None"',
            'content-length': 80,
        }, 'x' * 80)

        with mock.patch.object(http_wrapper, 'MakeRequest',
                               autospec=True) as mock_request:
            self.__ConfigureMock(
                mock_request,
                http_wrapper.Request(desired_url, 'POST', {
                    'content-type': 'multipart/mixed; boundary="None"',
                    'content-length': 419,
                }, 'x' * 419),
                http_wrapper.Response({
                    'status': '200',
                    'content-type': 'multipart/mixed; boundary="boundary"',
                }, textwrap.dedent("""\
                --boundary
                content-type: text/plain
                content-id: <id+0>

                HTTP/1.1 200 OK
                content
                --boundary--"""), None))

            batch_api_request.Add(mock_service, 'unused', None, {
                'desired_request': desired_request,
            })

            api_request_responses = batch_api_request.Execute(FakeHttp())

            self.assertEqual(1, len(api_request_responses))
            self.assertEqual(1, mock_request.call_count)

            self.assertFalse(api_request_responses[0].is_error)

            response = api_request_responses[0].response
            self.assertEqual({'status': '200'}, response.info)
            self.assertEqual('content', response.content)
            self.assertEqual(desired_url, response.request_url)

    def testRefreshOnAuthFailure(self):
        mock_service = FakeService()

        desired_url = 'https://www.example.com'
        batch_api_request = batch.BatchApiRequest(batch_url=desired_url)
        # The request to be added. The actual request sent will be somewhat
        # larger, as this is added to a batch.
        desired_request = http_wrapper.Request(desired_url, 'POST', {
            'content-type': 'multipart/mixed; boundary="None"',
            'content-length': 80,
        }, 'x' * 80)

        with mock.patch.object(http_wrapper, 'MakeRequest',
                               autospec=True) as mock_request:
            self.__ConfigureMock(
                mock_request,
                http_wrapper.Request(desired_url, 'POST', {
                    'content-type': 'multipart/mixed; boundary="None"',
                    'content-length': 419,
                }, 'x' * 419), [
                    http_wrapper.Response({
                        'status': '200',
                        'content-type': 'multipart/mixed; boundary="boundary"',
                    }, textwrap.dedent("""\
                    --boundary
                    content-type: text/plain
                    content-id: <id+0>

                    HTTP/1.1 401 UNAUTHORIZED
                    Invalid grant

                    --boundary--"""), None),
                    http_wrapper.Response({
                        'status': '200',
                        'content-type': 'multipart/mixed; boundary="boundary"',
                    }, textwrap.dedent("""\
                    --boundary
                    content-type: text/plain
                    content-id: <id+0>

                    HTTP/1.1 200 OK
                    content
                    --boundary--"""), None)
                ])

            batch_api_request.Add(mock_service, 'unused', None, {
                'desired_request': desired_request,
            })

            credentials = FakeCredentials()
            api_request_responses = batch_api_request.Execute(
                FakeHttp(credentials=credentials), sleep_between_polls=0)

            self.assertEqual(1, len(api_request_responses))
            self.assertEqual(2, mock_request.call_count)
            self.assertEqual(1, credentials.num_refreshes)

            self.assertFalse(api_request_responses[0].is_error)

            response = api_request_responses[0].response
            self.assertEqual({'status': '200'}, response.info)
            self.assertEqual('content', response.content)
            self.assertEqual(desired_url, response.request_url)

    def testNoAttempts(self):
        desired_url = 'https://www.example.com'
        batch_api_request = batch.BatchApiRequest(batch_url=desired_url)
        batch_api_request.Add(FakeService(), 'unused', None, {
            'desired_request': http_wrapper.Request(desired_url, 'POST', {
                'content-type': 'multipart/mixed; boundary="None"',
                'content-length': 80,
            }, 'x' * 80),
        })
        api_request_responses = batch_api_request.Execute(None, max_retries=0)
        self.assertEqual(1, len(api_request_responses))
        self.assertIsNone(api_request_responses[0].response)
        self.assertIsNone(api_request_responses[0].exception)

    def _DoTestConvertIdToHeader(self, test_id, expected_result):
        batch_request = batch.BatchHttpRequest('https://www.example.com')
        self.assertEqual(
            expected_result % batch_request._BatchHttpRequest__base_id,
            batch_request._ConvertIdToHeader(test_id))

    def testConvertIdSimple(self):
        self._DoTestConvertIdToHeader('blah', '<%s+blah>')

    def testConvertIdThatNeedsEscaping(self):
        self._DoTestConvertIdToHeader('~tilde1', '<%s+%%7Etilde1>')

    def _DoTestConvertHeaderToId(self, header, expected_id):
        batch_request = batch.BatchHttpRequest('https://www.example.com')
        self.assertEqual(expected_id,
                         batch_request._ConvertHeaderToId(header))

    def testConvertHeaderToIdSimple(self):
        self._DoTestConvertHeaderToId('<hello+blah>', 'blah')

    def testConvertHeaderToIdWithLotsOfPlus(self):
        self._DoTestConvertHeaderToId('<a+++++plus>', 'plus')

    def _DoTestConvertInvalidHeaderToId(self, invalid_header):
        batch_request = batch.BatchHttpRequest('https://www.example.com')
        self.assertRaises(exceptions.BatchError,
                          batch_request._ConvertHeaderToId, invalid_header)

    def testHeaderWithoutAngleBrackets(self):
        self._DoTestConvertInvalidHeaderToId('1+1')

    def testHeaderWithoutPlus(self):
        self._DoTestConvertInvalidHeaderToId('<HEADER>')

    def testSerializeRequest(self):
        request = http_wrapper.Request(body='Hello World', headers={
            'content-type': 'protocol/version',
        })
        expected_serialized_request = '\n'.join([
            'GET  HTTP/1.1',
            'Content-Type: protocol/version',
            'MIME-Version: 1.0',
            'content-length: 11',
            'Host: ',
            '',
            'Hello World',
        ])
        batch_request = batch.BatchHttpRequest('https://www.example.com')
        self.assertEqual(expected_serialized_request,
                         batch_request._SerializeRequest(request))

    def testSerializeRequestPreservesHeaders(self):
        # Now confirm that if an additional, arbitrary header is added
        # that it is successfully serialized to the request. Merely
        # check that it is included, because the order of the headers
        # in the request is arbitrary.
        request = http_wrapper.Request(body='Hello World', headers={
            'content-type': 'protocol/version',
            'key': 'value',
        })
        batch_request = batch.BatchHttpRequest('https://www.example.com')
        self.assertTrue(
            'key: value\n' in batch_request._SerializeRequest(request))

    def testSerializeRequestNoBody(self):
        request = http_wrapper.Request(body=None, headers={
            'content-type': 'protocol/version',
        })
        expected_serialized_request = '\n'.join([
            'GET  HTTP/1.1',
            'Content-Type: protocol/version',
            'MIME-Version: 1.0',
            'Host: ',
            '',
            '',
        ])
        batch_request = batch.BatchHttpRequest('https://www.example.com')
        self.assertEqual(expected_serialized_request,
                         batch_request._SerializeRequest(request))

    def testDeserializeRequest(self):
        serialized_payload = '\n'.join([
            'GET  HTTP/1.1',
            'Content-Type: protocol/version',
            'MIME-Version: 1.0',
            'content-length: 11',
            'key: value',
            'Host: ',
            '',
            'Hello World',
        ])
        example_url = 'https://www.example.com'
        expected_response = http_wrapper.Response({
            'content-length': str(len('Hello World')),
            'Content-Type': 'protocol/version',
            'key': 'value',
            'MIME-Version': '1.0',
            'status': '',
            'Host': ''
        }, 'Hello World', example_url)

        batch_request = batch.BatchHttpRequest(example_url)
        self.assertEqual(
            expected_response,
            batch_request._DeserializeResponse(serialized_payload))

    def testNewId(self):
        batch_request = batch.BatchHttpRequest('https://www.example.com')

        for i in range(100):
            self.assertEqual(str(i), batch_request._NewId())

    def testAdd(self):
        batch_request = batch.BatchHttpRequest('https://www.example.com')

        for x in range(100):
            batch_request.Add(http_wrapper.Request(body=str(x)))

        for key in batch_request._BatchHttpRequest__request_response_handlers:
            value = batch_request._BatchHttpRequest__request_response_handlers[
                key]
            self.assertEqual(key, value.request.body)
            self.assertFalse(value.request.url)
            self.assertEqual('GET', value.request.http_method)
            self.assertIsNone(value.response)
            self.assertIsNone(value.handler)

    def testInternalExecuteWithFailedRequest(self):
        with mock.patch.object(http_wrapper, 'MakeRequest',
                               autospec=True) as mock_request:
            self.__ConfigureMock(
                mock_request,
                http_wrapper.Request('https://www.example.com', 'POST', {
                    'content-type': 'multipart/mixed; boundary="None"',
                    'content-length': 80,
                }, 'x' * 80),
                http_wrapper.Response({'status': '300'}, None, None))

            batch_request = batch.BatchHttpRequest('https://www.example.com')

            self.assertRaises(
                exceptions.HttpError, batch_request._Execute, None)

    def testInternalExecuteWithNonMultipartResponse(self):
        with mock.patch.object(http_wrapper, 'MakeRequest',
                               autospec=True) as mock_request:
            self.__ConfigureMock(
                mock_request,
                http_wrapper.Request('https://www.example.com', 'POST', {
                    'content-type': 'multipart/mixed; boundary="None"',
                    'content-length': 80,
                }, 'x' * 80),
                http_wrapper.Response({
                    'status': '200',
                    'content-type': 'blah/blah'
                }, '', None))

            batch_request = batch.BatchHttpRequest('https://www.example.com')

            self.assertRaises(
                exceptions.BatchError, batch_request._Execute, None)

    def testInternalExecute(self):
        with mock.patch.object(http_wrapper, 'MakeRequest',
                               autospec=True) as mock_request:
            self.__ConfigureMock(
                mock_request,
                http_wrapper.Request('https://www.example.com', 'POST', {
                    'content-type': 'multipart/mixed; boundary="None"',
                    'content-length': 583,
                }, 'x' * 583),
                http_wrapper.Response({
                    'status': '200',
                    'content-type': 'multipart/mixed; boundary="boundary"',
                }, textwrap.dedent("""\
                --boundary
                content-type: text/plain
                content-id: <id+2>

                HTTP/1.1 200 OK
                Second response

                --boundary
                content-type: text/plain
                content-id: <id+1>

                HTTP/1.1 401 UNAUTHORIZED
                First response

                --boundary--"""), None))

            test_requests = {
                '1': batch.RequestResponseAndHandler(
                    http_wrapper.Request(body='first'), None, None),
                '2': batch.RequestResponseAndHandler(
                    http_wrapper.Request(body='second'), None, None),
            }

            batch_request = batch.BatchHttpRequest('https://www.example.com')
            batch_request._BatchHttpRequest__request_response_handlers = (
                test_requests)

            batch_request._Execute(FakeHttp())

            test_responses = (
                batch_request._BatchHttpRequest__request_response_handlers)

            self.assertEqual(http_client.UNAUTHORIZED,
                             test_responses['1'].response.status_code)
            self.assertEqual(http_client.OK,
                             test_responses['2'].response.status_code)

            self.assertIn(
                'First response', test_responses['1'].response.content)
            self.assertIn(
                'Second response', test_responses['2'].response.content)

    def testPublicExecute(self):

        def LocalCallback(response, exception):
            self.assertEqual({'status': '418'}, response.info)
            self.assertEqual('Teapot', response.content)
            self.assertIsNone(response.request_url)
            self.assertIsInstance(exception, exceptions.HttpError)

        global_callback = mock.Mock()
        batch_request = batch.BatchHttpRequest(
            'https://www.example.com', global_callback)

        with mock.patch.object(batch.BatchHttpRequest, '_Execute',
                               autospec=True) as mock_execute:
            mock_execute.return_value = None

            test_requests = {
                '0': batch.RequestResponseAndHandler(
                    None,
                    http_wrapper.Response({'status': '200'}, 'Hello!', None),
                    None),
                '1': batch.RequestResponseAndHandler(
                    None,
                    http_wrapper.Response({'status': '418'}, 'Teapot', None),
                    LocalCallback),
            }

            batch_request._BatchHttpRequest__request_response_handlers = (
                test_requests)
            batch_request.Execute(None)

            # Global callback was called once per handler.
            self.assertEqual(len(test_requests), global_callback.call_count)
