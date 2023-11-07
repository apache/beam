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
import base64
from dataclasses import dataclass
import sys
import unittest

import urllib3

from apache_beam.io.requestresponseio import Caller
from apache_beam.io.requestresponseio import UserCodeExecutionException
from apache_beam.io.requestresponseio import UserCodeQuotaException
from apache_beam.options.pipeline_options import PipelineOptions
try:
    # TODO(damondouglas, riteshgorse) clean up package import path
    from src.main.python.proto.echo.v1.echo_pb2 import EchoRequest
    from src.main.python.proto.echo.v1.echo_pb2 import EchoResponse
except ImportError:
    raise unittest.SkipTest('echo_pb2 requirement missing. Make sure you '
                            'installed specify the test extras_require with '
                            'pip install -e\".[test]\"')

_HTTP_PATH = "/v1/echo"
_PAYLOAD = base64.b64encode(bytes('payload', 'utf-8'))


class EchoITOptions(PipelineOptions):
    """Shared options for running integration tests on a deployed
    ``EchoServiceGrpc`` See https://github.com/apache/beam/tree/master/.test
    -infra/mock-apis#integration for details on how to acquire values
    required by ``EchoITOptions``.
    """
    @classmethod
    def _add_argparse_args(cls, parser) -> None:
        parser.add_argument(
            '--gRPCEndpointAddress',
            required=True,
            dest='grpc_endpoint_address',
            help='The gRPC address of the Echo API endpoint, typically of the '
                 'form <host>:<port>.'
        )
        parser.add_argument(
            '--httpEndpointAddress',
            required=True,
            dest='http_endpoint_address',
            help='The HTTP address of the Echo API endpoint; must being with '
                 'http(s)://'
        )
        parser.add_argument(
            '--neverExceedQuotaId',
            default='echo-should-never-exceed-quota',
            dest='never_exceed_quota_id',
            help='The ID for an allocated quota that should never exceed.'
        )
        parser.add_argument(
            '--shouldExceedQuotaId',
            default='echo-should-exceed-quota',
            dest='should_exceed_quota_id',
            help='The ID for an allocated quota that should exceed.'
        )


class EchoHTTPCaller(Caller):
    """Implements ``Caller`` to call the ``EchoServiceGrpc``'s HTTP handler.
    The purpose of ``EchoHTTPCaller`` is to support integration tests.
    """

    def __init__(self, url: str):
        self.url = url + _HTTP_PATH

    def call(self, request: EchoRequest) -> EchoResponse:
        """Overrides ``Caller``'s call method invoking the ``EchoServiceGrpc``'s
        HTTP handler with an ``EchoRequest``, returning either a successful
        ``EchoResponse`` or throwing either a ``UserCodeExecutionException``,
        ``UserCodeTimeoutException``, or a ``UserCodeQuotaException``.
        """
        try:
            resp = urllib3.request("POST", self.url,
                                   json={
                                       "id": request.id,
                                       "payload": str(request.payload, 'utf-8')
                                   },
                                   retries=False)

            if resp.status < 300:
                resp_body = resp.json()
                id = resp_body['id']
                payload = resp_body['payload']
                return EchoResponse(id=id, payload=bytes(payload, 'utf-8'))

            if resp.status == 429:  # Too Many Requests
                raise UserCodeQuotaException(resp.reason)

            raise UserCodeExecutionException(resp.reason)

        except urllib3.exceptions.HTTPError as e:
            raise UserCodeExecutionException(e)


class EchoHTTPCallerTestIT(unittest.TestCase):
    options: EchoITOptions = None
    client: EchoHTTPCaller = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.options = EchoITOptions()
        cls.client = EchoHTTPCaller(cls.options.http_endpoint_address)

    def setUp(self) -> None:
        """
        The challenge with building and deploying a real quota aware
        endpoint, the integration with which these tests validate, is that we
        need a value of at least 1. The allocated quota where we expect to
        exceed will be shared among many tests and across languages. Code
        below in this setup ensures that the API is in the state where we can
        expect a quota exceeded error. There are tests in this file that
        detect errors in expected responses. We only throw exceptions that
        are not UserCodeQuotaException.
        """
        req = EchoRequest(
            id=EchoHTTPCallerTestIT.options.should_exceed_quota_id,
            payload=_PAYLOAD
        )
        try:
            EchoHTTPCallerTestIT.client.call(req)
            EchoHTTPCallerTestIT.client.call(req)
            EchoHTTPCallerTestIT.client.call(req)
        except UserCodeExecutionException as e:
            if not isinstance(e, UserCodeQuotaException):
                raise e

    def test_given_valid_request_receives_response(self):
        req = EchoRequest(
            id=EchoHTTPCallerTestIT.options.never_exceed_quota_id,
            payload=_PAYLOAD
        )
        response: EchoResponse = EchoHTTPCallerTestIT.client.call(req)
        self.assertEqual(req.id, response.id)
        self.assertEqual(req.payload, response.payload)

    def test_given_exceeded_quota_should_raise(self):
        req = EchoRequest(
            id=EchoHTTPCallerTestIT.options.should_exceed_quota_id,
            payload=_PAYLOAD
        )
        self.assertRaises(
            UserCodeQuotaException,
            lambda: EchoHTTPCallerTestIT.client.call(req)
        )

    def test_not_found_should_raise(self):
        req = EchoRequest(
            id='i-dont-exist-quota-id',
            payload=_PAYLOAD
        )
        self.assertRaisesRegex(
            UserCodeExecutionException,
            "Not Found",
            lambda: EchoHTTPCallerTestIT.client.call(req)
        )


if __name__ == '__main__':
    unittest.main(argv=sys.argv[:1])
