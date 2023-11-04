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
import json
from dataclasses import dataclass

# TODO(damondouglas,riteshghorse): figure out how to add .test-infra/mockapis dependency
# import proto.echo.v1.echo_pb2 as echo
# from proto.echo.v1.echo_pb2_grpc import EchoServiceStub
import urllib3
from requestresponseio import Caller, UserCodeExecutionException, \
    UserCodeQuotaException


# TODO(damondouglas,riteshghorse): use echo.EchoRequest
#   instead of this custom class
@dataclass
class EchoRequest:
    id: str
    payload: str


# TODO(damondouglas,riteshghorse): use echo.EchoResponse
#   instead of this custom class
@dataclass
class EchoResponse:
    id: str
    payload: str


class EchoHTTPClient(Caller):

    def __init__(self, url: str):
        self.url = url

    def call(self, request: EchoRequest) -> EchoResponse:
        try:
            resp = urllib3.request("POST", self.url,
                                   json={
                                       "id": request.id,
                                       "payload": request.payload
                                   },
                                   retries=False)

            if resp.status < 300:
                resp_body = resp.json()
                return EchoResponse(**resp_body)

            if resp.status == 429:  # Too Many Requests
                raise UserCodeQuotaException(resp.reason)

            raise UserCodeExecutionException(resp.reason)

        except urllib3.exceptions.HTTPError as e:
            raise UserCodeExecutionException(e)


def run(argv=None):
    client = EchoHTTPClient("http://localhost:8080/v1/echo")
    req = EchoRequest
    req.id = "echo-should-exceed-quota"
    req.payload = "aGkK"
    resp = client.call(req)
    print(resp)
    resp = client.call(req)
    print(resp)


if __name__ == '__main__':
    run()
