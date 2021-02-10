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
"""Client Interceptor to inject token for authentication"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections

import grpc


class _ClientCallDetails(
    collections.namedtuple('_ClientCallDetails',
                           ('method', 'timeout', 'metadata', 'credentials')),
    grpc.ClientCallDetails):
    pass


class TokenAuthInterceptor(grpc.StreamStreamClientInterceptor,
                           grpc.UnaryUnaryClientInterceptor,
                           grpc.StreamUnaryClientInterceptor,
                           grpc.UnaryStreamClientInterceptor):

    def __init__(self, token=None):
        self._token = token

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return self._intercept(continuation, client_call_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return self._intercept(continuation, client_call_details, request_iterator)

    def _intercept(self, continuation, client_call_details, request):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        if 'fs_token' in metadata:
            raise RuntimeError('Header metadata already have fs_token.')
        if self._token:
            metadata.append(('fs_token', self._token))
        new_client_details = _ClientCallDetails(
            client_call_details.method, client_call_details.timeout, metadata,
            client_call_details.credentials)
        return continuation(new_client_details, request)
