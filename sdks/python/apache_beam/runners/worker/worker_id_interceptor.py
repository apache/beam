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

"""Client Interceptor to inject worker_id"""
# pytype: skip-file

import collections
import os
from typing import Optional

import grpc


class _ClientCallDetails(collections.namedtuple(
    '_ClientCallDetails', ('method', 'timeout', 'metadata', 'credentials')),
                         grpc.ClientCallDetails):
  pass


class WorkerIdInterceptor(grpc.UnaryUnaryClientInterceptor,
                          grpc.StreamStreamClientInterceptor):

  # TODO: (BEAM-3904) Removed defaulting to UUID when worker_id is not present
  # and throw exception in worker_id_interceptor.py after we have rolled out
  # the corresponding container changes.
  # Unique worker Id for this worker.
  _worker_id = os.environ.get('WORKER_ID')
  if not _worker_id:
    import uuid
    _worker_id = str(uuid.uuid4())

  def __init__(self, worker_id: Optional[str] = None) -> None:
    if worker_id:
      self._worker_id = worker_id

  def intercept_unary_unary(self, continuation, client_call_details, request):
    return self._intercept(continuation, client_call_details, request)

  def intercept_unary_stream(self, continuation, client_call_details, request):
    return self._intercept(continuation, client_call_details, request)

  def intercept_stream_unary(self, continuation, client_call_details, request):
    return self._intercept(continuation, client_call_details, request)

  def intercept_stream_stream(
      self, continuation, client_call_details, request_iterator):
    return self._intercept(continuation, client_call_details, request_iterator)

  def _intercept(self, continuation, client_call_details, request):
    metadata = []
    if client_call_details.metadata is not None:
      metadata = list(client_call_details.metadata)
    if 'worker_id' in metadata:
      raise RuntimeError('Header metadata already has a worker_id.')
    metadata.append(('worker_id', self._worker_id))
    new_client_details = _ClientCallDetails(
        client_call_details.method,
        client_call_details.timeout,
        metadata,
        client_call_details.credentials)
    return continuation(new_client_details, request)
