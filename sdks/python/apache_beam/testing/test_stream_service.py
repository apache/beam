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

from __future__ import absolute_import

from concurrent.futures import ThreadPoolExecutor

import grpc

from apache_beam.portability.api import beam_interactive_api_pb2
from apache_beam.portability.api import beam_interactive_api_pb2_grpc
from apache_beam.portability.api.beam_interactive_api_pb2_grpc import InteractiveServiceServicer


class InteractiveStreamController(InteractiveServiceServicer):
  def __init__(self, streaming_cache, endpoint=None):
    self._server = grpc.server(ThreadPoolExecutor(max_workers=10))

    if endpoint:
      self.endpoint = endpoint
      self._server.add_insecure_port(self.endpoint)
    else:
      port = self._server.add_insecure_port('[::]:0')
      self.endpoint = '[::]:{}'.format(port)

    beam_interactive_api_pb2_grpc.add_InteractiveServiceServicer_to_server(
        self, self._server)
    self._streaming_cache = streaming_cache

    self._sessions = {}
    self._session_id = 0

  def start(self):
    self._server.start()
    self._reader = self._streaming_cache.reader()

  def stop(self):
    self._server.stop(0)
    self._server.wait_for_termination()

  def Connect(self, request, context):
    """Starts a session.

    Callers should use the returned session id in all future requests.
    """
    session_id = str(self._session_id)
    self._session_id += 1
    self._sessions[session_id] = self._streaming_cache.reader().read()
    return beam_interactive_api_pb2.ConnectResponse(session_id=session_id)

  def Events(self, request, context):
    """Returns the next event from the streaming cache.

    Token behavior: the first request should have a token of "None". Each
    subsequent request should use the previously received token from the
    response. The stream ends when the returned token is the empty string.
    """
    assert request.session_id in self._sessions, (\
        'Session "{}" was not found. Did you forget to call Connect ' +
        'first?').format(request.session_id)

    reader = self._sessions[request.session_id]
    token = (int(request.token) if request.token else 0) + 1
    event = None
    try:
      event = next(reader)
    except StopIteration:
      token = None
    return beam_interactive_api_pb2.EventsResponse(
        event=event, token=str(token) if token else None)
