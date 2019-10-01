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

import grpc
import os
import tempfile

from apache_beam import coders
from apache_beam.portability.api import beam_interactive_api_pb2
from apache_beam.portability.api import beam_interactive_api_pb2_grpc
from apache_beam.portability.api.beam_interactive_api_pb2 import InteractiveStreamRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.caching.file_based_cache import TextBasedCache
from apache_beam.runners.interactive.caching.streaming_cache import StreamingCache
from apache_beam.utils import timestamp
from concurrent.futures import ThreadPoolExecutor

class InteractiveStreamController(beam_interactive_api_pb2_grpc.InteractiveServiceServicer):
  # TODO(srohde): Add real-time playback with speed multiplier

  def __init__(self, endpoint, streaming_cache):
    self._endpoint = endpoint
    self._server = grpc.server(ThreadPoolExecutor(max_workers=10))
    beam_interactive_api_pb2_grpc.add_InteractiveServiceServicer_to_server(
        self, self._server)
    self._server.add_insecure_port(self._endpoint)
    self._server.start()

    self._streaming_cache = streaming_cache
    self._interrupt = False
    self._state = 'STOPPED'

  def Start(self, request, context):
    self._next_state('RUNNING')
    return beam_interactive_api_pb2.StartResponse()

  def Stop(self, request, context):
    self._next_state('STOPPED')
    return beam_interactive_api_pb2.StartResponse()

  def Pause(self, request, context):
    self._next_state('PAUSED')
    return beam_interactive_api_pb2.PauseResponse()

  def Step(self, request, context):
    self._next_state('STEP')
    return beam_interactive_api_pb2.StepResponse()

  def Status(self, request, context):
    resp = beam_interactive_api_pb2.StatusResponse()
    resp.stream_time.GetCurrentTime()
    resp.state = self._to_api_state(self._state)
    return resp

  def _to_api_state(self, state):
    if state == 'STOPPED':
      return beam_interactive_api_pb2.StatusResponse.STOPPED
    if state == 'PAUSED':
      return beam_interactive_api_pb2.StatusResponse.PAUSED
    return beam_interactive_api_pb2.StatusResponse.RUNNING

  def _next_state(self, state):
    if not self._state or self._state == 'STOPPED':
      if state == 'RUNNING' or state == 'STEP':
        self._reader = self._streaming_cache.reader()
    elif self._state == 'RUNNING':
      if state == 'STOPPED':
        self._reader = None
    self._state = state

  def Events(self, request, context):
    import time
    while self._state != 'RUNNING' and self._state != 'STEP' and not self._interrupt:
      time.sleep(0.01)

    if self._interrupt:
      resp = beam_interactive_api_pb2.EventsResponse()
      resp.end_of_stream = True
      yield resp
      return

    events = self._reader.read()
    if events:
      for e in events:
        yield beam_interactive_api_pb2.EventsResponse(events=[e])
    else:
      resp = beam_interactive_api_pb2.EventsResponse()
      resp.end_of_stream = True
      self._next_state('STOPPED')
      yield resp
      return

    if self._state == 'STEP':
      self._next_state('PAUSED')
