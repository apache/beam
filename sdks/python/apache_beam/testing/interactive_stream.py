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
import time

from apache_beam.portability.api import beam_interactive_api_pb2
from apache_beam.portability.api import beam_interactive_api_pb2_grpc
from apache_beam.portability.api.beam_interactive_api_pb2_grpc import InteractiveServiceServicer
from concurrent.futures import ThreadPoolExecutor


def to_api_state(state):
  if state == 'STOPPED':
    return beam_interactive_api_pb2.StatusResponse.STOPPED
  if state == 'PAUSED':
    return beam_interactive_api_pb2.StatusResponse.PAUSED
  return beam_interactive_api_pb2.StatusResponse.RUNNING

class InteractiveStreamController(InteractiveServiceServicer):
  def __init__(self, endpoint, streaming_cache):
    self._endpoint = endpoint
    self._server = grpc.server(ThreadPoolExecutor(max_workers=10))
    beam_interactive_api_pb2_grpc.add_InteractiveServiceServicer_to_server(
        self, self._server)
    self._server.add_insecure_port(self._endpoint)
    self._server.start()

    self._streaming_cache = streaming_cache
    self._state = 'STOPPED'
    self._playback_speed = 1.0

  def Start(self, request, context):
    """Requests that the Service starts emitting elements.
    """

    self._next_state('RUNNING')
    self._playback_speed = request.playback_speed or 1.0
    self._playback_speed = max(min(self._playback_speed, 1000000.0), 0.001)
    return beam_interactive_api_pb2.StartResponse()

  def Stop(self, request, context):
    """Requests that the Service stop emitting elements.
    """
    self._next_state('STOPPED')
    return beam_interactive_api_pb2.StartResponse()

  def Pause(self, request, context):
    """Requests that the Service pause emitting elements.
    """
    self._next_state('PAUSED')
    return beam_interactive_api_pb2.PauseResponse()

  def Step(self, request, context):
    """Requests that the Service emit a single element from each cached source.
    """
    self._next_state('STEP')
    return beam_interactive_api_pb2.StepResponse()

  def Status(self, request, context):
    """Returns the status of the service.
    """
    resp = beam_interactive_api_pb2.StatusResponse()
    resp.stream_time.GetCurrentTime()
    resp.state = to_api_state(self._state)
    return resp

  def _reset_state(self):
    self._reader = None
    self._playback_speed = 1.0
    self._state = 'STOPPED'

  def _next_state(self, state):
    if not self._state or self._state == 'STOPPED':
      if state == 'RUNNING' or state == 'STEP':
        self._reader = self._streaming_cache.reader()
    elif self._state == 'RUNNING':
      if state == 'STOPPED':
        self._reset_state()
    self._state = state

  def Events(self, request, context):
    # The TestStream will wait until the stream starts.
    while self._state != 'RUNNING' and self._state != 'STEP':
      time.sleep(0.01)

    events = self._reader.read()
    if events:
      for e in events:
        # Here we assume that the first event is the processing_time_event so
        # that we can sleep and then emit the element. Thereby, trying to
        # emulate the original stream.
        if e.HasField('processing_time_event'):
          sleep_duration = (
              e.processing_time_event.advance_duration / self._playback_speed
              ) * 10**-6
          time.sleep(sleep_duration)
        yield beam_interactive_api_pb2.EventsResponse(events=[e])
    else:
      resp = beam_interactive_api_pb2.EventsResponse()
      resp.end_of_stream = True
      self._next_state('STOPPED')
      yield resp
      return

    # The Step command allows the user to send an individual element from each
    # source down into the pipeline. It immediately pauses afterwards.
    if self._state == 'STEP':
      self._next_state('PAUSED')
