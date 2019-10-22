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

import time
from concurrent.futures import ThreadPoolExecutor

import grpc

from apache_beam.portability.api import beam_interactive_api_pb2
from apache_beam.portability.api import beam_interactive_api_pb2_grpc
from apache_beam.portability.api.beam_interactive_api_pb2 import State as ServiceState
from apache_beam.portability.api.beam_interactive_api_pb2_grpc import InteractiveServiceServicer


class InteractiveStreamController(InteractiveServiceServicer):
  def __init__(self, streaming_cache, endpoint=None):
    self._server = grpc.server(ThreadPoolExecutor(max_workers=2))

    if endpoint:
      self.endpoint = endpoint
      self._server.add_insecure_port(self.endpoint)
    else:
      port = self._server.add_insecure_port('[::]:0')
      self.endpoint = '[::]:{}'.format(port)

    beam_interactive_api_pb2_grpc.add_InteractiveServiceServicer_to_server(
        self, self._server)
    self._streaming_cache = streaming_cache
    self._state = ServiceState.STOPPED
    self._playback_speed = 1.0

  def start(self):
    self._server.start()

  def stop(self):
    self._server.stop(0)
    self._server.wait_for_termination()

  def Start(self, request, context):
    """Requests that the Service starts emitting elements.
    """

    self._next_state(ServiceState.RUNNING)
    self._playback_speed = request.playback_speed or 1.0
    self._playback_speed = 1.0 / max(min(self._playback_speed, 1000000.0), 0.1)
    return beam_interactive_api_pb2.StartResponse()

  def Stop(self, request, context):
    """Requests that the Service stop emitting elements.
    """
    self._next_state(ServiceState.STOPPED)
    return beam_interactive_api_pb2.StartResponse()

  def Pause(self, request, context):
    """Requests that the Service pause emitting elements.
    """
    self._next_state(ServiceState.PAUSED)
    return beam_interactive_api_pb2.PauseResponse()

  def Step(self, request, context):
    """Requests that the Service emit a single element from each cached source.
    """
    self._next_state(ServiceState.STEPPING)
    return beam_interactive_api_pb2.StepResponse()

  def Status(self, request, context):
    """Returns the status of the service.
    """
    resp = beam_interactive_api_pb2.StatusResponse()
    resp.stream_time.GetCurrentTime()
    resp.state = self._state
    return resp

  def _reset_state(self):
    self._reader = None
    self._playback_speed = 1.0
    self._state = ServiceState.STOPPED

  def _next_state(self, state):
    if self._state == ServiceState.STOPPED:
      if state in (ServiceState.RUNNING, ServiceState.STEPPING):
        self._reader = self._streaming_cache.reader()
    elif self._state == ServiceState.RUNNING:
      if state == ServiceState.STOPPED:
        self._reset_state()
    self._state = state

  def Events(self, request, context):
    # The TestStream will wait until the stream starts.
    while (self._state != ServiceState.RUNNING and
           self._state != ServiceState.STEPPING):
      time.sleep(0.25)

    events = self._reader.read()
    if events:
      for e in events:
        # Here we assume that the first event is the processing_time_event so
        # that we can sleep and then emit the element. Thereby, trying to
        # emulate the original stream.
        if e.HasField('processing_time_event'):
          sleep_duration = (
              e.processing_time_event.advance_duration * self._playback_speed
              ) * 10**-6
          time.sleep(sleep_duration)
        yield beam_interactive_api_pb2.EventsResponse(events=[e])
    else:
      resp = beam_interactive_api_pb2.EventsResponse()
      resp.end_of_stream = True
      self._next_state(ServiceState.STOPPED)
      yield resp
      return

    # The Step command allows the user to send an individual element from each
    # source down into the pipeline. It immediately pauses afterwards.
    if self._state == ServiceState.STEPPING:
      self._next_state(ServiceState.PAUSED)
