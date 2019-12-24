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

from apache_beam.portability.api import beam_runner_api_pb2_grpc
from apache_beam.portability.api.beam_runner_api_pb2_grpc import TestStreamServiceServicer


class TestStreamServiceController(TestStreamServiceServicer):
  def __init__(self, events, endpoint=None):
    self._server = grpc.server(ThreadPoolExecutor(max_workers=10))

    if endpoint:
      self.endpoint = endpoint
      self._server.add_insecure_port(self.endpoint)
    else:
      port = self._server.add_insecure_port('[::]:0')
      self.endpoint = '[::]:{}'.format(port)

    beam_runner_api_pb2_grpc.add_TestStreamServiceServicer_to_server(
        self, self._server)
    self._events = events

  def start(self):
    self._server.start()

  def stop(self):
    self._server.stop(0)
    self._server.wait_for_termination()

  def Events(self, request, context):
    """Streams back all of the events from the streaming cache."""

    for e in self._events:
      yield e
