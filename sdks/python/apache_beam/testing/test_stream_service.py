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

# pytype: skip-file

from concurrent.futures import ThreadPoolExecutor

import grpc

from apache_beam.portability.api import beam_interactive_api_pb2_grpc


class TestStreamServiceController(
    beam_interactive_api_pb2_grpc.TestStreamServiceServicer):
  """A server that streams TestStreamPayload.Events from a single EventRequest.

  This server is used as a way for TestStreams to receive events from file.
  """
  def __init__(self, reader, endpoint=None, exception_handler=None):
    self._server = grpc.server(ThreadPoolExecutor(max_workers=10))
    self._server_started = False
    self._server_stopped = False

    if endpoint:
      self.endpoint = endpoint
      self._server.add_insecure_port(self.endpoint)
    else:
      port = self._server.add_insecure_port('localhost:0')
      self.endpoint = 'localhost:{}'.format(port)

    beam_interactive_api_pb2_grpc.add_TestStreamServiceServicer_to_server(
        self, self._server)
    self._reader = reader
    self._exception_handler = exception_handler
    if not self._exception_handler:
      self._exception_handler = lambda _: False

  def start(self):
    # A server can only be started if never started and never stopped before.
    if self._server_started or self._server_stopped:
      return
    self._server_started = True
    self._server.start()

  def stop(self):
    # A server can only be stopped if already started and never stopped before.
    if not self._server_started or self._server_stopped:
      return
    self._server_started = False
    self._server_stopped = True
    self._server.stop(0)
    # This was introduced in grpcio 1.24 and might be gone in the future. Keep
    # this check in case the runtime is on a older, current or future grpcio.
    if hasattr(self._server, 'wait_for_termination'):
      self._server.wait_for_termination()

  def Events(self, request, context):
    """Streams back all of the events from the streaming cache."""

    # TODO(srohde): Once we get rid of the CacheManager, get rid of this 'full'
    # label.
    tags = [None if tag == 'None' else tag for tag in request.output_ids]
    try:
      reader = self._reader.read_multiple([('full', tag) for tag in tags])
      while True:
        e = next(reader)
        yield e
    except StopIteration:
      pass
    except Exception as e:
      if not self._exception_handler(e):
        raise e
