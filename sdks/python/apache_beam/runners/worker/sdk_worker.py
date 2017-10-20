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

"""SDK harness for executing Python Fns via the Fn API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import logging
import Queue as queue
import sys
import threading
import traceback
from concurrent import futures

import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane


class SdkHarness(object):

  def __init__(self, control_address):
    self._control_channel = grpc.insecure_channel(control_address)
    self._data_channel_factory = data_plane.GrpcClientDataChannelFactory()
    # TODO: Ensure thread safety to run with more than 1 thread.
    self._default_work_thread_pool = futures.ThreadPoolExecutor(max_workers=1)
    self._progress_thread_pool = futures.ThreadPoolExecutor(max_workers=1)

  def run(self):
    control_stub = beam_fn_api_pb2_grpc.BeamFnControlStub(self._control_channel)
    state_stub = beam_fn_api_pb2_grpc.BeamFnStateStub(self._control_channel)
    state_handler = GrpcStateHandler(state_stub)
    state_handler.start()
    self.worker = SdkWorker(state_handler, self._data_channel_factory)

    responses = queue.Queue()
    no_more_work = object()

    def get_responses():
      while True:
        response = responses.get()
        if response is no_more_work:
          return
        yield response

    for work_request in control_stub.Control(get_responses()):
      logging.info('Got work %s', work_request.instruction_id)
      request_type = work_request.WhichOneof('request')
      if request_type == ['process_bundle_progress']:
        thread_pool = self._progress_thread_pool
      else:
        thread_pool = self._default_work_thread_pool

      # Need this wrapper to capture the original stack trace.
      def do_instruction(request):
        try:
          return self.worker.do_instruction(request)
        except Exception as e:  # pylint: disable=broad-except
          traceback_str = traceback.format_exc(e)
          raise Exception("Error processing request. Original traceback "
                          "is\n%s\n" % traceback_str)

      def handle_response(request, response_future):
        try:
          response = response_future.result()
        except Exception as e:  # pylint: disable=broad-except
          logging.error(
              'Error processing instruction %s',
              request.instruction_id,
              exc_info=True)
          response = beam_fn_api_pb2.InstructionResponse(
              instruction_id=request.instruction_id,
              error=str(e))
        responses.put(response)

      thread_pool.submit(do_instruction, work_request).add_done_callback(
          functools.partial(handle_response, work_request))

    logging.info("No more requests from control plane")
    logging.info("SDK Harness waiting for in-flight requests to complete")
    # Wait until existing requests are processed.
    self._progress_thread_pool.shutdown()
    self._default_work_thread_pool.shutdown()
    # get_responses may be blocked on responses.get(), but we need to return
    # control to its caller.
    responses.put(no_more_work)
    self._data_channel_factory.close()
    state_handler.done()
    logging.info('Done consuming work.')


class SdkWorker(object):

  def __init__(self, state_handler, data_channel_factory):
    self.fns = {}
    self.state_handler = state_handler
    self.data_channel_factory = data_channel_factory
    self.bundle_processors = {}

  def do_instruction(self, request):
    request_type = request.WhichOneof('request')
    if request_type:
      # E.g. if register is set, this will call self.register(request.register))
      return getattr(self, request_type)(
          getattr(request, request_type), request.instruction_id)
    else:
      raise NotImplementedError

  def register(self, request, instruction_id):
    for process_bundle_descriptor in request.process_bundle_descriptor:
      self.fns[process_bundle_descriptor.id] = process_bundle_descriptor
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        register=beam_fn_api_pb2.RegisterResponse())

  def process_bundle(self, request, instruction_id):
    self.bundle_processors[
        instruction_id] = processor = bundle_processor.BundleProcessor(
            self.fns[request.process_bundle_descriptor_reference],
            self.state_handler,
            self.data_channel_factory)
    try:
      processor.process_bundle(instruction_id)
    finally:
      del self.bundle_processors[instruction_id]

    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
            metrics=processor.metrics()))

  def process_bundle_progress(self, request, instruction_id):
    # It is an error to get progress for a not-in-flight bundle.
    return self.bundle_processors.get(instruction_id).metrics()


class GrpcStateHandler(object):

  _DONE = object()

  def __init__(self, state_stub):
    self._lock = threading.Lock()
    self._state_stub = state_stub
    self._requests = queue.Queue()
    self._responses = queue.Queue()
    self._responses_by_id = {}
    self._last_id = 0
    self._exc_info = None

  def start(self):
    self._done = False

    def request_iter():
      while True:
        request = self._requests.get()
        if request is self._DONE or self._done:
          break
        yield request
    responses = self._state_stub.State(request_iter())

    def pull_responses():
      try:
        for response in responses:
          self._responses.put(response)
          if self._done:
            break
      except:  # pylint: disable=bare-except
        self._exc_info = sys.exc_info()
        raise
    reader = threading.Thread(target=pull_responses, name='read_state')
    reader.daemon = True
    reader.start()

  def done(self):
    self._done = True
    self._requests.put(self._DONE)

  def blocking_get(self, state_key, instruction_reference):
    response = self._blocking_request(
        beam_fn_api_pb2.StateRequest(
            instruction_reference=instruction_reference,
            state_key=state_key,
            get=beam_fn_api_pb2.StateGetRequest()))
    if response.get.continuation_token:
      raise NotImplementedErrror
    return response.get.data

  def blocking_append(self, state_key, data, instruction_reference):
    self._blocking_request(
        beam_fn_api_pb2.StateRequest(
            instruction_reference=instruction_reference,
            state_key=state_key,
            append=beam_fn_api_pb2.StateAppendRequest(data=data)))

  def blocking_clear(self, state_key, instruction_reference):
    self._blocking_request(
        beam_fn_api_pb2.StateRequest(
            instruction_reference=instruction_reference,
            state_key=state_key,
            clear=beam_fn_api_pb2.StateClearRequest()))

  def _blocking_request(self, request):
    request.id = self._next_id()
    self._requests.put(request)
    while request.id not in self._responses_by_id:
      with self._lock:
        if request.id not in self._responses_by_id:
          response = self._responses.get(timeout=2)
          self._responses_by_id[response.id] = response
          if self._exc_info:
            raise self._exc_info[0], self._exc_info[1], self._exc_info[2]
    response = self._responses_by_id[response.id]
    del self._responses_by_id[response.id]
    return response

  def _next_id(self):
    self._last_id += 1
    return str(self._last_id)
