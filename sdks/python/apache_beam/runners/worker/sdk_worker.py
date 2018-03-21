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

import contextlib
import logging
import Queue as queue
import sys
import threading
import traceback
from concurrent import futures

import grpc
import six

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane


class SdkHarness(object):
  REQUEST_METHOD_PREFIX = '_request_'

  def __init__(self, control_address, worker_count):
    self._worker_count = worker_count
    self._worker_index = 0
    self._control_channel = grpc.insecure_channel(control_address)
    self._data_channel_factory = data_plane.GrpcClientDataChannelFactory()
    self.workers = queue.Queue()
    # one thread is enough for getting the progress report.
    # Assumption:
    # Progress report generation should not do IO or wait on other resources.
    #  Without wait, having multiple threads will not improve performance and
    #  will only add complexity.
    self._progress_thread_pool = futures.ThreadPoolExecutor(max_workers=1)
    self._process_thread_pool = futures.ThreadPoolExecutor(
        max_workers=self._worker_count)
    self._instruction_id_vs_worker = {}
    self._fns = {}
    self._responses = queue.Queue()
    self._process_bundle_queue = queue.Queue()
    self._unscheduled_process_bundle = set()
    logging.info('Initializing SDKHarness with %s workers.', self._worker_count)

  def run(self):
    control_stub = beam_fn_api_pb2_grpc.BeamFnControlStub(self._control_channel)
    no_more_work = object()

    # Create workers
    for _ in range(self._worker_count):
      state_handler = GrpcStateHandler(
          beam_fn_api_pb2_grpc.BeamFnStateStub(self._control_channel))
      state_handler.start()
      # SdkHarness manage function registration and share self._fns with all
      # the workers. This is needed because function registration (register)
      # and exceution(process_bundle) are send over different request and we
      # do not really know which woker is going to process bundle
      # for a function till we get process_bundle request. Moreover
      # same function is reused by different process bundle calls and
      # potentially get executed by different worker. Hence we need a
      # centralized function list shared among all the workers.
      self.workers.put(
          SdkWorker(
              state_handler=state_handler,
              data_channel_factory=self._data_channel_factory,
              fns=self._fns))

    def get_responses():
      while True:
        response = self._responses.get()
        if response is no_more_work:
          return
        yield response

    for work_request in control_stub.Control(get_responses()):
      logging.info('Got work %s', work_request.instruction_id)
      request_type = work_request.WhichOneof('request')
      # Name spacing the request method with 'request_'. The called method
      # will be like self.request_register(request)
      getattr(self, SdkHarness.REQUEST_METHOD_PREFIX + request_type)(
          work_request)

    logging.info('No more requests from control plane')
    logging.info('SDK Harness waiting for in-flight requests to complete')
    # Wait until existing requests are processed.
    self._progress_thread_pool.shutdown()
    self._process_thread_pool.shutdown()
    # get_responses may be blocked on responses.get(), but we need to return
    # control to its caller.
    self._responses.put(no_more_work)
    self._data_channel_factory.close()
    # Stop all the workers and clean all the associated resources
    for worker in self.workers.queue:
      worker.state_handler.done()
    logging.info('Done consuming work.')

  def _execute(self, task, request):
    try:
      response = task()
    except Exception as e:  # pylint: disable=broad-except
      traceback.print_exc(file=sys.stderr)
      logging.error(
          'Error processing instruction %s. Original traceback is\n%s\n',
          request.instruction_id,
          traceback.format_exc(e),
          exc_info=True)
      response = beam_fn_api_pb2.InstructionResponse(
          instruction_id=request.instruction_id, error=str(e))
    self._responses.put(response)

  def _request_register(self, request):

    def task():
      for process_bundle_descriptor in getattr(
          request, request.WhichOneof('request')).process_bundle_descriptor:
        self._fns[process_bundle_descriptor.id] = process_bundle_descriptor

      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=request.instruction_id,
          register=beam_fn_api_pb2.RegisterResponse())

    self._execute(task, request)

  def _request_process_bundle(self, request):

    def task():
      # Take the free worker. Wait till a worker is free.
      worker = self.workers.get()
      # Get the first work item in the queue
      work = self._process_bundle_queue.get()
      # add the instuction_id vs worker map for progress reporting lookup
      self._instruction_id_vs_worker[work.instruction_id] = worker
      self._unscheduled_process_bundle.discard(work.instruction_id)
      try:
        self._execute(lambda: worker.do_instruction(work), work)
      finally:
        # Delete the instruction_id <-> worker mapping
        self._instruction_id_vs_worker.pop(work.instruction_id, None)
        # Put the worker back in the free worker pool
        self.workers.put(worker)

    # Create a task for each process_bundle request and schedule it
    self._process_bundle_queue.put(request)
    self._unscheduled_process_bundle.add(request.instruction_id)
    self._process_thread_pool.submit(task)

  def _request_process_bundle_progress(self, request):

    def task():
      instruction_reference = getattr(
          request, request.WhichOneof('request')).instruction_reference
      if self._instruction_id_vs_worker.has_key(instruction_reference):
        self._execute(
            lambda: self._instruction_id_vs_worker[
                instruction_reference
            ].do_instruction(request), request)
      else:
        self._execute(lambda: beam_fn_api_pb2.InstructionResponse(
            instruction_id=request.instruction_id, error=(
                'Process bundle request not yet scheduled for instruction {}' if
                instruction_reference in self._unscheduled_process_bundle else
                'Unknown process bundle instruction {}').format(
                    instruction_reference)), request)

    self._progress_thread_pool.submit(task)


class SdkWorker(object):

  def __init__(self, state_handler, data_channel_factory, fns):
    self.fns = fns
    self.state_handler = state_handler
    self.data_channel_factory = data_channel_factory
    self.bundle_processors = {}

  def do_instruction(self, request):
    request_type = request.WhichOneof('request')
    if request_type:
      # E.g. if register is set, this will call self.register(request.register))
      return getattr(self, request_type)(getattr(request, request_type),
                                         request.instruction_id)
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
            self.state_handler, self.data_channel_factory)
    try:
      with self.state_handler.process_instruction_id(instruction_id):
        processor.process_bundle(instruction_id)
    finally:
      del self.bundle_processors[instruction_id]

    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
            metrics=processor.metrics()))

  def process_bundle_progress(self, request, instruction_id):
    # It is an error to get progress for a not-in-flight bundle.
    processor = self.bundle_processors.get(request.instruction_reference)
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        process_bundle_progress=beam_fn_api_pb2.ProcessBundleProgressResponse(
            metrics=processor.metrics() if processor else None))


class GrpcStateHandler(object):

  _DONE = object()

  def __init__(self, state_stub):
    self._lock = threading.Lock()
    self._state_stub = state_stub
    self._requests = queue.Queue()
    self._responses_by_id = {}
    self._last_id = 0
    self._exc_info = None
    self._context = threading.local()

  @contextlib.contextmanager
  def process_instruction_id(self, bundle_id):
    if getattr(self._context, 'process_instruction_id', None) is not None:
      raise RuntimeError(
          'Already bound to %r' % self._context.process_instruction_id)
    self._context.process_instruction_id = bundle_id
    try:
      yield
    finally:
      self._context.process_instruction_id = None

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
          self._responses_by_id[response.id].set(response)
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

  def blocking_get(self, state_key):
    response = self._blocking_request(
        beam_fn_api_pb2.StateRequest(
            state_key=state_key,
            get=beam_fn_api_pb2.StateGetRequest()))
    if response.get.continuation_token:
      raise NotImplementedError
    return response.get.data

  def blocking_append(self, state_key, data):
    self._blocking_request(
        beam_fn_api_pb2.StateRequest(
            state_key=state_key,
            append=beam_fn_api_pb2.StateAppendRequest(data=data)))

  def blocking_clear(self, state_key):
    self._blocking_request(
        beam_fn_api_pb2.StateRequest(
            state_key=state_key,
            clear=beam_fn_api_pb2.StateClearRequest()))

  def _blocking_request(self, request):
    request.id = self._next_id()
    request.instruction_reference = self._context.process_instruction_id
    self._responses_by_id[request.id] = future = _Future()
    self._requests.put(request)
    while not future.wait(timeout=1):
      if self._exc_info:
        t, v, tb = self._exc_info
        six.reraise(t, v, tb)
      elif self._done:
        raise RuntimeError()
    del self._responses_by_id[request.id]
    response = future.get()
    if response.error:
      raise RuntimeError(response.error)
    else:
      return response

  def _next_id(self):
    self._last_id += 1
    return str(self._last_id)


class _Future(object):
  """A simple future object to implement blocking requests.
  """

  def __init__(self):
    self._event = threading.Event()

  def wait(self, timeout=None):
    return self._event.wait(timeout)

  def get(self, timeout=None):
    if self.wait(timeout):
      return self._value
    else:
      raise LookupError()

  def set(self, value):
    self._value = value
    self._event.set()
