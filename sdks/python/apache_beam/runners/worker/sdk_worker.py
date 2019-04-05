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

import abc
import collections
import contextlib
import logging
import queue
import sys
import threading
import time
import traceback
from builtins import object
from builtins import range
from concurrent import futures

import grpc
from future.utils import raise_
from future.utils import with_metaclass

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor


class SdkHarness(object):
  REQUEST_METHOD_PREFIX = '_request_'
  SCHEDULING_DELAY_THRESHOLD_SEC = 5*60  # 5 Minutes

  def __init__(
      self, control_address, worker_count, credentials=None, worker_id=None,
      profiler_factory=None):
    self._alive = True
    self._worker_count = worker_count
    self._worker_index = 0
    self._worker_id = worker_id
    if credentials is None:
      logging.info('Creating insecure control channel for %s.', control_address)
      self._control_channel = GRPCChannelFactory.insecure_channel(
          control_address)
    else:
      logging.info('Creating secure control channel for %s.', control_address)
      self._control_channel = GRPCChannelFactory.secure_channel(
          control_address, credentials)
    grpc.channel_ready_future(self._control_channel).result(timeout=60)
    logging.info('Control channel established.')

    self._control_channel = grpc.intercept_channel(
        self._control_channel, WorkerIdInterceptor(self._worker_id))
    self._data_channel_factory = data_plane.GrpcClientDataChannelFactory(
        credentials)
    self._state_handler_factory = GrpcStateHandlerFactory(credentials)
    self._profiler_factory = profiler_factory
    self._fns = {}
    # BundleProcessor cache across all workers.
    self._bundle_processor_cache = BundleProcessorCache(
        state_handler_factory=self._state_handler_factory,
        data_channel_factory=self._data_channel_factory,
        fns=self._fns)
    # workers for process/finalize bundle.
    self.workers = queue.Queue()
    # one worker for progress/split request.
    self.progress_worker = SdkWorker(self._bundle_processor_cache,
                                     profiler_factory=self._profiler_factory)
    # one thread is enough for getting the progress report.
    # Assumption:
    # Progress report generation should not do IO or wait on other resources.
    #  Without wait, having multiple threads will not improve performance and
    #  will only add complexity.
    self._progress_thread_pool = futures.ThreadPoolExecutor(max_workers=1)
    # finalize and process share one thread pool.
    self._process_thread_pool = futures.ThreadPoolExecutor(
        max_workers=self._worker_count)
    self._responses = queue.Queue()
    self._process_bundle_queue = queue.Queue()
    self._unscheduled_process_bundle = {}
    logging.info('Initializing SDKHarness with %s workers.', self._worker_count)

  def run(self):
    control_stub = beam_fn_api_pb2_grpc.BeamFnControlStub(self._control_channel)
    no_more_work = object()

    # Create process workers
    for _ in range(self._worker_count):
      # SdkHarness manage function registration and share self._fns with all
      # the workers. This is needed because function registration (register)
      # and exceution(process_bundle) are send over different request and we
      # do not really know which woker is going to process bundle
      # for a function till we get process_bundle request. Moreover
      # same function is reused by different process bundle calls and
      # potentially get executed by different worker. Hence we need a
      # centralized function list shared among all the workers.
      self.workers.put(
          SdkWorker(self._bundle_processor_cache,
                    profiler_factory=self._profiler_factory))

    def get_responses():
      while True:
        response = self._responses.get()
        if response is no_more_work:
          return
        yield response

    self._alive = True
    monitoring_thread = threading.Thread(target=self._monitor_process_bundle)
    monitoring_thread.daemon = True
    monitoring_thread.start()

    try:
      for work_request in control_stub.Control(get_responses()):
        logging.debug('Got work %s', work_request.instruction_id)
        request_type = work_request.WhichOneof('request')
        # Name spacing the request method with 'request_'. The called method
        # will be like self.request_register(request)
        getattr(self, SdkHarness.REQUEST_METHOD_PREFIX + request_type)(
            work_request)
    finally:
      self._alive = False

    logging.info('No more requests from control plane')
    logging.info('SDK Harness waiting for in-flight requests to complete')
    # Wait until existing requests are processed.
    self._progress_thread_pool.shutdown()
    self._process_thread_pool.shutdown()
    # get_responses may be blocked on responses.get(), but we need to return
    # control to its caller.
    self._responses.put(no_more_work)
    # Stop all the workers and clean all the associated resources
    self._data_channel_factory.close()
    self._state_handler_factory.close()
    logging.info('Done consuming work.')

  def _execute(self, task, request):
    try:
      response = task()
    except Exception:  # pylint: disable=broad-except
      traceback_string = traceback.format_exc()
      print(traceback_string, file=sys.stderr)
      logging.error(
          'Error processing instruction %s. Original traceback is\n%s\n',
          request.instruction_id, traceback_string)
      response = beam_fn_api_pb2.InstructionResponse(
          instruction_id=request.instruction_id, error=traceback_string)
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
      self._unscheduled_process_bundle.pop(work.instruction_id, None)
      try:
        self._execute(lambda: worker.do_instruction(work), work)
      finally:
        # Put the worker back in the free worker pool
        self.workers.put(worker)
    # Create a task for each process_bundle request and schedule it
    self._process_bundle_queue.put(request)
    self._unscheduled_process_bundle[request.instruction_id] = time.time()
    self._process_thread_pool.submit(task)
    logging.debug(
        "Currently using %s threads." % len(self._process_thread_pool._threads))

  def _request_process_bundle_split(self, request):
    self._request_process_bundle_action(request)

  def _request_process_bundle_progress(self, request):
    self._request_process_bundle_action(request)

  def _request_process_bundle_action(self, request):

    def task():
      instruction_reference = getattr(
          request, request.WhichOneof('request')).instruction_reference
      # only process progress/split request when a bundle is in processing.
      if (instruction_reference in
          self._bundle_processor_cache.active_bundle_processors):
        self._execute(
            lambda: self.progress_worker.do_instruction(request), request)
      else:
        self._execute(lambda: beam_fn_api_pb2.InstructionResponse(
            instruction_id=request.instruction_id, error=(
                'Process bundle request not yet scheduled for instruction {}' if
                instruction_reference in self._unscheduled_process_bundle else
                'Unknown process bundle instruction {}').format(
                    instruction_reference)), request)

    self._progress_thread_pool.submit(task)

  def _request_finalize_bundle(self, request):

    def task():
      # Get one available worker.
      worker = self.workers.get()
      try:
        self._execute(
            lambda: worker.do_instruction(request), request)
      finally:
        # Put the worker back in the free worker pool.
        self.workers.put(worker)

    self._process_thread_pool.submit(task)

  def _monitor_process_bundle(self):
    """
    Monitor the unscheduled bundles and log if a bundle is not scheduled for
    more than SCHEDULING_DELAY_THRESHOLD_SEC.
    """
    while self._alive:
      time.sleep(SdkHarness.SCHEDULING_DELAY_THRESHOLD_SEC)
      # Check for bundles to be scheduled.
      if self._unscheduled_process_bundle:
        current_time = time.time()
        for instruction_id in self._unscheduled_process_bundle:
          request_time = None
          try:
            request_time = self._unscheduled_process_bundle[instruction_id]
          except KeyError:
            pass
          if request_time:
            scheduling_delay = current_time - request_time
            if scheduling_delay > SdkHarness.SCHEDULING_DELAY_THRESHOLD_SEC:
              logging.warn('Unable to schedule instruction %s for %s',
                           instruction_id, scheduling_delay)


class BundleProcessorCache(object):
  def __init__(self, state_handler_factory, data_channel_factory, fns):
    self.fns = fns
    self.state_handler_factory = state_handler_factory
    self.data_channel_factory = data_channel_factory
    self.active_bundle_processors = {}
    self.cached_bundle_processors = collections.defaultdict(list)

  def register(self, bundle_descriptor):
    self.fns[bundle_descriptor.id] = bundle_descriptor

  def get(self, instruction_id, bundle_descriptor_id):
    try:
      # pop() is threadsafe
      processor = self.cached_bundle_processors[bundle_descriptor_id].pop()
    except IndexError:
      processor = bundle_processor.BundleProcessor(
          self.fns[bundle_descriptor_id],
          self.state_handler_factory.create_state_handler(
              self.fns[bundle_descriptor_id].state_api_service_descriptor),
          self.data_channel_factory)
    self.active_bundle_processors[
        instruction_id] = bundle_descriptor_id, processor
    return processor

  def lookup(self, instruction_id):
    return self.active_bundle_processors.get(instruction_id, (None, None))[-1]

  def discard(self, instruction_id):
    del self.active_bundle_processors[instruction_id]

  def release(self, instruction_id):
    descriptor_id, processor = self.active_bundle_processors.pop(instruction_id)
    processor.reset()
    self.cached_bundle_processors[descriptor_id].append(processor)


class SdkWorker(object):

  def __init__(self, bundle_processor_cache, profiler_factory=None):
    self.bundle_processor_cache = bundle_processor_cache
    self.profiler_factory = profiler_factory

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
      self.bundle_processor_cache.register(process_bundle_descriptor)
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        register=beam_fn_api_pb2.RegisterResponse())

  def process_bundle(self, request, instruction_id):
    bundle_processor = self.bundle_processor_cache.get(
        instruction_id, request.process_bundle_descriptor_reference)
    try:
      with bundle_processor.state_handler.process_instruction_id(
          instruction_id):
        with self.maybe_profile(instruction_id):
          delayed_applications, requests_finalization = (
              bundle_processor.process_bundle(instruction_id))
          response = beam_fn_api_pb2.InstructionResponse(
              instruction_id=instruction_id,
              process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                  residual_roots=delayed_applications,
                  metrics=bundle_processor.metrics(),
                  monitoring_infos=bundle_processor.monitoring_infos(),
                  requires_finalization=requests_finalization))
      # Don't release here if finalize is needed.
      if not requests_finalization:
        self.bundle_processor_cache.release(instruction_id)
      return response
    except:  # pylint: disable=broad-except
      # Don't re-use bundle processors on failure.
      self.bundle_processor_cache.discard(instruction_id)
      raise

  def process_bundle_split(self, request, instruction_id):
    processor = self.bundle_processor_cache.lookup(
        request.instruction_reference)
    if processor:
      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=instruction_id,
          process_bundle_split=processor.try_split(request))
    else:
      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=instruction_id,
          error='Instruction not running: %s' % instruction_id)

  def process_bundle_progress(self, request, instruction_id):
    # It is an error to get progress for a not-in-flight bundle.
    processor = self.bundle_processor_cache.lookup(
        request.instruction_reference)
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        process_bundle_progress=beam_fn_api_pb2.ProcessBundleProgressResponse(
            metrics=processor.metrics() if processor else None,
            monitoring_infos=processor.monitoring_infos() if processor else []))

  def finalize_bundle(self, request, instruction_id):
    processor = self.bundle_processor_cache.lookup(
        request.instruction_reference)
    if processor:
      try:
        finalize_response = processor.finalize_bundle()
        self.bundle_processor_cache.release(request.instruction_reference)
        return beam_fn_api_pb2.InstructionResponse(
            instruction_id=instruction_id,
            finalize_bundle=finalize_response)
      except:
        self.bundle_processor_cache.discard(request.instruction_reference)
        raise
    else:
      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=instruction_id,
          error='Instruction not running: %s' % instruction_id)

  @contextlib.contextmanager
  def maybe_profile(self, instruction_id):
    if self.profiler_factory:
      profiler = self.profiler_factory(instruction_id)
      if profiler:
        with profiler:
          yield
      else:
        yield
    else:
      yield


class StateHandlerFactory(with_metaclass(abc.ABCMeta, object)):
  """An abstract factory for creating ``DataChannel``."""

  @abc.abstractmethod
  def create_state_handler(self, api_service_descriptor):
    """Returns a ``StateHandler`` from the given ApiServiceDescriptor."""
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def close(self):
    """Close all channels that this factory owns."""
    raise NotImplementedError(type(self))


class GrpcStateHandlerFactory(StateHandlerFactory):
  """A factory for ``GrpcStateHandler``.

  Caches the created channels by ``state descriptor url``.
  """

  def __init__(self, credentials=None):
    self._state_handler_cache = {}
    self._lock = threading.Lock()
    self._throwing_state_handler = ThrowingStateHandler()
    self._credentials = credentials

  def create_state_handler(self, api_service_descriptor):
    if not api_service_descriptor:
      return self._throwing_state_handler
    url = api_service_descriptor.url
    if url not in self._state_handler_cache:
      with self._lock:
        if url not in self._state_handler_cache:
          # Options to have no limits (-1) on the size of the messages
          # received or sent over the data plane. The actual buffer size is
          # controlled in a layer above.
          options = [('grpc.max_receive_message_length', -1),
                     ('grpc.max_send_message_length', -1)]
          if self._credentials is None:
            logging.info('Creating insecure state channel for %s.', url)
            grpc_channel = GRPCChannelFactory.insecure_channel(
                url, options=options)
          else:
            logging.info('Creating secure state channel for %s.', url)
            grpc_channel = GRPCChannelFactory.secure_channel(
                url, self._credentials, options=options)
          logging.info('State channel established.')
          # Add workerId to the grpc channel
          grpc_channel = grpc.intercept_channel(grpc_channel,
                                                WorkerIdInterceptor())
          self._state_handler_cache[url] = GrpcStateHandler(
              beam_fn_api_pb2_grpc.BeamFnStateStub(grpc_channel))
    return self._state_handler_cache[url]

  def close(self):
    logging.info('Closing all cached gRPC state handlers.')
    for _, state_handler in self._state_handler_cache.items():
      state_handler.done()
    self._state_handler_cache.clear()


class ThrowingStateHandler(object):
  """A state handler that errors on any requests."""

  def blocking_get(self, state_key, instruction_reference):
    raise RuntimeError(
        'Unable to handle state requests for ProcessBundleDescriptor without '
        'out state ApiServiceDescriptor for instruction %s and state key %s.'
        % (state_key, instruction_reference))

  def blocking_append(self, state_key, data, instruction_reference):
    raise RuntimeError(
        'Unable to handle state requests for ProcessBundleDescriptor without '
        'out state ApiServiceDescriptor for instruction %s and state key %s.'
        % (state_key, instruction_reference))

  def blocking_clear(self, state_key, instruction_reference):
    raise RuntimeError(
        'Unable to handle state requests for ProcessBundleDescriptor without '
        'out state ApiServiceDescriptor for instruction %s and state key %s.'
        % (state_key, instruction_reference))


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
    self.start()

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

  def blocking_get(self, state_key, continuation_token=None):
    response = self._blocking_request(
        beam_fn_api_pb2.StateRequest(
            state_key=state_key,
            get=beam_fn_api_pb2.StateGetRequest(
                continuation_token=continuation_token)))
    return response.get.data, response.get.continuation_token

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
        raise_(t, v, tb)
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
