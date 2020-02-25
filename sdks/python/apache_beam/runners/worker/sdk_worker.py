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

# pytype: skip-file

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
from concurrent import futures
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import grpc
from future.utils import raise_
from future.utils import with_metaclass

from apache_beam.coders import coder_impl
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import statesampler
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.data_plane import PeriodicThread
from apache_beam.runners.worker.statecache import StateCache
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor
from apache_beam.runners.worker.worker_status import FnApiWorkerStatusHandler
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor

if TYPE_CHECKING:
  from apache_beam.portability.api import endpoints_pb2
  from apache_beam.utils.profiler import Profile

_LOGGER = logging.getLogger(__name__)

# This SDK harness will (by default), log a "lull" in processing if it sees no
# transitions in over 5 minutes.
# 5 minutes * 60 seconds * 1020 millis * 1000 micros * 1000 nanoseconds
DEFAULT_LOG_LULL_TIMEOUT_NS = 5 * 60 * 1000 * 1000 * 1000

DEFAULT_BUNDLE_PROCESSOR_CACHE_SHUTDOWN_THRESHOLD_S = 60


class SdkHarness(object):
  REQUEST_METHOD_PREFIX = '_request_'

  def __init__(self,
               control_address,  # type: str
               credentials=None,
               worker_id=None,  # type: Optional[str]
               # Caching is disabled by default
               state_cache_size=0,
               # time-based data buffering is disabled by default
               data_buffer_time_limit_ms=0,
               profiler_factory=None,  # type: Optional[Callable[..., Profile]]
               status_address=None,  # type: Optional[str, unicode]
               ):
    self._alive = True
    self._worker_index = 0
    self._worker_id = worker_id
    self._state_cache = StateCache(state_cache_size)
    if credentials is None:
      _LOGGER.info('Creating insecure control channel for %s.', control_address)
      self._control_channel = GRPCChannelFactory.insecure_channel(
          control_address)
    else:
      _LOGGER.info('Creating secure control channel for %s.', control_address)
      self._control_channel = GRPCChannelFactory.secure_channel(
          control_address, credentials)
    grpc.channel_ready_future(self._control_channel).result(timeout=60)
    _LOGGER.info('Control channel established.')

    self._control_channel = grpc.intercept_channel(
        self._control_channel, WorkerIdInterceptor(self._worker_id))
    self._data_channel_factory = data_plane.GrpcClientDataChannelFactory(
        credentials, self._worker_id, data_buffer_time_limit_ms)
    self._state_handler_factory = GrpcStateHandlerFactory(
        self._state_cache, credentials)
    self._profiler_factory = profiler_factory
    self._fns = {}  # type: Dict[str, beam_fn_api_pb2.ProcessBundleDescriptor]
    # BundleProcessor cache across all workers.
    self._bundle_processor_cache = BundleProcessorCache(
        state_handler_factory=self._state_handler_factory,
        data_channel_factory=self._data_channel_factory,
        fns=self._fns)

    if status_address:
      try:
        self._status_handler = FnApiWorkerStatusHandler(
            status_address, self._bundle_processor_cache)
      except Exception:
        traceback_string = traceback.format_exc()
        _LOGGER.warning(
            'Error creating worker status request handler, '
            'skipping status report. Trace back: %s' % traceback_string)
    else:
      self._status_handler = None

    # TODO(BEAM-8998) use common UnboundedThreadPoolExecutor to process bundle
    #  progress once dataflow runner's excessive progress polling is removed.
    self._report_progress_executor = futures.ThreadPoolExecutor(max_workers=1)
    self._worker_thread_pool = UnboundedThreadPoolExecutor()
    self._responses = queue.Queue(
    )  # type: queue.Queue[beam_fn_api_pb2.InstructionResponse]
    _LOGGER.info('Initializing SDKHarness with unbounded number of workers.')

  def run(self):
    control_stub = beam_fn_api_pb2_grpc.BeamFnControlStub(self._control_channel)
    no_more_work = object()

    def get_responses():
      # type: () -> Iterator[beam_fn_api_pb2.InstructionResponse]
      while True:
        response = self._responses.get()
        if response is no_more_work:
          return
        yield response

    self._alive = True

    try:
      for work_request in control_stub.Control(get_responses()):
        _LOGGER.debug('Got work %s', work_request.instruction_id)
        request_type = work_request.WhichOneof('request')
        # Name spacing the request method with 'request_'. The called method
        # will be like self.request_register(request)
        getattr(self, SdkHarness.REQUEST_METHOD_PREFIX + request_type)(
            work_request)
    finally:
      self._alive = False

    _LOGGER.info('No more requests from control plane')
    _LOGGER.info('SDK Harness waiting for in-flight requests to complete')
    # Wait until existing requests are processed.
    self._worker_thread_pool.shutdown()
    # get_responses may be blocked on responses.get(), but we need to return
    # control to its caller.
    self._responses.put(no_more_work)
    # Stop all the workers and clean all the associated resources
    self._data_channel_factory.close()
    self._state_handler_factory.close()
    self._bundle_processor_cache.shutdown()
    if self._status_handler:
      self._status_handler.close()
    _LOGGER.info('Done consuming work.')

  def _execute(self,
               task,  # type: Callable[[], beam_fn_api_pb2.InstructionResponse]
               request  # type:  beam_fn_api_pb2.InstructionRequest
              ):
    # type: (...) -> None
    with statesampler.instruction_id(request.instruction_id):
      try:
        response = task()
      except Exception:  # pylint: disable=broad-except
        traceback_string = traceback.format_exc()
        print(traceback_string, file=sys.stderr)
        _LOGGER.error(
            'Error processing instruction %s. Original traceback is\n%s\n',
            request.instruction_id,
            traceback_string)
        response = beam_fn_api_pb2.InstructionResponse(
            instruction_id=request.instruction_id, error=traceback_string)
      self._responses.put(response)

  def _request_register(self, request):
    # type: (beam_fn_api_pb2.InstructionRequest) -> None
    # registration request is handled synchronously
    self._execute(lambda: self.create_worker().do_instruction(request), request)

  def _request_process_bundle(self, request):
    # type: (beam_fn_api_pb2.InstructionRequest) -> None
    self._request_execute(request)

  def _request_process_bundle_split(self, request):
    # type: (beam_fn_api_pb2.InstructionRequest) -> None
    self._request_process_bundle_action(request)

  def _request_process_bundle_progress(self, request):
    # type: (beam_fn_api_pb2.InstructionRequest) -> None
    self._request_process_bundle_action(request)

  def _request_process_bundle_action(self, request):
    # type: (beam_fn_api_pb2.InstructionRequest) -> None

    def task():
      instruction_id = getattr(
          request, request.WhichOneof('request')).instruction_id
      # only process progress/split request when a bundle is in processing.
      if (instruction_id in
          self._bundle_processor_cache.active_bundle_processors):
        self._execute(
            lambda: self.create_worker().do_instruction(request), request)
      else:
        self._execute(
            lambda: beam_fn_api_pb2.InstructionResponse(
                instruction_id=request.instruction_id,
                error=('Unknown process bundle instruction {}').format(
                    instruction_id)),
            request)

    self._report_progress_executor.submit(task)

  def _request_finalize_bundle(self, request):
    # type: (beam_fn_api_pb2.InstructionRequest) -> None
    self._request_execute(request)

  def _request_execute(self, request):
    def task():
      self._execute(
          lambda: self.create_worker().do_instruction(request), request)

    self._worker_thread_pool.submit(task)
    _LOGGER.debug(
        "Currently using %s threads." % len(self._worker_thread_pool._workers))

  def create_worker(self):
    return SdkWorker(
        self._bundle_processor_cache,
        state_cache_metrics_fn=self._state_cache.get_monitoring_infos,
        profiler_factory=self._profiler_factory)


class BundleProcessorCache(object):
  """A cache for ``BundleProcessor``s.

  ``BundleProcessor`` objects are cached by the id of their
  ``beam_fn_api_pb2.ProcessBundleDescriptor``.

  Attributes:
    fns (dict): A dictionary that maps bundle descriptor IDs to instances of
      ``beam_fn_api_pb2.ProcessBundleDescriptor``.
    state_handler_factory (``StateHandlerFactory``): Used to create state
      handlers to be used by a ``bundle_processor.BundleProcessor`` during
      processing.
    data_channel_factory (``data_plane.DataChannelFactory``)
    active_bundle_processors (dict): A dictionary, indexed by instruction IDs,
      containing ``bundle_processor.BundleProcessor`` objects that are currently
      active processing the corresponding instruction.
    cached_bundle_processors (dict): A dictionary, indexed by bundle processor
      id, of cached ``bundle_processor.BundleProcessor`` that are not currently
      performing processing.
  """

  def __init__(self,
               state_handler_factory,  # type: StateHandlerFactory
               data_channel_factory,  # type: data_plane.DataChannelFactory
               fns  # type: Dict[str, beam_fn_api_pb2.ProcessBundleDescriptor]
              ):
    self.fns = fns
    self.state_handler_factory = state_handler_factory
    self.data_channel_factory = data_channel_factory
    self.active_bundle_processors = {
    }  # type: Dict[str, Tuple[str, bundle_processor.BundleProcessor]]
    self.cached_bundle_processors = collections.defaultdict(
        list)  # type: DefaultDict[str, List[bundle_processor.BundleProcessor]]
    self.last_access_times = \
        collections.defaultdict(float)  # type: DefaultDict[str, float]
    self._schedule_periodic_shutdown()

  def register(self, bundle_descriptor):
    # type: (beam_fn_api_pb2.ProcessBundleDescriptor) -> None

    """Register a ``beam_fn_api_pb2.ProcessBundleDescriptor`` by its id."""
    self.fns[bundle_descriptor.id] = bundle_descriptor

  def get(self, instruction_id, bundle_descriptor_id):
    # type: (str, str) -> bundle_processor.BundleProcessor

    """
    Return the requested ``BundleProcessor``, creating it if necessary.

    Moves the ``BundleProcessor`` from the inactive to the active cache.
    """
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
    # type: (str) -> Optional[bundle_processor.BundleProcessor]

    """
    Return the requested ``BundleProcessor`` from the cache.
    """
    return self.active_bundle_processors.get(instruction_id, (None, None))[-1]

  def discard(self, instruction_id):
    # type: (str) -> None

    """
    Remove the ``BundleProcessor`` from the cache.
    """
    self.active_bundle_processors[instruction_id][1].shutdown()
    del self.active_bundle_processors[instruction_id]

  def release(self, instruction_id):
    # type: (str) -> None

    """
    Release the requested ``BundleProcessor``.

    Resets the ``BundleProcessor`` and moves it from the active to the
    inactive cache.
    """
    descriptor_id, processor = self.active_bundle_processors.pop(instruction_id)
    processor.reset()
    self.last_access_times[descriptor_id] = time.time()
    self.cached_bundle_processors[descriptor_id].append(processor)

  def shutdown(self):
    """
    Shutdown all ``BundleProcessor``s in the cache.
    """
    if self.periodic_shutdown:
      self.periodic_shutdown.cancel()
      self.periodic_shutdown.join()
      self.periodic_shutdown = None

    for instruction_id in self.active_bundle_processors:
      self.active_bundle_processors[instruction_id][1].shutdown()
      del self.active_bundle_processors[instruction_id]
    for cached_bundle_processors in self.cached_bundle_processors.values():
      BundleProcessorCache._shutdown_cached_bundle_processors(
          cached_bundle_processors)

  def _schedule_periodic_shutdown(self):
    def shutdown_inactive_bundle_processors():
      for descriptor_id, last_access_time in self.last_access_times.items():
        if (time.time() - last_access_time >
            DEFAULT_BUNDLE_PROCESSOR_CACHE_SHUTDOWN_THRESHOLD_S):
          BundleProcessorCache._shutdown_cached_bundle_processors(
              self.cached_bundle_processors[descriptor_id])

    self.periodic_shutdown = PeriodicThread(
        DEFAULT_BUNDLE_PROCESSOR_CACHE_SHUTDOWN_THRESHOLD_S,
        shutdown_inactive_bundle_processors)
    self.periodic_shutdown.daemon = True
    self.periodic_shutdown.start()

  @staticmethod
  def _shutdown_cached_bundle_processors(cached_bundle_processors):
    try:
      while True:
        # pop() is threadsafe
        bundle_processor = cached_bundle_processors.pop()
        bundle_processor.shutdown()
    except IndexError:
      pass


class SdkWorker(object):

  def __init__(self,
               bundle_processor_cache,  # type: BundleProcessorCache
               state_cache_metrics_fn=list,
               profiler_factory=None,  # type: Optional[Callable[..., Profile]]
               log_lull_timeout_ns=None,
              ):
    self.bundle_processor_cache = bundle_processor_cache
    self.state_cache_metrics_fn = state_cache_metrics_fn
    self.profiler_factory = profiler_factory
    self.log_lull_timeout_ns = (
        log_lull_timeout_ns or DEFAULT_LOG_LULL_TIMEOUT_NS)

  def do_instruction(self, request):
    # type: (beam_fn_api_pb2.InstructionRequest) -> beam_fn_api_pb2.InstructionResponse
    request_type = request.WhichOneof('request')
    if request_type:
      # E.g. if register is set, this will call self.register(request.register))
      return getattr(self, request_type)(
          getattr(request, request_type), request.instruction_id)
    else:
      raise NotImplementedError

  def register(self,
               request,  # type: beam_fn_api_pb2.RegisterRequest
               instruction_id  # type: str
              ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse

    """Registers a set of ``beam_fn_api_pb2.ProcessBundleDescriptor``s.

    This set of ``beam_fn_api_pb2.ProcessBundleDescriptor`` come as part of a
    ``beam_fn_api_pb2.RegisterRequest``, which the runner sends to the SDK
    worker before starting processing to register stages.
    """

    for process_bundle_descriptor in request.process_bundle_descriptor:
      self.bundle_processor_cache.register(process_bundle_descriptor)
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        register=beam_fn_api_pb2.RegisterResponse())

  def process_bundle(self,
                     request,  # type: beam_fn_api_pb2.ProcessBundleRequest
                     instruction_id  # type: str
                    ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse
    bundle_processor = self.bundle_processor_cache.get(
        instruction_id, request.process_bundle_descriptor_id)
    try:
      with bundle_processor.state_handler.process_instruction_id(
          instruction_id, request.cache_tokens):
        with self.maybe_profile(instruction_id):
          delayed_applications, requests_finalization = (
              bundle_processor.process_bundle(instruction_id))
          monitoring_infos = bundle_processor.monitoring_infos()
          monitoring_infos.extend(self.state_cache_metrics_fn())
          response = beam_fn_api_pb2.InstructionResponse(
              instruction_id=instruction_id,
              process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                  residual_roots=delayed_applications,
                  metrics=bundle_processor.metrics(),
                  monitoring_infos=monitoring_infos,
                  requires_finalization=requests_finalization))
      # Don't release here if finalize is needed.
      if not requests_finalization:
        self.bundle_processor_cache.release(instruction_id)
      return response
    except:  # pylint: disable=broad-except
      # Don't re-use bundle processors on failure.
      self.bundle_processor_cache.discard(instruction_id)
      raise

  def process_bundle_split(self,
                           request,  # type: beam_fn_api_pb2.ProcessBundleSplitRequest
                           instruction_id  # type: str
                          ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse
    processor = self.bundle_processor_cache.lookup(request.instruction_id)
    if processor:
      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=instruction_id,
          process_bundle_split=processor.try_split(request))
    else:
      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=instruction_id,
          error='Instruction not running: %s' % instruction_id)

  def _log_lull_in_bundle_processor(self, processor):
    state_sampler = processor.state_sampler
    sampler_info = state_sampler.get_info()
    if (sampler_info and sampler_info.time_since_transition and
        sampler_info.time_since_transition > self.log_lull_timeout_ns):
      step_name = sampler_info.state_name.step_name
      state_name = sampler_info.state_name.name
      state_lull_log = (
          'Operation ongoing for over %.2f seconds in state %s' %
          (sampler_info.time_since_transition / 1e9, state_name))
      step_name_log = (' in step %s ' % step_name) if step_name else ''

      exec_thread = getattr(sampler_info, 'tracked_thread', None)
      if exec_thread is not None:
        thread_frame = sys._current_frames().get(exec_thread.ident)  # pylint: disable=protected-access
        stack_trace = '\n'.join(
            traceback.format_stack(thread_frame)) if thread_frame else ''
      else:
        stack_trace = '-NOT AVAILABLE-'

      _LOGGER.warning(
          '%s%s without returning. Current Traceback:\n%s',
          state_lull_log,
          step_name_log,
          stack_trace)

  def process_bundle_progress(self,
                              request,  # type: beam_fn_api_pb2.ProcessBundleProgressRequest
                              instruction_id  # type: str
                             ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse
    # It is an error to get progress for a not-in-flight bundle.
    processor = self.bundle_processor_cache.lookup(request.instruction_id)
    if processor:
      self._log_lull_in_bundle_processor(processor)
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        process_bundle_progress=beam_fn_api_pb2.ProcessBundleProgressResponse(
            metrics=processor.metrics() if processor else None,
            monitoring_infos=processor.monitoring_infos() if processor else []))

  def finalize_bundle(self,
                      request,  # type: beam_fn_api_pb2.FinalizeBundleRequest
                      instruction_id  # type: str
                     ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse
    processor = self.bundle_processor_cache.lookup(request.instruction_id)
    if processor:
      try:
        finalize_response = processor.finalize_bundle()
        self.bundle_processor_cache.release(request.instruction_id)
        return beam_fn_api_pb2.InstructionResponse(
            instruction_id=instruction_id, finalize_bundle=finalize_response)
      except:
        self.bundle_processor_cache.discard(request.instruction_id)
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


class StateHandler(with_metaclass(abc.ABCMeta, object)):  # type: ignore[misc]
  """An abstract object representing a ``StateHandler``."""
  @abc.abstractmethod
  def get_raw(self,
              state_key,  # type: beam_fn_api_pb2.StateKey
              continuation_token=None  # type: Optional[bytes]
             ):
    # type: (...) -> Tuple[bytes, Optional[bytes]]
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def append_raw(
      self,
      state_key,  # type: beam_fn_api_pb2.StateKey
      data  # type: bytes
  ):
    # type: (...) -> _Future
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def clear(self, state_key):
    # type: (beam_fn_api_pb2.StateKey) -> _Future
    raise NotImplementedError(type(self))


class StateHandlerFactory(with_metaclass(abc.ABCMeta,
                                         object)):  # type: ignore[misc]
  """An abstract factory for creating ``DataChannel``."""
  @abc.abstractmethod
  def create_state_handler(self, api_service_descriptor):
    # type: (endpoints_pb2.ApiServiceDescriptor) -> CachingStateHandler

    """Returns a ``StateHandler`` from the given ApiServiceDescriptor."""
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def close(self):
    # type: () -> None

    """Close all channels that this factory owns."""
    raise NotImplementedError(type(self))


class GrpcStateHandlerFactory(StateHandlerFactory):
  """A factory for ``GrpcStateHandler``.

  Caches the created channels by ``state descriptor url``.
  """
  def __init__(self, state_cache, credentials=None):
    self._state_handler_cache = {}  # type: Dict[str, CachingStateHandler]
    self._lock = threading.Lock()
    self._throwing_state_handler = ThrowingStateHandler()
    self._credentials = credentials
    self._state_cache = state_cache

  def create_state_handler(self, api_service_descriptor):
    # type: (endpoints_pb2.ApiServiceDescriptor) -> CachingStateHandler
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
            _LOGGER.info('Creating insecure state channel for %s.', url)
            grpc_channel = GRPCChannelFactory.insecure_channel(
                url, options=options)
          else:
            _LOGGER.info('Creating secure state channel for %s.', url)
            grpc_channel = GRPCChannelFactory.secure_channel(
                url, self._credentials, options=options)
          _LOGGER.info('State channel established.')
          # Add workerId to the grpc channel
          grpc_channel = grpc.intercept_channel(
              grpc_channel, WorkerIdInterceptor())
          self._state_handler_cache[url] = CachingStateHandler(
              self._state_cache,
              GrpcStateHandler(
                  beam_fn_api_pb2_grpc.BeamFnStateStub(grpc_channel)))
    return self._state_handler_cache[url]

  def close(self):
    # type: () -> None
    _LOGGER.info('Closing all cached gRPC state handlers.')
    for _, state_handler in self._state_handler_cache.items():
      state_handler.done()
    self._state_handler_cache.clear()
    self._state_cache.evict_all()


class ThrowingStateHandler(StateHandler):
  """A state handler that errors on any requests."""
  def get_raw(self, state_key, coder):
    raise RuntimeError(
        'Unable to handle state requests for ProcessBundleDescriptor without '
        'state ApiServiceDescriptor for state key %s.' % state_key)

  def append_raw(self, state_key, coder, elements):
    raise RuntimeError(
        'Unable to handle state requests for ProcessBundleDescriptor without '
        'state ApiServiceDescriptor for state key %s.' % state_key)

  def clear(self, state_key):
    raise RuntimeError(
        'Unable to handle state requests for ProcessBundleDescriptor without '
        'state ApiServiceDescriptor for state key %s.' % state_key)


class GrpcStateHandler(StateHandler):

  _DONE = object()

  def __init__(self, state_stub):
    # type: (beam_fn_api_pb2_grpc.BeamFnStateStub) -> None
    self._lock = threading.Lock()
    self._state_stub = state_stub
    self._requests = queue.Queue(
    )  # type: queue.Queue[beam_fn_api_pb2.StateRequest]
    self._responses_by_id = {}  # type: Dict[str, _Future]
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
          # Popping an item from a dictionary is atomic in cPython
          future = self._responses_by_id.pop(response.id)
          future.set(response)
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

  def get_raw(self,
              state_key,  # type: beam_fn_api_pb2.StateKey
              continuation_token=None  # type: Optional[bytes]
             ):
    # type: (...) -> Tuple[bytes, Optional[bytes]]
    response = self._blocking_request(
        beam_fn_api_pb2.StateRequest(
            state_key=state_key,
            get=beam_fn_api_pb2.StateGetRequest(
                continuation_token=continuation_token)))
    return response.get.data, response.get.continuation_token

  def append_raw(self,
                 state_key,  # type: Optional[beam_fn_api_pb2.StateKey]
                 data  # type: bytes
                ):
    # type: (...) -> _Future
    return self._request(
        beam_fn_api_pb2.StateRequest(
            state_key=state_key,
            append=beam_fn_api_pb2.StateAppendRequest(data=data)))

  def clear(self, state_key):
    # type: (Optional[beam_fn_api_pb2.StateKey]) -> _Future
    return self._request(
        beam_fn_api_pb2.StateRequest(
            state_key=state_key, clear=beam_fn_api_pb2.StateClearRequest()))

  def _request(self, request):
    # type: (beam_fn_api_pb2.StateRequest) -> _Future
    request.id = self._next_id()
    request.instruction_id = self._context.process_instruction_id
    # Adding a new item to a dictionary is atomic in cPython
    self._responses_by_id[request.id] = future = _Future()
    # Request queue is thread-safe
    self._requests.put(request)
    return future

  def _blocking_request(self, request):
    # type: (beam_fn_api_pb2.StateRequest) -> beam_fn_api_pb2.StateResponse
    req_future = self._request(request)
    while not req_future.wait(timeout=1):
      if self._exc_info:
        t, v, tb = self._exc_info
        raise_(t, v, tb)
      elif self._done:
        raise RuntimeError()
    response = req_future.get()
    if response.error:
      raise RuntimeError(response.error)
    else:
      return response

  def _next_id(self):
    # type: () -> str
    with self._lock:
      # Use a lock here because this GrpcStateHandler is shared across all
      # requests which have the same process bundle descriptor. State requests
      # can concurrently access this section if a Runner uses threads / workers
      # (aka "parallelism") to send data to this SdkHarness and its workers.
      self._last_id += 1
      request_id = self._last_id
    return str(request_id)


class CachingStateHandler(object):
  """ A State handler which retrieves and caches state. """

  def __init__(self,
               global_state_cache,  # type: StateCache
               underlying_state  # type: StateHandler
              ):
    self._underlying = underlying_state
    self._state_cache = global_state_cache
    self._context = threading.local()

  @contextlib.contextmanager
  def process_instruction_id(self, bundle_id, cache_tokens):
    if getattr(self._context, 'user_state_cache_token', None) is not None:
      raise RuntimeError(
          'Cache tokens already set to %s' %
          self._context.user_state_cache_token)
    self._context.side_input_cache_tokens = {}
    user_state_cache_token = None
    for cache_token_struct in cache_tokens:
      if cache_token_struct.HasField("user_state"):
        # There should only be one user state token present
        assert not user_state_cache_token
        user_state_cache_token = cache_token_struct.token
      elif cache_token_struct.HasField("side_input"):
        self._context.side_input_cache_tokens[
            cache_token_struct.side_input.transform_id,
            cache_token_struct.side_input.
            side_input_id] = cache_token_struct.token
    # TODO: Consider a two-level cache to avoid extra logic and locking
    # for items cached at the bundle level.
    self._context.bundle_cache_token = bundle_id
    try:
      self._state_cache.initialize_metrics()
      self._context.user_state_cache_token = user_state_cache_token
      with self._underlying.process_instruction_id(bundle_id):
        yield
    finally:
      self._context.side_input_cache_tokens = {}
      self._context.user_state_cache_token = None
      self._context.bundle_cache_token = None

  def blocking_get(self,
                   state_key,  # type: beam_fn_api_pb2.StateKey
                   coder,  # type: coder_impl.CoderImpl
                   is_cached=False
                  ):
    # type: (...) -> Iterator[Any]
    cache_token = self._get_cache_token(state_key, is_cached)
    if not cache_token:
      # Cache disabled / no cache token. Can't do a lookup/store in the cache.
      # Fall back to lazily materializing the state, one element at a time.
      return self._lazy_iterator(state_key, coder)
    # Cache lookup
    cache_state_key = self._convert_to_cache_key(state_key)
    cached_value = self._state_cache.get(cache_state_key, cache_token)
    if cached_value is None:
      # Cache miss, need to retrieve from the Runner
      # Further size estimation or the use of the continuation token on the
      # runner side could fall back to materializing one item at a time.
      # https://jira.apache.org/jira/browse/BEAM-8297
      materialized = cached_value = (
          self._partially_cached_iterable(state_key, coder))
      self._state_cache.put(cache_state_key, cache_token, materialized)
    return iter(cached_value)

  def extend(self,
             state_key,  # type: beam_fn_api_pb2.StateKey
             coder,  # type: coder_impl.CoderImpl
             elements,  # type: Iterable[Any]
             is_cached=False
            ):
    # type: (...) -> _Future
    cache_token = self._get_cache_token(state_key, is_cached)
    if cache_token:
      # Update the cache
      cache_key = self._convert_to_cache_key(state_key)
      self._state_cache.extend(cache_key, cache_token, elements)
    # Write to state handler
    out = coder_impl.create_OutputStream()
    for element in elements:
      coder.encode_to_stream(element, out, True)
    return self._underlying.append_raw(state_key, out.get())

  def clear(self, state_key, is_cached=False):
    # type: (beam_fn_api_pb2.StateKey, bool) -> _Future
    cache_token = self._get_cache_token(state_key, is_cached)
    if cache_token:
      cache_key = self._convert_to_cache_key(state_key)
      self._state_cache.clear(cache_key, cache_token)
    return self._underlying.clear(state_key)

  def done(self):
    # type: () -> None
    self._underlying.done()

  def _lazy_iterator(
      self,
      state_key,  # type: beam_fn_api_pb2.StateKey
      coder,  # type: coder_impl.CoderImpl
      continuation_token=None  # type: Optional[bytes]
    ):
    # type: (...) -> Iterator[Any]

    """Materializes the state lazily, one element at a time.
       :return A generator which returns the next element if advanced.
    """
    while True:
      data, continuation_token = (
          self._underlying.get_raw(state_key, continuation_token))
      input_stream = coder_impl.create_InputStream(data)
      while input_stream.size() > 0:
        yield coder.decode_from_stream(input_stream, True)
      if not continuation_token:
        break

  def _get_cache_token(self, state_key, request_is_cached):
    if not self._state_cache.is_cache_enabled():
      return None
    elif state_key.HasField('bag_user_state'):
      if request_is_cached and self._context.user_state_cache_token:
        return self._context.user_state_cache_token
      else:
        return self._context.bundle_cache_token
    elif state_key.WhichOneof('type').endswith('_side_input'):
      side_input = getattr(state_key, state_key.WhichOneof('type'))
      return self._context.side_input_cache_tokens.get(
          (side_input.transform_id, side_input.side_input_id),
          self._context.bundle_cache_token)

  def _partially_cached_iterable(
      self,
      state_key,  # type: beam_fn_api_pb2.StateKey
      coder  # type: coder_impl.CoderImpl
    ):
    # type: (...) -> Iterable[Any]

    """Materialized the first page of data, concatenated with a lazy iterable
    of the rest, if any.
    """
    data, continuation_token = self._underlying.get_raw(state_key, None)
    head = []
    input_stream = coder_impl.create_InputStream(data)
    while input_stream.size() > 0:
      head.append(coder.decode_from_stream(input_stream, True))

    if continuation_token is None:
      return head
    else:

      def iter_func():
        for item in head:
          yield item
        if continuation_token:
          for item in self._lazy_iterator(state_key, coder, continuation_token):
            yield item

      return _IterableFromIterator(iter_func)

  @staticmethod
  def _convert_to_cache_key(state_key):
    return state_key.SerializeToString()


class _IterableFromIterator(object):
  """Wraps an iterator as an iterable."""
  def __init__(self, iter_func):
    self._iter_func = iter_func

  def __iter__(self):
    return self._iter_func()


coder_impl.FastPrimitivesCoderImpl.register_iterable_like_type(
    _IterableFromIterator)


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

  @classmethod
  def done(cls):
    # type: () -> _Future
    if not hasattr(cls, 'DONE'):
      done_future = _Future()
      done_future.set(None)
      cls.DONE = done_future  # type: ignore[attr-defined]
    return cls.DONE  # type: ignore[attr-defined]
