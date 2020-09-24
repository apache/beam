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
import functools
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
from typing import FrozenSet
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import grpc
from future.utils import raise_
from future.utils import with_metaclass

from apache_beam.coders import coder_impl
from apache_beam.metrics import monitoring_infos
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import metrics_pb2
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import statesampler
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.data_plane import PeriodicThread
from apache_beam.runners.worker.statecache import StateCache
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor
from apache_beam.runners.worker.worker_status import FnApiWorkerStatusHandler
from apache_beam.runners.worker.worker_status import thread_dump
from apache_beam.utils import thread_pool_executor

if TYPE_CHECKING:
  from apache_beam.portability.api import endpoints_pb2
  from apache_beam.utils.profiler import Profile

_LOGGER = logging.getLogger(__name__)

# This SDK harness will (by default), log a "lull" in processing if it sees no
# transitions in over 5 minutes.
# 5 minutes * 60 seconds * 1000 millis * 1000 micros * 1000 nanoseconds
DEFAULT_LOG_LULL_TIMEOUT_NS = 5 * 60 * 1000 * 1000 * 1000

DEFAULT_BUNDLE_PROCESSOR_CACHE_SHUTDOWN_THRESHOLD_S = 60

# Full thread dump is performed at most every 20 minutes.
LOG_LULL_FULL_THREAD_DUMP_INTERVAL_S = 20 * 60

# Full thread dump is performed if the lull is more than 20 minutes.
LOG_LULL_FULL_THREAD_DUMP_LULL_S = 20 * 60

# The number of ProcessBundleRequest instruction ids the BundleProcessorCache
# will remember for not running instructions.
MAX_KNOWN_NOT_RUNNING_INSTRUCTIONS = 1000
# The number of ProcessBundleRequest instruction ids that BundleProcessorCache
# will remember for failed instructions.
MAX_FAILED_INSTRUCTIONS = 10000


class ShortIdCache(object):
  """ Cache for MonitoringInfo "short ids"
  """
  def __init__(self):
    self._lock = threading.Lock()
    self._lastShortId = 0
    self._infoKeyToShortId = {}  # type: Dict[FrozenSet, str]
    self._shortIdToInfo = {}  # type: Dict[str, metrics_pb2.MonitoringInfo]

  def getShortId(self, monitoring_info):
    # type: (metrics_pb2.MonitoringInfo) -> str

    """ Returns the assigned shortId for a given MonitoringInfo, assigns one if
    not assigned already.
    """
    key = monitoring_infos.to_key(monitoring_info)
    with self._lock:
      try:
        return self._infoKeyToShortId[key]
      except KeyError:
        self._lastShortId += 1

        # Convert to a hex string (and drop the '0x') for some compression
        shortId = hex(self._lastShortId)[2:]

        payload_cleared = metrics_pb2.MonitoringInfo()
        payload_cleared.CopyFrom(monitoring_info)
        payload_cleared.ClearField('payload')

        self._infoKeyToShortId[key] = shortId
        self._shortIdToInfo[shortId] = payload_cleared
        return shortId

  def getInfos(self, short_ids):
    #type: (Iterable[str]) -> List[metrics_pb2.MonitoringInfo]

    """ Gets the base MonitoringInfo (with payload cleared) for each short ID.

    Throws KeyError if an unassigned short ID is encountered.
    """
    return [self._shortIdToInfo[short_id] for short_id in short_ids]


SHORT_ID_CACHE = ShortIdCache()


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
               status_address=None,  # type: Optional[str]
               # Heap dump through status api is disabled by default
               enable_heap_dump=False, # type: bool
               ):
    self._alive = True
    self._worker_index = 0
    self._worker_id = worker_id
    self._state_cache = StateCache(state_cache_size)
    options = [('grpc.max_receive_message_length', -1),
               ('grpc.max_send_message_length', -1)]
    if credentials is None:
      _LOGGER.info('Creating insecure control channel for %s.', control_address)
      self._control_channel = GRPCChannelFactory.insecure_channel(
          control_address, options=options)
    else:
      _LOGGER.info('Creating secure control channel for %s.', control_address)
      self._control_channel = GRPCChannelFactory.secure_channel(
          control_address, credentials, options=options)
    grpc.channel_ready_future(self._control_channel).result(timeout=60)
    _LOGGER.info('Control channel established.')

    self._control_channel = grpc.intercept_channel(
        self._control_channel, WorkerIdInterceptor(self._worker_id))
    self._data_channel_factory = data_plane.GrpcClientDataChannelFactory(
        credentials, self._worker_id, data_buffer_time_limit_ms)
    self._state_handler_factory = GrpcStateHandlerFactory(
        self._state_cache, credentials)
    self._profiler_factory = profiler_factory
    self._fns = KeyedDefaultDict(
        lambda id: self._control_stub.GetProcessBundleDescriptor(
            beam_fn_api_pb2.GetProcessBundleDescriptorRequest(
                process_bundle_descriptor_id=id))
    )  # type: Mapping[str, beam_fn_api_pb2.ProcessBundleDescriptor]
    # BundleProcessor cache across all workers.
    self._bundle_processor_cache = BundleProcessorCache(
        state_handler_factory=self._state_handler_factory,
        data_channel_factory=self._data_channel_factory,
        fns=self._fns)

    if status_address:
      try:
        self._status_handler = FnApiWorkerStatusHandler(
            status_address, self._bundle_processor_cache,
            enable_heap_dump)  # type: Optional[FnApiWorkerStatusHandler]
      except Exception:
        traceback_string = traceback.format_exc()
        _LOGGER.warning(
            'Error creating worker status request handler, '
            'skipping status report. Trace back: %s' % traceback_string)
    else:
      self._status_handler = None

    # TODO(BEAM-8998) use common
    # thread_pool_executor.shared_unbounded_instance() to process bundle
    # progress once dataflow runner's excessive progress polling is removed.
    self._report_progress_executor = futures.ThreadPoolExecutor(max_workers=1)
    self._worker_thread_pool = thread_pool_executor.shared_unbounded_instance()
    self._responses = queue.Queue(
    )  # type: queue.Queue[beam_fn_api_pb2.InstructionResponse]
    _LOGGER.info('Initializing SDKHarness with unbounded number of workers.')

  def run(self):
    self._control_stub = beam_fn_api_pb2_grpc.BeamFnControlStub(
        self._control_channel)
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
      for work_request in self._control_stub.Control(get_responses()):
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
    self._bundle_processor_cache.activate(request.instruction_id)
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
      self._execute(
          lambda: self.create_worker().do_instruction(request), request)

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
               fns  # type: Mapping[str, beam_fn_api_pb2.ProcessBundleDescriptor]
              ):
    self.fns = fns
    self.state_handler_factory = state_handler_factory
    self.data_channel_factory = data_channel_factory
    self.known_not_running_instruction_ids = collections.OrderedDict()
    self.failed_instruction_ids = collections.OrderedDict()
    self.active_bundle_processors = {
    }  # type: Dict[str, Tuple[str, bundle_processor.BundleProcessor]]
    self.cached_bundle_processors = collections.defaultdict(
        list)  # type: DefaultDict[str, List[bundle_processor.BundleProcessor]]
    self.last_access_times = collections.defaultdict(
        float)  # type: DefaultDict[str, float]
    self._schedule_periodic_shutdown()
    self._lock = threading.Lock()

  def register(self, bundle_descriptor):
    # type: (beam_fn_api_pb2.ProcessBundleDescriptor) -> None

    """Register a ``beam_fn_api_pb2.ProcessBundleDescriptor`` by its id."""
    self.fns[bundle_descriptor.id] = bundle_descriptor

  def activate(self, instruction_id):
    # type: str -> None

    """Makes the ``instruction_id`` known to the bundle processor.

    Allows ``lookup`` to return ``None``. Necessary if ``lookup`` can occur
    before ``get``.
    """
    with self._lock:
      self.known_not_running_instruction_ids[instruction_id] = True

  def get(self, instruction_id, bundle_descriptor_id):
    # type: (str, str) -> bundle_processor.BundleProcessor

    """
    Return the requested ``BundleProcessor``, creating it if necessary.

    Moves the ``BundleProcessor`` from the inactive to the active cache.
    """
    with self._lock:
      try:
        # pop() is threadsafe
        processor = self.cached_bundle_processors[bundle_descriptor_id].pop()
        self.active_bundle_processors[
          instruction_id] = bundle_descriptor_id, processor
        try:
          del self.known_not_running_instruction_ids[instruction_id]
        except KeyError:
          # The instruction may have not been pre-registered before execution
          # since activate() may have never been invoked
          pass
        return processor
      except IndexError:
        pass

    # Make sure we instantiate the processor while not holding the lock.
    processor = bundle_processor.BundleProcessor(
        self.fns[bundle_descriptor_id],
        self.state_handler_factory.create_state_handler(
            self.fns[bundle_descriptor_id].state_api_service_descriptor),
        self.data_channel_factory)
    with self._lock:
      self.active_bundle_processors[
        instruction_id] = bundle_descriptor_id, processor
      try:
        del self.known_not_running_instruction_ids[instruction_id]
      except KeyError:
        # The instruction may have not been pre-registered before execution
        # since activate() may have never been invoked
        pass
    return processor

  def lookup(self, instruction_id):
    # type: (str) -> Optional[bundle_processor.BundleProcessor]

    """
    Return the requested ``BundleProcessor`` from the cache.

    Will return ``None`` if the BundleProcessor is known but not yet ready. Will
    raise an error if the ``instruction_id`` is not known or has been discarded.
    """
    with self._lock:
      if instruction_id in self.failed_instruction_ids:
        raise RuntimeError(
            'Bundle processing associated with %s has failed. '
            'Check prior failing response for details.' % instruction_id)
      processor = self.active_bundle_processors.get(
          instruction_id, (None, None))[-1]
      if processor:
        return processor
      if instruction_id in self.known_not_running_instruction_ids:
        return None
      raise RuntimeError('Unknown process bundle id %s.' % instruction_id)

  def discard(self, instruction_id):
    # type: (str) -> None

    """
    Marks the instruction id as failed shutting down the ``BundleProcessor``.
    """
    with self._lock:
      self.failed_instruction_ids[instruction_id] = True
      while len(self.failed_instruction_ids) > MAX_FAILED_INSTRUCTIONS:
        self.failed_instruction_ids.popitem()
      processor = self.active_bundle_processors[instruction_id][1]
      del self.active_bundle_processors[instruction_id]

    # Perform the shutdown while not holding the lock.
    processor.shutdown()

  def release(self, instruction_id):
    # type: (str) -> None

    """
    Release the requested ``BundleProcessor``.

    Resets the ``BundleProcessor`` and moves it from the active to the
    inactive cache.
    """
    with self._lock:
      self.known_not_running_instruction_ids[instruction_id] = True
      while len(self.known_not_running_instruction_ids
                ) > MAX_KNOWN_NOT_RUNNING_INSTRUCTIONS:
        self.known_not_running_instruction_ids.popitem()
      descriptor_id, processor = (
          self.active_bundle_processors.pop(instruction_id))

    # Make sure that we reset the processor while not holding the lock.
    processor.reset()
    with self._lock:
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

    for instruction_id in list(self.active_bundle_processors.keys()):
      self.discard(instruction_id)
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
    self._last_full_thread_dump_secs = 0

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
                  monitoring_infos=monitoring_infos,
                  monitoring_data={
                      SHORT_ID_CACHE.getShortId(info): info.payload
                      for info in monitoring_infos
                  },
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
    try:
      processor = self.bundle_processor_cache.lookup(request.instruction_id)
    except RuntimeError:
      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=instruction_id, error=traceback.format_exc())
    # Return an empty response if we aren't running. This can happen
    # if the ProcessBundleRequest has not started or already finished.
    process_bundle_split = (
        processor.try_split(request)
        if processor else beam_fn_api_pb2.ProcessBundleSplitResponse())
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        process_bundle_split=process_bundle_split)

  def _log_lull_in_bundle_processor(self, processor):
    sampler_info = processor.state_sampler.get_info()
    self._log_lull_sampler_info(sampler_info)

  def _log_lull_sampler_info(self, sampler_info):
    if (sampler_info and sampler_info.time_since_transition and
        sampler_info.time_since_transition > self.log_lull_timeout_ns):
      step_name = sampler_info.state_name.step_name
      state_name = sampler_info.state_name.name
      lull_seconds = sampler_info.time_since_transition / 1e9
      state_lull_log = (
          'Operation ongoing for over %.2f seconds in state %s' %
          (lull_seconds, state_name))
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

      if self._should_log_full_thread_dump(lull_seconds):
        self._log_full_thread_dump()

  def _should_log_full_thread_dump(self, lull_seconds):
    if lull_seconds < LOG_LULL_FULL_THREAD_DUMP_LULL_S:
      return False
    now = time.time()
    if (self._last_full_thread_dump_secs + LOG_LULL_FULL_THREAD_DUMP_INTERVAL_S
        < now):
      self._last_full_thread_dump_secs = now
      return True
    return False

  def _log_full_thread_dump(self):
    thread_dump()

  def process_bundle_progress(self,
                              request,  # type: beam_fn_api_pb2.ProcessBundleProgressRequest
                              instruction_id  # type: str
                             ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse
    try:
      processor = self.bundle_processor_cache.lookup(request.instruction_id)
    except RuntimeError:
      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=instruction_id, error=traceback.format_exc())
    if processor:
      self._log_lull_in_bundle_processor(processor)
      monitoring_infos = processor.monitoring_infos()
    else:
      # Return an empty response if we aren't running. This can happen
      # if the ProcessBundleRequest has not started or already finished.
      monitoring_infos = []
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        process_bundle_progress=beam_fn_api_pb2.ProcessBundleProgressResponse(
            monitoring_infos=monitoring_infos,
            monitoring_data={
                SHORT_ID_CACHE.getShortId(info): info.payload
                for info in monitoring_infos
            }))

  def process_bundle_progress_metadata_request(self,
                                               request,  # type: beam_fn_api_pb2.ProcessBundleProgressMetadataRequest
                                               instruction_id  # type: str
                                              ):
    return beam_fn_api_pb2.InstructionResponse(
        instruction_id=instruction_id,
        process_bundle_progress=beam_fn_api_pb2.
        ProcessBundleProgressMetadataResponse(
            monitoring_info=SHORT_ID_CACHE.getInfos(
                request.monitoring_info_id)))

  def finalize_bundle(self,
                      request,  # type: beam_fn_api_pb2.FinalizeBundleRequest
                      instruction_id  # type: str
                     ):
    # type: (...) -> beam_fn_api_pb2.InstructionResponse
    try:
      processor = self.bundle_processor_cache.lookup(request.instruction_id)
    except RuntimeError:
      return beam_fn_api_pb2.InstructionResponse(
          instruction_id=instruction_id, error=traceback.format_exc())
    if processor:
      try:
        finalize_response = processor.finalize_bundle()
        self.bundle_processor_cache.release(request.instruction_id)
        return beam_fn_api_pb2.InstructionResponse(
            instruction_id=instruction_id, finalize_bundle=finalize_response)
      except:
        self.bundle_processor_cache.discard(request.instruction_id)
        raise
    # We can reach this state if there was an erroneous request to finalize
    # the bundle while it is being initialized or has already been finalized
    # and released.
    raise RuntimeError(
        'Bundle is not in a finalizable state for %s' % instruction_id)

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
  """ A State handler which retrieves and caches state.
   If caching is activated, caches across bundles using a supplied cache token.
   If activated but no cache token is supplied, caching is done at the bundle
   level.
  """

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
                  ):
    # type: (...) -> Iterable[Any]
    cache_token = self._get_cache_token(state_key)
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
      if isinstance(materialized, (list, self.ContinuationIterable)):
        self._state_cache.put(cache_state_key, cache_token, materialized)
      else:
        _LOGGER.error(
            "Uncacheable type %s for key %s. Not caching.",
            materialized,
            state_key)
    return cached_value

  def extend(self,
             state_key,  # type: beam_fn_api_pb2.StateKey
             coder,  # type: coder_impl.CoderImpl
             elements,  # type: Iterable[Any]
            ):
    # type: (...) -> _Future
    cache_token = self._get_cache_token(state_key)
    if cache_token:
      # Update the cache
      cache_key = self._convert_to_cache_key(state_key)
      cached_value = self._state_cache.get(cache_key, cache_token)
      # Keep in mind that the state for this key can be evicted
      # while executing this function. Either read or write to the cache
      # but never do both here!
      if cached_value is None:
        # We have never cached this key before, first retrieve state
        cached_value = self.blocking_get(state_key, coder)
      # Just extend the already cached value
      if isinstance(cached_value, list):
        # Materialize provided iterable to ensure reproducible iterations,
        # here and when writing to the state handler below.
        elements = list(elements)
        # The state is fully cached and can be extended
        cached_value.extend(elements)
      elif isinstance(cached_value, self.ContinuationIterable):
        # The state is too large to be fully cached (continuation token used),
        # only the first part is cached, the rest if enumerated via the runner.
        pass
      else:
        # When a corrupt value made it into the cache, we have to fail.
        raise Exception("Unexpected cached value: %s" % cached_value)
    # Write to state handler
    out = coder_impl.create_OutputStream()
    for element in elements:
      coder.encode_to_stream(element, out, True)
    return self._underlying.append_raw(state_key, out.get())

  def clear(self, state_key):
    # type: (beam_fn_api_pb2.StateKey) -> _Future
    cache_token = self._get_cache_token(state_key)
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

  def _get_cache_token(self, state_key):
    if not self._state_cache.is_cache_enabled():
      return None
    elif state_key.HasField('bag_user_state'):
      if self._context.user_state_cache_token:
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

    if not continuation_token:
      return head
    else:
      return self.ContinuationIterable(
          head,
          functools.partial(
              self._lazy_iterator, state_key, coder, continuation_token))

  class ContinuationIterable(object):
    def __init__(self, head, continue_iterator_fn):
      self.head = head
      self.continue_iterator_fn = continue_iterator_fn

    def __iter__(self):
      for item in self.head:
        yield item
      for item in self.continue_iterator_fn():
        yield item

  @staticmethod
  def _convert_to_cache_key(state_key):
    return state_key.SerializeToString()


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


class KeyedDefaultDict(collections.defaultdict):
  def __missing__(self, key):
    self[key] = self.default_factory(key)
    return self[key]
