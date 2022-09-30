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

"""Code for communicating with the Workers."""

# mypy: disallow-untyped-defs

import collections
import contextlib
import copy
import logging
import os
import queue
import subprocess
import sys
import threading
import time
from typing import TYPE_CHECKING
from typing import Any
from typing import BinaryIO  # pylint: disable=unused-import
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
from typing import cast
from typing import overload

import grpc

from apache_beam.io import filesystems
from apache_beam.io.filesystems import CompressionTypes
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.portability.api import beam_provision_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability.fn_api_runner.execution import Buffer
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import sdk_worker
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.runners.worker.log_handler import LOGENTRY_TO_LOG_LEVEL_MAP
from apache_beam.runners.worker.sdk_worker import _Future
from apache_beam.runners.worker.statecache import StateCache
from apache_beam.utils import proto_utils
from apache_beam.utils import thread_pool_executor
from apache_beam.utils.interactive_utils import is_in_notebook
from apache_beam.utils.sentinel import Sentinel

if TYPE_CHECKING:
  from grpc import ServicerContext
  from google.protobuf import message
  from apache_beam.runners.portability.fn_api_runner.fn_runner import ExtendedProvisionInfo  # pylint: disable=ungrouped-imports

# State caching is enabled in the fn_api_runner for testing, except for one
# test which runs without state caching (FnApiRunnerTestWithDisabledCaching).
# The cache is disabled in production for other runners.
STATE_CACHE_SIZE_MB = 100
MB_TO_BYTES = 1 << 20

# Time-based flush is enabled in the fn_api_runner by default.
DATA_BUFFER_TIME_LIMIT_MS = 1000

_LOGGER = logging.getLogger(__name__)

T = TypeVar('T')
ConstructorFn = Callable[[
    Union['message.Message', bytes],
    'sdk_worker.StateHandler',
    'ExtendedProvisionInfo',
    'GrpcServer'
],
                         'WorkerHandler']


class ControlConnection(object):

  _uid_counter = 0
  _lock = threading.Lock()

  def __init__(self):
    # type: () -> None
    self._push_queue = queue.Queue(
    )  # type: queue.Queue[Union[beam_fn_api_pb2.InstructionRequest, Sentinel]]
    self._input = None  # type: Optional[Iterable[beam_fn_api_pb2.InstructionResponse]]
    self._futures_by_id = {}  # type: Dict[str, ControlFuture]
    self._read_thread = threading.Thread(
        name='beam_control_read', target=self._read)
    self._state = BeamFnControlServicer.UNSTARTED_STATE

  def _read(self):
    # type: () -> None
    assert self._input is not None
    for data in self._input:
      self._futures_by_id.pop(data.instruction_id).set(data)

  @overload
  def push(self, req):
    # type: (Sentinel) -> None
    pass

  @overload
  def push(self, req):
    # type: (beam_fn_api_pb2.InstructionRequest) -> ControlFuture
    pass

  def push(self, req):
    # type: (Union[Sentinel, beam_fn_api_pb2.InstructionRequest]) -> Optional[ControlFuture]
    if req is BeamFnControlServicer._DONE_MARKER:
      self._push_queue.put(req)
      return None
    if not req.instruction_id:
      with ControlConnection._lock:
        ControlConnection._uid_counter += 1
        req.instruction_id = 'control_%s' % ControlConnection._uid_counter
    future = ControlFuture(req.instruction_id)
    self._futures_by_id[req.instruction_id] = future
    self._push_queue.put(req)
    return future

  def get_req(self):
    # type: () -> Union[Sentinel, beam_fn_api_pb2.InstructionRequest]
    return self._push_queue.get()

  def set_input(self, input):
    # type: (Iterable[beam_fn_api_pb2.InstructionResponse]) -> None
    with ControlConnection._lock:
      if self._input:
        raise RuntimeError('input is already set.')
      self._input = input
      self._read_thread.start()
      self._state = BeamFnControlServicer.STARTED_STATE

  def close(self):
    # type: () -> None
    with ControlConnection._lock:
      if self._state == BeamFnControlServicer.STARTED_STATE:
        self.push(BeamFnControlServicer._DONE_MARKER)
        self._read_thread.join()
      self._state = BeamFnControlServicer.DONE_STATE

  def abort(self, exn):
    # type: (Exception) -> None
    for future in self._futures_by_id.values():
      future.abort(exn)


class BeamFnControlServicer(beam_fn_api_pb2_grpc.BeamFnControlServicer):
  """Implementation of BeamFnControlServicer for clients."""

  UNSTARTED_STATE = 'unstarted'
  STARTED_STATE = 'started'
  DONE_STATE = 'done'

  _DONE_MARKER = Sentinel.sentinel

  def __init__(
      self,
      worker_manager,  # type: WorkerHandlerManager
  ):
    # type: (...) -> None
    self._worker_manager = worker_manager
    self._lock = threading.Lock()
    self._uid_counter = 0
    self._state = self.UNSTARTED_STATE
    # following self._req_* variables are used for debugging purpose, data is
    # added only when self._log_req is True.
    self._req_sent = collections.defaultdict(int)  # type: DefaultDict[str, int]
    self._log_req = logging.getLogger().getEffectiveLevel() <= logging.DEBUG
    self._connections_by_worker_id = collections.defaultdict(
        ControlConnection)  # type: DefaultDict[str, ControlConnection]

  def get_conn_by_worker_id(self, worker_id):
    # type: (str) -> ControlConnection
    with self._lock:
      return self._connections_by_worker_id[worker_id]

  def Control(self,
              iterator,  # type: Iterable[beam_fn_api_pb2.InstructionResponse]
              context  # type: ServicerContext
             ):
    # type: (...) -> Iterator[beam_fn_api_pb2.InstructionRequest]
    with self._lock:
      if self._state == self.DONE_STATE:
        return
      else:
        self._state = self.STARTED_STATE

    worker_id = dict(context.invocation_metadata()).get('worker_id')
    if not worker_id:
      raise RuntimeError(
          'All workers communicate through gRPC should have '
          'worker_id. Received None.')

    control_conn = self.get_conn_by_worker_id(worker_id)
    control_conn.set_input(iterator)

    while True:
      to_push = control_conn.get_req()
      if to_push is self._DONE_MARKER:
        return
      yield to_push
      if self._log_req:
        self._req_sent[to_push.instruction_id] += 1

  def done(self):
    # type: () -> None
    self._state = self.DONE_STATE
    _LOGGER.debug(
        'Runner: Requests sent by runner: %s',
        [(str(req), cnt) for req, cnt in self._req_sent.items()])

  def GetProcessBundleDescriptor(self, id, context=None):
    # type: (beam_fn_api_pb2.GetProcessBundleDescriptorRequest, Any) -> beam_fn_api_pb2.ProcessBundleDescriptor
    return self._worker_manager.get_process_bundle_descriptor(id)


class WorkerHandler(object):
  """worker_handler for a worker.

  It provides utilities to start / stop the worker, provision any resources for
  it, as well as provide descriptors for the data, state and logging APIs for
  it.
  """

  _registered_environments = {}  # type: Dict[str, Tuple[ConstructorFn, type]]
  _worker_id_counter = -1
  _lock = threading.Lock()

  control_conn = None  # type: ControlConnection
  data_conn = None  # type: data_plane._GrpcDataChannel

  def __init__(self,
               control_handler,  # type: Any
               data_plane_handler,  # type: Any
               state,  # type: sdk_worker.StateHandler
               provision_info  # type: ExtendedProvisionInfo
              ):
    # type: (...) -> None

    """Initialize a WorkerHandler.

    Args:
      control_handler:
      data_plane_handler (data_plane.DataChannel):
      state:
      provision_info:
    """
    self.control_handler = control_handler
    self.data_plane_handler = data_plane_handler
    self.state = state
    self.provision_info = provision_info

    with WorkerHandler._lock:
      WorkerHandler._worker_id_counter += 1
      self.worker_id = 'worker_%s' % WorkerHandler._worker_id_counter

  def close(self):
    # type: () -> None
    self.stop_worker()

  def start_worker(self):
    # type: () -> None
    raise NotImplementedError

  def stop_worker(self):
    # type: () -> None
    raise NotImplementedError

  def control_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    raise NotImplementedError

  def artifact_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    raise NotImplementedError

  def data_api_service_descriptor(self):
    # type: () -> Optional[endpoints_pb2.ApiServiceDescriptor]
    raise NotImplementedError

  def state_api_service_descriptor(self):
    # type: () -> Optional[endpoints_pb2.ApiServiceDescriptor]
    raise NotImplementedError

  def logging_api_service_descriptor(self):
    # type: () -> Optional[endpoints_pb2.ApiServiceDescriptor]
    raise NotImplementedError

  @classmethod
  def register_environment(
      cls,
      urn,  # type: str
      payload_type  # type: Optional[Type[T]]
  ):
    # type: (...) -> Callable[[Type[WorkerHandler]], Callable[[T, sdk_worker.StateHandler, ExtendedProvisionInfo, GrpcServer], WorkerHandler]]
    def wrapper(constructor):
      # type: (Callable) -> Callable
      cls._registered_environments[urn] = constructor, payload_type  # type: ignore[assignment]
      return constructor

    return wrapper

  @classmethod
  def create(cls,
             environment,  # type: beam_runner_api_pb2.Environment
             state,  # type: sdk_worker.StateHandler
             provision_info,  # type: ExtendedProvisionInfo
             grpc_server  # type: GrpcServer
            ):
    # type: (...) -> WorkerHandler
    constructor, payload_type = cls._registered_environments[environment.urn]
    return constructor(
        proto_utils.parse_Bytes(environment.payload, payload_type),
        state,
        provision_info,
        grpc_server)


# This takes a WorkerHandlerManager instead of GrpcServer, so it is not
# compatible with WorkerHandler.register_environment.  There is a special case
# in WorkerHandlerManager.get_worker_handlers() that allows it to work.
@WorkerHandler.register_environment(python_urns.EMBEDDED_PYTHON, None)
class EmbeddedWorkerHandler(WorkerHandler):
  """An in-memory worker_handler for fn API control, state and data planes."""

  def __init__(self,
               unused_payload,  # type: None
               state,  # type: sdk_worker.StateHandler
               provision_info,  # type: ExtendedProvisionInfo
               worker_manager,  # type: WorkerHandlerManager
              ):
    # type: (...) -> None
    super().__init__(
        self, data_plane.InMemoryDataChannel(), state, provision_info)
    self.control_conn = self  # type: ignore  # need Protocol to describe this
    self.data_conn = self.data_plane_handler
    state_cache = StateCache(STATE_CACHE_SIZE_MB * MB_TO_BYTES)
    self.bundle_processor_cache = sdk_worker.BundleProcessorCache(
        SingletonStateHandlerFactory(
            sdk_worker.GlobalCachingStateHandler(state_cache, state)),
        data_plane.InMemoryDataChannelFactory(
            self.data_plane_handler.inverse()),
        worker_manager._process_bundle_descriptors)
    self.worker = sdk_worker.SdkWorker(self.bundle_processor_cache)
    self._uid_counter = 0

  def push(self, request):
    # type: (beam_fn_api_pb2.InstructionRequest) -> ControlFuture
    if not request.instruction_id:
      self._uid_counter += 1
      request.instruction_id = 'control_%s' % self._uid_counter
    response = self.worker.do_instruction(request)
    return ControlFuture(request.instruction_id, response)

  def start_worker(self):
    # type: () -> None
    pass

  def stop_worker(self):
    # type: () -> None
    self.bundle_processor_cache.shutdown()

  def done(self):
    # type: () -> None
    pass

  def data_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    # A fake endpoint is needed for properly constructing timer info map in
    # bundle_processor for fnapi_runner.
    return endpoints_pb2.ApiServiceDescriptor(url='fake')

  def state_api_service_descriptor(self):
    # type: () -> None
    return None

  def logging_api_service_descriptor(self):
    # type: () -> None
    return None


class BasicLoggingService(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):
  def Logging(self, log_messages, context=None):
    # type: (Iterable[beam_fn_api_pb2.LogEntry.List], Any) -> Iterator[beam_fn_api_pb2.LogControl]
    yield beam_fn_api_pb2.LogControl()
    for log_message in log_messages:
      for log in log_message.log_entries:
        logging.log(LOGENTRY_TO_LOG_LEVEL_MAP[log.severity], str(log))


class BasicProvisionService(beam_provision_api_pb2_grpc.ProvisionServiceServicer
                            ):
  def __init__(self, base_info, worker_manager):
    # type: (beam_provision_api_pb2.ProvisionInfo, WorkerHandlerManager) -> None
    self._base_info = base_info
    self._worker_manager = worker_manager

  def GetProvisionInfo(self, request, context=None):
    # type: (Any, Optional[ServicerContext]) -> beam_provision_api_pb2.GetProvisionInfoResponse
    if context:
      worker_id = dict(context.invocation_metadata())['worker_id']
      worker = self._worker_manager.get_worker(worker_id)
      info = copy.copy(worker.provision_info.provision_info)
      info.logging_endpoint.CopyFrom(worker.logging_api_service_descriptor())  # type: ignore
      info.artifact_endpoint.CopyFrom(worker.artifact_api_service_descriptor())
      info.control_endpoint.CopyFrom(worker.control_api_service_descriptor())
    else:
      info = self._base_info
    return beam_provision_api_pb2.GetProvisionInfoResponse(info=info)


class GrpcServer(object):

  _DEFAULT_SHUTDOWN_TIMEOUT_SECS = 5

  def __init__(self,
               state,  # type: StateServicer
               provision_info,  # type: Optional[ExtendedProvisionInfo]
               worker_manager,  # type: WorkerHandlerManager
              ):
    # type: (...) -> None

    # Options to have no limits (-1) on the size of the messages
    # received or sent over the data plane. The actual buffer size
    # is controlled in a layer above. Also, options to keep the server alive
    # when too many pings are received.
    options = [("grpc.max_receive_message_length", -1),
               ("grpc.max_send_message_length", -1),
               ("grpc.http2.max_pings_without_data", 0),
               ("grpc.http2.max_ping_strikes", 0)]

    self.state = state
    self.provision_info = provision_info
    self.control_server = grpc.server(
        thread_pool_executor.shared_unbounded_instance(), options=options)
    self.control_port = self.control_server.add_insecure_port('[::]:0')
    self.control_address = 'localhost:%s' % self.control_port

    self.data_server = grpc.server(
        thread_pool_executor.shared_unbounded_instance(), options=options)
    self.data_port = self.data_server.add_insecure_port('[::]:0')

    self.state_server = grpc.server(
        thread_pool_executor.shared_unbounded_instance(), options=options)
    self.state_port = self.state_server.add_insecure_port('[::]:0')

    self.control_handler = BeamFnControlServicer(worker_manager)
    beam_fn_api_pb2_grpc.add_BeamFnControlServicer_to_server(
        self.control_handler, self.control_server)

    # If we have provision info, serve these off the control port as well.
    if self.provision_info:
      if self.provision_info.provision_info:
        beam_provision_api_pb2_grpc.add_ProvisionServiceServicer_to_server(
            BasicProvisionService(
                self.provision_info.provision_info, worker_manager),
            self.control_server)

      def open_uncompressed(f):
        # type: (str) -> BinaryIO
        return filesystems.FileSystems.open(
            f, compression_type=CompressionTypes.UNCOMPRESSED)

      beam_artifact_api_pb2_grpc.add_ArtifactRetrievalServiceServicer_to_server(
          artifact_service.ArtifactRetrievalService(
              file_reader=open_uncompressed),
          self.control_server)

    self.data_plane_handler = data_plane.BeamFnDataServicer(
        DATA_BUFFER_TIME_LIMIT_MS)
    beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(
        self.data_plane_handler, self.data_server)

    beam_fn_api_pb2_grpc.add_BeamFnStateServicer_to_server(
        GrpcStateServicer(state), self.state_server)

    self.logging_server = grpc.server(
        thread_pool_executor.shared_unbounded_instance(), options=options)
    self.logging_port = self.logging_server.add_insecure_port('[::]:0')
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        BasicLoggingService(), self.logging_server)

    _LOGGER.info('starting control server on port %s', self.control_port)
    _LOGGER.info('starting data server on port %s', self.data_port)
    _LOGGER.info('starting state server on port %s', self.state_port)
    _LOGGER.info('starting logging server on port %s', self.logging_port)
    self.logging_server.start()
    self.state_server.start()
    self.data_server.start()
    self.control_server.start()

  def close(self):
    # type: () -> None
    self.control_handler.done()
    to_wait = [
        self.control_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.data_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.state_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS),
        self.logging_server.stop(self._DEFAULT_SHUTDOWN_TIMEOUT_SECS)
    ]
    for w in to_wait:
      w.wait()


class GrpcWorkerHandler(WorkerHandler):
  """An grpc based worker_handler for fn API control, state and data planes."""

  def __init__(self,
               state,  # type: StateServicer
               provision_info,  # type: ExtendedProvisionInfo
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    self._grpc_server = grpc_server
    super().__init__(
        self._grpc_server.control_handler,
        self._grpc_server.data_plane_handler,
        state,
        provision_info)
    self.state = state

    self.control_address = self.port_from_worker(self._grpc_server.control_port)
    self.control_conn = self._grpc_server.control_handler.get_conn_by_worker_id(
        self.worker_id)

    self.data_conn = self._grpc_server.data_plane_handler.get_conn_by_worker_id(
        self.worker_id)

  def control_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.control_port))

  def artifact_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.control_port))

  def data_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.data_port))

  def state_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.state_port))

  def logging_api_service_descriptor(self):
    # type: () -> endpoints_pb2.ApiServiceDescriptor
    return endpoints_pb2.ApiServiceDescriptor(
        url=self.port_from_worker(self._grpc_server.logging_port))

  def close(self):
    # type: () -> None
    self.control_conn.close()
    self.data_conn.close()
    super().close()

  def port_from_worker(self, port):
    # type: (int) -> str
    return '%s:%s' % (self.host_from_worker(), port)

  def host_from_worker(self):
    # type: () -> str
    return 'localhost'


@WorkerHandler.register_environment(
    common_urns.environments.EXTERNAL.urn, beam_runner_api_pb2.ExternalPayload)
class ExternalWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               external_payload,  # type: beam_runner_api_pb2.ExternalPayload
               state,  # type: StateServicer
               provision_info,  # type: ExtendedProvisionInfo
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super().__init__(state, provision_info, grpc_server)
    self._external_payload = external_payload

  def start_worker(self):
    # type: () -> None
    _LOGGER.info("Requesting worker at %s", self._external_payload.endpoint.url)
    stub = beam_fn_api_pb2_grpc.BeamFnExternalWorkerPoolStub(
        GRPCChannelFactory.insecure_channel(
            self._external_payload.endpoint.url))
    control_descriptor = endpoints_pb2.ApiServiceDescriptor(
        url=self.control_address)
    response = stub.StartWorker(
        beam_fn_api_pb2.StartWorkerRequest(
            worker_id=self.worker_id,
            control_endpoint=control_descriptor,
            artifact_endpoint=control_descriptor,
            provision_endpoint=control_descriptor,
            logging_endpoint=self.logging_api_service_descriptor(),
            params=self._external_payload.params))
    if response.error:
      raise RuntimeError("Error starting worker: %s" % response.error)

  def stop_worker(self):
    # type: () -> None
    pass

  def host_from_worker(self):
    # type: () -> str
    # TODO(https://github.com/apache/beam/issues/19947): Reconcile across
    # platforms.
    if sys.platform in ['win32', 'darwin']:
      return 'localhost'
    import socket
    return socket.getfqdn()


@WorkerHandler.register_environment(python_urns.EMBEDDED_PYTHON_GRPC, bytes)
class EmbeddedGrpcWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               payload,  # type: bytes
               state,  # type: StateServicer
               provision_info,  # type: ExtendedProvisionInfo
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super().__init__(state, provision_info, grpc_server)

    from apache_beam.transforms.environments import EmbeddedPythonGrpcEnvironment
    config = EmbeddedPythonGrpcEnvironment.parse_config(payload.decode('utf-8'))
    self._state_cache_size = (
        config.get('state_cache_size') or STATE_CACHE_SIZE_MB) << 20
    self._data_buffer_time_limit_ms = \
        config.get('data_buffer_time_limit_ms') or DATA_BUFFER_TIME_LIMIT_MS

  def start_worker(self):
    # type: () -> None
    self.worker = sdk_worker.SdkHarness(
        self.control_address,
        state_cache_size=self._state_cache_size,
        data_buffer_time_limit_ms=self._data_buffer_time_limit_ms,
        worker_id=self.worker_id)
    self.worker_thread = threading.Thread(
        name='run_worker', target=self.worker.run)
    self.worker_thread.daemon = True
    self.worker_thread.start()

  def stop_worker(self):
    # type: () -> None
    self.worker_thread.join()


# The subprocesses module is not threadsafe on Python 2.7. Use this lock to
# prevent concurrent calls to Popen().
SUBPROCESS_LOCK = threading.Lock()


@WorkerHandler.register_environment(python_urns.SUBPROCESS_SDK, bytes)
class SubprocessSdkWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               worker_command_line,  # type: bytes
               state,  # type: StateServicer
               provision_info,  # type: ExtendedProvisionInfo
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super().__init__(state, provision_info, grpc_server)
    self._worker_command_line = worker_command_line

  def start_worker(self):
    # type: () -> None
    from apache_beam.runners.portability import local_job_service
    self.worker = local_job_service.SubprocessSdkWorker(
        self._worker_command_line,
        self.control_address,
        self.provision_info,
        self.worker_id)
    self.worker_thread = threading.Thread(
        name='run_worker', target=self.worker.run)
    self.worker_thread.start()

  def stop_worker(self):
    # type: () -> None
    self.worker_thread.join()


@WorkerHandler.register_environment(
    common_urns.environments.DOCKER.urn, beam_runner_api_pb2.DockerPayload)
class DockerSdkWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               payload,  # type: beam_runner_api_pb2.DockerPayload
               state,  # type: StateServicer
               provision_info,  # type: ExtendedProvisionInfo
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super().__init__(state, provision_info, grpc_server)
    self._container_image = payload.container_image
    self._container_id = None  # type: Optional[bytes]

  def host_from_worker(self):
    # type: () -> str
    if sys.platform == 'darwin':
      # See https://docs.docker.com/docker-for-mac/networking/
      return 'host.docker.internal'
    if sys.platform == 'linux' and is_in_notebook():
      import socket
      # Gets ipv4 address of current host. Note the host is not guaranteed to
      # be localhost because the python SDK could be running within a container.
      return socket.gethostbyname(socket.getfqdn())
    return super().host_from_worker()

  def start_worker(self):
    # type: () -> None
    credential_options = []
    try:
      # This is the public facing API, skip if it is not available.
      # (If this succeeds but the imports below fail, better to actually raise
      # an error below rather than silently fail.)
      # pylint: disable=unused-import
      import google.auth
    except ImportError:
      pass
    else:
      from google.auth import environment_vars
      from google.auth import _cloud_sdk
      gcloud_cred_file = os.environ.get(
          environment_vars.CREDENTIALS,
          _cloud_sdk.get_application_default_credentials_path())
      if os.path.exists(gcloud_cred_file):
        docker_cred_file = '/docker_cred_file.json'
        credential_options.extend([
            '--mount',
            f'type=bind,source={gcloud_cred_file},target={docker_cred_file}',
            '--env',
            f'{environment_vars.CREDENTIALS}={docker_cred_file}'
        ])
    with SUBPROCESS_LOCK:
      try:
        _LOGGER.info('Attempting to pull image %s', self._container_image)
        subprocess.check_call(['docker', 'pull', self._container_image])
      except Exception:
        _LOGGER.info(
            'Unable to pull image %s, defaulting to local image if it exists' %
            self._container_image)
      self._container_id = subprocess.check_output([
          'docker',
          'run',
          '-d',
          '--network=host',
      ] + credential_options + [
          self._container_image,
          '--id=%s' % self.worker_id,
          '--logging_endpoint=%s' % self.logging_api_service_descriptor().url,
          '--control_endpoint=%s' % self.control_address,
          '--artifact_endpoint=%s' % self.control_address,
          '--provision_endpoint=%s' % self.control_address,
      ]).strip()
      assert self._container_id is not None
      while True:
        status = subprocess.check_output([
            'docker', 'inspect', '-f', '{{.State.Status}}', self._container_id
        ]).strip()
        _LOGGER.info(
            'Waiting for docker to start up. Current status is %s' %
            status.decode('utf-8'))
        if status == b'running':
          _LOGGER.info(
              'Docker container is running. container_id = %s, '
              'worker_id = %s',
              self._container_id,
              self.worker_id)
          break
        elif status in (b'dead', b'exited'):
          subprocess.call(['docker', 'container', 'logs', self._container_id])
          raise RuntimeError(
              'SDK failed to start. Final status is %s' %
              status.decode('utf-8'))
      time.sleep(1)
    self._done = False
    t = threading.Thread(target=self.watch_container)
    t.daemon = True
    t.start()

  def watch_container(self):
    # type: () -> None
    while not self._done:
      assert self._container_id is not None
      status = subprocess.check_output(
          ['docker', 'inspect', '-f', '{{.State.Status}}',
           self._container_id]).strip()
      if status != b'running':
        if not self._done:
          logs = subprocess.check_output([
              'docker', 'container', 'logs', '--tail', '10', self._container_id
          ],
                                         stderr=subprocess.STDOUT)
          _LOGGER.info(logs)
          self.control_conn.abort(
              RuntimeError(
                  'SDK exited unexpectedly. '
                  'Final status is %s. Final log line is %s' % (
                      status.decode('utf-8'),
                      logs.decode('utf-8').strip().rsplit('\n',
                                                          maxsplit=1)[-1])))
      time.sleep(5)

  def stop_worker(self):
    # type: () -> None
    self._done = True
    if self._container_id:
      with SUBPROCESS_LOCK:
        subprocess.call(['docker', 'kill', self._container_id])


class WorkerHandlerManager(object):
  """
  Manages creation of ``WorkerHandler``s.

  Caches ``WorkerHandler``s based on environment id.
  """
  def __init__(self,
               environments,  # type: Mapping[str, beam_runner_api_pb2.Environment]
               job_provision_info  # type: ExtendedProvisionInfo
              ):
    # type: (...) -> None
    self._environments = environments
    self._job_provision_info = job_provision_info
    self._cached_handlers = collections.defaultdict(
        list)  # type: DefaultDict[str, List[WorkerHandler]]
    self._workers_by_id = {}  # type: Dict[str, WorkerHandler]
    self.state_servicer = StateServicer()
    self._grpc_server = None  # type: Optional[GrpcServer]
    self._process_bundle_descriptors = {
    }  # type: Dict[str, beam_fn_api_pb2.ProcessBundleDescriptor]

  def register_process_bundle_descriptor(self, process_bundle_descriptor):
    # type: (beam_fn_api_pb2.ProcessBundleDescriptor) -> None
    self._process_bundle_descriptors[
        process_bundle_descriptor.id] = process_bundle_descriptor

  def get_process_bundle_descriptor(self, request):
    # type: (beam_fn_api_pb2.GetProcessBundleDescriptorRequest) -> beam_fn_api_pb2.ProcessBundleDescriptor
    return self._process_bundle_descriptors[
        request.process_bundle_descriptor_id]

  def get_worker_handlers(
      self,
      environment_id,  # type: Optional[str]
      num_workers  # type: int
  ):
    # type: (...) -> List[WorkerHandler]
    if environment_id is None:
      # Any environment will do, pick one arbitrarily.
      environment_id = next(iter(self._environments.keys()))
    environment = self._environments[environment_id]

    # assume all environments except EMBEDDED_PYTHON use gRPC.
    if environment.urn == python_urns.EMBEDDED_PYTHON:
      # special case for EmbeddedWorkerHandler: there's no need for a gRPC
      # server, but we need to pass self instead.  Cast to make the type check
      # on WorkerHandler.create() think we have a GrpcServer instance.
      grpc_server = cast(GrpcServer, self)
    elif self._grpc_server is None:
      self._grpc_server = GrpcServer(
          self.state_servicer, self._job_provision_info, self)
      grpc_server = self._grpc_server
    else:
      grpc_server = self._grpc_server

    worker_handler_list = self._cached_handlers[environment_id]
    if len(worker_handler_list) < num_workers:
      for _ in range(len(worker_handler_list), num_workers):
        worker_handler = WorkerHandler.create(
            environment,
            self.state_servicer,
            self._job_provision_info.for_environment(environment),
            grpc_server)
        _LOGGER.info(
            "Created Worker handler %s for environment %s (%s, %r)",
            worker_handler,
            environment_id,
            environment.urn,
            environment.payload)
        self._cached_handlers[environment_id].append(worker_handler)
        self._workers_by_id[worker_handler.worker_id] = worker_handler
        worker_handler.start_worker()
    return self._cached_handlers[environment_id][:num_workers]

  def close_all(self):
    # type: () -> None
    for worker_handler_list in self._cached_handlers.values():
      for worker_handler in set(worker_handler_list):
        try:
          worker_handler.close()
        except Exception:
          _LOGGER.error(
              "Error closing worker_handler %s" % worker_handler, exc_info=True)
    self._cached_handlers = {}  # type: ignore[assignment]
    self._workers_by_id = {}
    if self._grpc_server is not None:
      self._grpc_server.close()
      self._grpc_server = None

  def get_worker(self, worker_id):
    # type: (str) -> WorkerHandler
    return self._workers_by_id[worker_id]


class StateServicer(beam_fn_api_pb2_grpc.BeamFnStateServicer,
                    sdk_worker.StateHandler):
  class CopyOnWriteState(object):
    def __init__(self, underlying):
      # type: (DefaultDict[bytes, Buffer]) -> None
      self._underlying = underlying
      self._overlay = {}  # type: Dict[bytes, Buffer]

    def __getitem__(self, key):
      # type: (bytes) -> Buffer
      if key in self._overlay:
        return self._overlay[key]
      else:
        return StateServicer.CopyOnWriteList(
            self._underlying, self._overlay, key)

    def __delitem__(self, key):
      # type: (bytes) -> None
      self._overlay[key] = []

    def commit(self):
      # type: () -> DefaultDict[bytes, Buffer]
      self._underlying.update(self._overlay)
      return self._underlying

  class CopyOnWriteList(object):
    def __init__(self,
        underlying,  # type: DefaultDict[bytes, Buffer]
        overlay,  # type: Dict[bytes, Buffer]
        key  # type: bytes
    ):
      # type: (...) -> None
      self._underlying = underlying
      self._overlay = overlay
      self._key = key

    def __iter__(self):
      # type: () -> Iterator[bytes]
      if self._key in self._overlay:
        return iter(self._overlay[self._key])
      else:
        return iter(self._underlying[self._key])

    def append(self, item):
      # type: (bytes) -> None
      if self._key not in self._overlay:
        self._overlay[self._key] = list(self._underlying[self._key])
      self._overlay[self._key].append(item)

    def extend(self, other: Buffer) -> None:
      raise NotImplementedError()

  StateType = Union[CopyOnWriteState, DefaultDict[bytes, Buffer]]

  def __init__(self):
    # type: () -> None
    self._lock = threading.Lock()
    self._state = collections.defaultdict(list)  # type: StateServicer.StateType
    self._checkpoint = None  # type: Optional[StateServicer.StateType]
    self._use_continuation_tokens = False
    self._continuations = {}  # type: Dict[bytes, Tuple[bytes, ...]]

  def checkpoint(self):
    # type: () -> None
    assert self._checkpoint is None and not \
      isinstance(self._state, StateServicer.CopyOnWriteState)
    self._checkpoint = self._state
    self._state = StateServicer.CopyOnWriteState(self._state)

  def commit(self):
    # type: () -> None
    assert isinstance(self._state,
                      StateServicer.CopyOnWriteState) and \
           isinstance(self._checkpoint,
                      StateServicer.CopyOnWriteState)
    self._state.commit()
    self._state = self._checkpoint.commit()
    self._checkpoint = None

  def restore(self):
    # type: () -> None
    assert self._checkpoint is not None
    self._state = self._checkpoint
    self._checkpoint = None

  @contextlib.contextmanager
  def process_instruction_id(self, unused_instruction_id):
    # type: (Any) -> Iterator
    yield

  def get_raw(self,
      state_key,  # type: beam_fn_api_pb2.StateKey
      continuation_token=None  # type: Optional[bytes]
              ):
    # type: (...) -> Tuple[bytes, Optional[bytes]]
    with self._lock:
      full_state = self._state[self._to_key(state_key)]
      if self._use_continuation_tokens:
        # The token is "nonce:index".
        if not continuation_token:
          token_base = b'token_%x' % len(self._continuations)
          self._continuations[token_base] = tuple(full_state)
          return b'', b'%s:0' % token_base
        else:
          token_base, index = continuation_token.split(b':')
          ix = int(index)
          full_state_cont = self._continuations[token_base]
          if ix == len(full_state_cont):
            return b'', None
          else:
            return full_state_cont[ix], b'%s:%d' % (token_base, ix + 1)
      else:
        assert not continuation_token
        return b''.join(full_state), None

  def append_raw(
      self,
      state_key,  # type: beam_fn_api_pb2.StateKey
      data  # type: bytes
  ):
    # type: (...) -> _Future
    with self._lock:
      self._state[self._to_key(state_key)].append(data)
    return _Future.done()

  def clear(self, state_key):
    # type: (beam_fn_api_pb2.StateKey) -> _Future
    with self._lock:
      try:
        del self._state[self._to_key(state_key)]
      except KeyError:
        # This may happen with the caching layer across bundles. Caching may
        # skip this storage layer for a blocking_get(key) request. Without
        # the caching, the state for a key would be initialized via the
        # defaultdict that _state uses.
        pass
    return _Future.done()

  def done(self):
    # type: () -> None
    pass

  @staticmethod
  def _to_key(state_key):
    # type: (beam_fn_api_pb2.StateKey) -> bytes
    return state_key.SerializeToString()


class GrpcStateServicer(beam_fn_api_pb2_grpc.BeamFnStateServicer):
  def __init__(self, state):
    # type: (StateServicer) -> None
    self._state = state

  def State(self,
      request_stream,  # type: Iterable[beam_fn_api_pb2.StateRequest]
      context=None  # type: Any
            ):
    # type: (...) -> Iterator[beam_fn_api_pb2.StateResponse]
    # Note that this eagerly mutates state, assuming any failures are fatal.
    # Thus it is safe to ignore instruction_id.
    for request in request_stream:
      request_type = request.WhichOneof('request')
      if request_type == 'get':
        data, continuation_token = self._state.get_raw(
            request.state_key, request.get.continuation_token)
        yield beam_fn_api_pb2.StateResponse(
            id=request.id,
            get=beam_fn_api_pb2.StateGetResponse(
                data=data, continuation_token=continuation_token))
      elif request_type == 'append':
        self._state.append_raw(request.state_key, request.append.data)
        yield beam_fn_api_pb2.StateResponse(
            id=request.id, append=beam_fn_api_pb2.StateAppendResponse())
      elif request_type == 'clear':
        self._state.clear(request.state_key)
        yield beam_fn_api_pb2.StateResponse(
            id=request.id, clear=beam_fn_api_pb2.StateClearResponse())
      else:
        raise NotImplementedError('Unknown state request: %s' % request_type)


class SingletonStateHandlerFactory(sdk_worker.StateHandlerFactory):
  """A singleton cache for a StateServicer."""
  def __init__(self, state_handler):
    # type: (sdk_worker.CachingStateHandler) -> None
    self._state_handler = state_handler

  def create_state_handler(self, api_service_descriptor):
    # type: (endpoints_pb2.ApiServiceDescriptor) -> sdk_worker.CachingStateHandler

    """Returns the singleton state handler."""
    return self._state_handler

  def close(self):
    # type: () -> None

    """Does nothing."""
    pass


class ControlFuture(object):
  def __init__(self,
               instruction_id,  # type: str
               response=None  # type: Optional[beam_fn_api_pb2.InstructionResponse]
              ):
    # type: (...) -> None
    self.instruction_id = instruction_id
    self._response = response
    if response is None:
      self._condition = threading.Condition()
    self._exception = None  # type: Optional[Exception]

  def is_done(self):
    # type: () -> bool
    return self._response is not None

  def set(self, response):
    # type: (beam_fn_api_pb2.InstructionResponse) -> None
    with self._condition:
      self._response = response
      self._condition.notify_all()

  def get(self, timeout=None):
    # type: (Optional[float]) -> beam_fn_api_pb2.InstructionResponse
    if not self._response and not self._exception:
      with self._condition:
        if not self._response and not self._exception:
          self._condition.wait(timeout)
    if self._exception:
      raise self._exception
    else:
      assert self._response is not None
      return self._response

  def abort(self, exception):
    # type: (Exception) -> None
    with self._condition:
      self._exception = exception
      self._condition.notify_all()
