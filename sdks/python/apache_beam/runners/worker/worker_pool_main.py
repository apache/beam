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
"""
Worker pool entry point.

The worker pool exposes an RPC service that is used with EXTERNAL
environment to start and stop the SDK workers.

The worker pool uses child processes for parallelism; threads are
subject to the GIL and not sufficient.

This entry point is used by the Python SDK container in worker pool mode.
"""

from __future__ import absolute_import

import argparse
import atexit
import logging
import subprocess
import sys
import threading
import time
from typing import Dict
from typing import Optional
from typing import Tuple

import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import sdk_worker
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor

_LOGGER = logging.getLogger(__name__)


class BeamFnExternalWorkerPoolServicer(
    beam_fn_api_pb2_grpc.BeamFnExternalWorkerPoolServicer):

  def __init__(self,
               use_process=False,
               container_executable=None,  # type: Optional[str]
               state_cache_size=0,
               data_buffer_time_limit_ms=0
              ):
    self._use_process = use_process
    self._container_executable = container_executable
    self._state_cache_size = state_cache_size
    self._data_buffer_time_limit_ms = data_buffer_time_limit_ms
    self._worker_processes = {}  # type: Dict[str, subprocess.Popen]

  @classmethod
  def start(cls,
            use_process=False,
            port=0,
            state_cache_size=0,
            data_buffer_time_limit_ms=-1,
            container_executable=None  # type: Optional[str]
            ):
    # type: (...) -> Tuple[str, grpc.Server]
    worker_server = grpc.server(UnboundedThreadPoolExecutor())
    worker_address = 'localhost:%s' % worker_server.add_insecure_port(
        '[::]:%s' % port)
    worker_pool = cls(use_process=use_process,
                      container_executable=container_executable,
                      state_cache_size=state_cache_size,
                      data_buffer_time_limit_ms=data_buffer_time_limit_ms)
    beam_fn_api_pb2_grpc.add_BeamFnExternalWorkerPoolServicer_to_server(
        worker_pool,
        worker_server)
    worker_server.start()

    # Register to kill the subprocesses on exit.
    def kill_worker_processes():
      for worker_process in worker_pool._worker_processes.values():
        worker_process.kill()
    atexit.register(kill_worker_processes)

    return worker_address, worker_server

  def StartWorker(self,
                  start_worker_request,  # type: beam_fn_api_pb2.StartWorkerRequest
                  unused_context
                 ):
    # type: (...) -> beam_fn_api_pb2.StartWorkerResponse
    try:
      if self._use_process:
        command = ['python', '-c',
                   'from apache_beam.runners.worker.sdk_worker '
                   'import SdkHarness; '
                   'SdkHarness('
                   '"%s",'
                   'worker_id="%s",'
                   'state_cache_size=%d'
                   'data_buffer_time_limit_ms=%d'
                   ')'
                   '.run()' % (
                       start_worker_request.control_endpoint.url,
                       start_worker_request.worker_id,
                       self._state_cache_size,
                       self._data_buffer_time_limit_ms)]
        if self._container_executable:
          # command as per container spec
          # the executable is responsible to handle concurrency
          # for artifact retrieval and other side effects
          command = [self._container_executable,
                     '--id=%s' % start_worker_request.worker_id,
                     '--logging_endpoint=%s'
                     % start_worker_request.logging_endpoint.url,
                     '--artifact_endpoint=%s'
                     % start_worker_request.artifact_endpoint.url,
                     '--provision_endpoint=%s'
                     % start_worker_request.provision_endpoint.url,
                     '--control_endpoint=%s'
                     % start_worker_request.control_endpoint.url,
                    ]

        _LOGGER.warning("Starting worker with command %s" % command)
        worker_process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                          close_fds=True)
        self._worker_processes[start_worker_request.worker_id] = worker_process
      else:
        worker = sdk_worker.SdkHarness(
            start_worker_request.control_endpoint.url,
            worker_id=start_worker_request.worker_id,
            state_cache_size=self._state_cache_size,
            data_buffer_time_limit_ms=self._data_buffer_time_limit_ms)
        worker_thread = threading.Thread(
            name='run_worker_%s' % start_worker_request.worker_id,
            target=worker.run)
        worker_thread.daemon = True
        worker_thread.start()

      return beam_fn_api_pb2.StartWorkerResponse()
    except Exception as exn:
      return beam_fn_api_pb2.StartWorkerResponse(error=str(exn))

  def StopWorker(self,
                 stop_worker_request,  # type: beam_fn_api_pb2.StopWorkerRequest
                 unused_context
                ):
    # type: (...) -> beam_fn_api_pb2.StopWorkerResponse
    # applicable for process mode to ensure process cleanup
    # thread based workers terminate automatically
    worker_process = self._worker_processes.pop(stop_worker_request.worker_id,
                                                None)
    if worker_process:
      def kill_worker_process():
        try:
          worker_process.kill()
        except OSError:
          # ignore already terminated process
          return
      _LOGGER.info("Stopping worker %s" % stop_worker_request.worker_id)
      # communicate is necessary to avoid zombie process
      # time box communicate (it has no timeout parameter in Py2)
      threading.Timer(1, kill_worker_process).start()
      worker_process.communicate()
    return beam_fn_api_pb2.StopWorkerResponse()


def main(argv=None):
  """Entry point for worker pool service for external environments."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--container_executable',
                      type=str,
                      default=None,
                      help='Executable that implements the Beam SDK '
                           'container contract.')
  parser.add_argument('--service_port',
                      type=int,
                      required=True,
                      dest='port',
                      help='Bind port for the worker pool service.')

  args, _ = parser.parse_known_args(argv)

  address, server = (BeamFnExternalWorkerPoolServicer.start(use_process=True,
                                                            **vars(args)))
  logging.getLogger().setLevel(logging.INFO)
  _LOGGER.info('Started worker pool servicer at port: %s with executable: %s',
               address, args.container_executable)
  try:
    while True:
      time.sleep(60 * 60 * 24)
  except KeyboardInterrupt:
    server.stop(0)


if __name__ == '__main__':
  main(sys.argv)
