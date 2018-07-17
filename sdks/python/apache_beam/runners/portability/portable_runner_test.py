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
from __future__ import print_function

import logging
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import unittest

import grpc

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.runners.portability import fn_api_runner_test
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability.local_job_service import LocalJobServicer
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class PortableRunnerTest(fn_api_runner_test.FnApiRunnerTest):

  TIMEOUT_SECS = 30

  _use_grpc = False
  _use_subprocesses = False

  def setUp(self):
    if platform.system() != 'Windows':
      def handler(signum, frame):
        msg = 'Timed out after %s seconds.' % self.TIMEOUT_SECS
        print('=' * 20, msg, '=' * 20)
        traceback.print_stack(frame)
        threads_by_id = {th.ident: th for th in threading.enumerate()}
        for thread_id, stack in sys._current_frames().items():
          th = threads_by_id.get(thread_id)
          print()
          print('# Thread:', th or thread_id)
          traceback.print_stack(stack)
        raise BaseException(msg)
      signal.signal(signal.SIGALRM, handler)
      signal.alarm(self.TIMEOUT_SECS)

  def tearDown(self):
    if platform.system() != 'Windows':
      signal.alarm(0)

  @staticmethod
  def _pick_unused_port():
    """Not perfect, but we have to provide a port to the subprocess."""
    # TODO(robertwb): Consider letting the subprocess communicate a choice of
    # port back.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    _, port = s.getsockname()
    s.close()
    return port

  @classmethod
  def _start_local_runner_subprocess_job_service(cls):
    if cls._subprocess:
      # Kill the old one if it exists.
      cls._subprocess.kill()
    # TODO(robertwb): Consider letting the subprocess pick one and
    # communicate it back...
    port = cls._pick_unused_port()
    logging.info('Starting server on port %d.', port)
    cls._subprocess = subprocess.Popen([
        sys.executable, '-m',
        'apache_beam.runners.portability.local_job_service_main', '-p',
        str(port), '--worker_command_line',
        '%s -m apache_beam.runners.worker.sdk_worker_main' % sys.executable
    ])
    address = 'localhost:%d' % port
    job_service = beam_job_api_pb2_grpc.JobServiceStub(
        grpc.insecure_channel(address))
    logging.info('Waiting for server to be ready...')
    start = time.time()
    timeout = 30
    while True:
      time.sleep(0.1)
      if cls._subprocess.poll() is not None:
        raise RuntimeError(
            'Subprocess terminated unexpectedly with exit code %d.' %
            cls._subprocess.returncode)
      elif time.time() - start > timeout:
        raise RuntimeError(
            'Pipeline timed out waiting for job service subprocess.')
      else:
        try:
          job_service.GetState(
              beam_job_api_pb2.GetJobStateRequest(job_id='[fake]'))
          break
        except grpc.RpcError as exn:
          if exn.code != grpc.StatusCode.UNAVAILABLE:
            # We were able to contact the service for our fake state request.
            break
    logging.info('Server ready.')
    return address

  @classmethod
  def _get_job_endpoint(cls):
    if '_job_endpoint' not in cls.__dict__:
      cls._job_endpoint = cls._create_job_endpoint()
    return cls._job_endpoint

  @classmethod
  def _create_job_endpoint(cls):
    if cls._use_subprocesses:
      return cls._start_local_runner_subprocess_job_service()
    elif cls._use_grpc:
      # Use GRPC for workers.
      cls._servicer = LocalJobServicer(use_grpc=True)
      return 'localhost:%d' % cls._servicer.start_grpc_server()
    else:
      # Do not use GRPC for worker.
      cls._servicer = LocalJobServicer(use_grpc=False)
      return 'localhost:%d' % cls._servicer.start_grpc_server()

  @classmethod
  def get_runner(cls):
    return portable_runner.PortableRunner(is_embedded_fnapi_runner=True)

  @classmethod
  def tearDownClass(cls):
    if hasattr(cls, '_subprocess'):
      cls._subprocess.kill()
      time.sleep(0.1)

  def create_pipeline(self):
    options = PipelineOptions()
    options.view_as(PortableOptions).job_endpoint = self._get_job_endpoint()
    return beam.Pipeline(self.get_runner(), options)

  def test_assert_that(self):
    # TODO: figure out a way for runner to parse and raise the
    # underlying exception.
    with self.assertRaises(Exception):
      with self.create_pipeline() as p:
        assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

  def test_error_message_includes_stage(self):
    # TODO: figure out a way for runner to parse and raise the
    # underlying exception.
    with self.assertRaises(Exception):
      with self.create_pipeline() as p:
        def raise_error(x):
          raise RuntimeError('x')
        # pylint: disable=expression-not-assigned
        (p
         | beam.Create(['a', 'b'])
         | 'StageA' >> beam.Map(lambda x: x)
         | 'StageB' >> beam.Map(lambda x: x)
         | 'StageC' >> beam.Map(raise_error)
         | 'StageD' >> beam.Map(lambda x: x))

  def test_error_traceback_includes_user_code(self):
    # TODO: figure out a way for runner to parse and raise the
    # underlying exception.
    raise unittest.SkipTest('TODO')

  # Inherits all tests from fn_api_runner_test.FnApiRunnerTest


class PortableRunnerTestWithGrpc(PortableRunnerTest):
  _use_grpc = True


@unittest.skip("BEAM-3040")
class PortableRunnerTestWithSubprocesses(PortableRunnerTest):
  _use_grpc = True
  _use_subprocesses = True


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
