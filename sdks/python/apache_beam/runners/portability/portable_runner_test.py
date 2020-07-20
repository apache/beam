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

from __future__ import absolute_import
from __future__ import print_function

import inspect
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
import pytest

import apache_beam as beam
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability.fn_api_runner import fn_runner_test
from apache_beam.runners.portability.local_job_service import LocalJobServicer
from apache_beam.runners.portability.portable_runner import PortableRunner
from apache_beam.runners.worker import worker_pool_main
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import environments
from apache_beam.transforms import userstate

_LOGGER = logging.getLogger(__name__)


# Disable timeout since this test implements its own.
# TODO(BEAM-9011): Consider using pytest-timeout's mechanism instead.
@pytest.mark.timeout(0)
class PortableRunnerTest(fn_runner_test.FnApiRunnerTest):

  TIMEOUT_SECS = 60

  # Controls job service interaction, not sdk harness interaction.
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

  @classmethod
  def _pick_unused_port(cls):
    return cls._pick_unused_ports(num_ports=1)[0]

  @staticmethod
  def _pick_unused_ports(num_ports):
    """Not perfect, but we have to provide a port to the subprocess."""
    # TODO(robertwb): Consider letting the subprocess communicate a choice of
    # port back.
    sockets = []
    ports = []
    for _ in range(0, num_ports):
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sockets.append(s)
      s.bind(('localhost', 0))
      _, port = s.getsockname()
      ports.append(port)
    try:
      return ports
    finally:
      for s in sockets:
        s.close()

  @classmethod
  def _start_local_runner_subprocess_job_service(cls):
    cls._maybe_kill_subprocess()
    # TODO(robertwb): Consider letting the subprocess pick one and
    # communicate it back...
    # pylint: disable=unbalanced-tuple-unpacking
    job_port, expansion_port = cls._pick_unused_ports(num_ports=2)
    _LOGGER.info('Starting server on port %d.', job_port)
    cls._subprocess = subprocess.Popen(
        cls._subprocess_command(job_port, expansion_port))
    address = 'localhost:%d' % job_port
    job_service = beam_job_api_pb2_grpc.JobServiceStub(
        GRPCChannelFactory.insecure_channel(address))
    _LOGGER.info('Waiting for server to be ready...')
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
          if exn.code() != grpc.StatusCode.UNAVAILABLE:
            # We were able to contact the service for our fake state request.
            break
    _LOGGER.info('Server ready.')
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
    else:
      cls._servicer = LocalJobServicer()
      return 'localhost:%d' % cls._servicer.start_grpc_server()

  @classmethod
  def get_runner(cls):
    return portable_runner.PortableRunner()

  @classmethod
  def tearDownClass(cls):
    cls._maybe_kill_subprocess()

  @classmethod
  def _maybe_kill_subprocess(cls):
    if hasattr(cls, '_subprocess') and cls._subprocess.poll() is None:
      cls._subprocess.kill()
      time.sleep(0.1)

  def create_options(self):
    def get_pipeline_name():
      for _, _, _, method_name, _, _ in inspect.stack():
        if method_name.find('test') != -1:
          return method_name
      return 'unknown_test'

    # Set the job name for better debugging.
    options = PipelineOptions.from_dictionary(
        {'job_name': get_pipeline_name() + '_' + str(time.time())})
    options.view_as(PortableOptions).job_endpoint = self._get_job_endpoint()
    # Override the default environment type for testing.
    options.view_as(PortableOptions).environment_type = (
        python_urns.EMBEDDED_PYTHON)
    # Enable caching (disabled by default)
    options.view_as(DebugOptions).add_experiment('state_cache_size=100')
    # Enable time-based data buffer (disabled by default)
    options.view_as(DebugOptions).add_experiment(
        'data_buffer_time_limit_ms=1000')
    return options

  def create_pipeline(self, is_drain=False):
    return beam.Pipeline(self.get_runner(), self.create_options())

  def test_pardo_state_with_custom_key_coder(self):
    """Tests that state requests work correctly when the key coder is an
    SDK-specific coder, i.e. non standard coder. This is additionally enforced
    by Java's ProcessBundleDescriptorsTest and by Flink's
    ExecutableStageDoFnOperator which detects invalid encoding by checking for
    the correct key group of the encoded key."""
    index_state_spec = userstate.CombiningValueStateSpec('index', sum)

    # Test params
    # Ensure decent amount of elements to serve all partitions
    n = 200
    duplicates = 1

    split = n // (duplicates + 1)
    inputs = [(i % split, str(i % split)) for i in range(0, n)]

    # Use a DoFn which has to use FastPrimitivesCoder because the type cannot
    # be inferred
    class Input(beam.DoFn):
      def process(self, impulse):
        for i in inputs:
          yield i

    class AddIndex(beam.DoFn):
      def process(self, kv, index=beam.DoFn.StateParam(index_state_spec)):
        k, v = kv
        index.add(1)
        yield k, v, index.read()

    expected = [(i % split, str(i % split), i // split + 1)
                for i in range(0, n)]

    with self.create_pipeline() as p:
      assert_that(
          p
          | beam.Impulse()
          | beam.ParDo(Input())
          | beam.ParDo(AddIndex()),
          equal_to(expected))

  # Inherits all other tests from fn_api_runner_test.FnApiRunnerTest

  def test_sdf_default_truncate_when_bounded(self):
    raise unittest.SkipTest("Portable runners don't support drain yet.")

  def test_sdf_default_truncate_when_unbounded(self):
    raise unittest.SkipTest("Portable runners don't support drain yet.")

  def test_sdf_with_truncate(self):
    raise unittest.SkipTest("Portable runners don't support drain yet.")

  def test_draining_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("Portable runners don't support drain yet.")


@unittest.skip("BEAM-7248")
class PortableRunnerOptimized(PortableRunnerTest):
  def create_options(self):
    options = super(PortableRunnerOptimized, self).create_options()
    options.view_as(DebugOptions).add_experiment('pre_optimize=all')
    options.view_as(DebugOptions).add_experiment('state_cache_size=100')
    options.view_as(DebugOptions).add_experiment(
        'data_buffer_time_limit_ms=1000')
    return options


class PortableRunnerTestWithExternalEnv(PortableRunnerTest):
  @classmethod
  def setUpClass(cls):
    cls._worker_address, cls._worker_server = (
        worker_pool_main.BeamFnExternalWorkerPoolServicer.start(
            state_cache_size=100, data_buffer_time_limit_ms=1000))

  @classmethod
  def tearDownClass(cls):
    cls._worker_server.stop(1)

  def create_options(self):
    options = super(PortableRunnerTestWithExternalEnv, self).create_options()
    options.view_as(PortableOptions).environment_type = 'EXTERNAL'
    options.view_as(PortableOptions).environment_config = self._worker_address
    return options


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
class PortableRunnerTestWithSubprocesses(PortableRunnerTest):
  _use_subprocesses = True

  def create_options(self):
    options = super(PortableRunnerTestWithSubprocesses, self).create_options()
    options.view_as(PortableOptions).environment_type = (
        python_urns.SUBPROCESS_SDK)
    options.view_as(PortableOptions).environment_config = (
        b'%s -m apache_beam.runners.worker.sdk_worker_main' %
        sys.executable.encode('ascii')).decode('utf-8')
    # Enable caching (disabled by default)
    options.view_as(DebugOptions).add_experiment('state_cache_size=100')
    # Enable time-based data buffer (disabled by default)
    options.view_as(DebugOptions).add_experiment(
        'data_buffer_time_limit_ms=1000')
    return options

  @classmethod
  def _subprocess_command(cls, job_port, _):
    return [
        sys.executable,
        '-m',
        'apache_beam.runners.portability.local_job_service_main',
        '-p',
        str(job_port),
    ]


class PortableRunnerTestWithSubprocessesAndMultiWorkers(
    PortableRunnerTestWithSubprocesses):
  _use_subprocesses = True

  def create_options(self):
    options = super(PortableRunnerTestWithSubprocessesAndMultiWorkers, self) \
      .create_options()
    options.view_as(DirectOptions).direct_num_workers = 2
    return options


class PortableRunnerInternalTest(unittest.TestCase):
  def test__create_default_environment(self):
    docker_image = environments.DockerEnvironment.default_docker_image()
    self.assertEqual(
        PortableRunner._create_environment(
            PipelineOptions.from_dictionary({'sdk_location': 'container'})),
        environments.DockerEnvironment(container_image=docker_image))

  def test__create_docker_environment(self):
    docker_image = 'py-docker'
    self.assertEqual(
        PortableRunner._create_environment(
            PipelineOptions.from_dictionary({
                'environment_type': 'DOCKER',
                'environment_config': docker_image,
                'sdk_location': 'container',
            })),
        environments.DockerEnvironment(container_image=docker_image))

  def test__create_process_environment(self):
    self.assertEqual(
        PortableRunner._create_environment(
            PipelineOptions.from_dictionary({
                'environment_type': "PROCESS",
                'environment_config': '{"os": "linux", "arch": "amd64", '
                '"command": "run.sh", '
                '"env":{"k1": "v1"} }',
                'sdk_location': 'container',
            })),
        environments.ProcessEnvironment(
            'run.sh', os='linux', arch='amd64', env={'k1': 'v1'}))
    self.assertEqual(
        PortableRunner._create_environment(
            PipelineOptions.from_dictionary({
                'environment_type': 'PROCESS',
                'environment_config': '{"command": "run.sh"}',
                'sdk_location': 'container',
            })),
        environments.ProcessEnvironment('run.sh'))

  def test__create_external_environment(self):
    self.assertEqual(
        PortableRunner._create_environment(
            PipelineOptions.from_dictionary({
                'environment_type': "EXTERNAL",
                'environment_config': 'localhost:50000',
                'sdk_location': 'container',
            })),
        environments.ExternalEnvironment('localhost:50000'))
    raw_config = ' {"url":"localhost:50000", "params":{"k1":"v1"}} '
    for env_config in (raw_config, raw_config.lstrip(), raw_config.strip()):
      self.assertEqual(
          PortableRunner._create_environment(
              PipelineOptions.from_dictionary({
                  'environment_type': "EXTERNAL",
                  'environment_config': env_config,
                  'sdk_location': 'container',
              })),
          environments.ExternalEnvironment(
              'localhost:50000', params={"k1": "v1"}))
    with self.assertRaises(ValueError):
      PortableRunner._create_environment(
          PipelineOptions.from_dictionary({
              'environment_type': "EXTERNAL",
              'environment_config': '{invalid}',
              'sdk_location': 'container',
          }))
    with self.assertRaises(ValueError) as ctx:
      PortableRunner._create_environment(
          PipelineOptions.from_dictionary({
              'environment_type': "EXTERNAL",
              'environment_config': '{"params":{"k1":"v1"}}',
              'sdk_location': 'container',
          }))
    self.assertIn(
        'External environment endpoint must be set.', ctx.exception.args)


def hasDockerImage():
  image = environments.DockerEnvironment.default_docker_image()
  try:
    check_image = subprocess.check_output(
        "docker images -q %s" % image, shell=True)
    return check_image != ''
  except Exception:
    return False


@unittest.skipIf(
    not hasDockerImage(), "docker not installed or "
    "no docker image")
class PortableRunnerTestWithLocalDocker(PortableRunnerTest):
  def create_options(self):
    options = super(PortableRunnerTestWithLocalDocker, self).create_options()
    options.view_as(PortableOptions).job_endpoint = 'embed'
    return options


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
