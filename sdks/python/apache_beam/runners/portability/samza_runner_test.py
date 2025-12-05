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

# Run as
#
# pytest samza_runner_test.py[::TestClass::test_case] \
#     --test-pipeline-options="--environment_type=LOOPBACK"
import argparse
import logging
import shlex
import unittest
from shutil import rmtree
from tempfile import mkdtemp

import pytest

from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import portable_runner_test
from apache_beam.utils import subprocess_server

_LOGGER = logging.getLogger(__name__)


class SamzaRunnerTest(portable_runner_test.PortableRunnerTest):
  _use_grpc = True
  _use_subprocesses = True

  expansion_port = None
  samza_job_server_jar = None

  @pytest.fixture(autouse=True)
  def parse_options(self, request):
    if not request.config.option.test_pipeline_options:
      raise unittest.SkipTest(
          'Skipping because --test-pipeline-options is not specified.')
    test_pipeline_options = request.config.option.test_pipeline_options
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        '--samza_job_server_jar',
        help='Job server jar to submit jobs.',
        action='store')
    parser.add_argument(
        '--environment_type',
        default='LOOPBACK',
        choices=['DOCKER', 'PROCESS', 'LOOPBACK'],
        help='Set the environment type for running user code. DOCKER runs '
        'user code in a container. PROCESS runs user code in '
        'automatically started processes. LOOPBACK runs user code on '
        'the same process that originally submitted the job.')
    parser.add_argument(
        '--environment_option',
        '--environment_options',
        dest='environment_options',
        action='append',
        default=None,
        help=(
            'Environment configuration for running the user code. '
            'Recognized options depend on --environment_type.\n '
            'For DOCKER: docker_container_image (optional)\n '
            'For PROCESS: process_command (required), process_variables '
            '(optional, comma-separated)\n '
            'For EXTERNAL: external_service_address (required)'))
    known_args, unknown_args = parser.parse_known_args(
        shlex.split(test_pipeline_options))
    if unknown_args:
      _LOGGER.warning('Discarding unrecognized arguments %s' % unknown_args)
    self.set_samza_job_server_jar(
        known_args.samza_job_server_jar or
        job_server.JavaJarJobServer.path_to_beam_jar(
            ':runners:samza:job-server:shadowJar'))
    self.environment_type = known_args.environment_type
    self.environment_options = known_args.environment_options\

  @classmethod
  def _subprocess_command(cls, job_port, expansion_port):
    # will be cleaned up at the end of this method, and recreated and used by
    # the job server
    tmp_dir = mkdtemp(prefix='samzatest')

    cls.expansion_port = expansion_port

    try:
      return [
          subprocess_server.JavaHelper.get_java(),
          '-jar',
          cls.samza_job_server_jar,
          '--artifacts-dir',
          tmp_dir,
          '--job-port',
          str(job_port),
          '--artifact-port',
          '0',
          '--expansion-port',
          str(expansion_port),
      ]
    finally:
      rmtree(tmp_dir)

  @classmethod
  def set_samza_job_server_jar(cls, samza_job_server_jar):
    cls.samza_job_server_jar = samza_job_server_jar

  @classmethod
  def get_runner(cls):
    return portable_runner.PortableRunner()

  @classmethod
  def get_expansion_service(cls):
    # TODO Move expansion address resides into PipelineOptions
    return 'localhost:%s' % cls.expansion_port

  def create_options(self):
    options = super().create_options()
    options.view_as(PortableOptions).environment_type = self.environment_type
    options.view_as(
        PortableOptions).environment_options = self.environment_options

    return options

  def test_metrics(self):
    # Skip until Samza portable runner supports distribution metrics.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21043")

  def test_flattened_side_input(self):
    # Blocked on support for transcoding
    # https://github.com/apache/beam/issues/20984
    super().test_flattened_side_input(with_transcoding=False)

  def test_flatten_and_gbk(self):
    # Blocked on support for transcoding
    # https://github.com/apache/beam/issues/20984
    # Also blocked on support of flatten and groupby sharing the same input
    # https://github.com/apache/beam/issues/34647
    raise unittest.SkipTest("https://github.com/apache/beam/issues/34647")

  def test_pack_combiners(self):
    # Stages produced by translations.pack_combiners are fused
    # by translations.greedily_fuse, which prevent the stages
    # from being detecting using counters by the test.
    self._test_pack_combiners(assert_using_counter_names=False)

  def test_pardo_timers(self):
    # Skip until Samza portable runner supports clearing timer.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21059")

  def test_register_finalizations(self):
    # Skip until Samza runner supports bundle finalization.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21044")

  def test_callbacks_with_exception(self):
    # Skip until Samza runner supports bundle finalization.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21044")

  def test_sdf_with_dofn_as_watermark_estimator(self):
    # Skip until Samza runner supports SDF and self-checkpoint.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21045")

  def test_sdf_with_sdf_initiated_checkpointing(self):
    # Skip until Samza runner supports SDF.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21045")

  def test_sdf_with_watermark_tracking(self):
    # Skip until Samza runner supports SDF.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21045")

  def test_custom_merging_window(self):
    # Skip until Samza runner supports merging window fns
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21049")

  def test_custom_window_type(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/21049")

  def test_reshuffle_after_custom_window(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/34831")

  def test_sliding_windows(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/35429")


if __name__ == '__main__':
  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
