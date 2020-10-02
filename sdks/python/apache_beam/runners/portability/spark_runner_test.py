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

# Run as
#
# pytest spark_runner_test.py \
#     [--test_pipeline_options "--spark_job_server_jar=/path/to/job_server.jar \
#                               --environment_type=DOCKER"] \
#     [SparkRunnerTest.test_method, ...]

_LOGGER = logging.getLogger(__name__)


class SparkRunnerTest(portable_runner_test.PortableRunnerTest):
  _use_grpc = True
  _use_subprocesses = True

  expansion_port = None
  spark_job_server_jar = None

  @pytest.fixture(autouse=True)
  def parse_options(self, request):
    if not request.config.option.test_pipeline_options:
      raise unittest.SkipTest(
          'Skipping because --test-pipeline-options is not specified.')
    test_pipeline_options = request.config.option.test_pipeline_options
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        '--spark_job_server_jar',
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
    self.set_spark_job_server_jar(
        known_args.spark_job_server_jar or
        job_server.JavaJarJobServer.path_to_beam_jar(
            ':runners:job-server:shadowJar'))
    self.environment_type = known_args.environment_type
    self.environment_options = known_args.environment_options

  @classmethod
  def _subprocess_command(cls, job_port, expansion_port):
    # will be cleaned up at the end of this method, and recreated and used by
    # the job server
    tmp_dir = mkdtemp(prefix='sparktest')

    cls.expansion_port = expansion_port

    try:
      return [
          'java',
          '-Dbeam.spark.test.reuseSparkContext=true',
          '-jar',
          cls.spark_job_server_jar,
          '--spark-master-url',
          'local',
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
  def get_runner(cls):
    return portable_runner.PortableRunner()

  @classmethod
  def get_expansion_service(cls):
    # TODO Move expansion address resides into PipelineOptions
    return 'localhost:%s' % cls.expansion_port

  @classmethod
  def set_spark_job_server_jar(cls, spark_job_server_jar):
    cls.spark_job_server_jar = spark_job_server_jar

  def create_options(self):
    options = super(SparkRunnerTest, self).create_options()
    options.view_as(PortableOptions).environment_type = self.environment_type
    options.view_as(
        PortableOptions).environment_options = self.environment_options

    return options

  def test_metrics(self):
    # Skip until Spark runner supports metrics.
    raise unittest.SkipTest("BEAM-7219")

  def test_sdf(self):
    # Skip until Spark runner supports SDF.
    raise unittest.SkipTest("BEAM-7222")

  def test_sdf_with_watermark_tracking(self):
    # Skip until Spark runner supports SDF.
    raise unittest.SkipTest("BEAM-7222")

  def test_sdf_with_sdf_initiated_checkpointing(self):
    # Skip until Spark runner supports SDF.
    raise unittest.SkipTest("BEAM-7222")

  def test_sdf_synthetic_source(self):
    # Skip until Spark runner supports SDF.
    raise unittest.SkipTest("BEAM-7222")

  def test_callbacks_with_exception(self):
    # Skip until Spark runner supports bundle finalization.
    raise unittest.SkipTest("BEAM-7233")

  def test_register_finalizations(self):
    # Skip until Spark runner supports bundle finalization.
    raise unittest.SkipTest("BEAM-7233")

  def test_flattened_side_input(self):
    # Blocked on support for transcoding
    # https://jira.apache.org/jira/browse/BEAM-7236
    super(SparkRunnerTest,
          self).test_flattened_side_input(with_transcoding=False)

  # Inherits all other tests from PortableRunnerTest.


if __name__ == '__main__':
  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
