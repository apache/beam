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
import sys
import unittest
from shutil import rmtree
from tempfile import mkdtemp

from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import portable_runner_test

if __name__ == '__main__':
  # Run as
  #
  # python -m apache_beam.runners.portability.spark_runner_test \
  #     --spark_job_server_jar=/path/to/job_server.jar \
  #     [SparkRunnerTest.test_method, ...]

  parser = argparse.ArgumentParser(add_help=True)
  parser.add_argument(
      '--spark_job_server_jar', help='Job server jar to submit jobs.')
  parser.add_argument(
      '--environment_type',
      default='loopback',
      help='Environment type. docker, process, or loopback')
  parser.add_argument('--environment_config', help='Environment config.')
  parser.add_argument(
      '--environment_cache_millis',
      help='Environment cache TTL in milliseconds.')
  parser.add_argument(
      '--extra_experiments',
      default=[],
      action='append',
      help='Beam experiments config.')
  known_args, args = parser.parse_known_args(sys.argv)
  sys.argv = args

  spark_job_server_jar = (
      known_args.spark_job_server_jar or
      job_server.JavaJarJobServer.path_to_beam_jar(
          'runners:spark:job-server:shadowJar'))
  environment_type = known_args.environment_type.lower()
  environment_config = (
      known_args.environment_config if known_args.environment_config else None)
  environment_cache_millis = known_args.environment_cache_millis
  extra_experiments = known_args.extra_experiments

  # This is defined here to only be run when we invoke this file explicitly.
  class SparkRunnerTest(portable_runner_test.PortableRunnerTest):
    _use_grpc = True
    _use_subprocesses = True

    @classmethod
    def _subprocess_command(cls, job_port, expansion_port):
      # will be cleaned up at the end of this method, and recreated and used by
      # the job server
      tmp_dir = mkdtemp(prefix='sparktest')

      try:
        return [
            'java',
            '-Dbeam.spark.test.reuseSparkContext=true',
            '-jar',
            spark_job_server_jar,
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

    def create_options(self):
      options = super(SparkRunnerTest, self).create_options()
      options.view_as(
          DebugOptions).experiments = ['beam_fn_api'] + extra_experiments
      portable_options = options.view_as(PortableOptions)
      portable_options.environment_type = environment_type.upper()
      if environment_config:
        portable_options.environment_config = environment_config
      if environment_cache_millis:
        portable_options.environment_cache_millis = environment_cache_millis

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

    def test_external_transforms(self):
      # Skip until Spark runner supports external transforms.
      raise unittest.SkipTest("BEAM-7232")

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

  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
