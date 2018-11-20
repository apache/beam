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

import argparse
import logging
import shutil
import sys
import tempfile
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import FlinkOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import portable_runner_test
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

if __name__ == '__main__':
  # Run as
  #
  # python -m apache_beam.runners.portability.flink_runner_test \
  #     --flink_job_server_jar=/path/to/job_server.jar \
  #     --type=Batch \
  #     --environment_type=docker \
  #     [FlinkRunnerTest.test_method, ...]

  parser = argparse.ArgumentParser(add_help=True)
  parser.add_argument('--flink_job_server_jar',
                      help='Job server jar to submit jobs.')
  parser.add_argument('--streaming', default=False, action='store_true',
                      help='Job type. batch or streaming')
  parser.add_argument('--environment_type', default='docker',
                      help='Environment type. docker or process')
  parser.add_argument('--environment_config', help='Environment config.')
  known_args, args = parser.parse_known_args(sys.argv)
  sys.argv = args

  flink_job_server_jar = known_args.flink_job_server_jar
  streaming = known_args.streaming
  environment_type = known_args.environment_type.lower()
  environment_config = (
      known_args.environment_config if known_args.environment_config else None)

  # This is defined here to only be run when we invoke this file explicitly.
  class FlinkRunnerTest(portable_runner_test.PortableRunnerTest):
    _use_grpc = True
    _use_subprocesses = True

    @classmethod
    def _subprocess_command(cls, port):
      tmp_dir = tempfile.mkdtemp(prefix='flinktest')
      try:
        return [
            'java',
            '-jar', flink_job_server_jar,
            '--artifacts-dir', tmp_dir,
            '--job-port', str(port),
            '--artifact-port', '0',
        ]
      finally:
        shutil.rmtree(tmp_dir)

    @classmethod
    def get_runner(cls):
      return portable_runner.PortableRunner()

    def create_options(self):
      options = super(FlinkRunnerTest, self).create_options()
      options.view_as(DebugOptions).experiments = ['beam_fn_api']
      options.view_as(FlinkOptions).parallelism = 1
      # Default environment is Docker.
      if environment_type == 'process':
        options.view_as(PortableOptions).environment_type = 'PROCESS'

      if environment_config:
        options.view_as(PortableOptions).environment_config = environment_config

      if streaming:
        options.view_as(StandardOptions).streaming = True
      return options

    # Can't read host files from within docker, read a "local" file there.
    def test_read(self):
      with self.create_pipeline() as p:
        lines = p | beam.io.ReadFromText('/etc/profile')
        assert_that(lines, lambda lines: len(lines) > 0)

    def test_no_subtransform_composite(self):
      raise unittest.SkipTest("BEAM-4781")

    def test_assert_that(self):
      # We still want to make sure asserts fail, even if the message
      # isn't right (BEAM-6019).
      with self.assertRaises(Exception):
        with self.create_pipeline() as p:
          assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

    def test_error_message_includes_stage(self):
      raise unittest.SkipTest("BEAM-6019")

    def test_error_traceback_includes_user_code(self):
      raise unittest.SkipTest("BEAM-6019")

    # Inherits all other tests.

  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
