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

# This file is an entry point for running validatesRunner tests with the Python
# SDK and the Java Reference Runner. Executing this file starts up an instance
# of the Java Reference Runner's job server before executing tests and teardown
# the job server afterwards.
from __future__ import absolute_import

import argparse
import logging
import sys
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import portable_runner_test
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

if __name__ == '__main__':
  # Run as
  #
  # python -m apache_beam.runners.portability.java_reference_runner_test \
  #     --job_server_jar=/path/to/job_server.jar \
  #     --environment_type=docker \
  #     [Test.test_method, ...]

  parser = argparse.ArgumentParser(add_help=True)
  parser.add_argument('--job_server_jar',
                      help='Job server jar to submit jobs.')
  parser.add_argument('--environment_type', default='docker',
                      help='Environment type. docker or process')
  parser.add_argument('--environment_config', help='Environment config.')

  known_args, args = parser.parse_known_args(sys.argv)
  sys.argv = args

  job_server_jar = known_args.job_server_jar
  environment_type = known_args.environment_type.lower()
  environment_config = (
      known_args.environment_config if known_args.environment_config else None)

  # This is defined here to only be run when we invoke this file explicitly.
  class JavaReferenceRunnerTest(portable_runner_test.PortableRunnerTest):
    _use_grpc = True
    _use_subprocesses = True

    @classmethod
    def _subprocess_command(cls, port):
      return [
          'java',
          #'-Dorg.slf4j.simpleLogger.defaultLogLevel=info'
          '-jar', job_server_jar,
          '--port', str(port),
      ]

    @classmethod
    def get_runner(cls):
      return portable_runner.PortableRunner()

    def create_options(self):
      options = super(JavaReferenceRunnerTest, self).create_options()
      options.view_as(DebugOptions).experiments = ['beam_fn_api']
      options.view_as(PortableOptions).environment_type = (
          environment_type.upper())
      if environment_config:
        options.view_as(PortableOptions).environment_config = environment_config
      return options

    def test_assert_that(self):
      # We still want to make sure asserts fail, even if the message
      # isn't right (BEAM-6600).
      with self.assertRaises(Exception):
        with self.create_pipeline() as p:
          assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

    def test_pardo_side_inputs(self):
      # Skip until Reference Runner supports side unputs.
      raise unittest.SkipTest("BEAM-2928")

    def test_pardo_windowed_side_inputs(self):
      # Skip until Reference Runner supports side unputs.
      raise unittest.SkipTest("BEAM-2928")

    def test_flattened_side_input(self):
      # Skip until Reference Runner supports side unputs.
      raise unittest.SkipTest("BEAM-2928")

    def test_gbk_side_input(self):
      # Skip until Reference Runner supports side unputs.
      raise unittest.SkipTest("BEAM-2928")

    def test_multimap_side_input(self):
      # Skip until Reference Runner supports side unputs.
      raise unittest.SkipTest("BEAM-2928")

    def test_pardo_unfusable_side_inputs(self):
      # Skip until Reference Runner supports side unputs.
      raise unittest.SkipTest("BEAM-2928")

    def test_pardo_state_only(self):
      # Skip until Reference Runner supports state.
      raise unittest.SkipTest("BEAM-2917")

    def test_pardo_timers(self):
      # Skip until Reference Runner supports state.
      raise unittest.SkipTest("BEAM-2917")

    def test_pardo_state_timers(self):
      # Skip until Reference Runner supports state.
      raise unittest.SkipTest("BEAM-2917")

    def test_sdf(self):
      # Skip until Reference Runner supports SDF.
      raise unittest.SkipTest("BEAM-6651")

    # Can't read host files from within docker, read a "local" file there.
    def test_read(self):
      with self.create_pipeline() as p:
        lines = p | beam.io.ReadFromText('/etc/profile')
        assert_that(lines, lambda lines: len(lines) > 0)

    def test_large_elements(self):
      # Skip until Reference Runner supports large elements.
      raise unittest.SkipTest("BEAM-6622")

    def test_error_message_includes_stage(self):
      # Skip until Reference Runner provides message support.
      raise unittest.SkipTest("BEAM-6600")

    def test_error_traceback_includes_user_code(self):
      # Skip until Reference Runner provides message support.
      raise unittest.SkipTest("BEAM-6600")

    def test_metrics(self):
      # Skip until Reference Runner provides metrics support.
      raise unittest.SkipTest("BEAM-5452")

    def test_non_user_metrics(self):
      # Skip until Reference Runner provides metrics support.
      raise unittest.SkipTest("BEAM-5452")

    def test_progress_metrics(self):
      # Skip until Reference Runner provides metrics support.
      raise unittest.SkipTest("BEAM-5452")

  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
