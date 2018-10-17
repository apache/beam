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
import shutil
import sys
import tempfile
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import portable_runner_test
from apache_beam.testing.util import assert_that

if __name__ == '__main__':
  # Run as
  #
  # python -m apache_beam.runners.portability.flink_runner_test \
  #     /path/to/job_server.jar \
  #     [FlinkRunnerTest.test_method, ...]
  flinkJobServerJar = sys.argv.pop(1)
  streaming = sys.argv.pop(1).lower() == 'streaming'

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
            '-jar', flinkJobServerJar,
            '--artifacts-dir', tmp_dir,
            '--job-host', 'localhost',
            '--job-port', str(port),
        ]
      finally:
        shutil.rmtree(tmp_dir)

    @classmethod
    def get_runner(cls):
      return portable_runner.PortableRunner()

    def create_options(self):
      options = super(FlinkRunnerTest, self).create_options()
      options.view_as(DebugOptions).experiments = ['beam_fn_api']
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

    def test_pardo_state_only(self):
      raise unittest.SkipTest("BEAM-2918 - User state not yet supported.")

    def test_pardo_timers(self):
      raise unittest.SkipTest("BEAM-4681 - User timers not yet supported.")

    # Inherits all other tests.

  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
