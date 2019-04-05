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
import sys
import unittest
from os import linesep
from os import path
from os.path import exists
from shutil import rmtree
from tempfile import mkdtemp

import apache_beam as beam
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import DebugOptions
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
  #     --extra_experiments=beam_experiments \
  #     [FlinkRunnerTest.test_method, ...]

  parser = argparse.ArgumentParser(add_help=True)
  parser.add_argument('--flink_job_server_jar',
                      help='Job server jar to submit jobs.')
  parser.add_argument('--streaming', default=False, action='store_true',
                      help='Job type. batch or streaming')
  parser.add_argument('--environment_type', default='docker',
                      help='Environment type. docker or process')
  parser.add_argument('--environment_config', help='Environment config.')
  parser.add_argument('--extra_experiments', default=[], action='append',
                      help='Beam experiments config.')
  known_args, args = parser.parse_known_args(sys.argv)
  sys.argv = args

  flink_job_server_jar = known_args.flink_job_server_jar
  streaming = known_args.streaming
  environment_type = known_args.environment_type.lower()
  environment_config = (
      known_args.environment_config if known_args.environment_config else None)
  extra_experiments = known_args.extra_experiments

  # This is defined here to only be run when we invoke this file explicitly.
  class FlinkRunnerTest(portable_runner_test.PortableRunnerTest):
    _use_grpc = True
    _use_subprocesses = True

    conf_dir = None
    expansion_port = None

    @classmethod
    def tearDownClass(cls):
      if cls.conf_dir and exists(cls.conf_dir):
        logging.info("removing conf dir: %s" % cls.conf_dir)
        rmtree(cls.conf_dir)
      super(FlinkRunnerTest, cls).tearDownClass()

    @classmethod
    def _create_conf_dir(cls):
      """Create (and save a static reference to) a "conf dir", used to provide
       metrics configs and verify metrics output

       It gets cleaned up when the suite is done executing"""

      if hasattr(cls, 'conf_dir'):
        cls.conf_dir = mkdtemp(prefix='flinktest-conf')

        # path for a FileReporter to write metrics to
        cls.test_metrics_path = path.join(cls.conf_dir, 'test-metrics.txt')

        # path to write Flink configuration to
        conf_path = path.join(cls.conf_dir, 'flink-conf.yaml')
        file_reporter = 'org.apache.beam.runners.flink.metrics.FileReporter'
        with open(conf_path, 'w') as f:
          f.write(linesep.join([
              'metrics.reporters: file',
              'metrics.reporter.file.class: %s' % file_reporter,
              'metrics.reporter.file.path: %s' % cls.test_metrics_path
          ]))

    @classmethod
    def _subprocess_command(cls, job_port, expansion_port):
      # will be cleaned up at the end of this method, and recreated and used by
      # the job server
      tmp_dir = mkdtemp(prefix='flinktest')

      cls._create_conf_dir()
      cls.expansion_port = expansion_port

      try:
        return [
            'java',
            '-jar', flink_job_server_jar,
            '--flink-master-url', '[local]',
            '--flink-conf-dir', cls.conf_dir,
            '--artifacts-dir', tmp_dir,
            '--job-port', str(job_port),
            '--artifact-port', '0',
            '--expansion-port', str(expansion_port),
        ]
      finally:
        rmtree(tmp_dir)

    @classmethod
    def get_runner(cls):
      return portable_runner.PortableRunner()

    def create_options(self):
      options = super(FlinkRunnerTest, self).create_options()
      options.view_as(DebugOptions).experiments = [
          'beam_fn_api'] + extra_experiments
      options._all_options['parallelism'] = 1
      options._all_options['shutdown_sources_on_final_watermark'] = True
      options.view_as(PortableOptions).environment_type = (
          environment_type.upper())
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

    def test_external_transform(self):
      options = self.create_options()
      options._all_options['parallelism'] = 1
      options._all_options['streaming'] = True

      expansion_address = "localhost:" + str(FlinkRunnerTest.expansion_port)

      with self.create_pipeline() as p:
        res = (
            p
            | GenerateSequence(start=1, stop=10,
                               expansion_service=expansion_address))

        assert_that(res, equal_to([i for i in range(1, 10)]))

    def test_flattened_side_input(self):
      # Blocked on support for transcoding
      # https://jira.apache.org/jira/browse/BEAM-6523
      super(FlinkRunnerTest, self).test_flattened_side_input(
          with_transcoding=False)

    def test_metrics(self):
      """Run a simple DoFn that increments a counter, and verify that its
       expected value is written to a temporary file by the FileReporter"""

      counter_name = 'elem_counter'

      class DoFn(beam.DoFn):
        def __init__(self):
          self.counter = Metrics.counter(self.__class__, counter_name)
          logging.info('counter: %s' % self.counter.metric_name)

        def process(self, v):
          self.counter.inc()

      p = self.create_pipeline()
      n = 100

      # pylint: disable=expression-not-assigned
      p \
      | beam.Create(list(range(n))) \
      | beam.ParDo(DoFn())

      result = p.run()
      result.wait_until_finish()

      with open(self.test_metrics_path, 'r') as f:
        lines = [line for line in f.readlines() if counter_name in line]
        self.assertEqual(
            len(lines), 1,
            msg='Expected 1 line matching "%s":\n%s' % (
                counter_name, '\n'.join(lines))
        )
        line = lines[0]
        self.assertTrue(
            '%s: 100' % counter_name in line,
            msg='Failed to find expected counter %s in line %s' % (
                counter_name, line)
        )

    def test_sdf(self):
      raise unittest.SkipTest("BEAM-2939")

    def test_callbacks_with_exception(self):
      raise unittest.SkipTest("BEAM-6868")

    def test_register_finalizations(self):
      raise unittest.SkipTest("BEAM-6868")

    # Inherits all other tests.

  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
