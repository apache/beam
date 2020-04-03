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
from os import linesep
from os import path
from os.path import exists
from shutil import rmtree
from tempfile import mkdtemp

import apache_beam as beam
from apache_beam import Impulse
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.coders import VarIntCoder
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.io.external.kafka import ReadFromKafka
from apache_beam.io.external.kafka import WriteToKafka
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import portable_runner_test
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import userstate

_LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
  # Run as
  #
  # python -m apache_beam.runners.portability.flink_runner_test \
  #     --flink_job_server_jar=/path/to/job_server.jar \
  #     --environment_type=docker \
  #     --extra_experiments=beam_experiments \
  #     [FlinkRunnerTest.test_method, ...]

  parser = argparse.ArgumentParser(add_help=True)
  parser.add_argument(
      '--flink_job_server_jar', help='Job server jar to submit jobs.')
  parser.add_argument(
      '--streaming',
      default=False,
      action='store_true',
      help='Job type. batch or streaming')
  parser.add_argument(
      '--environment_type',
      default='docker',
      help='Environment type. docker or process')
  parser.add_argument('--environment_config', help='Environment config.')
  parser.add_argument(
      '--extra_experiments',
      default=[],
      action='append',
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
        _LOGGER.info("removing conf dir: %s" % cls.conf_dir)
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
          f.write(
              linesep.join([
                  'metrics.reporters: file',
                  'metrics.reporter.file.class: %s' % file_reporter,
                  'metrics.reporter.file.path: %s' % cls.test_metrics_path,
                  'metrics.scope.operator: <operator_name>',
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
            '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn',
            '-jar',
            flink_job_server_jar,
            '--flink-master',
            '[local]',
            '--flink-conf-dir',
            cls.conf_dir,
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
      options = super(FlinkRunnerTest, self).create_options()
      options.view_as(
          DebugOptions).experiments = ['beam_fn_api'] + extra_experiments
      options._all_options['parallelism'] = 2
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

    def test_external_transforms(self):
      # TODO Move expansion address resides into PipelineOptions
      def get_expansion_service():
        return "localhost:" + str(self.expansion_port)

      with self.create_pipeline() as p:
        res = (
            p
            | GenerateSequence(
                start=1, stop=10, expansion_service=get_expansion_service()))

        assert_that(res, equal_to([i for i in range(1, 10)]))

      # We expect to fail here because we do not have a Kafka cluster handy.
      # Nevertheless, we check that the transform is expanded by the
      # ExpansionService and that the pipeline fails during execution.
      with self.assertRaises(Exception) as ctx:
        with self.create_pipeline() as p:
          # pylint: disable=expression-not-assigned
          (
              p
              | ReadFromKafka(
                  consumer_config={
                      'bootstrap.servers': 'notvalid1:7777, notvalid2:3531'
                  },
                  topics=['topic1', 'topic2'],
                  key_deserializer='org.apache.kafka.'
                  'common.serialization.'
                  'ByteArrayDeserializer',
                  value_deserializer='org.apache.kafka.'
                  'common.serialization.'
                  'LongDeserializer',
                  expansion_service=get_expansion_service()))
      self.assertTrue(
          'No resolvable bootstrap urls given in bootstrap.servers' in str(
              ctx.exception),
          'Expected to fail due to invalid bootstrap.servers, but '
          'failed due to:\n%s' % str(ctx.exception))

      # We just test the expansion but do not execute.
      # pylint: disable=expression-not-assigned
      (
          self.create_pipeline()
          | Impulse()
          | Map(lambda input: (1, input))
          | WriteToKafka(
              producer_config={
                  'bootstrap.servers': 'localhost:9092, notvalid2:3531'
              },
              topic='topic1',
              key_serializer='org.apache.kafka.'
              'common.serialization.'
              'LongSerializer',
              value_serializer='org.apache.kafka.'
              'common.serialization.'
              'ByteArraySerializer',
              expansion_service=get_expansion_service()))

    def test_flattened_side_input(self):
      # Blocked on support for transcoding
      # https://jira.apache.org/jira/browse/BEAM-6523
      super(FlinkRunnerTest,
            self).test_flattened_side_input(with_transcoding=False)

    def test_metrics(self):
      """Run a simple DoFn that increments a counter and verifies state
      caching metrics. Verifies that its expected value is written to a
      temporary file by the FileReporter"""

      counter_name = 'elem_counter'
      state_spec = userstate.BagStateSpec('state', VarIntCoder())

      class DoFn(beam.DoFn):
        def __init__(self):
          self.counter = Metrics.counter(self.__class__, counter_name)
          _LOGGER.info('counter: %s' % self.counter.metric_name)

        def process(self, kv, state=beam.DoFn.StateParam(state_spec)):
          # Trigger materialization
          list(state.read())
          state.add(1)
          self.counter.inc()

      options = self.create_options()
      # Test only supports parallelism of 1
      options._all_options['parallelism'] = 1
      # Create multiple bundles to test cache metrics
      options._all_options['max_bundle_size'] = 10
      options._all_options['max_bundle_time_millis'] = 95130590130
      experiments = options.view_as(DebugOptions).experiments or []
      experiments.append('state_cache_size=123')
      options.view_as(DebugOptions).experiments = experiments
      with Pipeline(self.get_runner(), options) as p:
        # pylint: disable=expression-not-assigned
        (
            p
            | "create" >> beam.Create(list(range(0, 110)))
            | "mapper" >> beam.Map(lambda x: (x % 10, 'val'))
            | "stateful" >> beam.ParDo(DoFn()))

      lines_expected = {'counter: 110'}
      if streaming:
        lines_expected.update([
            # Gauges for the last finished bundle
            'stateful.beam.metric:statecache:capacity: 123',
            # These are off by 10 because the first bundle contains all the keys
            # once. Caching is only initialized after the first bundle. Caching
            # depends on the cache token which is lazily initialized by the
            # Runner's StateRequestHandlers.
            'stateful.beam.metric:statecache:size: 20',
            'stateful.beam.metric:statecache:get: 10',
            'stateful.beam.metric:statecache:miss: 0',
            'stateful.beam.metric:statecache:hit: 10',
            'stateful.beam.metric:statecache:put: 0',
            'stateful.beam.metric:statecache:extend: 10',
            'stateful.beam.metric:statecache:evict: 0',
            # Counters
            # (total of get/hit will be off by 10 due to the cross-bundle
            # caching only getting initialized after the first bundle.
            # Cross-bundle caching depends on the cache token which is lazily
            # initialized by the Runner's StateRequestHandlers).
            # If cross-bundle caching is not requested, caching is done
            # at the bundle level.
            'stateful.beam.metric:statecache:get_total: 110',
            'stateful.beam.metric:statecache:miss_total: 20',
            'stateful.beam.metric:statecache:hit_total: 90',
            'stateful.beam.metric:statecache:put_total: 20',
            'stateful.beam.metric:statecache:extend_total: 110',
            'stateful.beam.metric:statecache:evict_total: 0',
        ])
      else:
        # Batch has a different processing model. All values for
        # a key are processed at once.
        lines_expected.update([
            # Gauges
            'stateful).beam.metric:statecache:capacity: 123',
            # For the first key, the cache token will not be set yet.
            # It's lazily initialized after first access in StateRequestHandlers
            'stateful).beam.metric:statecache:size: 10',
            # We have 11 here because there are 110 / 10 elements per key
            'stateful).beam.metric:statecache:get: 11',
            'stateful).beam.metric:statecache:miss: 1',
            'stateful).beam.metric:statecache:hit: 10',
            # State is flushed back once per key
            'stateful).beam.metric:statecache:put: 1',
            'stateful).beam.metric:statecache:extend: 1',
            'stateful).beam.metric:statecache:evict: 0',
            # Counters
            'stateful).beam.metric:statecache:get_total: 110',
            'stateful).beam.metric:statecache:miss_total: 10',
            'stateful).beam.metric:statecache:hit_total: 100',
            'stateful).beam.metric:statecache:put_total: 10',
            'stateful).beam.metric:statecache:extend_total: 10',
            'stateful).beam.metric:statecache:evict_total: 0',
        ])
      lines_actual = set()
      with open(self.test_metrics_path, 'r') as f:
        for line in f:
          for metric_str in lines_expected:
            metric_name = metric_str.split()[0]
            if metric_str in line:
              lines_actual.add(metric_str)
            elif metric_name in line:
              lines_actual.add(line)
      self.assertSetEqual(lines_actual, lines_expected)

    def test_sdf_with_watermark_tracking(self):
      raise unittest.SkipTest("BEAM-2939")

    def test_sdf_with_sdf_initiated_checkpointing(self):
      raise unittest.SkipTest("BEAM-2939")

    def test_callbacks_with_exception(self):
      raise unittest.SkipTest("BEAM-6868")

    def test_register_finalizations(self):
      raise unittest.SkipTest("BEAM-6868")

    # Inherits all other tests.

  class FlinkRunnerTestOptimized(FlinkRunnerTest):
    # TODO: Remove these tests after resolving BEAM-7248 and enabling
    #  PortableRunnerOptimized
    def create_options(self):
      options = super(FlinkRunnerTestOptimized, self).create_options()
      options.view_as(DebugOptions).experiments = [
          'pre_optimize=all'
      ] + options.view_as(DebugOptions).experiments
      return options

    def test_external_transforms(self):
      raise unittest.SkipTest("BEAM-7252")

  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
