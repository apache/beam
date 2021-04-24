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

import argparse
import logging
import shlex
import typing
import unittest
from os import linesep
from os import path
from os.path import exists
from shutil import rmtree
from tempfile import mkdtemp

import pytest

import apache_beam as beam
from apache_beam import Impulse
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.coders import VarIntCoder
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import FlinkRunnerOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import portable_runner_test
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import userstate
from apache_beam.transforms.sql import SqlTransform

# Run as
#
# pytest flink_runner_test.py[::TestClass::test_case] \
#     --test-pipeline-options="--environment_type=LOOPBACK"

_LOGGER = logging.getLogger(__name__)

Row = typing.NamedTuple("Row", [("col1", int), ("col2", str)])
beam.coders.registry.register_coder(Row, beam.coders.RowCoder)


class FlinkRunnerTest(portable_runner_test.PortableRunnerTest):
  _use_grpc = True
  _use_subprocesses = True

  conf_dir = None
  expansion_port = None
  flink_job_server_jar = None

  def __init__(self, *args, **kwargs):
    super(FlinkRunnerTest, self).__init__(*args, **kwargs)
    self.environment_type = None
    self.environment_config = None

  @pytest.fixture(autouse=True)
  def parse_options(self, request):
    if not request.config.option.test_pipeline_options:
      raise unittest.SkipTest(
          'Skipping because --test-pipeline-options is not specified.')
    test_pipeline_options = request.config.option.test_pipeline_options
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        '--flink_job_server_jar',
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
    self.set_flink_job_server_jar(
        known_args.flink_job_server_jar or
        job_server.JavaJarJobServer.path_to_beam_jar((
            ':runners:flink:%s:job-server:shadowJar' %
            FlinkRunnerOptions.PUBLISHED_FLINK_VERSIONS[-1])))
    self.environment_type = known_args.environment_type
    self.environment_options = known_args.environment_options

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
          cls.flink_job_server_jar,
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

  @classmethod
  def get_expansion_service(cls):
    # TODO Move expansion address resides into PipelineOptions
    return 'localhost:%s' % cls.expansion_port

  @classmethod
  def set_flink_job_server_jar(cls, flink_job_server_jar):
    cls.flink_job_server_jar = flink_job_server_jar

  def create_options(self):
    options = super(FlinkRunnerTest, self).create_options()
    options.view_as(DebugOptions).experiments = ['beam_fn_api']
    options._all_options['parallelism'] = 2
    options.view_as(PortableOptions).environment_type = self.environment_type
    options.view_as(
        PortableOptions).environment_options = self.environment_options

    return options

  # Can't read host files from within docker, read a "local" file there.
  def test_read(self):
    print('name:', __name__)
    with self.create_pipeline() as p:
      lines = p | beam.io.ReadFromText('/etc/profile')
      assert_that(lines, lambda lines: len(lines) > 0)

  def test_no_subtransform_composite(self):
    raise unittest.SkipTest("BEAM-4781")

  def test_external_transform(self):
    with self.create_pipeline() as p:
      res = (
          p
          | GenerateSequence(
              start=1, stop=10, expansion_service=self.get_expansion_service()))

      assert_that(res, equal_to([i for i in range(1, 10)]))

  def test_expand_kafka_read(self):
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
                    'bootstrap.servers': 'notvalid1:7777, notvalid2:3531',
                    'group.id': 'any_group'
                },
                topics=['topic1', 'topic2'],
                key_deserializer='org.apache.kafka.'
                'common.serialization.'
                'ByteArrayDeserializer',
                value_deserializer='org.apache.kafka.'
                'common.serialization.'
                'LongDeserializer',
                commit_offset_in_finalize=True,
                timestamp_policy=ReadFromKafka.create_time_policy,
                expansion_service=self.get_expansion_service()))
    self.assertTrue(
        'No resolvable bootstrap urls given in bootstrap.servers' in str(
            ctx.exception),
        'Expected to fail due to invalid bootstrap.servers, but '
        'failed due to:\n%s' % str(ctx.exception))

  def test_expand_kafka_write(self):
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
            expansion_service=self.get_expansion_service()))

  def test_sql(self):
    with self.create_pipeline() as p:
      output = (
          p
          | 'Create' >> beam.Create([Row(x, str(x)) for x in range(5)])
          | 'Sql' >> SqlTransform(
              """SELECT col1, col2 || '*' || col2 as col2,
                    power(col1, 2) as col3
             FROM PCOLLECTION
          """,
              expansion_service=self.get_expansion_service()))
      assert_that(
          output,
          equal_to([(x, '{x}*{x}'.format(x=x), x * x) for x in range(5)]))

  def test_flattened_side_input(self):
    # Blocked on support for transcoding
    # https://jira.apache.org/jira/browse/BEAM-6523
    super(FlinkRunnerTest,
          self).test_flattened_side_input(with_transcoding=False)

  def test_metrics(self):
    super(FlinkRunnerTest, self).test_metrics(check_gauge=False)

  def test_flink_metrics(self):
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
    if options.view_as(StandardOptions).streaming:
      lines_expected.update([
          # Gauges for the last finished bundle
          'stateful.beam.metric:statecache:capacity: 123',
          'stateful.beam.metric:statecache:size: 10',
          'stateful.beam.metric:statecache:get: 20',
          'stateful.beam.metric:statecache:miss: 0',
          'stateful.beam.metric:statecache:hit: 20',
          'stateful.beam.metric:statecache:put: 0',
          'stateful.beam.metric:statecache:evict: 0',
          # Counters
          'stateful.beam.metric:statecache:get_total: 220',
          'stateful.beam.metric:statecache:miss_total: 10',
          'stateful.beam.metric:statecache:hit_total: 210',
          'stateful.beam.metric:statecache:put_total: 10',
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
          'stateful).beam.metric:statecache:get: 12',
          'stateful).beam.metric:statecache:miss: 1',
          'stateful).beam.metric:statecache:hit: 11',
          # State is flushed back once per key
          'stateful).beam.metric:statecache:put: 1',
          'stateful).beam.metric:statecache:evict: 0',
          # Counters
          'stateful).beam.metric:statecache:get_total: 120',
          'stateful).beam.metric:statecache:miss_total: 10',
          'stateful).beam.metric:statecache:hit_total: 110',
          'stateful).beam.metric:statecache:put_total: 10',
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

  def test_callbacks_with_exception(self):
    raise unittest.SkipTest("BEAM-11021")

  def test_register_finalizations(self):
    raise unittest.SkipTest("BEAM-11021")

  def test_custom_merging_window(self):
    raise unittest.SkipTest("BEAM-11004")

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

  def test_external_transform(self):
    raise unittest.SkipTest("BEAM-7252")

  def test_expand_kafka_read(self):
    raise unittest.SkipTest("BEAM-7252")

  def test_expand_kafka_write(self):
    raise unittest.SkipTest("BEAM-7252")

  def test_sql(self):
    raise unittest.SkipTest("BEAM-7252")

  def test_pack_combiners_disabled_by_default(self):
    raise unittest.SkipTest(
        "Base test has expectations on counter names that fail because "
        "stage fusion modifies counter names")


class FlinkRunnerTestStreaming(FlinkRunnerTest):
  def __init__(self, *args, **kwargs):
    super(FlinkRunnerTestStreaming, self).__init__(*args, **kwargs)
    self.enable_commit = False

  def setUp(self):
    self.enable_commit = False

  def create_options(self):
    options = super(FlinkRunnerTestStreaming, self).create_options()
    options.view_as(StandardOptions).streaming = True
    if self.enable_commit:
      options._all_options['checkpointing_interval'] = 3000
      options._all_options['shutdown_sources_after_idle_ms'] = 60000
    return options

  def test_callbacks_with_exception(self):
    self.enable_commit = True
    super(FlinkRunnerTest, self).test_callbacks_with_exception()

  def test_register_finalizations(self):
    self.enable_commit = True
    super(FlinkRunnerTest, self).test_register_finalizations()


if __name__ == '__main__':
  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
