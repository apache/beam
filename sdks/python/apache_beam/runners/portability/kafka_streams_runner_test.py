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
import unittest
import uuid
from shutil import rmtree
from tempfile import mkdtemp

import pytest

from apache_beam.options.pipeline_options import KafkaStreamsRunnerOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability import portable_runner_test
from apache_beam.utils import subprocess_server

# Runs Beam's portable ValidatesRunner suite against the Kafka Streams runner, which is what shows
# the runner works for a pipeline that was not written in Java.
#
# Needs a Kafka broker, unlike the Flink and Spark suites, because the runner executes on Kafka
# rather than on a cluster of its own. Point it at one with --bootstrap_servers.
#
# Run as
#
# pytest kafka_streams_runner_test.py[::TestClass::test_case] \
#     --test-pipeline-options="--bootstrap_servers=localhost:9092"

_LOGGER = logging.getLogger(__name__)


class KafkaStreamsRunnerTest(portable_runner_test.PortableRunnerTest):
  _use_grpc = True
  _use_subprocesses = True

  expansion_port = None
  kafka_streams_job_server_jar = None
  bootstrap_servers = 'localhost:9092'
  environment_type = 'LOOPBACK'
  environment_options = None

  @pytest.fixture(autouse=True)
  def parse_options(self, request):
    if not request.config.option.test_pipeline_options:
      raise unittest.SkipTest(
          'Skipping because --test-pipeline-options is not specified.')
    test_pipeline_options = request.config.option.test_pipeline_options
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        '--kafka_streams_job_server_jar',
        help='Job server jar to submit jobs.',
        action='store')
    parser.add_argument(
        '--bootstrap_servers',
        default='localhost:9092',
        help='Kafka the runner executes on, and creates its own topics in.')
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
            'Recognized options depend on --environment_type.'))
    known_args, unknown_args = parser.parse_known_args(
        shlex.split(test_pipeline_options))
    if unknown_args:
      _LOGGER.warning('Discarding unrecognized arguments %s' % unknown_args)
    self.set_kafka_streams_job_server_jar(
        known_args.kafka_streams_job_server_jar or
        job_server.JavaJarJobServer.path_to_beam_jar(
            ':runners:kafka-streams:job-server:shadowJar'))
    type(self).bootstrap_servers = known_args.bootstrap_servers
    self.environment_type = known_args.environment_type
    self.environment_options = known_args.environment_options

  @classmethod
  def _subprocess_command(cls, job_port, expansion_port):
    # Created and used by the job server; removed here so the job server makes it itself.
    tmp_dir = mkdtemp(prefix='kafkastreamstest')

    cls.expansion_port = expansion_port

    try:
      return [
          subprocess_server.JavaHelper.get_java(),
          '-jar',
          cls.kafka_streams_job_server_jar,
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
    return 'localhost:%s' % cls.expansion_port

  @classmethod
  def set_kafka_streams_job_server_jar(cls, kafka_streams_job_server_jar):
    cls.kafka_streams_job_server_jar = kafka_streams_job_server_jar

  def create_options(self):
    options = super().create_options()
    options.view_as(PortableOptions).environment_type = self.environment_type
    options.view_as(
        PortableOptions).environment_options = self.environment_options

    kafka_streams_options = options.view_as(KafkaStreamsRunnerOptions)
    kafka_streams_options.bootstrap_servers = self.bootstrap_servers
    # A fresh application id per pipeline. The id names the consumer group and the runner's own
    # topics, so reusing one would have a test resume another test's offsets and read its data.
    kafka_streams_options.application_id = 'beam-vr-%s' % uuid.uuid4()
    return options

  # ---------------------------------------------------------------------------
  # Features the runner does not support yet. Each skip points at the issue that
  # would implement it, so this list doubles as the runner's capability gaps.
  # ---------------------------------------------------------------------------

  def test_pardo_side_inputs(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_pardo_windowed_side_inputs(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_flattened_side_input(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_multimap_side_input(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_multimap_multiside_input(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_multimap_side_input_type_coercion(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_pardo_unfusable_side_inputs(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_pardo_state_only(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_pardo_timers(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_pardo_timers_clear(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_pardo_state_timers(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_pardo_state_timers_non_standard_coder(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_windowed_pardo_state_timers(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_pardo_dynamic_timer(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_custom_merging_window(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39630")

  def test_custom_window_type(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39630")

  def test_sdf_with_watermark_tracking(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39631")

  def test_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39631")

  def test_sdf_synthetic_source(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39631")

  def test_sdf_with_dofn_as_watermark_estimator(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39631")

  def test_callbacks_with_exception(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/18479")

  def test_register_finalizations(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/18479")

  def test_batch_pardo_fusion_break(self):
    # CombineGlobally expands to a stage with side inputs.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_batch_to_element_pardo(self):
    # CombineGlobally expands to a stage with side inputs.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_gbk_side_input(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_pack_combiners(self):
    # The packed combiners are CombineGlobally, which needs side inputs.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_pardo_side_input_dependencies(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_pardo_unfusable_side_inputs_with_separation(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39628")

  def test_pardo_state_with_custom_key_coder(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_pardo_et_timer_with_no_firing(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_pardo_et_timer_with_no_reset(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_pardo_et_timer_with_no_reset_and_no_clear(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39629")

  def test_windowing(self):
    # Sessions, which are merging windows.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39630")

  def test_windowed_combine_per_key(self):
    # The fixed and sliding parts pass; the sessions part does not, sessions being merging windows.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39630")

  def test_reshuffle_after_custom_window(self):
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39630")

  def test_metrics(self):
    # The runner reports attempted values, which do reach a Python pipeline; this asserts on
    # committed ones.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39635")

  def test_read(self):
    # The runner reads sources through the deprecated primitive Read, which carries a serialized
    # Java source; a Python pipeline's source is a Python object, so reading one needs splittable
    # DoFn support rather than anything specific to Read.
    raise unittest.SkipTest("https://github.com/apache/beam/issues/39631")

  # Inherits all other tests from PortableRunnerTest.


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
