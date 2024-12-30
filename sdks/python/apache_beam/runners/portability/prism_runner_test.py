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
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.runners.portability import portable_runner_test
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window
from apache_beam.utils import timestamp

# Run as
#
# pytest prism_runner_test.py[::TestClass::test_case] \
#     --test-pipeline-options="--environment_type=LOOPBACK"

_LOGGER = logging.getLogger(__name__)

Row = typing.NamedTuple("Row", [("col1", int), ("col2", str)])
beam.coders.registry.register_coder(Row, beam.coders.RowCoder)


class PrismRunnerTest(portable_runner_test.PortableRunnerTest):
  _use_grpc = True
  _use_subprocesses = True

  conf_dir = None
  expansion_port = None

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.environment_type = None
    self.environment_config = None
    self.enable_commit = False

  def setUp(self):
    self.enable_commit = False

  @pytest.fixture(autouse=True)
  def parse_options(self, request):
    if not request.config.option.test_pipeline_options:
      raise unittest.SkipTest(
          'Skipping because --test-pipeline-options is not specified.')
    test_pipeline_options = request.config.option.test_pipeline_options
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        '--prism_bin', help='Prism binary to submit jobs.', action='store')
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
    self.set_prism_bin(known_args.prism_bin)
    self.environment_type = known_args.environment_type
    self.environment_options = known_args.environment_options

  @classmethod
  def tearDownClass(cls):
    if cls.conf_dir and exists(cls.conf_dir):
      _LOGGER.info("removing conf dir: %s" % cls.conf_dir)
      rmtree(cls.conf_dir)
    super().tearDownClass()

  @classmethod
  def _create_conf_dir(cls):
    """Create (and save a static reference to) a "conf dir", used to provide
     metrics configs and verify metrics output

     It gets cleaned up when the suite is done executing"""

    if hasattr(cls, 'conf_dir'):
      cls.conf_dir = mkdtemp(prefix='prismtest-conf')

      # path for a FileReporter to write metrics to
      cls.test_metrics_path = path.join(cls.conf_dir, 'test-metrics.txt')

      # path to write Prism configuration to
      conf_path = path.join(cls.conf_dir, 'prism-conf.yaml')
      file_reporter = 'org.apache.beam.runners.prism.metrics.FileReporter'
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
    tmp_dir = mkdtemp(prefix='prismtest')

    cls._create_conf_dir()
    cls.expansion_port = expansion_port

    try:
      return [
          cls.prism_bin,
          '--job_port',
          str(job_port),
      ]
    finally:
      rmtree(tmp_dir)

  @classmethod
  def get_expansion_service(cls):
    # TODO Move expansion address resides into PipelineOptions
    return 'localhost:%s' % cls.expansion_port

  @classmethod
  def set_prism_bin(cls, prism_bin):
    cls.prism_bin = prism_bin

  def create_options(self):
    options = super().create_options()
    options.view_as(DebugOptions).experiments = ['beam_fn_api']
    options.view_as(DebugOptions).experiments = [
        'pre_optimize=default'
    ] + options.view_as(DebugOptions).experiments
    options.view_as(PortableOptions).environment_type = self.environment_type
    options.view_as(
        PortableOptions).environment_options = self.environment_options

    return options

  # Slightly more robust session window test:
  # Validates that an inner grouping doesn't duplicate data either.
  # Copied also because the timestamp in fn_runner_test.py isn't being
  # inferred correctly as seconds for some reason, but as micros.
  # The belabored specification is validating the timestamp type works at least.
  # See https://github.com/apache/beam/issues/32085
  def test_windowing(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create([1, 2, 100, 101, 102, 123])
          | beam.Map(
              lambda t: window.TimestampedValue(
                  ('k', t), timestamp.Timestamp.of(t).micros))
          | beam.WindowInto(beam.transforms.window.Sessions(10))
          | beam.GroupByKey()
          | beam.Map(lambda k_vs1: (k_vs1[0], sorted(k_vs1[1]))))
      assert_that(
          res, equal_to([('k', [1, 2]), ('k', [100, 101, 102]), ('k', [123])]))

  # Can't read host files from within docker, read a "local" file there.
  def test_read(self):
    print('name:', __name__)
    with self.create_pipeline() as p:
      lines = p | beam.io.ReadFromText('/etc/profile')
      assert_that(lines, lambda lines: len(lines) > 0)

  def test_external_transform(self):
    raise unittest.SkipTest("Requires an expansion service to execute.")

  def test_expand_kafka_read(self):
    raise unittest.SkipTest("Requires an expansion service to execute.")

  def test_expand_kafka_write(self):
    raise unittest.SkipTest("Requires an expansion service to execute.")

  def test_sql(self):
    raise unittest.SkipTest("Requires an expansion service to execute.")

  # The following tests require additional implementation in Prism.

  def test_custom_merging_window(self):
    raise unittest.SkipTest(
        "Requires Prism to support Custom Window " +
        "Coders, and Merging Custom Windows. " +
        "https://github.com/apache/beam/issues/31921")

  def test_custom_window_type(self):
    raise unittest.SkipTest(
        "Requires Prism to support Custom Window Coders." +
        " https://github.com/apache/beam/issues/31921")

  def test_pack_combiners(self):
    raise unittest.SkipTest(
        "Requires Prism to support coder:" +
        " 'beam:coder:tuple:v1'. https://github.com/apache/beam/issues/32636")

  def test_metrics(self):
    super().test_metrics(check_bounded_trie=False)


# Inherits all other tests.

if __name__ == '__main__':
  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
