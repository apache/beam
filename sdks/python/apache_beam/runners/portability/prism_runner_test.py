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
import os.path
import shlex
import typing
import unittest
import zipfile
from os import linesep
from os.path import exists
from shutil import rmtree
from tempfile import mkdtemp
from unittest import mock

import pytest
from parameterized import parameterized

import apache_beam as beam
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.runners.portability import portable_runner_test
from apache_beam.runners.portability import prism_runner
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.utils import shared

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
    self.streaming = False
    self.allow_unsafe_triggers = False

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
      cls.test_metrics_path = os.path.join(cls.conf_dir, 'test-metrics.txt')

      # path to write Prism configuration to
      conf_path = os.path.join(cls.conf_dir, 'prism-conf.yaml')
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

    options.view_as(StandardOptions).streaming = self.streaming
    options.view_as(
        TypeOptions).allow_unsafe_triggers = self.allow_unsafe_triggers
    return options

  # Can't read host files from within docker, read a "local" file there.
  def test_read(self):
    print('name:', __name__)
    with self.create_pipeline() as p:
      lines = p | beam.io.ReadFromText('/etc/profile')
      assert_that(lines, lambda lines: len(lines) > 0)

  def test_create_transform(self):
    with self.create_pipeline() as p:
      out = (p | beam.Create([1]))
      assert_that(out, equal_to([1]))

    with self.create_pipeline() as p:
      out = (p | beam.Create([1, 2], reshuffle=False))
      assert_that(out, equal_to([1, 2]))

    with self.create_pipeline() as p:
      out = (p | beam.Create([1, 2], reshuffle=True))
      assert_that(out, equal_to([1, 2]))

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

  def test_metrics(self):
    super().test_metrics(check_bounded_trie=False)

  def construct_timestamped(k, t):
    return window.TimestampedValue((k, t), t)

  def format_result(k, vs):
    return ('%s-%s' % (k, len(list(vs))), set(vs))

  def test_after_count_trigger_batch(self):
    self.allow_unsafe_triggers = True
    with self.create_pipeline() as p:
      result = (
          p
          | beam.Create([1, 2, 3, 4, 5, 10, 11])
          | beam.FlatMap(lambda t: [('A', t), ('B', t + 5)])
          #A1, A2, A3, A4, A5, A10, A11, B6, B7, B8, B9, B10, B15, B16
          | beam.MapTuple(PrismRunnerTest.construct_timestamped)
          | beam.WindowInto(
              window.FixedWindows(10),
              trigger=trigger.AfterCount(3),
              accumulation_mode=trigger.AccumulationMode.DISCARDING,
          )
          | beam.GroupByKey()
          | beam.MapTuple(PrismRunnerTest.format_result))
      assert_that(
          result,
          equal_to(
              list([
                  ('A-5', {1, 2, 3, 4, 5}),
                  ('A-2', {10, 11}),
                  ('B-4', {6, 7, 8, 9}),
                  ('B-3', {10, 15, 16}),
              ])))

  def test_after_count_trigger_streaming(self):
    self.allow_unsafe_triggers = True
    self.streaming = True
    with self.create_pipeline() as p:
      result = (
          p
          | beam.Create([1, 2, 3, 4, 5, 10, 11])
          | beam.FlatMap(lambda t: [('A', t), ('B', t + 5)])
          #A1, A2, A3, A4, A5, A10, A11, B6, B7, B8, B9, B10, B15, B16
          | beam.MapTuple(PrismRunnerTest.construct_timestamped)
          | beam.WindowInto(
              window.FixedWindows(10),
              trigger=trigger.AfterCount(3),
              accumulation_mode=trigger.AccumulationMode.DISCARDING,
          )
          | beam.GroupByKey()
          | beam.MapTuple(PrismRunnerTest.format_result))
      assert_that(
          result,
          equal_to(
              list([
                  ('A-3', {1, 2, 3}),
                  ('A-2', {4, 5}),
                  ('A-2', {10, 11}),
                  ('B-3', {6, 7, 8}),
                  ('B-1', {9}),
                  ('B-3', {10, 15, 16}),
              ])))


class PrismJobServerTest(unittest.TestCase):
  def setUp(self) -> None:
    self.local_dir = mkdtemp()
    self.cache_dir = os.path.join(self.local_dir, "cache")
    os.mkdir(self.cache_dir)

    self.job_server = prism_runner.PrismJobServer(options=PortableOptions())
    self.local_bin_path = os.path.join(self.local_dir, "my_prism_bin")
    self.local_zip_path = os.path.join(self.local_dir, "my_prism_bin.zip")
    self.cache_bin_path = os.path.join(self.cache_dir, "my_prism_bin")
    self.cache_zip_path = os.path.join(self.cache_dir, "my_prism_bin.zip")
    self.remote_zip_path = "https://github.com/apache/beam/releases/download/fake_ver/my_prism_bin.zip"  # pylint: disable=line-too-long

  def tearDown(self) -> None:
    rmtree(self.local_dir)
    pass

  def _make_local_bin(self, fn=None):
    fn = fn or self.local_bin_path
    with open(fn, 'wb'):
      pass

  def _make_local_zip(self, fn=None):
    fn = fn or self.local_zip_path
    with zipfile.ZipFile(fn, 'w', zipfile.ZIP_DEFLATED):
      pass

  def _make_cache_bin(self, fn=None):
    fn = fn or self.cache_bin_path
    with open(fn, 'wb'):
      pass

  def _make_cache_zip(self, fn=None):
    fn = fn or self.cache_zip_path
    with zipfile.ZipFile(fn, 'w', zipfile.ZIP_DEFLATED):
      pass

  def _extract_side_effect(self, fn, path=None):
    if path is None:
      return fn

    full_path = os.path.join(str(path), fn)
    if path.startswith(self.cache_dir):
      self._make_cache_bin(full_path)
    else:
      self._make_local_bin(full_path)

    return full_path

  @parameterized.expand([[True, True], [True, False], [False, True],
                         [False, False]])
  def test_with_unknown_path(self, custom_bin_cache, ignore_cache):
    self.assertRaises(
        FileNotFoundError, lambda: self.job_server.local_bin(
            "/path/unknown", bin_cache=self.cache_dir
            if custom_bin_cache else '', ignore_cache=ignore_cache))

  @parameterized.expand([
      [True, True, True],
      [True, True, False],
      [True, False, True],
      [True, False, False],
      [False, True, True],
      [False, True, False],
      [False, False, True],
      [False, False, False],
  ])
  def test_with_local_binary_and_zip(
      self, has_cache_bin, has_cache_zip, ignore_cache):
    self._make_local_bin()
    self._make_local_zip()
    if has_cache_bin:
      self._make_cache_bin()
    if has_cache_zip:
      self._make_cache_zip()

    with mock.patch('zipfile.is_zipfile') as mock_is_zipfile:
      with mock.patch('zipfile.ZipFile') as mock_zipfile_init:
        mock_zipfile = mock.MagicMock()
        mock_zipfile.extract = mock.Mock(side_effect=self._extract_side_effect)
        mock_zipfile_init.return_value = mock_zipfile

        # path is set to local binary
        # always use local binary even if we have a cached copy, no unzipping
        mock_is_zipfile.return_value = False
        self.assertEqual(
            self.job_server.local_bin(
                self.local_bin_path, self.cache_dir, ignore_cache),
            self.local_bin_path)

        mock_zipfile_init.assert_not_called()

        # path is set to local zip
        # use local zip and unzip only if cache binary not available or
        # ignore_cache is true
        mock_is_zipfile.return_value = True
        self.assertEqual(
            self.job_server.local_bin(
                self.local_zip_path, self.cache_dir, ignore_cache),
            self.cache_bin_path)

        if has_cache_bin and not ignore_cache:
          # if cache is enabled and binary is in cache, we wont't unzip
          mock_zipfile_init.assert_not_called()
        else:
          mock_zipfile_init.assert_called_once()
          mock_zipfile_init.reset_mock()

  @parameterized.expand([
      [True, True, True],
      [True, True, False],
      [True, False, True],
      [True, False, False],
      [False, True, True],
      [False, True, False],
      [False, False, True],
      [False, False, False],
  ])
  def test_with_remote_path(self, has_cache_bin, has_cache_zip, ignore_cache):
    if has_cache_bin:
      self._make_cache_bin()
    if has_cache_zip:
      self._make_cache_zip()

    with mock.patch(
        'apache_beam.runners.portability.prism_runner.urlopen') as mock_urlopen:
      mock_response = mock.MagicMock()
      if has_cache_zip:
        with open(self.cache_zip_path, 'rb') as f:
          mock_response.read.side_effect = [f.read(), b'']
      else:
        mock_response.read.return_value = b''
      mock_urlopen.return_value = mock_response
      with mock.patch('zipfile.is_zipfile') as mock_is_zipfile:
        with mock.patch('zipfile.ZipFile') as mock_zipfile_init:
          mock_zipfile = mock.MagicMock()
          mock_zipfile.extract = mock.Mock(
              side_effect=self._extract_side_effect)
          mock_zipfile_init.return_value = mock_zipfile
          mock_is_zipfile.return_value = True
          self.assertEqual(
              self.job_server.local_bin(
                  self.remote_zip_path,
                  self.cache_dir,
                  ignore_cache=ignore_cache),
              self.cache_bin_path)

          if has_cache_zip and not ignore_cache:
            # if cache is enabled and zip is in cache, we wont't download
            mock_urlopen.assert_not_called()
          else:
            mock_urlopen.assert_called_once()

          if has_cache_bin and has_cache_zip and not ignore_cache:
            # if cache is enabled and both binary and zip are in cache, we
            # wont't unzip
            mock_zipfile_init.assert_not_called()
          else:
            mock_zipfile_init.assert_called_once()


class PrismRunnerSingletonTest(unittest.TestCase):
  @parameterized.expand([True, False])
  def test_singleton(self, enable_singleton):
    if enable_singleton:
      options = DebugOptions()  # prism singleton is enabled by default
    else:
      options = DebugOptions(["--experiment=disable_prism_server_singleton"])

    runner = prism_runner.PrismRunner()
    with mock.patch(
        'apache_beam.runners.portability.prism_runner.PrismJobServer'
    ) as mock_prism_server:

      # Reset the class-level singleton for every fresh run
      prism_runner.PrismRunner.shared_handle = shared.Shared()

      runner = prism_runner.PrismRunner()
      runner.default_job_server(options)

      mock_prism_server.assert_called_once()
      mock_prism_server.reset_mock()

      runner = prism_runner.PrismRunner()
      runner.default_job_server(options)
      if enable_singleton:
        # If singleton is enabled, we won't try to create a new server for the
        # second run.
        mock_prism_server.assert_not_called()
      else:
        mock_prism_server.assert_called_once()


if __name__ == '__main__':
  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
