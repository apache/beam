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

"""Tests for apache_beam.runners.worker.sdk_worker_main."""

# pytype: skip-file

import io
import logging
import os
import unittest

from hamcrest import all_of
from hamcrest import assert_that
from hamcrest import has_entry

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.runners.worker import sdk_worker_main
from apache_beam.runners.worker import worker_status
from apache_beam.utils.plugin import BeamPlugin


class SdkWorkerMainTest(unittest.TestCase):

  # Used for testing newly added flags.
  class MockOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--eam:option:m_option:v', help='mock option')
      parser.add_argument('--eam:option:m_option:v1', help='mock option')
      parser.add_argument('--beam:option:m_option:v', help='mock option')
      parser.add_argument('--m_flag', action='store_true', help='mock flag')
      parser.add_argument('--m_option', help='mock option')
      parser.add_argument(
          '--m_m_option', action='append', help='mock multi option')

  def test_status_server(self):
    # Wrapping the method to see if it appears in threadump
    def wrapped_method_for_test():
      threaddump = worker_status.thread_dump()
      self.assertRegex(threaddump, '.*wrapped_method_for_test.*')

    wrapped_method_for_test()

  def test_parse_pipeline_options(self):
    assert_that(
        sdk_worker_main._parse_pipeline_options(
            '{"options": {' + '"m_option": "/tmp/requirements.txt", ' +
            '"m_m_option":["beam_fn_api"]' + '}}').get_all_options(),
        all_of(
            has_entry('m_m_option', ['beam_fn_api']),
            has_entry('m_option', '/tmp/requirements.txt')))

    assert_that(
        sdk_worker_main._parse_pipeline_options(
            '{"beam:option:m_option:v1": "/tmp/requirements.txt", ' +
            '"beam:option:m_m_option:v1":["beam_fn_api"]}').get_all_options(),
        all_of(
            has_entry('m_m_option', ['beam_fn_api']),
            has_entry('m_option', '/tmp/requirements.txt')))

    assert_that(
        sdk_worker_main._parse_pipeline_options(
            '{"options": {"beam:option:m_option:v":"mock_val"}}').
        get_all_options(),
        has_entry('beam:option:m_option:v', 'mock_val'))

    assert_that(
        sdk_worker_main._parse_pipeline_options(
            '{"options": {"eam:option:m_option:v1":"mock_val"}}').
        get_all_options(),
        has_entry('eam:option:m_option:v1', 'mock_val'))

    assert_that(
        sdk_worker_main._parse_pipeline_options(
            '{"options": {"eam:option:m_option:v":"mock_val"}}').
        get_all_options(),
        has_entry('eam:option:m_option:v', 'mock_val'))

  def test_runtime_values(self):
    test_runtime_provider = RuntimeValueProvider('test_param', int, None)
    sdk_worker_main.create_harness({
        'CONTROL_API_SERVICE_DESCRIPTOR': '',
        'PIPELINE_OPTIONS': '{"test_param": 37}',
    },
                                   dry_run=True)
    self.assertTrue(test_runtime_provider.is_accessible())
    self.assertEqual(test_runtime_provider.get(), 37)

  def test_create_sdk_harness_log_handler_received_log(self):
    # tests that the log handler created in create_harness() does not miss
    # logs emitted from create_harness() itself.
    logstream = io.StringIO()

    class InMemoryHandler(logging.StreamHandler):
      def __init__(self, *unused):
        super().__init__(stream=logstream)

    with unittest.mock.patch(
        'apache_beam.runners.worker.sdk_worker_main.FnApiLogRecordHandler',
        InMemoryHandler):
      sdk_worker_main.create_harness({
          'LOGGING_API_SERVICE_DESCRIPTOR': '',
          'CONTROL_API_SERVICE_DESCRIPTOR': '',
          'PIPELINE_OPTIONS': '{"default_sdk_harness_log_level":"INVALID",'
          '"sdk_harness_log_level_overrides":[]}',
      },
                                     dry_run=True)
    logstream.seek(0)
    logs = logstream.read()
    self.assertIn('Unknown log level', logs)
    self.assertIn('Unable to parse sdk_harness_log_level_overrides', logs)

  def test_import_beam_plugins(self):
    sdk_worker_main._import_beam_plugins(BeamPlugin.get_all_plugin_paths())

  @staticmethod
  def _overrides_case_to_option_dict(case):
    """
    Return logging level overrides from command line strings via PipelineOption.
    """
    options_list = []
    for c in case:
      options_list += ['--sdk_harness_log_level_overrides', c]
    options = PipelineOptions(options_list)
    return options.get_all_options()

  def test__get_log_level_from_options_dict(self):
    test_cases = [
        {},
        {
            'default_sdk_harness_log_level': 'DEBUG'
        },
        {
            'default_sdk_harness_log_level': '30'
        },
        {
            'default_sdk_harness_log_level': 'INVALID_ENTRY'
        },
    ]
    expected_results = [logging.INFO, logging.DEBUG, 30, logging.INFO]
    for case, expected in zip(test_cases, expected_results):
      self.assertEqual(
          sdk_worker_main._get_log_level_from_options_dict(case), expected)

  def test__set_log_level_overrides(self):
    test_cases = [
        ([], {}),  # not provided, as a smoke test
        (
            # single overrides
            ['{"fake_module_1a.b":"DEBUG","fake_module_1c.d":"INFO"}'],
            {
                "fake_module_1a.b": logging.DEBUG,
                "fake_module_1a.b.f": logging.DEBUG,
                "fake_module_1c.d": logging.INFO
            }),
        (
            # multiple overrides, the last takes precedence
            [
                '{"fake_module_2a.b":"DEBUG"}',
                '{"fake_module_2c.d":"WARNING","fake_module_2c.d.e":15}',
                '{"fake_module_2c.d":"ERROR"}'
            ],
            {
                "fake_module_2a.b": logging.DEBUG,
                "fake_module_2a.b.f": logging.DEBUG,
                "fake_module_2c.d": logging.ERROR,
                "fake_module_2c.d.e": 15,
                "fake_module_2c.d.f": logging.ERROR
            })
    ]
    for case, expected in test_cases:
      overrides = self._overrides_case_to_option_dict(case)
      sdk_worker_main._set_log_level_overrides(overrides)
      for name, level in expected.items():
        self.assertEqual(logging.getLogger(name).getEffectiveLevel(), level)

  def test__set_log_level_overrides_error(self):
    test_cases = [
        (['{"json.value.is.not.level": ["ERROR"]}'],
         "Error occurred when setting log level"),
        (['{"invalid.level":"INVALID"}'],
         "Error occurred when setting log level"),
    ]
    for case, expected in test_cases:
      overrides = self._overrides_case_to_option_dict(case)
      with self.assertLogs('apache_beam.runners.worker.sdk_worker_main',
                           level='ERROR') as cm:
        sdk_worker_main._set_log_level_overrides(overrides)
        self.assertIn(expected, cm.output[0])

  def test_gcp_profiler_uses_provided_service_name_when_specified(self):
    options = PipelineOptions(
        ['--dataflow_service_options=enable_google_cloud_profiler=sample'])
    gcp_profiler_name = sdk_worker_main._get_gcp_profiler_name_if_enabled(
        options)
    sdk_worker_main._start_profiler = unittest.mock.MagicMock()
    sdk_worker_main._start_profiler(gcp_profiler_name, "version")
    sdk_worker_main._start_profiler.assert_called_with("sample", "version")

  @unittest.mock.patch.dict(os.environ, {"JOB_NAME": "sample_job"}, clear=True)
  def test_gcp_profiler_uses_job_name_when_service_name_not_specified(self):
    options = PipelineOptions(
        ['--dataflow_service_options=enable_google_cloud_profiler'])
    gcp_profiler_name = sdk_worker_main._get_gcp_profiler_name_if_enabled(
        options)
    sdk_worker_main._start_profiler = unittest.mock.MagicMock()
    sdk_worker_main._start_profiler(gcp_profiler_name, "version")
    sdk_worker_main._start_profiler.assert_called_with("sample_job", "version")

  @unittest.mock.patch.dict(os.environ, {"JOB_NAME": "sample_job"}, clear=True)
  def test_gcp_profiler_uses_job_name_when_enabled_as_experiment(self):
    options = PipelineOptions(['--experiment=enable_google_cloud_profiler'])
    gcp_profiler_name = sdk_worker_main._get_gcp_profiler_name_if_enabled(
        options)
    sdk_worker_main._start_profiler = unittest.mock.MagicMock()
    sdk_worker_main._start_profiler(gcp_profiler_name, "version")
    sdk_worker_main._start_profiler.assert_called_with("sample_job", "version")

  @unittest.mock.patch.dict(os.environ, {"JOB_NAME": "sample_job"}, clear=True)
  def test_pipeline_option_max_cache_memory_usage_mb(self):
    options = PipelineOptions(flags=['--max_cache_memory_usage_mb=50'])

    cache_size = sdk_worker_main._get_state_cache_size_bytes(options)
    self.assertEqual(cache_size, 50 << 20)

  @unittest.mock.patch.dict(os.environ, {"JOB_NAME": "sample_job"}, clear=True)
  def test_pipeline_option_max_cache_memory_usage_mb_with_experiments(self):
    options = PipelineOptions(flags=['--experiments=state_cache_size=50'])
    cache_size = sdk_worker_main._get_state_cache_size_bytes(options)
    self.assertEqual(cache_size, 50 << 20)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
