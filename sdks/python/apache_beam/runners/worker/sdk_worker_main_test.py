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

import logging
import unittest

from hamcrest import all_of
from hamcrest import assert_that
from hamcrest import has_entry

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.runners.worker import sdk_worker_main
from apache_beam.runners.worker import worker_status


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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
