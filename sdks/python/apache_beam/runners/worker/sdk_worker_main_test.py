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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import unittest

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.worker import sdk_worker_main


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
      lines = sdk_worker_main.StatusServer.get_thread_dump()
      threaddump = '\n'.join(lines)
      self.assertRegexpMatches(threaddump, '.*wrapped_method_for_test.*')

    wrapped_method_for_test()

  def test_work_count_default_value(self):
    self._check_worker_count('{}', 12)

  def test_parse_pipeline_options(self):
    expected_options = PipelineOptions([])
    expected_options.view_as(
        SdkWorkerMainTest.MockOptions).m_m_option = [
            'worker_threads=1', 'beam_fn_api'
        ]
    expected_options.view_as(
        SdkWorkerMainTest.MockOptions).m_option = '/tmp/requirements.txt'
    self.assertEqual(
        {'m_m_option': ['worker_threads=1']},
        sdk_worker_main._parse_pipeline_options(
            '{"options": {"m_m_option":["worker_threads=1"]}}')
        .get_all_options(drop_default=True))
    self.assertEqual(
        expected_options.get_all_options(),
        sdk_worker_main._parse_pipeline_options(
            '{"options": {' +
            '"m_option": "/tmp/requirements.txt", ' +
            '"m_m_option":["worker_threads=1", "beam_fn_api"]' +
            '}}').get_all_options())
    self.assertEqual(
        {'m_m_option': ['worker_threads=1']},
        sdk_worker_main._parse_pipeline_options(
            '{"beam:option:m_m_option:v1":["worker_threads=1"]}')
        .get_all_options(drop_default=True))
    self.assertEqual(
        expected_options.get_all_options(),
        sdk_worker_main._parse_pipeline_options(
            '{"beam:option:m_option:v1": "/tmp/requirements.txt", ' +
            '"beam:option:m_m_option:v1":["worker_threads=1", ' +
            '"beam_fn_api"]}').get_all_options())
    self.assertEqual(
        {'beam:option:m_option:v': 'mock_val'},
        sdk_worker_main._parse_pipeline_options(
            '{"options": {"beam:option:m_option:v":"mock_val"}}')
        .get_all_options(drop_default=True))
    self.assertEqual(
        {'eam:option:m_option:v1': 'mock_val'},
        sdk_worker_main._parse_pipeline_options(
            '{"options": {"eam:option:m_option:v1":"mock_val"}}')
        .get_all_options(drop_default=True))
    self.assertEqual(
        {'eam:option:m_option:v': 'mock_val'},
        sdk_worker_main._parse_pipeline_options(
            '{"options": {"eam:option:m_option:v":"mock_val"}}')
        .get_all_options(drop_default=True))

  def test_work_count_custom_value(self):
    self._check_worker_count('{"experiments":["worker_threads=1"]}', 1)
    self._check_worker_count('{"experiments":["worker_threads=4"]}', 4)
    self._check_worker_count('{"experiments":["worker_threads=12"]}', 12)

  def test_work_count_wrong_format(self):
    self._check_worker_count(
        '{"experiments":["worker_threads="]}', exception=True)
    self._check_worker_count(
        '{"experiments":["worker_threads=a"]}', exception=True)
    self._check_worker_count(
        '{"experiments":["worker_threads=1a"]}', exception=True)

  def _check_worker_count(self, pipeline_options, expected=0, exception=False):
    if exception:
      self.assertRaises(
          Exception, sdk_worker_main._get_worker_count,
          PipelineOptions.from_dictionary(json.loads(pipeline_options)))
    else:
      self.assertEquals(
          sdk_worker_main._get_worker_count(
              PipelineOptions.from_dictionary(json.loads(pipeline_options))),
          expected)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
