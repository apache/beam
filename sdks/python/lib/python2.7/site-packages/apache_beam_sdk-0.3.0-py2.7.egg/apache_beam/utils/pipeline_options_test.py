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

"""Unit tests for the pipeline options module."""

import logging
import unittest

from apache_beam.utils.options import PipelineOptions


class PipelineOptionsTest(unittest.TestCase):

  TEST_CASES = [
      {'flags': ['--num_workers', '5'],
       'expected': {'num_workers': 5, 'mock_flag': False, 'mock_option': None}},
      {
          'flags': [
              '--profile', '--profile_location', 'gs://bucket/', 'ignored'],
          'expected': {
              'profile': True, 'profile_location': 'gs://bucket/',
              'mock_flag': False, 'mock_option': None}
      },
      {'flags': ['--num_workers', '5', '--mock_flag'],
       'expected': {'num_workers': 5, 'mock_flag': True, 'mock_option': None}},
      {'flags': ['--mock_option', 'abc'],
       'expected': {'mock_flag': False, 'mock_option': 'abc'}},
      {'flags': ['--mock_option', ' abc def '],
       'expected': {'mock_flag': False, 'mock_option': ' abc def '}},
      {'flags': ['--mock_option= abc xyz '],
       'expected': {'mock_flag': False, 'mock_option': ' abc xyz '}},
      {'flags': ['--mock_option=gs://my bucket/my folder/my file'],
       'expected': {'mock_flag': False,
                    'mock_option': 'gs://my bucket/my folder/my file'}},
  ]

  # Used for testing newly added flags.
  class MockOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--mock_flag', action='store_true', help='mock flag')
      parser.add_argument('--mock_option', help='mock option')
      parser.add_argument('--option with space', help='mock option with space')

  def test_get_all_options(self):
    for case in PipelineOptionsTest.TEST_CASES:
      options = PipelineOptions(flags=case['flags'])
      self.assertDictContainsSubset(case['expected'], options.get_all_options())
      self.assertEqual(options.view_as(
          PipelineOptionsTest.MockOptions).mock_flag,
                       case['expected']['mock_flag'])
      self.assertEqual(options.view_as(
          PipelineOptionsTest.MockOptions).mock_option,
                       case['expected']['mock_option'])

  def test_from_dictionary(self):
    for case in PipelineOptionsTest.TEST_CASES:
      options = PipelineOptions(flags=case['flags'])
      all_options_dict = options.get_all_options()
      options_from_dict = PipelineOptions.from_dictionary(all_options_dict)
      self.assertEqual(options_from_dict.view_as(
          PipelineOptionsTest.MockOptions).mock_flag,
                       case['expected']['mock_flag'])
      self.assertEqual(options.view_as(
          PipelineOptionsTest.MockOptions).mock_option,
                       case['expected']['mock_option'])

  def test_option_with_spcae(self):
    options = PipelineOptions(flags=['--option with space= value with space'])
    self.assertEqual(
        getattr(options.view_as(PipelineOptionsTest.MockOptions),
                'option with space'), ' value with space')
    options_from_dict = PipelineOptions.from_dictionary(
        options.get_all_options())
    self.assertEqual(
        getattr(options_from_dict.view_as(PipelineOptionsTest.MockOptions),
                'option with space'), ' value with space')

  def test_override_options(self):
    base_flags = ['--num_workers', '5']
    options = PipelineOptions(base_flags)
    self.assertEqual(options.get_all_options()['num_workers'], 5)
    self.assertEqual(options.get_all_options()['mock_flag'], False)

    options.view_as(PipelineOptionsTest.MockOptions).mock_flag = True
    self.assertEqual(options.get_all_options()['num_workers'], 5)
    self.assertEqual(options.get_all_options()['mock_flag'], True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
