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

import hamcrest as hc
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam.utils.pipeline_options import PipelineOptions
from apache_beam.utils.value_provider import StaticValueProvider
from apache_beam.utils.value_provider import RuntimeValueProvider


class PipelineOptionsTest(unittest.TestCase):
  def tearDown(self):
    # Clean up the global variable used by RuntimeValueProvider
    RuntimeValueProvider.runtime_options = None

  TEST_CASES = [
      {'flags': ['--num_workers', '5'],
       'expected': {'num_workers': 5, 'mock_flag': False, 'mock_option': None},
       'display_data': [DisplayDataItemMatcher('num_workers', 5)]},
      {
          'flags': [
              '--profile_cpu', '--profile_location', 'gs://bucket/', 'ignored'],
          'expected': {
              'profile_cpu': True, 'profile_location': 'gs://bucket/',
              'mock_flag': False, 'mock_option': None},
          'display_data': [
              DisplayDataItemMatcher('profile_cpu',
                                     True),
              DisplayDataItemMatcher('profile_location',
                                     'gs://bucket/')]
      },
      {'flags': ['--num_workers', '5', '--mock_flag'],
       'expected': {'num_workers': 5, 'mock_flag': True, 'mock_option': None},
       'display_data': [
           DisplayDataItemMatcher('num_workers', 5),
           DisplayDataItemMatcher('mock_flag', True)]
      },
      {'flags': ['--mock_option', 'abc'],
       'expected': {'mock_flag': False, 'mock_option': 'abc'},
       'display_data': [
           DisplayDataItemMatcher('mock_option', 'abc')]
      },
      {'flags': ['--mock_option', ' abc def '],
       'expected': {'mock_flag': False, 'mock_option': ' abc def '},
       'display_data': [
           DisplayDataItemMatcher('mock_option', ' abc def ')]
      },
      {'flags': ['--mock_option= abc xyz '],
       'expected': {'mock_flag': False, 'mock_option': ' abc xyz '},
       'display_data': [
           DisplayDataItemMatcher('mock_option', ' abc xyz ')]
      },
      {'flags': ['--mock_option=gs://my bucket/my folder/my file'],
       'expected': {'mock_flag': False,
                    'mock_option': 'gs://my bucket/my folder/my file'},
       'display_data': [
           DisplayDataItemMatcher(
               'mock_option', 'gs://my bucket/my folder/my file')]
      },
  ]

  # Used for testing newly added flags.
  class MockOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--mock_flag', action='store_true', help='mock flag')
      parser.add_argument('--mock_option', help='mock option')
      parser.add_argument('--option with space', help='mock option with space')

  def test_display_data(self):
    for case in PipelineOptionsTest.TEST_CASES:
      options = PipelineOptions(flags=case['flags'])
      dd = DisplayData.create_from(options)
      hc.assert_that(dd.items, hc.contains_inanyorder(*case['display_data']))

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

  def test_option_with_space(self):
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
    self.assertTrue(options.get_all_options()['mock_flag'])

  def test_experiments(self):
    options = PipelineOptions(['--experiment', 'abc', '--experiment', 'def'])
    self.assertEqual(
        sorted(options.get_all_options()['experiments']), ['abc', 'def'])

    options = PipelineOptions(['--experiments', 'abc', '--experiments', 'def'])
    self.assertEqual(
        sorted(options.get_all_options()['experiments']), ['abc', 'def'])

    options = PipelineOptions(flags=[''])
    self.assertEqual(options.get_all_options()['experiments'], None)

  def test_extra_package(self):
    options = PipelineOptions(['--extra_package', 'abc',
                               '--extra_packages', 'def',
                               '--extra_packages', 'ghi'])
    self.assertEqual(
        sorted(options.get_all_options()['extra_packages']),
        ['abc', 'def', 'ghi'])

    options = PipelineOptions(flags=[''])
    self.assertEqual(options.get_all_options()['extra_packages'], None)

  def test_dataflow_job_file(self):
    options = PipelineOptions(['--dataflow_job_file', 'abc'])
    self.assertEqual(options.get_all_options()['dataflow_job_file'], 'abc')

    options = PipelineOptions(flags=[''])
    self.assertEqual(options.get_all_options()['dataflow_job_file'], None)

  def test_template_location(self):
    options = PipelineOptions(['--template_location', 'abc'])
    self.assertEqual(options.get_all_options()['template_location'], 'abc')

    options = PipelineOptions(flags=[''])
    self.assertEqual(options.get_all_options()['template_location'], None)

  def test_redefine_options(self):

    class TestRedefinedOptios(PipelineOptions):  # pylint: disable=unused-variable

      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument('--redefined_flag', action='store_true')

    class TestRedefinedOptios(PipelineOptions):

      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument('--redefined_flag', action='store_true')

    options = PipelineOptions(['--redefined_flag'])
    self.assertTrue(options.get_all_options()['redefined_flag'])

  def test_value_provider_options(self):
    class UserOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vp_arg',
            help='This flag is a value provider')

        parser.add_value_provider_argument(
            '--vp_arg2',
            default=1,
            type=int)

        parser.add_argument(
            '--non_vp_arg',
            default=1,
            type=int
        )

    # Provide values: if not provided, the option becomes of the type runtime vp
    options = UserOptions(['--vp_arg', 'hello'])
    self.assertIsInstance(options.vp_arg, StaticValueProvider)
    self.assertIsInstance(options.vp_arg2, RuntimeValueProvider)
    self.assertIsInstance(options.non_vp_arg, int)

    # Values can be overwritten
    options = UserOptions(vp_arg=5,
                          vp_arg2=StaticValueProvider(value_type=str,
                                                      value='bye'),
                          non_vp_arg=RuntimeValueProvider(
                              option_name='foo',
                              value_type=int,
                              default_value=10))
    self.assertEqual(options.vp_arg, 5)
    self.assertTrue(options.vp_arg2.is_accessible(),
                    '%s is not accessible' % options.vp_arg2)
    self.assertEqual(options.vp_arg2.get(), 'bye')
    self.assertFalse(options.non_vp_arg.is_accessible())

    with self.assertRaises(RuntimeError):
      options.non_vp_arg.get()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
