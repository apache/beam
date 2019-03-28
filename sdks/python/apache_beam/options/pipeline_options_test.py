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

from __future__ import absolute_import

import logging
import unittest

import hamcrest as hc

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import ProfilingOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher


class PipelineOptionsTest(unittest.TestCase):
  def tearDown(self):
    # Clean up the global variable used by RuntimeValueProvider
    RuntimeValueProvider.runtime_options = None

  TEST_CASES = [
      {'flags': ['--num_workers', '5'],
       'expected': {'num_workers': 5,
                    'mock_flag': False,
                    'mock_option': None,
                    'mock_multi_option': None},
       'display_data': [DisplayDataItemMatcher('num_workers', 5)]},
      {
          'flags': [
              '--profile_cpu', '--profile_location', 'gs://bucket/', 'ignored'],
          'expected': {
              'profile_cpu': True, 'profile_location': 'gs://bucket/',
              'mock_flag': False, 'mock_option': None,
              'mock_multi_option': None},
          'display_data': [
              DisplayDataItemMatcher('profile_cpu',
                                     True),
              DisplayDataItemMatcher('profile_location',
                                     'gs://bucket/')]
      },
      {'flags': ['--num_workers', '5', '--mock_flag'],
       'expected': {'num_workers': 5,
                    'mock_flag': True,
                    'mock_option': None,
                    'mock_multi_option': None},
       'display_data': [
           DisplayDataItemMatcher('num_workers', 5),
           DisplayDataItemMatcher('mock_flag', True)]
      },
      {'flags': ['--mock_option', 'abc'],
       'expected': {'mock_flag': False,
                    'mock_option': 'abc',
                    'mock_multi_option': None},
       'display_data': [
           DisplayDataItemMatcher('mock_option', 'abc')]
      },
      {'flags': ['--mock_option', ' abc def '],
       'expected': {'mock_flag': False,
                    'mock_option': ' abc def ',
                    'mock_multi_option': None},
       'display_data': [
           DisplayDataItemMatcher('mock_option', ' abc def ')]
      },
      {'flags': ['--mock_option= abc xyz '],
       'expected': {'mock_flag': False,
                    'mock_option': ' abc xyz ',
                    'mock_multi_option': None},
       'display_data': [
           DisplayDataItemMatcher('mock_option', ' abc xyz ')]
      },
      {'flags': ['--mock_option=gs://my bucket/my folder/my file',
                 '--mock_multi_option=op1',
                 '--mock_multi_option=op2'],
       'expected': {'mock_flag': False,
                    'mock_option': 'gs://my bucket/my folder/my file',
                    'mock_multi_option': ['op1', 'op2']},
       'display_data': [
           DisplayDataItemMatcher(
               'mock_option', 'gs://my bucket/my folder/my file'),
           DisplayDataItemMatcher('mock_multi_option', ['op1', 'op2'])]
      },
      {'flags': ['--mock_multi_option=op1', '--mock_multi_option=op2'],
       'expected': {'mock_flag': False,
                    'mock_option': None,
                    'mock_multi_option': ['op1', 'op2']},
       'display_data': [
           DisplayDataItemMatcher('mock_multi_option', ['op1', 'op2'])]
      },
  ]

  # Used for testing newly added flags.
  class MockOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--mock_flag', action='store_true', help='mock flag')
      parser.add_argument('--mock_option', help='mock option')
      parser.add_argument(
          '--mock_multi_option', action='append', help='mock multi option')
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
      self.assertEqual(options.view_as(
          PipelineOptionsTest.MockOptions).mock_multi_option,
                       case['expected']['mock_multi_option'])

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

  # TODO(BEAM-1319): Require unique names only within a test.
  # For now, <file name acronym>_vp_arg<number> will be the convention
  # to name value-provider arguments in tests, as opposed to
  # <file name acronym>_non_vp_arg<number> for non-value-provider arguments.
  # The number will grow per file as tests are added.
  def test_value_provider_options(self):
    class UserOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--pot_vp_arg1',
            help='This flag is a value provider')

        parser.add_value_provider_argument(
            '--pot_vp_arg2',
            default=1,
            type=int)

        parser.add_argument(
            '--pot_non_vp_arg1',
            default=1,
            type=int
        )

    # Provide values: if not provided, the option becomes of the type runtime vp
    options = UserOptions(['--pot_vp_arg1', 'hello'])
    self.assertIsInstance(options.pot_vp_arg1, StaticValueProvider)
    self.assertIsInstance(options.pot_vp_arg2, RuntimeValueProvider)
    self.assertIsInstance(options.pot_non_vp_arg1, int)

    # Values can be overwritten
    options = UserOptions(pot_vp_arg1=5,
                          pot_vp_arg2=StaticValueProvider(value_type=str,
                                                          value='bye'),
                          pot_non_vp_arg1=RuntimeValueProvider(
                              option_name='foo',
                              value_type=int,
                              default_value=10))
    self.assertEqual(options.pot_vp_arg1, 5)
    self.assertTrue(options.pot_vp_arg2.is_accessible(),
                    '%s is not accessible' % options.pot_vp_arg2)
    self.assertEqual(options.pot_vp_arg2.get(), 'bye')
    self.assertFalse(options.pot_non_vp_arg1.is_accessible())

    with self.assertRaises(RuntimeError):
      options.pot_non_vp_arg1.get()

  # Converts extra arguments to list value.
  def test_extra_args(self):
    options = PipelineOptions([
        '--extra_arg', 'val1',
        '--extra_arg', 'val2',
        '--extra_arg=val3',
        '--unknown_arg', 'val4'])

    def add_extra_options(parser):
      parser.add_argument("--extra_arg", action='append')

    self.assertEqual(options.get_all_options(
        add_extra_args_fn=add_extra_options)
                     ['extra_arg'], ['val1', 'val2', 'val3'])

  # The argparse package by default tries to autocomplete option names. This
  # results in an "ambiguous option" error from argparse when an unknown option
  # matching multiple known ones are used. This tests that we suppress this
  # error.
  def test_unknown_option_prefix(self):
    # Test that the "ambiguous option" error is suppressed.
    options = PipelineOptions(['--profi', 'val1'])
    options.view_as(ProfilingOptions)

    # Test that valid errors are not suppressed.
    with self.assertRaises(SystemExit):
      # Invalid option choice.
      options = PipelineOptions(['--type_check_strictness', 'blahblah'])
      options.view_as(TypeOptions)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
