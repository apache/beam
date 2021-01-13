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

# pytype: skip-file

from __future__ import absolute_import

import json
import logging
import unittest

import hamcrest as hc

from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import ProfilingOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher


class PipelineOptionsTest(unittest.TestCase):
  def setUp(self):
    # Reset runtime options to avoid side-effects caused by other tests.
    # Note that is_accessible assertions require runtime_options to
    # be uninitialized.
    RuntimeValueProvider.set_runtime_options(None)

  def tearDown(self):
    # Reset runtime options to avoid side-effects in other tests.
    RuntimeValueProvider.set_runtime_options(None)

  TEST_CASES = [
      {
          'flags': ['--num_workers', '5'],
          'expected': {
              'num_workers': 5,
              'mock_flag': False,
              'mock_option': None,
              'mock_multi_option': None
          },
          'display_data': [DisplayDataItemMatcher('num_workers', 5)]
      },
      {
          'flags': ['--direct_num_workers', '5'],
          'expected': {
              'direct_num_workers': 5,
              'mock_flag': False,
              'mock_option': None,
              'mock_multi_option': None
          },
          'display_data': [DisplayDataItemMatcher('direct_num_workers', 5)]
      },
      {
          'flags': ['--direct_running_mode', 'multi_threading'],
          'expected': {
              'direct_running_mode': 'multi_threading',
              'mock_flag': False,
              'mock_option': None,
              'mock_multi_option': None
          },
          'display_data': [
              DisplayDataItemMatcher('direct_running_mode', 'multi_threading')
          ]
      },
      {
          'flags': ['--direct_running_mode', 'multi_processing'],
          'expected': {
              'direct_running_mode': 'multi_processing',
              'mock_flag': False,
              'mock_option': None,
              'mock_multi_option': None
          },
          'display_data': [
              DisplayDataItemMatcher('direct_running_mode', 'multi_processing')
          ]
      },
      {
          'flags': [
              '--profile_cpu', '--profile_location', 'gs://bucket/', 'ignored'
          ],
          'expected': {
              'profile_cpu': True,
              'profile_location': 'gs://bucket/',
              'mock_flag': False,
              'mock_option': None,
              'mock_multi_option': None
          },
          'display_data': [
              DisplayDataItemMatcher('profile_cpu', True),
              DisplayDataItemMatcher('profile_location', 'gs://bucket/')
          ]
      },
      {
          'flags': ['--num_workers', '5', '--mock_flag'],
          'expected': {
              'num_workers': 5,
              'mock_flag': True,
              'mock_option': None,
              'mock_multi_option': None
          },
          'display_data': [
              DisplayDataItemMatcher('num_workers', 5),
              DisplayDataItemMatcher('mock_flag', True)
          ]
      },
      {
          'flags': ['--mock_option', 'abc'],
          'expected': {
              'mock_flag': False,
              'mock_option': 'abc',
              'mock_multi_option': None
          },
          'display_data': [DisplayDataItemMatcher('mock_option', 'abc')]
      },
      {
          'flags': ['--mock_option', ' abc def '],
          'expected': {
              'mock_flag': False,
              'mock_option': ' abc def ',
              'mock_multi_option': None
          },
          'display_data': [DisplayDataItemMatcher('mock_option', ' abc def ')]
      },
      {
          'flags': ['--mock_option= abc xyz '],
          'expected': {
              'mock_flag': False,
              'mock_option': ' abc xyz ',
              'mock_multi_option': None
          },
          'display_data': [DisplayDataItemMatcher('mock_option', ' abc xyz ')]
      },
      {
          'flags': [
              '--mock_option=gs://my bucket/my folder/my file',
              '--mock_multi_option=op1',
              '--mock_multi_option=op2'
          ],
          'expected': {
              'mock_flag': False,
              'mock_option': 'gs://my bucket/my folder/my file',
              'mock_multi_option': ['op1', 'op2']
          },
          'display_data': [
              DisplayDataItemMatcher(
                  'mock_option', 'gs://my bucket/my folder/my file'),
              DisplayDataItemMatcher('mock_multi_option', ['op1', 'op2'])
          ]
      },
      {
          'flags': ['--mock_multi_option=op1', '--mock_multi_option=op2'],
          'expected': {
              'mock_flag': False,
              'mock_option': None,
              'mock_multi_option': ['op1', 'op2']
          },
          'display_data': [
              DisplayDataItemMatcher('mock_multi_option', ['op1', 'op2'])
          ]
      },
      {
          'flags': ['--mock_json_option={"11a": 0, "37a": 1}'],
          'expected': {
              'mock_flag': False,
              'mock_option': None,
              'mock_multi_option': None,
              'mock_json_option': {
                  '11a': 0, '37a': 1
              },
          },
          'display_data': [
              DisplayDataItemMatcher('mock_json_option', {
                  '11a': 0, '37a': 1
              })
          ]
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
      parser.add_argument('--mock_json_option', type=json.loads, default={})

  # Use with MockOptions in test cases where multiple option classes are needed.
  class FakeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--fake_flag', action='store_true', help='fake flag')
      parser.add_argument('--fake_option', help='fake option')
      parser.add_argument(
          '--fake_multi_option', action='append', help='fake multi option')

  def test_display_data(self):
    for case in PipelineOptionsTest.TEST_CASES:
      options = PipelineOptions(flags=case['flags'])
      dd = DisplayData.create_from(options)
      hc.assert_that(dd.items, hc.contains_inanyorder(*case['display_data']))

  def test_get_all_options_subclass(self):
    for case in PipelineOptionsTest.TEST_CASES:
      options = PipelineOptionsTest.MockOptions(flags=case['flags'])
      self.assertDictContainsSubset(case['expected'], options.get_all_options())
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_flag,
          case['expected']['mock_flag'])
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_option,
          case['expected']['mock_option'])
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_multi_option,
          case['expected']['mock_multi_option'])

  def test_get_all_options(self):
    for case in PipelineOptionsTest.TEST_CASES:
      options = PipelineOptions(flags=case['flags'])
      self.assertDictContainsSubset(case['expected'], options.get_all_options())
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_flag,
          case['expected']['mock_flag'])
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_option,
          case['expected']['mock_option'])
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_multi_option,
          case['expected']['mock_multi_option'])

  def test_sublcalsses_of_pipeline_options_can_be_instantiated(self):
    for case in PipelineOptionsTest.TEST_CASES:
      mock_options = PipelineOptionsTest.MockOptions(flags=case['flags'])
      self.assertEqual(mock_options.mock_flag, case['expected']['mock_flag'])
      self.assertEqual(
          mock_options.mock_option, case['expected']['mock_option'])
      self.assertEqual(
          mock_options.mock_multi_option, case['expected']['mock_multi_option'])

  def test_views_can_be_constructed_from_pipeline_option_subclasses(self):
    for case in PipelineOptionsTest.TEST_CASES:
      fake_options = PipelineOptionsTest.FakeOptions(flags=case['flags'])
      mock_options = fake_options.view_as(PipelineOptionsTest.MockOptions)

      self.assertEqual(mock_options.mock_flag, case['expected']['mock_flag'])
      self.assertEqual(
          mock_options.mock_option, case['expected']['mock_option'])
      self.assertEqual(
          mock_options.mock_multi_option, case['expected']['mock_multi_option'])

  def test_views_do_not_expose_options_defined_by_other_views(self):
    flags = ['--mock_option=mock_value', '--fake_option=fake_value']

    options = PipelineOptions(flags)
    assert options.view_as(
        PipelineOptionsTest.MockOptions).mock_option == 'mock_value'
    assert options.view_as(
        PipelineOptionsTest.FakeOptions).fake_option == 'fake_value'
    assert options.view_as(PipelineOptionsTest.MockOptions).view_as(
        PipelineOptionsTest.FakeOptions).fake_option == 'fake_value'

    self.assertRaises(
        AttributeError,
        lambda: options.view_as(PipelineOptionsTest.MockOptions).fake_option)
    self.assertRaises(
        AttributeError,
        lambda: options.view_as(PipelineOptionsTest.MockOptions).view_as(
            PipelineOptionsTest.FakeOptions).view_as(
                PipelineOptionsTest.MockOptions).fake_option)

  def test_from_dictionary(self):
    for case in PipelineOptionsTest.TEST_CASES:
      options = PipelineOptions(flags=case['flags'])
      all_options_dict = options.get_all_options()
      options_from_dict = PipelineOptions.from_dictionary(all_options_dict)
      self.assertEqual(
          options_from_dict.view_as(PipelineOptionsTest.MockOptions).mock_flag,
          case['expected']['mock_flag'])
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_option,
          case['expected']['mock_option'])
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_multi_option,
          case['expected']['mock_multi_option'])
      self.assertEqual(
          options.view_as(PipelineOptionsTest.MockOptions).mock_json_option,
          case['expected'].get('mock_json_option', {}))

  def test_option_with_space(self):
    options = PipelineOptions(flags=['--option with space= value with space'])
    self.assertEqual(
        getattr(
            options.view_as(PipelineOptionsTest.MockOptions),
            'option with space'),
        ' value with space')
    options_from_dict = PipelineOptions.from_dictionary(
        options.get_all_options())
    self.assertEqual(
        getattr(
            options_from_dict.view_as(PipelineOptionsTest.MockOptions),
            'option with space'),
        ' value with space')

  def test_retain_unknown_options_binary_store_string(self):
    options = PipelineOptions(['--unknown_option', 'some_value'])
    result = options.get_all_options(retain_unknown_options=True)
    self.assertEqual(result['unknown_option'], 'some_value')

  def test_retain_unknown_options_binary_equals_store_string(self):
    options = PipelineOptions(['--unknown_option=some_value'])
    result = options.get_all_options(retain_unknown_options=True)
    self.assertEqual(result['unknown_option'], 'some_value')

  def test_retain_unknown_options_binary_multi_equals_store_string(self):
    options = PipelineOptions(['--unknown_option=expr = "2 + 2 = 5"'])
    result = options.get_all_options(retain_unknown_options=True)
    self.assertEqual(result['unknown_option'], 'expr = "2 + 2 = 5"')

  def test_retain_unknown_options_binary_single_dash_store_string(self):
    options = PipelineOptions(['-i', 'some_value'])
    result = options.get_all_options(retain_unknown_options=True)
    self.assertEqual(result['i'], 'some_value')

  def test_retain_unknown_options_unary_store_true(self):
    options = PipelineOptions(['--unknown_option'])
    result = options.get_all_options(retain_unknown_options=True)
    self.assertEqual(result['unknown_option'], True)

  def test_retain_unknown_options_consecutive_unary_store_true(self):
    options = PipelineOptions(['--option_foo', '--option_bar'])
    result = options.get_all_options(retain_unknown_options=True)
    self.assertEqual(result['option_foo'], True)
    self.assertEqual(result['option_bar'], True)

  def test_retain_unknown_options_unary_single_dash_store_true(self):
    options = PipelineOptions(['-i'])
    result = options.get_all_options(retain_unknown_options=True)
    self.assertEqual(result['i'], True)

  def test_retain_unknown_options_unary_missing_prefix(self):
    options = PipelineOptions(['bad_option'])
    with self.assertRaises(SystemExit):
      options.get_all_options(retain_unknown_options=True)

  def test_override_options(self):
    base_flags = ['--num_workers', '5']
    options = PipelineOptions(base_flags)
    self.assertEqual(options.get_all_options()['num_workers'], 5)
    self.assertEqual(options.get_all_options()['mock_flag'], False)

    options.view_as(PipelineOptionsTest.MockOptions).mock_flag = True
    self.assertEqual(options.get_all_options()['num_workers'], 5)
    self.assertTrue(options.get_all_options()['mock_flag'])

  def test_override_init_options(self):
    base_flags = ['--num_workers', '5']
    options = PipelineOptions(base_flags, mock_flag=True)
    self.assertEqual(options.get_all_options()['num_workers'], 5)
    self.assertEqual(options.get_all_options()['mock_flag'], True)

  def test_invalid_override_init_options(self):
    base_flags = ['--num_workers', '5']
    options = PipelineOptions(base_flags, mock_invalid_flag=True)
    self.assertEqual(options.get_all_options()['num_workers'], 5)
    self.assertEqual(options.get_all_options()['mock_flag'], False)

  def test_experiments(self):
    options = PipelineOptions(['--experiment', 'abc', '--experiment', 'def'])
    self.assertEqual(
        sorted(options.get_all_options()['experiments']), ['abc', 'def'])

    options = PipelineOptions(['--experiments', 'abc', '--experiments', 'def'])
    self.assertEqual(
        sorted(options.get_all_options()['experiments']), ['abc', 'def'])

    options = PipelineOptions(flags=[''])
    self.assertEqual(options.get_all_options()['experiments'], None)

  def test_worker_options(self):
    options = PipelineOptions(['--machine_type', 'abc', '--disk_type', 'def'])
    worker_options = options.view_as(WorkerOptions)
    self.assertEqual(worker_options.machine_type, 'abc')
    self.assertEqual(worker_options.disk_type, 'def')

    options = PipelineOptions(
        ['--worker_machine_type', 'abc', '--worker_disk_type', 'def'])
    worker_options = options.view_as(WorkerOptions)
    self.assertEqual(worker_options.machine_type, 'abc')
    self.assertEqual(worker_options.disk_type, 'def')

  def test_option_modifications_are_shared_between_views(self):
    pipeline_options = PipelineOptions([
        '--mock_option',
        'value',
        '--mock_flag',
        '--mock_multi_option',
        'value1',
        '--mock_multi_option',
        'value2',
    ])

    mock_options = PipelineOptionsTest.MockOptions([
        '--mock_option',
        'value',
        '--mock_flag',
        '--mock_multi_option',
        'value1',
        '--mock_multi_option',
        'value2',
    ])

    for options in [pipeline_options, mock_options]:
      view1 = options.view_as(PipelineOptionsTest.MockOptions)
      view2 = options.view_as(PipelineOptionsTest.MockOptions)

      view1.mock_option = 'new_value'
      view1.mock_flag = False
      view1.mock_multi_option.append('value3')

      view3 = options.view_as(PipelineOptionsTest.MockOptions)
      view4 = view1.view_as(PipelineOptionsTest.MockOptions)
      view5 = options.view_as(TypeOptions).view_as(
          PipelineOptionsTest.MockOptions)

      for view in [view1, view2, view3, view4, view5]:
        self.assertEqual('new_value', view.mock_option)
        self.assertFalse(view.mock_flag)
        self.assertEqual(['value1', 'value2', 'value3'], view.mock_multi_option)

  def test_uninitialized_option_modifications_are_shared_between_views(self):
    options = PipelineOptions([])

    view1 = options.view_as(PipelineOptionsTest.MockOptions)
    view2 = options.view_as(PipelineOptionsTest.MockOptions)

    view1.mock_option = 'some_value'
    view1.mock_flag = False
    view1.mock_multi_option = ['value1', 'value2']

    view3 = options.view_as(PipelineOptionsTest.MockOptions)
    view4 = view1.view_as(PipelineOptionsTest.MockOptions)
    view5 = options.view_as(TypeOptions).view_as(
        PipelineOptionsTest.MockOptions)

    for view in [view1, view2, view3, view4, view5]:
      self.assertEqual('some_value', view.mock_option)
      self.assertFalse(view.mock_flag)
      self.assertEqual(['value1', 'value2'], view.mock_multi_option)

  def test_extra_package(self):
    options = PipelineOptions([
        '--extra_package',
        'abc',
        '--extra_packages',
        'def',
        '--extra_packages',
        'ghi'
    ])
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
    class TestRedefinedOptions(PipelineOptions):  # pylint: disable=unused-variable
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument('--redefined_flag', action='store_true')

    class TestRedefinedOptions(PipelineOptions):
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
            '--pot_vp_arg1', help='This flag is a value provider')

        parser.add_value_provider_argument('--pot_vp_arg2', default=1, type=int)

        parser.add_argument('--pot_non_vp_arg1', default=1, type=int)

    # Provide values: if not provided, the option becomes of the type runtime vp
    options = UserOptions(['--pot_vp_arg1', 'hello'])
    self.assertIsInstance(options.pot_vp_arg1, StaticValueProvider)
    self.assertIsInstance(options.pot_vp_arg2, RuntimeValueProvider)
    self.assertIsInstance(options.pot_non_vp_arg1, int)

    # Values can be overwritten
    options = UserOptions(
        pot_vp_arg1=5,
        pot_vp_arg2=StaticValueProvider(value_type=str, value='bye'),
        pot_non_vp_arg1=RuntimeValueProvider(
            option_name='foo', value_type=int, default_value=10))
    self.assertEqual(options.pot_vp_arg1, 5)
    self.assertTrue(
        options.pot_vp_arg2.is_accessible(),
        '%s is not accessible' % options.pot_vp_arg2)
    self.assertEqual(options.pot_vp_arg2.get(), 'bye')
    self.assertFalse(options.pot_non_vp_arg1.is_accessible())

    with self.assertRaises(RuntimeError):
      options.pot_non_vp_arg1.get()

  # Converts extra arguments to list value.
  def test_extra_args(self):
    options = PipelineOptions([
        '--extra_arg',
        'val1',
        '--extra_arg',
        'val2',
        '--extra_arg=val3',
        '--unknown_arg',
        'val4'
    ])

    def add_extra_options(parser):
      parser.add_argument("--extra_arg", action='append')

    self.assertEqual(
        options.get_all_options(
            add_extra_args_fn=add_extra_options)['extra_arg'],
        ['val1', 'val2', 'val3'])

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

  def test_add_experiment(self):
    options = PipelineOptions([])
    options.view_as(DebugOptions).add_experiment('new_experiment')
    self.assertEqual(['new_experiment'],
                     options.view_as(DebugOptions).experiments)

  def test_add_experiment_preserves_existing_experiments(self):
    options = PipelineOptions(['--experiment=existing_experiment'])
    options.view_as(DebugOptions).add_experiment('new_experiment')
    self.assertEqual(['existing_experiment', 'new_experiment'],
                     options.view_as(DebugOptions).experiments)

  def test_lookup_experiments(self):
    options = PipelineOptions([
        '--experiment=existing_experiment',
        '--experiment',
        'key=value',
        '--experiment',
        'master_key=k1=v1,k2=v2',
    ])
    debug_options = options.view_as(DebugOptions)
    self.assertEqual(
        'default_value',
        debug_options.lookup_experiment('nonexistent', 'default_value'))
    self.assertEqual(
        'value', debug_options.lookup_experiment('key', 'default_value'))
    self.assertEqual(
        'k1=v1,k2=v2', debug_options.lookup_experiment('master_key'))
    self.assertEqual(
        True, debug_options.lookup_experiment('existing_experiment'))

  def test_transform_name_mapping(self):
    options = PipelineOptions(['--transform_name_mapping={\"from\":\"to\"}'])
    mapping = options.view_as(GoogleCloudOptions).transform_name_mapping
    self.assertEqual(mapping['from'], 'to')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
