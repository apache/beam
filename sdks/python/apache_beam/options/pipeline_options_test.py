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

import json
import logging
import os
import unittest

import hamcrest as hc
import mock
from parameterized import parameterized

from apache_beam.options.pipeline_options import CrossLanguageOptions
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import ProfilingOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import _BeamArgumentParser
from apache_beam.options.pipeline_options_validator import PipelineOptionsValidator
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher

_LOGGER = logging.getLogger(__name__)

try:
  import apache_beam.io.gcp.gcsio  # pylint: disable=unused-import
  has_gcsio = True
except ImportError:
  has_gcsio = False


# Mock runners to use for validations.
class MockRunners(object):
  class DataflowRunner(object):
    def get_default_gcp_region(self):
      # Return a default so we don't have to specify --region in every test
      # (unless specifically testing it).
      return 'us-central1'


class MockGoogleCloudOptionsNoBucket(GoogleCloudOptions):
  def _create_default_gcs_bucket(self):
    return None


class MockGoogleCloudOptionsWithBucket(GoogleCloudOptions):
  def _create_default_gcs_bucket(self):
    return "gs://default/bucket"


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
      (['--num_workers', '5'],
       {
           'num_workers': 5,
           'mock_flag': False,
           'mock_option': None,
           'mock_multi_option': None
       }, [DisplayDataItemMatcher('num_workers', 5)]),
      (['--direct_num_workers', '5'],
       {
           'direct_num_workers': 5,
           'mock_flag': False,
           'mock_option': None,
           'mock_multi_option': None
       }, [DisplayDataItemMatcher('direct_num_workers', 5)]),
      (['--direct_running_mode', 'multi_threading'],
       {
           'direct_running_mode': 'multi_threading',
           'mock_flag': False,
           'mock_option': None,
           'mock_multi_option': None
       }, [DisplayDataItemMatcher('direct_running_mode', 'multi_threading')]),
      (['--direct_running_mode', 'multi_processing'],
       {
           'direct_running_mode': 'multi_processing',
           'mock_flag': False,
           'mock_option': None,
           'mock_multi_option': None
       }, [DisplayDataItemMatcher('direct_running_mode', 'multi_processing')]),
      (['--profile_cpu', '--profile_location', 'gs://bucket/', 'ignored'],
       {
           'profile_cpu': True,
           'profile_location': 'gs://bucket/',
           'mock_flag': False,
           'mock_option': None,
           'mock_multi_option': None
       },
       [
           DisplayDataItemMatcher('profile_cpu', True),
           DisplayDataItemMatcher('profile_location', 'gs://bucket/')
       ]),
      (['--num_workers', '5', '--mock_flag'],
       {
           'num_workers': 5,
           'mock_flag': True,
           'mock_option': None,
           'mock_multi_option': None
       },
       [
           DisplayDataItemMatcher('num_workers', 5),
           DisplayDataItemMatcher('mock_flag', True)
       ]),
      (['--mock_option', 'abc'], {
          'mock_flag': False, 'mock_option': 'abc', 'mock_multi_option': None
      }, [DisplayDataItemMatcher('mock_option', 'abc')]),
      (['--mock_option', ' abc def '],
       {
           'mock_flag': False,
           'mock_option': ' abc def ',
           'mock_multi_option': None
       }, [DisplayDataItemMatcher('mock_option', ' abc def ')]),
      (['--mock_option= abc xyz '],
       {
           'mock_flag': False,
           'mock_option': ' abc xyz ',
           'mock_multi_option': None
       }, [DisplayDataItemMatcher('mock_option', ' abc xyz ')]),
      ([
          '--mock_option=gs://my bucket/my folder/my file',
          '--mock_multi_option=op1',
          '--mock_multi_option=op2'
      ],
       {
           'mock_flag': False,
           'mock_option': 'gs://my bucket/my folder/my file',
           'mock_multi_option': ['op1', 'op2']
       },
       [
           DisplayDataItemMatcher(
               'mock_option', 'gs://my bucket/my folder/my file'),
           DisplayDataItemMatcher('mock_multi_option', ['op1', 'op2'])
       ]),
      (['--mock_multi_option=op1', '--mock_multi_option=op2'],
       {
           'mock_flag': False,
           'mock_option': None,
           'mock_multi_option': ['op1', 'op2']
       }, [DisplayDataItemMatcher('mock_multi_option', ['op1', 'op2'])]),
      (['--mock_json_option={"11a": 0, "37a": 1}'],
       {
           'mock_flag': False,
           'mock_option': None,
           'mock_multi_option': None,
           'mock_json_option': {
               '11a': 0, '37a': 1
           },
       }, [DisplayDataItemMatcher('mock_json_option', {
           '11a': 0, '37a': 1
       })]),
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

  @parameterized.expand(TEST_CASES)
  def test_display_data(self, flags, _, display_data):
    options = PipelineOptions(flags=flags)
    dd = DisplayData.create_from(options)
    hc.assert_that(dd.items, hc.contains_inanyorder(*display_data))

  @parameterized.expand(TEST_CASES)
  def test_get_all_options_subclass(self, flags, expected, _):
    options = PipelineOptionsTest.MockOptions(flags=flags)
    self.assertLessEqual(expected.items(), options.get_all_options().items())
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_flag,
        expected['mock_flag'])
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_option,
        expected['mock_option'])
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_multi_option,
        expected['mock_multi_option'])

  @parameterized.expand(TEST_CASES)
  def test_get_all_options(self, flags, expected, _):
    options = PipelineOptions(flags=flags)
    self.assertLessEqual(expected.items(), options.get_all_options().items())
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_flag,
        expected['mock_flag'])
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_option,
        expected['mock_option'])
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_multi_option,
        expected['mock_multi_option'])

  @parameterized.expand(TEST_CASES)
  def test_subclasses_of_pipeline_options_can_be_instantiated(
      self, flags, expected, _):
    mock_options = PipelineOptionsTest.MockOptions(flags=flags)
    self.assertEqual(mock_options.mock_flag, expected['mock_flag'])
    self.assertEqual(mock_options.mock_option, expected['mock_option'])
    self.assertEqual(
        mock_options.mock_multi_option, expected['mock_multi_option'])

  @parameterized.expand(TEST_CASES)
  def test_views_can_be_constructed_from_pipeline_option_subclasses(
      self, flags, expected, _):
    fake_options = PipelineOptionsTest.FakeOptions(flags=flags)
    mock_options = fake_options.view_as(PipelineOptionsTest.MockOptions)

    self.assertEqual(mock_options.mock_flag, expected['mock_flag'])
    self.assertEqual(mock_options.mock_option, expected['mock_option'])
    self.assertEqual(
        mock_options.mock_multi_option, expected['mock_multi_option'])

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

  @parameterized.expand(TEST_CASES)
  def test_from_dictionary(self, flags, expected, _):
    options = PipelineOptions(flags=flags)
    all_options_dict = options.get_all_options()
    options_from_dict = PipelineOptions.from_dictionary(all_options_dict)
    self.assertEqual(
        options_from_dict.view_as(PipelineOptionsTest.MockOptions).mock_flag,
        expected['mock_flag'])
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_option,
        expected['mock_option'])
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_multi_option,
        expected['mock_multi_option'])
    self.assertEqual(
        options.view_as(PipelineOptionsTest.MockOptions).mock_json_option,
        expected.get('mock_json_option', {}))

  def test_none_from_dictionary(self):
    class NoneDefaultOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument('--test_arg_none', default=None, type=int)
        parser.add_argument('--test_arg_int', default=1, type=int)

    options_dict = {'test_arg_none': None, 'test_arg_int': 5}
    options_from_dict = NoneDefaultOptions.from_dictionary(options_dict)
    result = options_from_dict.get_all_options()
    self.assertEqual(result['test_arg_int'], 5)
    self.assertEqual(result['test_arg_none'], None)

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
    with self.assertRaises(KeyError):
      _ = options.get_all_options(retain_unknown_options=True)['i']

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

  def test_beam_services_empty(self):
    with mock.patch.dict(os.environ, {}, clear=True):
      options = PipelineOptions().view_as(CrossLanguageOptions)
      self.assertEqual(options.beam_services, {})

  def test_beam_services_from_env(self):
    with mock.patch.dict(os.environ,
                         {'BEAM_SERVICE_OVERRIDES': '{"foo": "bar"}'},
                         clear=True):
      options = PipelineOptions().view_as(CrossLanguageOptions)
      self.assertEqual(options.beam_services, {'foo': 'bar'})

  def test_beam_services_from_flag(self):
    with mock.patch.dict(os.environ, {}, clear=True):
      options = PipelineOptions(['--beam_services={"foo": "bar"}'
                                 ]).view_as(CrossLanguageOptions)
      self.assertEqual(options.beam_services, {'foo': 'bar'})

  def test_beam_services_from_env_and_flag(self):
    with mock.patch.dict(
        os.environ,
        {'BEAM_SERVICE_OVERRIDES': '{"foo": "bar", "other": "zzz"}'},
        clear=True):
      options = PipelineOptions(['--beam_services={"foo": "override"}'
                                 ]).view_as(CrossLanguageOptions)
      self.assertEqual(
          options.beam_services, {
              'foo': 'override', 'other': 'zzz'
          })

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

    class TestRedefinedOptions(PipelineOptions):  # pylint: disable=function-redefined
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument('--redefined_flag', action='store_true')

    options = PipelineOptions(['--redefined_flag'])
    self.assertTrue(options.get_all_options()['redefined_flag'])

  # TODO(https://github.com/apache/beam/issues/18197): Require unique names
  # only within a test. For now, <file name acronym>_vp_arg<number> will be
  # the convention to name value-provider arguments in tests, as opposed to
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

  def test_dataflow_service_options(self):
    options = PipelineOptions([
        '--dataflow_service_option',
        'whizz=bang',
        '--dataflow_service_option',
        'beep=boop'
    ])
    self.assertEqual(
        sorted(options.get_all_options()['dataflow_service_options']),
        ['beep=boop', 'whizz=bang'])

    options = PipelineOptions([
        '--dataflow_service_options',
        'whizz=bang',
        '--dataflow_service_options',
        'beep=boop'
    ])
    self.assertEqual(
        sorted(options.get_all_options()['dataflow_service_options']),
        ['beep=boop', 'whizz=bang'])

    options = PipelineOptions(flags=[''])
    self.assertEqual(
        options.get_all_options()['dataflow_service_options'], None)

  def test_options_store_false_with_different_dest(self):
    parser = _BeamArgumentParser()
    for cls in PipelineOptions.__subclasses__():
      cls._add_argparse_args(parser)

    actions = parser._actions.copy()
    options_to_flags = {}
    options_diff_dest_store_true = {}

    for i in range(len(actions)):
      flag_names = actions[i].option_strings
      option_name = actions[i].dest

      if isinstance(actions[i].const, bool):
        for flag_name in flag_names:
          flag_name = flag_name.strip('-')
          if flag_name != option_name:
            # Capture flags which has store_action=True and has a
            # different dest. This behavior would be confusing.
            if actions[i].const:
              options_diff_dest_store_true[flag_name] = option_name
              continue
            # check the flags like no_use_public_ips
            # default is None, action is {True, False}
            if actions[i].default is None:
              options_to_flags[option_name] = flag_name

    self.assertEqual(
        len(options_diff_dest_store_true),
        0,
        _LOGGER.error(
            "There should be no flags that have a dest "
            "different from flag name and action as "
            "store_true. It would be confusing "
            "to the user. Please specify the dest as the "
            "flag_name instead."))
    from apache_beam.options.pipeline_options import (
        _FLAG_THAT_SETS_FALSE_VALUE)

    self.assertDictEqual(
        _FLAG_THAT_SETS_FALSE_VALUE,
        options_to_flags,
        "If you are adding a new boolean flag with default=None,"
        " with different dest/option_name from the flag name, please add "
        "the dest and the flag name to the map "
        "_FLAG_THAT_SETS_FALSE_VALUE in PipelineOptions.py")

  def _check_errors(self, options, validator, expected):
    if has_gcsio:
      with mock.patch('apache_beam.io.gcp.gcsio.GcsIO.is_soft_delete_enabled',
                      return_value=False):
        errors = options._handle_temp_and_staging_locations(validator)
        self.assertEqual(errors, expected)
    else:
      errors = options._handle_temp_and_staging_locations(validator)
      self.assertEqual(errors, expected)

  def test_validation_good_stg_good_temp(self):
    runner = MockRunners.DataflowRunner()
    options = GoogleCloudOptions([
        '--project=myproject',
        '--staging_location=gs://beam/stg',
        '--temp_location=gs://beam/tmp'
    ])
    validator = PipelineOptionsValidator(options, runner)
    self._check_errors(options, validator, [])
    self.assertEqual(
        options.get_all_options()['staging_location'], "gs://beam/stg")
    self.assertEqual(
        options.get_all_options()['temp_location'], "gs://beam/tmp")

  def test_validation_bad_stg_good_temp(self):
    runner = MockRunners.DataflowRunner()
    options = GoogleCloudOptions([
        '--project=myproject',
        '--staging_location=badGSpath',
        '--temp_location=gs://beam/tmp'
    ])
    validator = PipelineOptionsValidator(options, runner)
    self._check_errors(options, validator, [])
    self.assertEqual(
        options.get_all_options()['staging_location'], "gs://beam/tmp")
    self.assertEqual(
        options.get_all_options()['temp_location'], "gs://beam/tmp")

  def test_validation_good_stg_bad_temp(self):
    runner = MockRunners.DataflowRunner()
    options = GoogleCloudOptions([
        '--project=myproject',
        '--staging_location=gs://beam/stg',
        '--temp_location=badGSpath'
    ])
    validator = PipelineOptionsValidator(options, runner)
    self._check_errors(options, validator, [])
    self.assertEqual(
        options.get_all_options()['staging_location'], "gs://beam/stg")
    self.assertEqual(
        options.get_all_options()['temp_location'], "gs://beam/stg")

  def test_validation_bad_stg_bad_temp_with_default(self):
    runner = MockRunners.DataflowRunner()
    options = MockGoogleCloudOptionsWithBucket([
        '--project=myproject',
        '--staging_location=badGSpath',
        '--temp_location=badGSpath'
    ])
    validator = PipelineOptionsValidator(options, runner)
    self._check_errors(options, validator, [])
    self.assertEqual(
        options.get_all_options()['staging_location'], "gs://default/bucket")
    self.assertEqual(
        options.get_all_options()['temp_location'], "gs://default/bucket")

  def test_validation_bad_stg_bad_temp_no_default(self):
    runner = MockRunners.DataflowRunner()
    options = MockGoogleCloudOptionsNoBucket([
        '--project=myproject',
        '--staging_location=badGSpath',
        '--temp_location=badGSpath'
    ])
    validator = PipelineOptionsValidator(options, runner)
    self._check_errors(
        options,
        validator,
        [
          'Invalid GCS path (badGSpath), given for the option: ' \
            'temp_location.',
          'Invalid GCS path (badGSpath), given for the option: ' \
            'staging_location.'
        ])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
