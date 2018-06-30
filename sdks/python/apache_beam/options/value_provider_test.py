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

"""Unit tests for the ValueProvider class."""

from __future__ import absolute_import

import logging
import unittest

from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.value_provider import StaticValueProvider


# TODO(BEAM-1319): Require unique names only within a test.
# For now, <file name acronym>_vp_arg<number> will be the convention
# to name value-provider arguments in tests, as opposed to
# <file name acronym>_non_vp_arg<number> for non-value-provider arguments.
# The number will grow per file as tests are added.
class ValueProviderTests(unittest.TestCase):
  def test_static_value_provider_keyword_argument(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vpt_vp_arg1',
            help='This keyword argument is a value provider',
            default='some value')
    options = UserDefinedOptions(['--vpt_vp_arg1', 'abc'])
    self.assertTrue(isinstance(options.vpt_vp_arg1, StaticValueProvider))
    self.assertTrue(options.vpt_vp_arg1.is_accessible())
    self.assertEqual(options.vpt_vp_arg1.get(), 'abc')

  def test_runtime_value_provider_keyword_argument(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vpt_vp_arg2',
            help='This keyword argument is a value provider')
    options = UserDefinedOptions()
    self.assertTrue(isinstance(options.vpt_vp_arg2, RuntimeValueProvider))
    self.assertFalse(options.vpt_vp_arg2.is_accessible())
    with self.assertRaises(RuntimeError):
      options.vpt_vp_arg2.get()

  def test_static_value_provider_positional_argument(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            'vpt_vp_arg3',
            help='This positional argument is a value provider',
            default='some value')
    options = UserDefinedOptions(['abc'])
    self.assertTrue(isinstance(options.vpt_vp_arg3, StaticValueProvider))
    self.assertTrue(options.vpt_vp_arg3.is_accessible())
    self.assertEqual(options.vpt_vp_arg3.get(), 'abc')

  def test_runtime_value_provider_positional_argument(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            'vpt_vp_arg4',
            help='This positional argument is a value provider')
    options = UserDefinedOptions([])
    self.assertTrue(isinstance(options.vpt_vp_arg4, RuntimeValueProvider))
    self.assertFalse(options.vpt_vp_arg4.is_accessible())
    with self.assertRaises(RuntimeError):
      options.vpt_vp_arg4.get()

  def test_static_value_provider_type_cast(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vpt_vp_arg5',
            type=int,
            help='This flag is a value provider')

    options = UserDefinedOptions(['--vpt_vp_arg5', '123'])
    self.assertTrue(isinstance(options.vpt_vp_arg5, StaticValueProvider))
    self.assertTrue(options.vpt_vp_arg5.is_accessible())
    self.assertEqual(options.vpt_vp_arg5.get(), 123)

  def test_set_runtime_option(self):
    # define ValueProvider ptions, with and without default values
    class UserDefinedOptions1(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vpt_vp_arg6',
            help='This keyword argument is a value provider')   # set at runtime

        parser.add_value_provider_argument(         # not set, had default int
            '-v', '--vpt_vp_arg7',                      # with short form
            default=123,
            type=int)

        parser.add_value_provider_argument(         # not set, had default str
            '--vpt_vp-arg8',                            # with dash in name
            default='123',
            type=str)

        parser.add_value_provider_argument(         # not set and no default
            '--vpt_vp_arg9',
            type=float)

        parser.add_value_provider_argument(         # positional argument set
            'vpt_vp_arg10',                         # default & runtime ignored
            help='This positional argument is a value provider',
            type=float,
            default=5.4)

    # provide values at graph-construction time
    # (options not provided here become of the type RuntimeValueProvider)
    options = UserDefinedOptions1(['1.2'])
    self.assertFalse(options.vpt_vp_arg6.is_accessible())
    self.assertFalse(options.vpt_vp_arg7.is_accessible())
    self.assertFalse(options.vpt_vp_arg8.is_accessible())
    self.assertFalse(options.vpt_vp_arg9.is_accessible())
    self.assertTrue(options.vpt_vp_arg10.is_accessible())

    # provide values at job-execution time
    # (options not provided here will use their default, if they have one)
    RuntimeValueProvider.set_runtime_options({'vpt_vp_arg6': 'abc',
                                              'vpt_vp_arg10':'3.2'})
    self.assertTrue(options.vpt_vp_arg6.is_accessible())
    self.assertEqual(options.vpt_vp_arg6.get(), 'abc')
    self.assertTrue(options.vpt_vp_arg7.is_accessible())
    self.assertEqual(options.vpt_vp_arg7.get(), 123)
    self.assertTrue(options.vpt_vp_arg8.is_accessible())
    self.assertEqual(options.vpt_vp_arg8.get(), '123')
    self.assertTrue(options.vpt_vp_arg9.is_accessible())
    self.assertIsNone(options.vpt_vp_arg9.get())
    self.assertTrue(options.vpt_vp_arg10.is_accessible())
    self.assertEqual(options.vpt_vp_arg10.get(), 1.2)

  def test_choices(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--vpt_vp_arg11',
            choices=['a', 'b'],
            help='This flag is a value provider with concrete choices')
        parser.add_argument(
            '--vpt_vp_arg12',
            choices=[1, 2],
            type=int,
            help='This flag is a value provider with concrete choices')
    options = UserDefinedOptions(['--vpt_vp_arg11', 'a', '--vpt_vp_arg12', '2'])
    self.assertEqual(options.vpt_vp_arg11, 'a')
    self.assertEqual(options.vpt_vp_arg12, 2)

  def test_static_value_provider_choices(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vpt_vp_arg13',
            choices=['a', 'b'],
            help='This flag is a value provider with concrete choices')
        parser.add_value_provider_argument(
            '--vpt_vp_arg14',
            choices=[1, 2],
            type=int,
            help='This flag is a value provider with concrete choices')
    options = UserDefinedOptions(['--vpt_vp_arg13', 'a', '--vpt_vp_arg14', '2'])
    self.assertEqual(options.vpt_vp_arg13.get(), 'a')
    self.assertEqual(options.vpt_vp_arg14.get(), 2)

  def test_experiments_setup(self):
    self.assertFalse('feature_1' in RuntimeValueProvider.experiments)

    RuntimeValueProvider.set_runtime_options(
        {'experiments': ['feature_1', 'feature_2']}
    )
    self.assertTrue(isinstance(RuntimeValueProvider.experiments, set))
    self.assertTrue('feature_1' in RuntimeValueProvider.experiments)
    self.assertTrue('feature_2' in RuntimeValueProvider.experiments)
    # Clean up runtime_options after this test case finish, otherwise, it'll
    # affect other cases since runtime_options is static attr
    RuntimeValueProvider.set_runtime_options(None)

  def test_experiments_options_setup(self):
    options = PipelineOptions(['--experiments', 'a', '--experiments', 'b,c'])
    options = options.view_as(DebugOptions)
    self.assertIn('a', options.experiments)
    self.assertIn('b,c', options.experiments)
    self.assertNotIn('c', options.experiments)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
