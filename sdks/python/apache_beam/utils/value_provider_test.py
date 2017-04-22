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

import unittest

from apache_beam.utils.pipeline_options import PipelineOptions
from apache_beam.utils.value_provider import RuntimeValueProvider
from apache_beam.utils.value_provider import StaticValueProvider


class ValueProviderTests(unittest.TestCase):
  def test_static_value_provider_keyword_argument(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vp_arg',
            help='This keyword argument is a value provider',
            default='some value')
    options = UserDefinedOptions(['--vp_arg', 'abc'])
    self.assertTrue(isinstance(options.vp_arg, StaticValueProvider))
    self.assertTrue(options.vp_arg.is_accessible())
    self.assertEqual(options.vp_arg.get(), 'abc')

  def test_runtime_value_provider_keyword_argument(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vp_arg',
            help='This keyword argument is a value provider')
    options = UserDefinedOptions()
    self.assertTrue(isinstance(options.vp_arg, RuntimeValueProvider))
    self.assertFalse(options.vp_arg.is_accessible())
    with self.assertRaises(RuntimeError):
      options.vp_arg.get()

  def test_static_value_provider_positional_argument(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            'vp_pos_arg',
            help='This positional argument is a value provider',
            default='some value')
    options = UserDefinedOptions(['abc'])
    self.assertTrue(isinstance(options.vp_pos_arg, StaticValueProvider))
    self.assertTrue(options.vp_pos_arg.is_accessible())
    self.assertEqual(options.vp_pos_arg.get(), 'abc')

  def test_runtime_value_provider_positional_argument(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            'vp_pos_arg',
            help='This positional argument is a value provider')
    options = UserDefinedOptions([])
    self.assertTrue(isinstance(options.vp_pos_arg, RuntimeValueProvider))
    self.assertFalse(options.vp_pos_arg.is_accessible())
    with self.assertRaises(RuntimeError):
      options.vp_pos_arg.get()

  def test_static_value_provider_type_cast(self):
    class UserDefinedOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vp_arg',
            type=int,
            help='This flag is a value provider')

    options = UserDefinedOptions(['--vp_arg', '123'])
    self.assertTrue(isinstance(options.vp_arg, StaticValueProvider))
    self.assertTrue(options.vp_arg.is_accessible())
    self.assertEqual(options.vp_arg.get(), 123)

  def test_set_runtime_option(self):
    # define ValueProvider ptions, with and without default values
    class UserDefinedOptions1(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--vp_arg',
            help='This keyword argument is a value provider')   # set at runtime

        parser.add_value_provider_argument(         # not set, had default int
            '-v', '--vp_arg2',                      # with short form
            default=123,
            type=int)

        parser.add_value_provider_argument(         # not set, had default str
            '--vp-arg3',                            # with dash in name
            default='123',
            type=str)

        parser.add_value_provider_argument(         # not set and no default
            '--vp_arg4',
            type=float)

        parser.add_value_provider_argument(         # positional argument set
            'vp_pos_arg',                           # default & runtime ignored
            help='This positional argument is a value provider',
            type=float,
            default=5.4)

    # provide values at graph-construction time
    # (options not provided here become of the type RuntimeValueProvider)
    options = UserDefinedOptions1(['1.2'])
    self.assertFalse(options.vp_arg.is_accessible())
    self.assertFalse(options.vp_arg2.is_accessible())
    self.assertFalse(options.vp_arg3.is_accessible())
    self.assertFalse(options.vp_arg4.is_accessible())
    self.assertTrue(options.vp_pos_arg.is_accessible())

    # provide values at job-execution time
    # (options not provided here will use their default, if they have one)
    RuntimeValueProvider.set_runtime_options({'vp_arg': 'abc',
                                              'vp_pos_arg':'3.2'})
    self.assertTrue(options.vp_arg.is_accessible())
    self.assertEqual(options.vp_arg.get(), 'abc')
    self.assertTrue(options.vp_arg2.is_accessible())
    self.assertEqual(options.vp_arg2.get(), 123)
    self.assertTrue(options.vp_arg3.is_accessible())
    self.assertEqual(options.vp_arg3.get(), '123')
    self.assertTrue(options.vp_arg4.is_accessible())
    self.assertIsNone(options.vp_arg4.get())
    self.assertTrue(options.vp_pos_arg.is_accessible())
    self.assertEqual(options.vp_pos_arg.get(), 1.2)
