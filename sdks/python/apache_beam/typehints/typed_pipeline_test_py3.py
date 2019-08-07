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

"""Unit tests for type-hint objects and decorators - Python 3 syntax specific.
"""

from __future__ import absolute_import

import sys
import typing
import unittest

import apache_beam as beam
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.options.pipeline_options import OptionsContext
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints import WithTypeHints
from apache_beam.typehints.decorators import get_signature


# T1 = typehints.TypeVariable('T1')
# T2 = typehints.TypeVariable('T2')
# T3 = typehints.TypeVariable('T3')
# T4 = typehints.TypeVariable('T4')


class MainInputTest(unittest.TestCase):

  def test_typed_dofn_method(self):
    # process annotations are recognized and take precedence over decorators.
    @typehints.with_input_types(typehints.Tuple[int, int])
    @typehints.with_output_types(int)
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> str:
        return str(element)

    result = [1, 2, 3] | beam.ParDo(MyDoFn())
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      ['a', 'b', 'c'] | beam.ParDo(MyDoFn())

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      [1, 2, 3] | (beam.ParDo(MyDoFn()) | 'again' >> beam.ParDo(MyDoFn()))

  def test_typed_dofn_instance(self):
    # Type hints applied to DoFn instance take precedence over decorators and
    # process annotations.
    @typehints.with_input_types(typehints.Tuple[int, int])
    @typehints.with_output_types(int)
    class MyDoFn(beam.DoFn):
      def process(self, element: typehints.Tuple[int, int]) -> int:
        return str(element)
    my_do_fn = MyDoFn().with_input_types(int).with_output_types(str)

    result = [1, 2, 3] | beam.ParDo(my_do_fn)
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      ['a', 'b', 'c'] | beam.ParDo(my_do_fn)

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      [1, 2, 3] | (beam.ParDo(my_do_fn) | 'again' >> beam.ParDo(my_do_fn))

  def test_typed_callable_instance(self):
    # Type hints applied to ParDo instance take precedence over callable
    # decorators and annotations.
    @typehints.with_input_types(typehints.Tuple[int, int])
    @typehints.with_output_types(int)
    def do_fn(element: typehints.Tuple[int, int]) -> int:
      return str(element)
    pardo = beam.ParDo(do_fn).with_input_types(int).with_output_types(str)

    result = [1, 2, 3] | pardo
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      ['a', 'b', 'c'] | pardo

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      [1, 2, 3] | (pardo | 'again' >> pardo)


class AnnotationsTest(unittest.TestCase):

  def test_dofn(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> str:
        return str(element)

    th = beam.ParDo(MyDoFn()).get_type_hints()
    self.assertEqual(th.input_types, ([int], {}))
    self.assertEqual(th.output_types, ([str], {}))

  def test_dofn_wrapper(self):
    def do_fn(element: int) -> str:
      return str(element)

    th = beam.ParDo(do_fn).get_type_hints()
    self.assertEqual(th.input_types, ([int], {}))
    self.assertEqual(th.output_types, ([str], {}))

  # TODO: test all other transforms that accept user functions?
  #   'CombineFn',
  #   'PartitionFn',
  #   'ParDo',
  #   'FlatMap',
  #   'FlatMapTuple',
  #   'Map',
  #   'MapTuple',


  # def test_filter_wrapper(self):
  #   def filter_fn(element: int) -> bool:
  #     return bool(element % 2)
  #
  #   # TODO: self._fn in CallableWrapperDoFn points to a lambda (no annotations). what does pardo do?
  #   th = beam.Filter(filter_fn).get_type_hints()
  #   self.assertEqual(th.input_types, ([int], {}))
  #   self.assertEqual(th.output_types, ([bool], {}))

  # TODO: test all other transforms that accept user functions?
  #   'CombineGlobally',
  #   'CombinePerKey',
  #   'CombineValues',
  #   'GroupByKey',
  #   'Partition',
  #   'Windowing',
  #   'WindowInto',
  #   'Flatten',
  #   'Create',
  #   'Impulse',
  #   'RestrictionProvider'
