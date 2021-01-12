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

"""Unit tests for the type-hint objects and decorators with Python 3 syntax not
supported by 2.7."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import typing
import unittest

import apache_beam.typehints.typehints as typehints
from apache_beam import Map
from apache_beam import PTransform
from apache_beam.pvalue import PBegin
from apache_beam.pvalue import PCollection
from apache_beam.pvalue import PDone
from apache_beam.transforms.core import DoFn
from apache_beam.typehints import KV
from apache_beam.typehints import Iterable
from apache_beam.typehints.typehints import Any


class TestParDoAnnotations(unittest.TestCase):
  def test_with_side_input(self):
    class MyDoFn(DoFn):
      def process(self, element: float, side_input: str) -> \
          Iterable[KV[str, float]]:
        pass

    th = MyDoFn().get_type_hints()
    self.assertEqual(th.input_types, ((float, str), {}))
    self.assertEqual(th.output_types, ((KV[str, float], ), {}))

  def test_pep484_annotations(self):
    class MyDoFn(DoFn):
      def process(self, element: int) -> Iterable[str]:
        pass

    th = MyDoFn().get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))


class TestPTransformAnnotations(unittest.TestCase):
  def test_pep484_annotations(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: PCollection[int]) -> PCollection[str]:
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_annotations_without_input_pcollection_wrapper(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: int) -> PCollection[str]:
        return pcoll | Map(lambda num: str(num))

    error_str = (
        r'This input type hint will be ignored and not used for '
        r'type-checking purposes. Typically, input type hints for a '
        r'PTransform are single (or nested) types wrapped by a '
        r'PCollection, or PBegin. Got: {} instead.'.format(int))

    with self.assertLogs(level='WARN') as log:
      MyPTransform().get_type_hints()
      self.assertIn(error_str, log.output[0])

  def test_annotations_without_output_pcollection_wrapper(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: PCollection[int]) -> str:
        return pcoll | Map(lambda num: str(num))

    error_str = (
        r'This output type hint will be ignored and not used for '
        r'type-checking purposes. Typically, output type hints for a '
        r'PTransform are single (or nested) types wrapped by a '
        r'PCollection, PDone, or None. Got: {} instead.'.format(str))

    with self.assertLogs(level='WARN') as log:
      th = MyPTransform().get_type_hints()
      self.assertIn(error_str, log.output[0])
      self.assertEqual(th.input_types, ((int, ), {}))
      self.assertEqual(th.output_types, None)

  def test_annotations_without_input_internal_type(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: PCollection) -> PCollection[str]:
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((Any, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_annotations_without_output_internal_type(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: PCollection[int]) -> PCollection:
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((Any, ), {}))

  def test_annotations_without_any_internal_type(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((Any, ), {}))
    self.assertEqual(th.output_types, ((Any, ), {}))

  def test_annotations_without_input_typehint(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll) -> PCollection[str]:
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((Any, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_annotations_without_output_typehint(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: PCollection[int]):
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((Any, ), {}))

  def test_annotations_without_any_typehints(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll):
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, None)
    self.assertEqual(th.output_types, None)

  def test_annotations_with_pbegin(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: PBegin):
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((Any, ), {}))
    self.assertEqual(th.output_types, ((Any, ), {}))

  def test_annotations_with_pdone(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll) -> PDone:
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((Any, ), {}))
    self.assertEqual(th.output_types, ((Any, ), {}))

  def test_annotations_with_none_input(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: None) -> PCollection[str]:
        return pcoll | Map(lambda num: str(num))

    error_str = (
        r'This input type hint will be ignored and not used for '
        r'type-checking purposes. Typically, input type hints for a '
        r'PTransform are single (or nested) types wrapped by a '
        r'PCollection, or PBegin. Got: {} instead.'.format(None))

    with self.assertLogs(level='WARN') as log:
      th = MyPTransform().get_type_hints()
      self.assertIn(error_str, log.output[0])
      self.assertEqual(th.input_types, None)
      self.assertEqual(th.output_types, ((str, ), {}))

  def test_annotations_with_none_output(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll) -> None:
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((Any, ), {}))
    self.assertEqual(th.output_types, ((Any, ), {}))

  def test_annotations_with_arbitrary_output(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll) -> str:
        return pcoll | Map(lambda num: str(num))

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((Any, ), {}))
    self.assertEqual(th.output_types, None)

  def test_annotations_with_arbitrary_input_and_output(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: int) -> str:
        return pcoll | Map(lambda num: str(num))

    input_error_str = (
        r'This input type hint will be ignored and not used for '
        r'type-checking purposes. Typically, input type hints for a '
        r'PTransform are single (or nested) types wrapped by a '
        r'PCollection, or PBegin. Got: {} instead.'.format(int))

    output_error_str = (
        r'This output type hint will be ignored and not used for '
        r'type-checking purposes. Typically, output type hints for a '
        r'PTransform are single (or nested) types wrapped by a '
        r'PCollection, PDone, or None. Got: {} instead.'.format(str))

    with self.assertLogs(level='WARN') as log:
      th = MyPTransform().get_type_hints()
      self.assertIn(input_error_str, log.output[0])
      self.assertIn(output_error_str, log.output[1])
      self.assertEqual(th.input_types, None)
      self.assertEqual(th.output_types, None)

  def test_typing_module_annotations_are_converted_to_beam_annotations(self):
    class MyPTransform(PTransform):
      def expand(
          self, pcoll: PCollection[typing.Dict[str, str]]
      ) -> PCollection[typing.Dict[str, str]]:
        return pcoll

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((typehints.Dict[str, str], ), {}))
    self.assertEqual(th.input_types, ((typehints.Dict[str, str], ), {}))

  def test_nested_typing_annotations_are_converted_to_beam_annotations(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll:
         PCollection[typing.Union[int, typing.Any, typing.Dict[str, float]]]) \
      -> PCollection[typing.Union[int, typing.Any, typing.Dict[str, float]]]:
        return pcoll

    th = MyPTransform().get_type_hints()
    self.assertEqual(
        th.input_types,
        ((typehints.Union[int, typehints.Any, typehints.Dict[str,
                                                             float]], ), {}))
    self.assertEqual(
        th.input_types,
        ((typehints.Union[int, typehints.Any, typehints.Dict[str,
                                                             float]], ), {}))

  def test_mixed_annotations_are_converted_to_beam_annotations(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: typing.Any) -> typehints.Any:
        return pcoll

    th = MyPTransform().get_type_hints()
    self.assertEqual(th.input_types, ((typehints.Any, ), {}))
    self.assertEqual(th.input_types, ((typehints.Any, ), {}))


if __name__ == '__main__':
  unittest.main()
