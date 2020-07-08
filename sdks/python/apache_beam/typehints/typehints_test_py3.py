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

import unittest

from apache_beam import Map
from apache_beam import PTransform
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import DoFn
from apache_beam.typehints import KV
from apache_beam.typehints import Iterable
from apache_beam.typehints import TypeCheckError
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

    error_str = r'An input typehint to a PTransform must be a single' \
                r' \(or nested\) type wrapped by a PCollection or PBegin. '

    with self.assertRaisesRegex(TypeCheckError, error_str):
      MyPTransform().get_type_hints()

  def test_annotations_without_output_pcollection_wrapper(self):
    class MyPTransform(PTransform):
      def expand(self, pcoll: PCollection[int]) -> str:
        return pcoll | Map(lambda num: str(num))

    error_str = r'An output typehint to a PTransform must be a single ' \
                r'\(or nested\) type wrapped by a PCollection or PDone. '

    with self.assertRaisesRegex(TypeCheckError, error_str):
      MyPTransform().get_type_hints()

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


if __name__ == '__main__':
  unittest.main()
