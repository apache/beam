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

import unittest

import apache_beam as beam
from apache_beam import typehints
from apache_beam.typehints import decorators

decorators._enable_from_callable = True


class MainInputTest(unittest.TestCase):

  def test_typed_dofn_method(self):
    # process annotations are recognized and take precedence over decorators.
    @typehints.with_input_types(typehints.Tuple[int, int])
    @typehints.with_output_types(int)
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> typehints.Tuple[str]:
        return tuple(str(element))

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
      def process(self, element: typehints.Tuple[int, int]) -> \
          typehints.List[int]:
        return [str(element)]
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
    def do_fn(element: typehints.Tuple[int, int]) -> typehints.Generator[int]:
      yield str(element)
    pardo = beam.ParDo(do_fn).with_input_types(int).with_output_types(str)

    result = [1, 2, 3] | pardo
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      ['a', 'b', 'c'] | pardo

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      [1, 2, 3] | (pardo | 'again' >> pardo)

  @unittest.skip('BEAM-7981: Iterable in output type should not be removed.')
  def test_typed_callable_iterable_output(self):
    # TODO(BEAM-7981): Both Iterables get stripped in
    #   CallableWrapperDoFn.default_type_hints, but only one should.
    def do_fn(element: int) -> typehints.Iterable[typehints.Iterable[str]]:
      return [[str(element)] * 2]

    result = [1, 2] | beam.ParDo(do_fn)
    self.assertEqual([['1', '1'], ['2', '2']], sorted(result))

  def test_typed_dofn_method_not_iterable(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> str:
        return str(element)

    with self.assertRaisesRegex(ValueError, r'str.*is not iterable'):
      [1, 2, 3] | beam.ParDo(MyDoFn())

  def test_typed_callable_not_iterable(self):
    def do_fn(element: typehints.Tuple[int, int]) -> int:
      return element[0]
    with self.assertRaisesRegex(ValueError, r'int.*is not iterable'):
      [1, 2, 3] | beam.ParDo(do_fn)

  def test_typed_dofn_kwonly(self):
    class MyDoFn(beam.DoFn):
      # TODO(BEAM-5878): A kwonly argument like
      #   timestamp=beam.DoFn.TimestampParam would not work here.
      def process(self, element: int, *, side_input: str) -> \
          typehints.Generator[typehints.Optional[int]]:
        yield str(element) if side_input else None
    my_do_fn = MyDoFn()

    result = [1, 2, 3] | beam.ParDo(my_do_fn, side_input='abc')
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*str.*got.*int.*side_input'):
      [1, 2, 3] | beam.ParDo(my_do_fn, side_input=1)

  def test_type_dofn_var_kwargs(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int, **side_inputs: typehints.Dict[str, str]) \
          -> typehints.Generator[typehints.Optional[int]]:
        yield str(element) if side_inputs else None
    my_do_fn = MyDoFn()

    result = [1, 2, 3] | beam.ParDo(my_do_fn, foo='abc', bar='def')
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*str.*got.*int.*side_inputs'):
      [1, 2, 3] | beam.ParDo(my_do_fn, a=1)


class AnnotationsTest(unittest.TestCase):

  def test_pardo_dofn(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> typehints.Generator[str]:
        yield str(element)

    th = beam.ParDo(MyDoFn()).get_type_hints()
    self.assertEqual(th.input_types, ((int,), {}))
    self.assertEqual(th.output_types, ((str,), {}))

  def test_pardo_dofn_not_iterable(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> str:
        return str(element)

    with self.assertRaisesRegexp(ValueError, r'Return value not iterable'):
      _ = beam.ParDo(MyDoFn()).get_type_hints()

  def test_pardo_wrapper(self):
    def do_fn(element: int) -> typehints.Iterable[str]:
      return [str(element)]

    th = beam.ParDo(do_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int,), {}))
    self.assertEqual(th.output_types, ((str,), {}))

  def test_pardo_wrapper_not_iterable(self):
    def do_fn(element: int) -> str:
      return str(element)

    with self.assertRaisesRegexp(ValueError, r'Return value not iterable'):
      _ = beam.ParDo(do_fn).get_type_hints()

  def test_flat_map_wrapper(self):
    def map_fn(element: int) -> typehints.Iterable[int]:
      return [element, element + 1]

    th = beam.FlatMap(map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int,), {}))
    self.assertEqual(th.output_types, ((int,), {}))

  def test_flat_map_tuple_wrapper(self):
    def tuple_map_fn(a: str, b: str, c: str) -> typehints.Iterable[str]:
      return [a, b, c]

    th = beam.FlatMapTuple(tuple_map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((str, str, str), {}))
    self.assertEqual(th.output_types, ((str,), {}))

  def test_map_wrapper(self):
    def map_fn(unused_element: int) -> int:
      return 1

    th = beam.Map(map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int,), {}))
    self.assertEqual(th.output_types, ((int,), {}))

  def test_map_tuple(self):
    def tuple_map_fn(a: str, b: str, c: str) -> str:
      return a + b + c

    th = beam.MapTuple(tuple_map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((str, str, str), {}))
    self.assertEqual(th.output_types, ((str,), {}))

  def test_filter_wrapper(self):
    def filter_fn(element: int) -> bool:
      return bool(element % 2)

    th = beam.Filter(filter_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int,), {}))
    self.assertEqual(th.output_types, ((bool,), {}))


if __name__ == '__main__':
  unittest.main()
