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

"""Unit tests for the type-hint objects and decorators."""

# pytype: skip-file

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

# These test often construct a pipeline as value | PTransform to test side
# effects (e.g. errors).
# pylint: disable=expression-not-assigned


class MainInputTest(unittest.TestCase):
  def assertStartswith(self, msg, prefix):
    self.assertTrue(
        msg.startswith(prefix), '"%s" does not start with "%s"' % (msg, prefix))

  def test_bad_main_input(self):
    @typehints.with_input_types(str, int)
    def repeat(s, times):
      return s * times

    with self.assertRaises(typehints.TypeCheckError):
      [1, 2, 3] | beam.Map(repeat, 3)

  def test_non_function(self):
    result = ['a', 'bb', 'c'] | beam.Map(str.upper)
    self.assertEqual(['A', 'BB', 'C'], sorted(result))

    result = ['xa', 'bbx', 'xcx'] | beam.Map(str.strip, 'x')
    self.assertEqual(['a', 'bb', 'c'], sorted(result))

    result = ['1', '10', '100'] | beam.Map(int)
    self.assertEqual([1, 10, 100], sorted(result))

    result = ['1', '10', '100'] | beam.Map(int, 16)
    self.assertEqual([1, 16, 256], sorted(result))

  def test_non_function_fails(self):
    with self.assertRaises(typehints.TypeCheckError):
      [1, 2, 3] | beam.Map(str.upper)

  def test_loose_bounds(self):
    @typehints.with_input_types(typing.Union[int, float])
    @typehints.with_output_types(str)
    def format_number(x):
      return '%g' % x

    result = [1, 2, 3] | beam.Map(format_number)
    self.assertEqual(['1', '2', '3'], sorted(result))

  def test_typed_dofn_class(self):
    @typehints.with_input_types(int)
    @typehints.with_output_types(str)
    class MyDoFn(beam.DoFn):
      def process(self, element):
        return [str(element)]

    result = [1, 2, 3] | beam.ParDo(MyDoFn())
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*applied.*str'):
      ['a', 'b', 'c'] | beam.ParDo(MyDoFn())

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*applied.*str'):
      [1, 2, 3] | (beam.ParDo(MyDoFn()) | 'again' >> beam.ParDo(MyDoFn()))

  def test_typed_dofn_method(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> typehints.Tuple[str]:
        return tuple(str(element))

    result = [1, 2, 3] | beam.ParDo(MyDoFn())
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*applied.*str'):
      _ = ['a', 'b', 'c'] | beam.ParDo(MyDoFn())

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*applied.*str'):
      _ = [1, 2, 3] | (beam.ParDo(MyDoFn()) | 'again' >> beam.ParDo(MyDoFn()))

  def test_typed_dofn_method_with_class_decorators(self):
    # Class decorators take precedence over PEP 484 hints.
    @typehints.with_input_types(typehints.Tuple[int, int])
    @typehints.with_output_types(int)
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> typehints.Tuple[str]:
        yield element[0]

    result = [(1, 2)] | beam.ParDo(MyDoFn())
    self.assertEqual([1], sorted(result))

    with self.assertRaisesRegex(
        typehints.TypeCheckError,
        r'requires.*Tuple\[<class \'int\'>, <class \'int\'>\].*applied.*str'):
      _ = ['a', 'b', 'c'] | beam.ParDo(MyDoFn())

    with self.assertRaisesRegex(
        typehints.TypeCheckError,
        r'requires.*Tuple\[<class \'int\'>, <class \'int\'>\].*applied.*int'):
      _ = [1, 2, 3] | (beam.ParDo(MyDoFn()) | 'again' >> beam.ParDo(MyDoFn()))

  def test_typed_callable_iterable_output(self):
    # Only the outer Iterable should be stripped.
    def do_fn(element: int) -> typehints.Iterable[typehints.Iterable[str]]:
      return [[str(element)] * 2]

    result = [1, 2] | beam.ParDo(do_fn)
    self.assertEqual([['1', '1'], ['2', '2']], sorted(result))

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
                                r'requires.*int.*applied.*str'):
      _ = ['a', 'b', 'c'] | beam.ParDo(my_do_fn)

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*applied.*str'):
      _ = [1, 2, 3] | (beam.ParDo(my_do_fn) | 'again' >> beam.ParDo(my_do_fn))

  def test_typed_callable_instance(self):
    # Type hints applied to ParDo instance take precedence over callable
    # decorators and annotations.
    @typehints.with_input_types(typehints.Tuple[int, int])
    @typehints.with_output_types(typehints.Generator[int])
    def do_fn(element: typehints.Tuple[int, int]) -> typehints.Generator[str]:
      yield str(element)

    pardo = beam.ParDo(do_fn).with_input_types(int).with_output_types(str)

    result = [1, 2, 3] | pardo
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*applied.*str'):
      _ = ['a', 'b', 'c'] | pardo

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*applied.*str'):
      _ = [1, 2, 3] | (pardo | 'again' >> pardo)

  def test_filter_type_hint(self):
    @typehints.with_input_types(int)
    def filter_fn(data):
      return data % 2

    self.assertEqual([1, 3], [1, 2, 3] | beam.Filter(filter_fn))

  def test_partition(self):
    with TestPipeline() as p:
      even, odd = (p
                   | beam.Create([1, 2, 3])
                   | 'even_odd' >> beam.Partition(lambda e, _: e % 2, 2))
      self.assertIsNotNone(even.element_type)
      self.assertIsNotNone(odd.element_type)
      res_even = (
          even
          | 'IdEven' >> beam.ParDo(lambda e: [e]).with_input_types(int))
      res_odd = (
          odd
          | 'IdOdd' >> beam.ParDo(lambda e: [e]).with_input_types(int))
      assert_that(res_even, equal_to([2]), label='even_check')
      assert_that(res_odd, equal_to([1, 3]), label='odd_check')

  def test_typed_dofn_multi_output(self):
    class MyDoFn(beam.DoFn):
      def process(self, element):
        if element % 2:
          yield beam.pvalue.TaggedOutput('odd', element)
        else:
          yield beam.pvalue.TaggedOutput('even', element)

    with TestPipeline() as p:
      res = (
          p
          | beam.Create([1, 2, 3])
          | beam.ParDo(MyDoFn()).with_outputs('odd', 'even'))
      self.assertIsNotNone(res[None].element_type)
      self.assertIsNotNone(res['even'].element_type)
      self.assertIsNotNone(res['odd'].element_type)
      res_main = (
          res[None]
          | 'id_none' >> beam.ParDo(lambda e: [e]).with_input_types(int))
      res_even = (
          res['even']
          | 'id_even' >> beam.ParDo(lambda e: [e]).with_input_types(int))
      res_odd = (
          res['odd']
          | 'id_odd' >> beam.ParDo(lambda e: [e]).with_input_types(int))
      assert_that(res_main, equal_to([]), label='none_check')
      assert_that(res_even, equal_to([2]), label='even_check')
      assert_that(res_odd, equal_to([1, 3]), label='odd_check')

    with self.assertRaises(ValueError):
      _ = res['undeclared tag']

  def test_typed_dofn_multi_output_no_tags(self):
    class MyDoFn(beam.DoFn):
      def process(self, element):
        if element % 2:
          yield beam.pvalue.TaggedOutput('odd', element)
        else:
          yield beam.pvalue.TaggedOutput('even', element)

    with TestPipeline() as p:
      res = (p | beam.Create([1, 2, 3]) | beam.ParDo(MyDoFn()).with_outputs())
      self.assertIsNotNone(res[None].element_type)
      self.assertIsNotNone(res['even'].element_type)
      self.assertIsNotNone(res['odd'].element_type)
      res_main = (
          res[None]
          | 'id_none' >> beam.ParDo(lambda e: [e]).with_input_types(int))
      res_even = (
          res['even']
          | 'id_even' >> beam.ParDo(lambda e: [e]).with_input_types(int))
      res_odd = (
          res['odd']
          | 'id_odd' >> beam.ParDo(lambda e: [e]).with_input_types(int))
      assert_that(res_main, equal_to([]), label='none_check')
      assert_that(res_even, equal_to([2]), label='even_check')
      assert_that(res_odd, equal_to([1, 3]), label='odd_check')

  def test_typed_ptransform_fn_pre_hints(self):
    # Test that type hints are propagated to the created PTransform.
    # Decorator appears before type hints. This is the more common style.
    @beam.ptransform_fn
    @typehints.with_input_types(int)
    def MyMap(pcoll):
      return pcoll | beam.ParDo(lambda x: [x])

    self.assertListEqual([1, 2, 3], [1, 2, 3] | MyMap())
    with self.assertRaises(typehints.TypeCheckError):
      _ = ['a'] | MyMap()

  def test_typed_ptransform_fn_post_hints(self):
    # Test that type hints are propagated to the created PTransform.
    # Decorator appears after type hints. This style is required for Cython
    # functions, since they don't accept assigning attributes to them.
    @typehints.with_input_types(int)
    @beam.ptransform_fn
    def MyMap(pcoll):
      return pcoll | beam.ParDo(lambda x: [x])

    self.assertListEqual([1, 2, 3], [1, 2, 3] | MyMap())
    with self.assertRaises(typehints.TypeCheckError):
      _ = ['a'] | MyMap()

  def test_typed_ptransform_fn_multi_input_types_pos(self):
    @beam.ptransform_fn
    @beam.typehints.with_input_types(str, int)
    def multi_input(pcoll_tuple, additional_arg):
      _, _ = pcoll_tuple
      assert additional_arg == 'additional_arg'

    with TestPipeline() as p:
      pcoll1 = p | 'c1' >> beam.Create(['a'])
      pcoll2 = p | 'c2' >> beam.Create([1])
      _ = (pcoll1, pcoll2) | multi_input('additional_arg')
      with self.assertRaises(typehints.TypeCheckError):
        _ = (pcoll2, pcoll1) | 'fails' >> multi_input('additional_arg')

  def test_typed_ptransform_fn_multi_input_types_kw(self):
    @beam.ptransform_fn
    @beam.typehints.with_input_types(strings=str, integers=int)
    def multi_input(pcoll_dict, additional_arg):
      _ = pcoll_dict['strings']
      _ = pcoll_dict['integers']
      assert additional_arg == 'additional_arg'

    with TestPipeline() as p:
      pcoll1 = p | 'c1' >> beam.Create(['a'])
      pcoll2 = p | 'c2' >> beam.Create([1])
      _ = {
          'strings': pcoll1, 'integers': pcoll2
      } | multi_input('additional_arg')
      with self.assertRaises(typehints.TypeCheckError):
        _ = {
            'strings': pcoll2, 'integers': pcoll1
        } | 'fails' >> multi_input('additional_arg')

  def test_typed_dofn_method_not_iterable(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> str:
        return str(element)

    with self.assertRaisesRegex(ValueError, r'str.*is not iterable'):
      _ = [1, 2, 3] | beam.ParDo(MyDoFn())

  def test_typed_dofn_method_return_none(self):
    class MyDoFn(beam.DoFn):
      def process(self, unused_element: int) -> None:
        pass

    result = [1, 2, 3] | beam.ParDo(MyDoFn())
    self.assertListEqual([], result)

  def test_typed_dofn_method_return_optional(self):
    class MyDoFn(beam.DoFn):
      def process(
          self,
          unused_element: int) -> typehints.Optional[typehints.Iterable[int]]:
        pass

    result = [1, 2, 3] | beam.ParDo(MyDoFn())
    self.assertListEqual([], result)

  def test_typed_dofn_method_return_optional_not_iterable(self):
    class MyDoFn(beam.DoFn):
      def process(self, unused_element: int) -> typehints.Optional[int]:
        pass

    with self.assertRaisesRegex(ValueError, r'int.*is not iterable'):
      _ = [1, 2, 3] | beam.ParDo(MyDoFn())

  def test_typed_callable_not_iterable(self):
    def do_fn(element: int) -> int:
      return element

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'int.*is not iterable'):
      _ = [1, 2, 3] | beam.ParDo(do_fn)

  def test_typed_dofn_kwonly(self):
    class MyDoFn(beam.DoFn):
      # TODO(BEAM-5878): A kwonly argument like
      #   timestamp=beam.DoFn.TimestampParam would not work here.
      def process(self, element: int, *, side_input: str) -> \
          typehints.Generator[typehints.Optional[str]]:
        yield str(element) if side_input else None

    my_do_fn = MyDoFn()

    result = [1, 2, 3] | beam.ParDo(my_do_fn, side_input='abc')
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*str.*got.*int.*side_input'):
      _ = [1, 2, 3] | beam.ParDo(my_do_fn, side_input=1)

  def test_typed_dofn_var_kwargs(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int, **side_inputs: typehints.Dict[str, str]) \
          -> typehints.Generator[typehints.Optional[str]]:
        yield str(element) if side_inputs else None

    my_do_fn = MyDoFn()

    result = [1, 2, 3] | beam.ParDo(my_do_fn, foo='abc', bar='def')
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*str.*got.*int.*side_inputs'):
      _ = [1, 2, 3] | beam.ParDo(my_do_fn, a=1)

  def test_typed_callable_string_literals(self):
    def do_fn(element: 'int') -> 'typehints.List[str]':
      return [[str(element)] * 2]

    result = [1, 2] | beam.ParDo(do_fn)
    self.assertEqual([['1', '1'], ['2', '2']], sorted(result))

  def test_typed_ptransform_fn(self):
    # Test that type hints are propagated to the created PTransform.
    @beam.ptransform_fn
    @typehints.with_input_types(int)
    def MyMap(pcoll):
      def fn(element: int):
        yield element

      return pcoll | beam.ParDo(fn)

    self.assertListEqual([1, 2, 3], [1, 2, 3] | MyMap())
    with self.assertRaisesRegex(typehints.TypeCheckError, r'int.*got.*str'):
      _ = ['a'] | MyMap()

  def test_typed_ptransform_fn_conflicting_hints(self):
    # In this case, both MyMap and its contained ParDo have separate type
    # checks (that disagree with each other).
    @beam.ptransform_fn
    @typehints.with_input_types(int)
    def MyMap(pcoll):
      def fn(element: float):
        yield element

      return pcoll | beam.ParDo(fn)

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'ParDo.*requires.*float.*applied.*int'):
      _ = [1, 2, 3] | MyMap()
    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'MyMap.*expected.*int.*got.*str'):
      _ = ['a'] | MyMap()

  def test_typed_dofn_string_literals(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: 'int') -> 'typehints.List[str]':
        return [[str(element)] * 2]

    result = [1, 2] | beam.ParDo(MyDoFn())
    self.assertEqual([['1', '1'], ['2', '2']], sorted(result))

  def test_typed_map(self):
    def fn(element: int) -> int:
      return element * 2

    result = [1, 2, 3] | beam.Map(fn)
    self.assertEqual([2, 4, 6], sorted(result))

  def test_typed_map_return_optional(self):
    # None is a valid element value for Map.
    def fn(element: int) -> typehints.Optional[int]:
      if element > 1:
        return element

    result = [1, 2, 3] | beam.Map(fn)
    self.assertCountEqual([None, 2, 3], result)

  def test_typed_flatmap(self):
    def fn(element: int) -> typehints.Iterable[int]:
      yield element * 2

    result = [1, 2, 3] | beam.FlatMap(fn)
    self.assertCountEqual([2, 4, 6], result)

  def test_typed_flatmap_output_hint_not_iterable(self):
    def fn(element: int) -> int:
      return element * 2

    # This is raised (originally) in strip_iterable.
    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'int.*is not iterable'):
      _ = [1, 2, 3] | beam.FlatMap(fn)

  def test_typed_flatmap_output_value_not_iterable(self):
    def fn(element: int) -> typehints.Iterable[int]:
      return element * 2

    # This is raised in runners/common.py (process_outputs).
    with self.assertRaisesRegex(TypeError, r'int.*is not iterable'):
      _ = [1, 2, 3] | beam.FlatMap(fn)

  def test_typed_flatmap_optional(self):
    def fn(element: int) -> typehints.Optional[typehints.Iterable[int]]:
      if element > 1:
        yield element * 2

    # Verify that the output type of fn is int and not Optional[int].
    def fn2(element: int) -> int:
      return element

    result = [1, 2, 3] | beam.FlatMap(fn) | beam.Map(fn2)
    self.assertCountEqual([4, 6], result)

  def test_typed_ptransform_with_no_error(self):
    class StrToInt(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[str]) -> beam.pvalue.PCollection[int]:
        return pcoll | beam.Map(lambda x: int(x))

    class IntToStr(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[int]) -> beam.pvalue.PCollection[str]:
        return pcoll | beam.Map(lambda x: str(x))

    _ = ['1', '2', '3'] | StrToInt() | IntToStr()

  def test_typed_ptransform_with_bad_typehints(self):
    class StrToInt(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[str]) -> beam.pvalue.PCollection[int]:
        return pcoll | beam.Map(lambda x: int(x))

    class IntToStr(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[str]) -> beam.pvalue.PCollection[str]:
        return pcoll | beam.Map(lambda x: str(x))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                "Input type hint violation at IntToStr: "
                                "expected <class 'str'>, got <class 'int'>"):
      _ = ['1', '2', '3'] | StrToInt() | IntToStr()

  def test_typed_ptransform_with_bad_input(self):
    class StrToInt(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[str]) -> beam.pvalue.PCollection[int]:
        return pcoll | beam.Map(lambda x: int(x))

    class IntToStr(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[int]) -> beam.pvalue.PCollection[str]:
        return pcoll | beam.Map(lambda x: str(x))

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                "Input type hint violation at StrToInt: "
                                "expected <class 'str'>, got <class 'int'>"):
      # Feed integers to a PTransform that expects strings
      _ = [1, 2, 3] | StrToInt() | IntToStr()

  def test_typed_ptransform_with_partial_typehints(self):
    class StrToInt(beam.PTransform):
      def expand(self, pcoll) -> beam.pvalue.PCollection[int]:
        return pcoll | beam.Map(lambda x: int(x))

    class IntToStr(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[int]) -> beam.pvalue.PCollection[str]:
        return pcoll | beam.Map(lambda x: str(x))

    # Feed integers to a PTransform that should expect strings
    # but has no typehints so it expects any
    _ = [1, 2, 3] | StrToInt() | IntToStr()

  def test_typed_ptransform_with_bare_wrappers(self):
    class StrToInt(beam.PTransform):
      def expand(
          self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
        return pcoll | beam.Map(lambda x: int(x))

    class IntToStr(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[int]) -> beam.pvalue.PCollection[str]:
        return pcoll | beam.Map(lambda x: str(x))

    _ = [1, 2, 3] | StrToInt() | IntToStr()

  def test_typed_ptransform_with_no_typehints(self):
    class StrToInt(beam.PTransform):
      def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: int(x))

    class IntToStr(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[int]) -> beam.pvalue.PCollection[str]:
        return pcoll | beam.Map(lambda x: str(x))

    # Feed integers to a PTransform that should expect strings
    # but has no typehints so it expects any
    _ = [1, 2, 3] | StrToInt() | IntToStr()

  def test_typed_ptransform_with_generic_annotations(self):
    T = typing.TypeVar('T')

    class IntToInt(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[T]) -> beam.pvalue.PCollection[T]:
        return pcoll | beam.Map(lambda x: x)

    class IntToStr(beam.PTransform):
      def expand(
          self,
          pcoll: beam.pvalue.PCollection[T]) -> beam.pvalue.PCollection[str]:
        return pcoll | beam.Map(lambda x: str(x))

    _ = [1, 2, 3] | IntToInt() | IntToStr()

  def test_typed_ptransform_with_do_outputs_tuple_compiles(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int, *args, **kwargs):
        if element % 2:
          yield beam.pvalue.TaggedOutput('odd', 1)
        else:
          yield beam.pvalue.TaggedOutput('even', 1)

    class MyPTransform(beam.PTransform):
      def expand(self, pcoll: beam.pvalue.PCollection[int]):
        return pcoll | beam.ParDo(MyDoFn()).with_outputs('odd', 'even')

    # This test fails if you remove the following line from ptransform.py
    # if isinstance(pvalue_, DoOutputsTuple): continue
    _ = [1, 2, 3] | MyPTransform()

  def test_typed_ptransform_with_unknown_type_vars_tuple_compiles(self):
    @typehints.with_input_types(typing.TypeVar('T'))
    @typehints.with_output_types(typing.TypeVar('U'))
    def produces_unkown(e):
      return e

    @typehints.with_input_types(int)
    def accepts_int(e):
      return e

    class MyPTransform(beam.PTransform):
      def expand(self, pcoll):
        unknowns = pcoll | beam.Map(produces_unkown)
        ints = pcoll | beam.Map(int)
        return (unknowns, ints) | beam.Flatten() | beam.Map(accepts_int)

    _ = [1, 2, 3] | MyPTransform()


class NativeTypesTest(unittest.TestCase):
  def test_good_main_input(self):
    @typehints.with_input_types(typing.Tuple[str, int])
    def munge(s_i):
      (s, i) = s_i
      return (s + 's', i * 2)

    result = [('apple', 5), ('pear', 3)] | beam.Map(munge)
    self.assertEqual([('apples', 10), ('pears', 6)], sorted(result))

  def test_bad_main_input(self):
    @typehints.with_input_types(typing.Tuple[str, str])
    def munge(s_i):
      (s, i) = s_i
      return (s + 's', i * 2)

    with self.assertRaises(typehints.TypeCheckError):
      [('apple', 5), ('pear', 3)] | beam.Map(munge)

  def test_bad_main_output(self):
    @typehints.with_input_types(typing.Tuple[int, int])
    @typehints.with_output_types(typing.Tuple[str, str])
    def munge(a_b):
      (a, b) = a_b
      return (str(a), str(b))

    with self.assertRaises(typehints.TypeCheckError):
      [(5, 4), (3, 2)] | beam.Map(munge) | 'Again' >> beam.Map(munge)


class SideInputTest(unittest.TestCase):
  def _run_repeat_test(self, repeat):
    self._run_repeat_test_good(repeat)
    self._run_repeat_test_bad(repeat)

  @OptionsContext(pipeline_type_check=True)
  def _run_repeat_test_good(self, repeat):
    # As a positional argument.
    result = ['a', 'bb', 'c'] | beam.Map(repeat, 3)
    self.assertEqual(['aaa', 'bbbbbb', 'ccc'], sorted(result))

    # As a keyword argument.
    result = ['a', 'bb', 'c'] | beam.Map(repeat, times=3)
    self.assertEqual(['aaa', 'bbbbbb', 'ccc'], sorted(result))

  def _run_repeat_test_bad(self, repeat):
    # Various mismatches.
    with self.assertRaises(typehints.TypeCheckError):
      ['a', 'bb', 'c'] | beam.Map(repeat, 'z')
    with self.assertRaises(typehints.TypeCheckError):
      ['a', 'bb', 'c'] | beam.Map(repeat, times='z')
    with self.assertRaises(typehints.TypeCheckError):
      ['a', 'bb', 'c'] | beam.Map(repeat, 3, 4)
    if all(param.default == param.empty
           for param in get_signature(repeat).parameters.values()):
      with self.assertRaisesRegex(typehints.TypeCheckError,
                                  r'(takes exactly|missing a required)'):
        ['a', 'bb', 'c'] | beam.Map(repeat)

  def test_basic_side_input_hint(self):
    @typehints.with_input_types(str, int)
    def repeat(s, times):
      return s * times

    self._run_repeat_test(repeat)

  def test_keyword_side_input_hint(self):
    @typehints.with_input_types(str, times=int)
    def repeat(s, times):
      return s * times

    self._run_repeat_test(repeat)

  def test_default_typed_hint(self):
    @typehints.with_input_types(str, int)
    def repeat(s, times=3):
      return s * times

    self._run_repeat_test(repeat)

  def test_default_untyped_hint(self):
    @typehints.with_input_types(str)
    def repeat(s, times=3):
      return s * times

    # No type checking on default arg.
    self._run_repeat_test_good(repeat)

  @OptionsContext(pipeline_type_check=True)
  def test_varargs_side_input_hint(self):
    @typehints.with_input_types(str, int)
    def repeat(s, *times):
      return s * times[0]

    result = ['a', 'bb', 'c'] | beam.Map(repeat, 3)
    self.assertEqual(['aaa', 'bbbbbb', 'ccc'], sorted(result))

    with self.assertRaisesRegex(
        typehints.TypeCheckError,
        (r'requires Tuple\[<class \'int\'>, ...\] but got '
         r'Tuple\[<class \'str\'>, ...\]')):
      ['a', 'bb', 'c'] | beam.Map(repeat, 'z')

  def test_var_positional_only_side_input_hint(self):
    # Test that a lambda that accepts only a VAR_POSITIONAL can accept
    # side-inputs.
    # TODO(https://github.com/apache/beam/issues/19824): There's a bug with
    #   trivial_inference inferring the output type when side-inputs are used
    #   (their type hints are not passed). Remove with_output_types(...) when
    #   this bug is fixed.
    result = (['a', 'b', 'c']
              | beam.Map(lambda *args: args, 5).with_input_types(
                  str, int).with_output_types(typehints.Tuple[str, int]))
    self.assertEqual([('a', 5), ('b', 5), ('c', 5)], sorted(result))

    with self.assertRaisesRegex(
        typehints.TypeCheckError,
        r'requires.*Tuple\[Union\[<class \'int\'>, <class \'str\'>\], ...\].*'
        r'applied.*Tuple\[Union\[<class \'float\'>, <class \'int\'>\], ...\]'):
      _ = [1.2] | beam.Map(lambda *_: 'a', 5).with_input_types(int, str)

  def test_var_keyword_side_input_hint(self):
    # Test that a lambda that accepts a VAR_KEYWORD can accept
    # side-inputs.
    result = (['a', 'b', 'c']
              | beam.Map(lambda e, **kwargs:
                         (e, kwargs), kw=5).with_input_types(str, ignored=int))
    self.assertEqual([('a', {
        'kw': 5
    }), ('b', {
        'kw': 5
    }), ('c', {
        'kw': 5
    })],
                     sorted(result))

    with self.assertRaisesRegex(
        typehints.TypeCheckError,
        r'requires Dict\[<class \'str\'>, <class \'str\'>\] but got '
        r'Dict\[<class \'str\'>, <class \'int\'>\]'):
      _ = (['a', 'b', 'c']
           | beam.Map(lambda e, **_: 'a', kw=5).with_input_types(
               str, ignored=str))

  def test_deferred_side_inputs(self):
    @typehints.with_input_types(str, int)
    def repeat(s, times):
      return s * times

    with TestPipeline() as p:
      main_input = p | beam.Create(['a', 'bb', 'c'])
      side_input = p | 'side' >> beam.Create([3])
      result = main_input | beam.Map(repeat, pvalue.AsSingleton(side_input))
      assert_that(result, equal_to(['aaa', 'bbbbbb', 'ccc']))

    bad_side_input = p | 'bad_side' >> beam.Create(['z'])
    with self.assertRaises(typehints.TypeCheckError):
      main_input | 'bis' >> beam.Map(repeat, pvalue.AsSingleton(bad_side_input))

  def test_deferred_side_input_iterable(self):
    @typehints.with_input_types(str, typing.Iterable[str])
    def concat(glue, items):
      return glue.join(sorted(items))

    with TestPipeline() as p:
      main_input = p | beam.Create(['a', 'bb', 'c'])
      side_input = p | 'side' >> beam.Create(['x', 'y', 'z'])
      result = main_input | beam.Map(concat, pvalue.AsIter(side_input))
      assert_that(result, equal_to(['xayaz', 'xbbybbz', 'xcycz']))

    bad_side_input = p | 'bad_side' >> beam.Create([1, 2, 3])
    with self.assertRaises(typehints.TypeCheckError):
      main_input | 'fail' >> beam.Map(concat, pvalue.AsIter(bad_side_input))


class CustomTransformTest(unittest.TestCase):
  class CustomTransform(beam.PTransform):
    def _extract_input_pvalues(self, pvalueish):
      return pvalueish, (pvalueish['in0'], pvalueish['in1'])

    def expand(self, pvalueish):
      return {'out0': pvalueish['in0'], 'out1': pvalueish['in1']}

    # TODO(robertwb): (typecheck) Make these the default?
    def with_input_types(self, *args, **kwargs):
      return WithTypeHints.with_input_types(self, *args, **kwargs)

    def with_output_types(self, *args, **kwargs):
      return WithTypeHints.with_output_types(self, *args, **kwargs)

  test_input = {'in0': ['a', 'b', 'c'], 'in1': [1, 2, 3]}

  def check_output(self, result):
    self.assertEqual(['a', 'b', 'c'], sorted(result['out0']))
    self.assertEqual([1, 2, 3], sorted(result['out1']))

  def test_custom_transform(self):
    self.check_output(self.test_input | self.CustomTransform())

  def test_keyword_type_hints(self):
    self.check_output(
        self.test_input
        | self.CustomTransform().with_input_types(in0=str, in1=int))
    self.check_output(
        self.test_input | self.CustomTransform().with_input_types(in0=str))
    self.check_output(
        self.test_input
        | self.CustomTransform().with_output_types(out0=str, out1=int))
    with self.assertRaises(typehints.TypeCheckError):
      self.test_input | self.CustomTransform().with_input_types(in0=int)
    with self.assertRaises(typehints.TypeCheckError):
      self.test_input | self.CustomTransform().with_output_types(out0=int)

  def test_flat_type_hint(self):
    # Type hint is applied to both.
    ({
        'in0': ['a', 'b', 'c'], 'in1': ['x', 'y', 'z']
    }
     | self.CustomTransform().with_input_types(str))
    with self.assertRaises(typehints.TypeCheckError):
      self.test_input | self.CustomTransform().with_input_types(str)
    with self.assertRaises(typehints.TypeCheckError):
      self.test_input | self.CustomTransform().with_input_types(int)
    with self.assertRaises(typehints.TypeCheckError):
      self.test_input | self.CustomTransform().with_output_types(int)


class AnnotationsTest(unittest.TestCase):
  def test_pardo_wrapper_builtin_method(self):
    th = beam.ParDo(str.strip).get_type_hints()
    if sys.version_info < (3, 7):
      self.assertEqual(th.input_types, ((str, ), {}))
    else:
      # Python 3.7+ has annotations for CPython builtins
      # (_MethodDescriptorType).
      self.assertEqual(th.input_types, ((str, typehints.Any), {}))
    self.assertEqual(th.output_types, ((typehints.Any, ), {}))

  def test_pardo_wrapper_builtin_type(self):
    th = beam.ParDo(list).get_type_hints()
    self.assertEqual(th.input_types, ((typehints.Any, ), {}))

    self.assertEqual(th.output_types, ((typehints.Any, ), {}))

  def test_pardo_wrapper_builtin_func(self):
    th = beam.ParDo(len).get_type_hints()
    self.assertIsNone(th.input_types)
    self.assertIsNone(th.output_types)

  def test_pardo_dofn(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> typehints.Generator[str]:
        yield str(element)

    th = beam.ParDo(MyDoFn()).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_pardo_dofn_not_iterable(self):
    class MyDoFn(beam.DoFn):
      def process(self, element: int) -> str:
        return str(element)

    with self.assertRaisesRegex(ValueError, r'str.*is not iterable'):
      _ = beam.ParDo(MyDoFn()).get_type_hints()

  def test_pardo_wrapper(self):
    def do_fn(element: int) -> typehints.Iterable[str]:
      return [str(element)]

    th = beam.ParDo(do_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_pardo_wrapper_tuple(self):
    # Test case for callables that return key-value pairs for GBK. The outer
    # Iterable should be stripped but the inner Tuple left intact.
    def do_fn(element: int) -> typehints.Iterable[typehints.Tuple[str, int]]:
      return [(str(element), element)]

    th = beam.ParDo(do_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((typehints.Tuple[str, int], ), {}))

  def test_pardo_wrapper_not_iterable(self):
    def do_fn(element: int) -> str:
      return str(element)

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'str.*is not iterable'):
      _ = beam.ParDo(do_fn).get_type_hints()

  def test_flat_map_wrapper(self):
    def map_fn(element: int) -> typehints.Iterable[int]:
      return [element, element + 1]

    th = beam.FlatMap(map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((int, ), {}))

  def test_flat_map_wrapper_optional_output(self):
    # Optional should not affect output type (Nones are ignored).
    def map_fn(element: int) -> typehints.Optional[typehints.Iterable[int]]:
      return [element, element + 1]

    th = beam.FlatMap(map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((int, ), {}))

  @unittest.skip(
      'https://github.com/apache/beam/issues/19961: Py3 annotations not yet '
      'supported for MapTuple')
  def test_flat_map_tuple_wrapper(self):
    # TODO(https://github.com/apache/beam/issues/19961): Also test with a fn
    # that accepts default arguments.
    def tuple_map_fn(a: str, b: str, c: str) -> typehints.Iterable[str]:
      return [a, b, c]

    th = beam.FlatMapTuple(tuple_map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((str, str, str), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_map_wrapper(self):
    def map_fn(unused_element: int) -> int:
      return 1

    th = beam.Map(map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((int, ), {}))

  def test_map_wrapper_optional_output(self):
    # Optional does affect output type (Nones are NOT ignored).
    def map_fn(unused_element: int) -> typehints.Optional[int]:
      return 1

    th = beam.Map(map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((typehints.Optional[int], ), {}))

  @unittest.skip(
      'https://github.com/apache/beam/issues/19961: Py3 annotations not yet '
      'supported for MapTuple')
  def test_map_tuple(self):
    # TODO(https://github.com/apache/beam/issues/19961): Also test with a fn
    # that accepts default arguments.
    def tuple_map_fn(a: str, b: str, c: str) -> str:
      return a + b + c

    th = beam.MapTuple(tuple_map_fn).get_type_hints()
    self.assertEqual(th.input_types, ((str, str, str), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_filter_wrapper(self):
    def filter_fn(element: int) -> bool:
      return bool(element % 2)

    th = beam.Filter(filter_fn).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((int, ), {}))


if __name__ == '__main__':
  unittest.main()
