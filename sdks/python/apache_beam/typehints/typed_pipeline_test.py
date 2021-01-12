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

from __future__ import absolute_import

import sys
import typing
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

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

  @unittest.skipIf(
      sys.version_info.major >= 3 and sys.version_info < (3, 7, 0),
      'Function signatures for builtins are not available in Python 3 before '
      'version 3.7.')
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
                                r'requires.*int.*got.*str'):
      ['a', 'b', 'c'] | beam.ParDo(MyDoFn())

    with self.assertRaisesRegex(typehints.TypeCheckError,
                                r'requires.*int.*got.*str'):
      [1, 2, 3] | (beam.ParDo(MyDoFn()) | 'again' >> beam.ParDo(MyDoFn()))

  def test_typed_callable_iterable_output(self):
    @typehints.with_input_types(int)
    @typehints.with_output_types(typehints.Iterable[typehints.Iterable[str]])
    def do_fn(element):
      return [[str(element)] * 2]

    result = [1, 2] | beam.ParDo(do_fn)
    self.assertEqual([['1', '1'], ['2', '2']], sorted(result))

  def test_typed_dofn_instance(self):
    class MyDoFn(beam.DoFn):
      def process(self, element):
        return [str(element)]

    my_do_fn = MyDoFn().with_input_types(int).with_output_types(str)

    result = [1, 2, 3] | beam.ParDo(my_do_fn)
    self.assertEqual(['1', '2', '3'], sorted(result))

    with self.assertRaises(typehints.TypeCheckError):
      ['a', 'b', 'c'] | beam.ParDo(my_do_fn)

    with self.assertRaises(typehints.TypeCheckError):
      [1, 2, 3] | (beam.ParDo(my_do_fn) | 'again' >> beam.ParDo(my_do_fn))

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

    if sys.version_info >= (3, ):
      with self.assertRaisesRegex(
          typehints.TypeCheckError,
          r'requires Tuple\[int, ...\] but got Tuple\[str, ...\]'):
        ['a', 'bb', 'c'] | beam.Map(repeat, 'z')

  def test_var_positional_only_side_input_hint(self):
    # Test that a lambda that accepts only a VAR_POSITIONAL can accept
    # side-inputs.
    # TODO(BEAM-8247): There's a bug with trivial_inference inferring the output
    #   type when side-inputs are used (their type hints are not passed). Remove
    #   with_output_types(...) when this bug is fixed.
    result = (['a', 'b', 'c']
              | beam.Map(lambda *args: args, 5).with_input_types(
                  str, int).with_output_types(typehints.Tuple[str, int]))
    self.assertEqual([('a', 5), ('b', 5), ('c', 5)], sorted(result))

    if sys.version_info >= (3, ):
      with self.assertRaisesRegex(
          typehints.TypeCheckError,
          r'requires Tuple\[Union\[int, str\], ...\] but got '
          r'Tuple\[Union\[float, int\], ...\]'):
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

    if sys.version_info >= (3, ):
      with self.assertRaisesRegex(
          typehints.TypeCheckError,
          r'requires Dict\[str, str\] but got Dict\[str, int\]'):
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
    if sys.version_info < (3, 7):
      self.assertEqual(
          th.input_types,
          ((typehints.Any, typehints.decorators._ANY_VAR_POSITIONAL), {
              '__unknown__keywords': typehints.decorators._ANY_VAR_KEYWORD
          }))
    else:
      # Python 3.7+ supports signatures for builtins like 'list'.
      self.assertEqual(th.input_types, ((typehints.Any, ), {}))

    self.assertEqual(th.output_types, ((typehints.Any, ), {}))

  def test_pardo_wrapper_builtin_func(self):
    th = beam.ParDo(len).get_type_hints()
    self.assertIsNone(th.input_types)
    self.assertIsNone(th.output_types)


if __name__ == '__main__':
  unittest.main()
