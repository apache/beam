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

"""
Unit tests for typecheck.

See additional runtime_type_check=True tests in ptransform_test.py.
"""

# pytype: skip-file

# ruff: noqa: UP006
import os
import tempfile
import unittest
from typing import Iterable
from typing import Tuple

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.pipeline import PipelineVisitor
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import combiners
from apache_beam.transforms import userstate
from apache_beam.typehints import decorators
from apache_beam.typehints import typecheck
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types

decorators._enable_from_callable = True

# Disable frequent lint warning due to pipe operator for chaining transforms.
# pylint: disable=expression-not-assigned


class MyDoFn(beam.DoFn):
  def __init__(self, output_filename):
    super().__init__()
    self.output_filename = output_filename

  def _output(self):
    """Returns a file used to record function calls."""
    if not hasattr(self, 'output_file'):
      self._output_file = open(self.output_filename, 'at', buffering=1)
    return self._output_file

  def start_bundle(self):
    self._output().write('start_bundle\n')

  def finish_bundle(self):
    self._output().write('finish_bundle\n')

  def setup(self):
    self._output().write('setup\n')

  def teardown(self):
    self._output().write('teardown\n')
    self._output().close()

  def process(self, element: int, *args, **kwargs) -> Iterable[int]:
    self._output().write('process\n')
    yield element


class MyDoFnBadAnnotation(MyDoFn):
  def process(self, element: int, *args, **kwargs) -> int:
    # Should raise an exception about return type not being iterable.
    return super().process()


class RuntimeTypeCheckTest(unittest.TestCase):
  def setUp(self):
    # Use FnApiRunner since it guarantees all lifecycle methods will be called.
    self.p = TestPipeline(
        'FnApiRunner',
        options=PipelineOptions(
            runtime_type_check=True, performance_runtime_type_check=False))

  def test_setup(self):
    # Verifies that runtime type checking is enabled for test cases.
    def fn(e: int) -> int:
      return str(e)  # type: ignore

    with self.assertRaisesRegex(Exception,
                                r'output should be.*int.*received.*str'):
      _ = self.p | beam.Create([1, 2, 3]) | beam.Map(fn)
      self.p.run()

  def test_wrapper_pass_through(self):
    # We use a file to check the result because the MyDoFn instance passed is
    # not the same one that actually runs in the pipeline (it is serialized
    # here and deserialized in the worker).
    with tempfile.TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname, "tmp_filename")
      dofn = MyDoFn(path)
      result = self.p | beam.Create([1, 2, 3]) | beam.ParDo(dofn)
      assert_that(result, equal_to([1, 2, 3]))
      self.p.run()
      with open(path, mode="r") as ft:
        lines = [line.strip() for line in ft]
        self.assertListEqual([
            'setup',
            'start_bundle',
            'process',
            'process',
            'process',
            'finish_bundle',
            'teardown',
        ],
                             lines)

  def test_wrapper_pipeline_type_check(self):
    # Verifies that type hints are not masked by the wrapper. What actually
    # happens is that the wrapper is applied during self.p.run() (not invoked
    # in this case), while pipeline type checks happen during pipeline creation.
    # Thus, the wrapper does not have to implement: default_type_hints,
    # infer_output_type, get_type_hints.
    with tempfile.NamedTemporaryFile(mode='w+t') as f:
      dofn = MyDoFnBadAnnotation(f.name)
      with self.assertRaisesRegex(ValueError, r'int.*is not iterable'):
        _ = self.p | beam.Create([1, 2, 3]) | beam.ParDo(dofn)


class PerformanceRuntimeTypeCheckTest(unittest.TestCase):
  def setUp(self):
    self.p = Pipeline(
        options=PipelineOptions(
            performance_runtime_type_check=True, pipeline_type_check=False))

  def assertStartswith(self, msg, prefix):
    self.assertTrue(
        msg.startswith(prefix), '"%s" does not start with "%s"' % (msg, prefix))

  def test_simple_input_error(self):
    with self.assertRaisesRegex(Exception,
                                "Type-hint for argument: 'x' violated. "
                                "Expected an instance of {}, "
                                "instead found 1, an instance of {}".format(
                                    str, int)):
      (
          self.p
          | beam.Create([1, 1])
          | beam.FlatMap(lambda x: [int(x)]).with_input_types(
              str).with_output_types(int))
      self.p.run().wait_until_finish()

  def test_simple_output_error(self):
    with self.assertRaisesRegex(Exception,
                                "Type-hint for argument: 'x' violated. "):
      (
          self.p
          | beam.Create(['1', '1'])
          | beam.FlatMap(lambda x: [int(x)]).with_input_types(
              int).with_output_types(int))
      self.p.run().wait_until_finish()

  def test_simple_input_error_with_kwarg_typehints(self):
    @with_input_types(element=int)
    @with_output_types(int)
    class ToInt(beam.DoFn):
      def process(self, element, *args, **kwargs):
        yield int(element)

    with self.assertRaisesRegex(Exception,
                                "Type-hint for argument: 'element' violated"):
      (self.p | beam.Create(['1', '1']) | beam.ParDo(ToInt()))
      self.p.run().wait_until_finish()

  def test_bad_flatten_input(self):
    with self.assertRaisesRegex(
        TypeError,
        "Inputs to Flatten cannot include an iterable of PCollections. "):
      with beam.Pipeline() as p:
        pc = p | beam.Create([1, 1])
        flatten_inputs = [pc, (pc, )]
        flatten_inputs | beam.Flatten()

  def test_do_fn_returning_non_iterable_throws_error(self):
    # This function is incorrect because it returns a non-iterable object
    def incorrect_par_do_fn(x):
      return x + 5

    with self.assertRaisesRegex(Exception, "'int' object is not iterable "):
      (self.p | beam.Create([1, 1]) | beam.FlatMap(incorrect_par_do_fn))
      self.p.run().wait_until_finish()

  def test_simple_type_satisfied(self):
    @with_input_types(int, int)
    @with_output_types(int)
    class AddWithNum(beam.DoFn):
      def process(self, element, num):
        return [element + num]

    results = (
        self.p
        | 'T' >> beam.Create([1, 2, 3]).with_output_types(int)
        | 'Add' >> beam.ParDo(AddWithNum(), 1))

    assert_that(results, equal_to([2, 3, 4]))
    self.p.run()

  def test_simple_type_violation(self):
    self.p._options.view_as(TypeOptions).pipeline_type_check = False

    @with_output_types(str)
    @with_input_types(x=int)
    def int_to_string(x):
      return str(x)

    (
        self.p
        | 'Create' >> beam.Create(['some_string'])
        | 'ToStr' >> beam.Map(int_to_string))
    with self.assertRaisesRegex(Exception,
                                "Type-hint for argument: 'x' violated. "):
      self.p.run().wait_until_finish()

  def test_pipeline_checking_satisfied_but_run_time_types_violate(self):
    self.p._options.view_as(TypeOptions).pipeline_type_check = False

    @with_output_types(Tuple[bool, int])
    @with_input_types(a=int)
    def is_even_as_key(a):
      # Simulate a programming error, should be: return (a % 2 == 0, a)
      # However this returns Tuple[int, int]
      return (a % 2, a)

    (
        self.p
        | 'Nums' >> beam.Create(range(1)).with_output_types(int)
        | 'IsEven' >> beam.Map(is_even_as_key)
        | 'Parity' >> beam.GroupByKey())

    with self.assertRaisesRegex(
        Exception,
        ("Type-hint for return type violated: Tuple\\[<class 'bool'>, <class " +
         "'int'>\\] hint type-constraint violated. The type of element #0 in " +
         "the passed tuple is incorrect. Expected an instance of type <class " +
         "'bool'>, instead received an instance of type int.")):
      self.p.run().wait_until_finish()

  def test_pipeline_runtime_checking_violation_composite_type_output(self):
    self.p._options.view_as(TypeOptions).pipeline_type_check = False

    # The type-hinted applied via the 'returns()' method indicates the ParDo
    # should return an instance of type: Tuple[float, int]. However, an instance
    # of 'int' will be generated instead.
    with self.assertRaisesRegex(
        Exception,
        ("Type-hint for return type violated. Expected an instance of {}, " +
         "instead found 4.0, an instance of {}.").format(int, float)):
      (
          self.p
          | beam.Create([(1, 3.0)])
          | (
              'Swap' >>
              beam.FlatMap(lambda x_y1: [x_y1[0] + x_y1[1]]).with_input_types(
                  Tuple[int, float]).with_output_types(int)))
      self.p.run().wait_until_finish()

  def test_downstream_input_type_hint_error_has_descriptive_error_msg(self):
    @with_input_types(int)
    @with_output_types(int)
    class IntToInt(beam.DoFn):
      def process(self, element, *args, **kwargs):
        yield element

    @with_input_types(str)
    @with_output_types(int)
    class StrToInt(beam.DoFn):
      def process(self, element, *args, **kwargs):
        yield int(element)

    # This will raise a type check error in IntToInt even though the actual
    # type check error won't happen until StrToInt. The user will be told that
    # StrToInt's input type hints were not satisfied while running IntToInt.
    with self.assertRaisesRegex(
        Exception,
        ("Type-hint for argument: 'element' violated. Expected an instance of "
         + "{}, instead found 9, an instance of {}.").format(str, int)):
      (
          self.p
          | beam.Create([9])
          | beam.ParDo(IntToInt())
          | beam.ParDo(StrToInt()))
      self.p.run().wait_until_finish()


@with_input_types(int)
@with_output_types(int)
class _AddOneDoFn(beam.DoFn):
  def process(self, element):
    yield element + 1


class RuntimeTypeCheckWrapperDoFnTest(unittest.TestCase):
  """Tests for the merged runtime type-checking wrapper (BEAM-9489)."""
  def test_visitor_applies_single_wrapper_layer(self):
    # A single RuntimeTypeCheckWrapperDoFn should wrap the user's DoFn
    # directly, with no intermediate wrapper layers.
    p = beam.Pipeline(options=PipelineOptions(runtime_type_check=True))
    _ = (p | beam.Create([1, 2]) | 'TypedStep' >> beam.ParDo(_AddOneDoFn()))
    p.visit(typecheck.TypeCheckVisitor())

    wrapped_dofns = []

    class _Collector(PipelineVisitor):
      def visit_transform(self, applied_transform):
        transform = applied_transform.transform
        if isinstance(transform, beam.ParDo) and isinstance(
            getattr(transform, 'fn', None), typecheck.AbstractDoFnWrapper):
          wrapped_dofns.append(transform.fn)

    p.visit(_Collector())
    self.assertTrue(wrapped_dofns)
    for wrapper in wrapped_dofns:
      self.assertIsInstance(wrapper, typecheck.RuntimeTypeCheckWrapperDoFn)
      # The wrapped DoFn must be the user's DoFn, not another wrapper.
      self.assertNotIsInstance(wrapper.dofn, typecheck.AbstractDoFnWrapper)

  def test_wrapper_labels_type_check_errors(self):
    dofn = typecheck.RuntimeTypeCheckWrapperDoFn(
        _AddOneDoFn(), _AddOneDoFn().get_type_hints(), 'MyStep')
    with self.assertRaisesRegex(
        typecheck.TypeCheckError,
        r'Runtime type violation detected within ParDo\(MyStep\)'):
      dofn.process('not-an-int')

  def test_wrapper_preserves_results(self):
    dofn = typecheck.RuntimeTypeCheckWrapperDoFn(
        _AddOneDoFn(), _AddOneDoFn().get_type_hints(), 'MyStep')
    self.assertEqual(list(dofn.process(1)), [2])


def _make_stateful_dofn():
  # Defined inside a function so that the class is serialized by value
  # (like the DoFn created by GroupIntoBatches), which is the case where a
  # cached bound method diverges from the reconstructed class's process.
  count_state = userstate.CombiningValueStateSpec(
      'count', combiners.CountCombineFn())

  class _CountingStatefulDoFn(beam.DoFn):
    def process(self, element, count=beam.DoFn.StateParam(count_state)):
      count.add(1)
      yield element[1]

  return _CountingStatefulDoFn()


class RuntimeTypeCheckStatefulDoFnTest(unittest.TestCase):
  """Regression tests for runtime_type_check with stateful DoFns.

  The type-check wrapper must not expose duplicate StateSpecs/TimerSpecs to
  stateful DoFn validation, including after the runner serializes and
  reconstructs the wrapped DoFn.
  """
  def test_wrapper_does_not_cache_bound_process(self):
    dofn = _make_stateful_dofn()
    wrapper = typecheck.RuntimeTypeCheckWrapperDoFn(
        dofn, dofn.get_type_hints(), 'Step')
    # A bound method cached in the instance __dict__ is visible to
    # userstate.get_dofn_specs and can diverge from self.dofn.process
    # across (de)serialization.
    for attr, value in wrapper.__dict__.items():
      self.assertFalse(
          hasattr(value, '__self__'),
          'wrapper caches bound method %r, which breaks stateful DoFn '
          'validation' % attr)
    userstate.validate_stateful_dofn(wrapper)

  def test_stateful_dofn_with_runtime_type_check(self):
    options = PipelineOptions()
    options.view_as(TypeOptions).runtime_type_check = True
    with TestPipeline(options=options) as p:
      result = (
          p
          | beam.Create([('k', 1), ('k', 2), ('k', 3)])
          | beam.ParDo(_make_stateful_dofn()))
      assert_that(result, equal_to([1, 2, 3]))


if __name__ == '__main__':
  unittest.main()
