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

"""Unit tests for the Pipeline class."""

# pytype: skip-file

from __future__ import absolute_import

import copy
import platform
import unittest
from builtins import object
from builtins import range

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam import typehints
from apache_beam.coders import BytesCoder
from apache_beam.io import Read
from apache_beam.metrics import Metrics
from apache_beam.pipeline import Pipeline
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import PipelineVisitor
from apache_beam.pipeline import PTransformOverride
from apache_beam.portability import common_urns
from apache_beam.pvalue import AsSingleton
from apache_beam.pvalue import TaggedOutput
from apache_beam.runners.dataflow.native_io.iobase import NativeSource
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import CombineGlobally
from apache_beam.transforms import Create
from apache_beam.transforms import DoFn
from apache_beam.transforms import FlatMap
from apache_beam.transforms import Map
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms import WindowInto
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MIN_TIMESTAMP

# TODO(BEAM-1555): Test is failing on the service, with FakeSource.
# from nose.plugins.attrib import attr


class FakeSource(NativeSource):
  """Fake source returning a fixed list of values."""
  class _Reader(object):
    def __init__(self, vals):
      self._vals = vals
      self._output_counter = Metrics.counter('main', 'outputs')

    def __enter__(self):
      return self

    def __exit__(self, exception_type, exception_value, traceback):
      pass

    def __iter__(self):
      for v in self._vals:
        self._output_counter.inc()
        yield v

  def __init__(self, vals):
    self._vals = vals

  def reader(self):
    return FakeSource._Reader(self._vals)


class FakeUnboundedSource(NativeSource):
  """Fake unbounded source. Does not work at runtime"""
  def reader(self):
    return None

  def is_bounded(self):
    return False


class DoubleParDo(beam.PTransform):
  def expand(self, input):
    return input | 'Inner' >> beam.Map(lambda a: a * 2)

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)


class TripleParDo(beam.PTransform):
  def expand(self, input):
    # Keeping labels the same intentionally to make sure that there is no label
    # conflict due to replacement.
    return input | 'Inner' >> beam.Map(lambda a: a * 3)


class ToStringParDo(beam.PTransform):
  def expand(self, input):
    # We use copy.copy() here to make sure the typehint mechanism doesn't
    # automatically infer that the output type is str.
    return input | 'Inner' >> beam.Map(lambda a: copy.copy(str(a)))


class PipelineTest(unittest.TestCase):
  @staticmethod
  def custom_callable(pcoll):
    return pcoll | '+1' >> FlatMap(lambda x: [x + 1])

  # Some of these tests designate a runner by name, others supply a runner.
  # This variation is just to verify that both means of runner specification
  # work and is not related to other aspects of the tests.

  class CustomTransform(PTransform):
    def expand(self, pcoll):
      return pcoll | '+1' >> FlatMap(lambda x: [x + 1])

  class Visitor(PipelineVisitor):
    def __init__(self, visited):
      self.visited = visited
      self.enter_composite = []
      self.leave_composite = []

    def visit_value(self, value, _):
      self.visited.append(value)

    def enter_composite_transform(self, transform_node):
      self.enter_composite.append(transform_node)

    def leave_composite_transform(self, transform_node):
      self.leave_composite.append(transform_node)

  def test_create(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'label1' >> Create([1, 2, 3])
      assert_that(pcoll, equal_to([1, 2, 3]))

      # Test if initial value is an iterator object.
      pcoll2 = pipeline | 'label2' >> Create(iter((4, 5, 6)))
      pcoll3 = pcoll2 | 'do' >> FlatMap(lambda x: [x + 10])
      assert_that(pcoll3, equal_to([14, 15, 16]), label='pcoll3')

  def test_flatmap_builtin(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'label1' >> Create([1, 2, 3])
      assert_that(pcoll, equal_to([1, 2, 3]))

      pcoll2 = pcoll | 'do' >> FlatMap(lambda x: [x + 10])
      assert_that(pcoll2, equal_to([11, 12, 13]), label='pcoll2')

      pcoll3 = pcoll2 | 'm1' >> Map(lambda x: [x, 12])
      assert_that(
          pcoll3, equal_to([[11, 12], [12, 12], [13, 12]]), label='pcoll3')

      pcoll4 = pcoll3 | 'do2' >> FlatMap(set)
      assert_that(pcoll4, equal_to([11, 12, 12, 12, 13]), label='pcoll4')

  def test_maptuple_builtin(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | Create([('e1', 'e2')])
      side1 = beam.pvalue.AsSingleton(pipeline | 'side1' >> Create(['s1']))
      side2 = beam.pvalue.AsSingleton(pipeline | 'side2' >> Create(['s2']))

      # A test function with a tuple input, an auxiliary parameter,
      # and some side inputs.
      fn = lambda e1, e2, t=DoFn.TimestampParam, s1=None, s2=None: (
          e1, e2, t, s1, s2)
      assert_that(
          pcoll | 'NoSides' >> beam.core.MapTuple(fn),
          equal_to([('e1', 'e2', MIN_TIMESTAMP, None, None)]),
          label='NoSidesCheck')
      assert_that(
          pcoll | 'StaticSides' >> beam.core.MapTuple(fn, 's1', 's2'),
          equal_to([('e1', 'e2', MIN_TIMESTAMP, 's1', 's2')]),
          label='StaticSidesCheck')
      assert_that(
          pcoll | 'DynamicSides' >> beam.core.MapTuple(fn, side1, side2),
          equal_to([('e1', 'e2', MIN_TIMESTAMP, 's1', 's2')]),
          label='DynamicSidesCheck')
      assert_that(
          pcoll | 'MixedSides' >> beam.core.MapTuple(fn, s2=side2),
          equal_to([('e1', 'e2', MIN_TIMESTAMP, None, 's2')]),
          label='MixedSidesCheck')

  def test_flatmaptuple_builtin(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | Create([('e1', 'e2')])
      side1 = beam.pvalue.AsSingleton(pipeline | 'side1' >> Create(['s1']))
      side2 = beam.pvalue.AsSingleton(pipeline | 'side2' >> Create(['s2']))

      # A test function with a tuple input, an auxiliary parameter,
      # and some side inputs.
      fn = lambda e1, e2, t=DoFn.TimestampParam, s1=None, s2=None: (
          e1, e2, t, s1, s2)
      assert_that(
          pcoll | 'NoSides' >> beam.core.FlatMapTuple(fn),
          equal_to(['e1', 'e2', MIN_TIMESTAMP, None, None]),
          label='NoSidesCheck')
      assert_that(
          pcoll | 'StaticSides' >> beam.core.FlatMapTuple(fn, 's1', 's2'),
          equal_to(['e1', 'e2', MIN_TIMESTAMP, 's1', 's2']),
          label='StaticSidesCheck')
      assert_that(
          pcoll
          | 'DynamicSides' >> beam.core.FlatMapTuple(fn, side1, side2),
          equal_to(['e1', 'e2', MIN_TIMESTAMP, 's1', 's2']),
          label='DynamicSidesCheck')
      assert_that(
          pcoll | 'MixedSides' >> beam.core.FlatMapTuple(fn, s2=side2),
          equal_to(['e1', 'e2', MIN_TIMESTAMP, None, 's2']),
          label='MixedSidesCheck')

  def test_create_singleton_pcollection(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'label' >> Create([[1, 2, 3]])
      assert_that(pcoll, equal_to([[1, 2, 3]]))

  # TODO(BEAM-1555): Test is failing on the service, with FakeSource.
  # @attr('ValidatesRunner')
  def test_metrics_in_fake_source(self):
    pipeline = TestPipeline()
    pcoll = pipeline | Read(FakeSource([1, 2, 3, 4, 5, 6]))
    assert_that(pcoll, equal_to([1, 2, 3, 4, 5, 6]))
    res = pipeline.run()
    metric_results = res.metrics().query()
    outputs_counter = metric_results['counters'][0]
    self.assertEqual(outputs_counter.key.step, 'Read')
    self.assertEqual(outputs_counter.key.metric.name, 'outputs')
    self.assertEqual(outputs_counter.committed, 6)

  def test_fake_read(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'read' >> Read(FakeSource([1, 2, 3]))
      assert_that(pcoll, equal_to([1, 2, 3]))

  def test_visit_entire_graph(self):
    pipeline = Pipeline()
    pcoll1 = pipeline | 'pcoll' >> beam.Impulse()
    pcoll2 = pcoll1 | 'do1' >> FlatMap(lambda x: [x + 1])
    pcoll3 = pcoll2 | 'do2' >> FlatMap(lambda x: [x + 1])
    pcoll4 = pcoll2 | 'do3' >> FlatMap(lambda x: [x + 1])
    transform = PipelineTest.CustomTransform()
    pcoll5 = pcoll4 | transform

    visitor = PipelineTest.Visitor(visited=[])
    pipeline.visit(visitor)
    self.assertEqual({pcoll1, pcoll2, pcoll3, pcoll4, pcoll5},
                     set(visitor.visited))
    self.assertEqual(set(visitor.enter_composite), set(visitor.leave_composite))
    self.assertEqual(2, len(visitor.enter_composite))
    self.assertEqual(visitor.enter_composite[1].transform, transform)
    self.assertEqual(visitor.leave_composite[0].transform, transform)

  def test_apply_custom_transform(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'pcoll' >> Create([1, 2, 3])
      result = pcoll | PipelineTest.CustomTransform()
      assert_that(result, equal_to([2, 3, 4]))

  def test_reuse_custom_transform_instance(self):
    pipeline = Pipeline()
    pcoll1 = pipeline | 'pcoll1' >> Create([1, 2, 3])
    pcoll2 = pipeline | 'pcoll2' >> Create([4, 5, 6])
    transform = PipelineTest.CustomTransform()
    pcoll1 | transform
    with self.assertRaises(RuntimeError) as cm:
      pipeline.apply(transform, pcoll2)
    self.assertEqual(
        cm.exception.args[0],
        'A transform with label "CustomTransform" already exists in the '
        'pipeline. To apply a transform with a specified label write '
        'pvalue | "label" >> transform')

  def test_reuse_cloned_custom_transform_instance(self):
    with TestPipeline() as pipeline:
      pcoll1 = pipeline | 'pc1' >> Create([1, 2, 3])
      pcoll2 = pipeline | 'pc2' >> Create([4, 5, 6])
      transform = PipelineTest.CustomTransform()
      result1 = pcoll1 | transform
      result2 = pcoll2 | 'new_label' >> transform
      assert_that(result1, equal_to([2, 3, 4]), label='r1')
      assert_that(result2, equal_to([5, 6, 7]), label='r2')

  def test_transform_no_super_init(self):
    class AddSuffix(PTransform):
      def __init__(self, suffix):
        # No call to super(...).__init__
        self.suffix = suffix

      def expand(self, pcoll):
        return pcoll | Map(lambda x: x + self.suffix)

    self.assertEqual(['a-x', 'b-x', 'c-x'],
                     sorted(['a', 'b', 'c'] | 'AddSuffix' >> AddSuffix('-x')))

  @unittest.skip("Fails on some platforms with new urllib3.")
  def test_memory_usage(self):
    try:
      import resource
    except ImportError:
      # Skip the test if resource module is not available (e.g. non-Unix os).
      self.skipTest('resource module not available.')
    if platform.mac_ver()[0]:
      # Skip the test on macos, depending on version it returns ru_maxrss in
      # different units.
      self.skipTest('ru_maxrss is not in standard units.')

    def get_memory_usage_in_bytes():
      return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (2**10)

    def check_memory(value, memory_threshold):
      memory_usage = get_memory_usage_in_bytes()
      if memory_usage > memory_threshold:
        raise RuntimeError(
            'High memory usage: %d > %d' % (memory_usage, memory_threshold))
      return value

    len_elements = 1000000
    num_elements = 10
    num_maps = 100

    # TODO(robertwb): reduce memory usage of FnApiRunner so that this test
    # passes.
    with TestPipeline(runner='BundleBasedDirectRunner') as pipeline:

      # Consumed memory should not be proportional to the number of maps.
      memory_threshold = (
          get_memory_usage_in_bytes() + (5 * len_elements * num_elements))

      # Plus small additional slack for memory fluctuations during the test.
      memory_threshold += 10 * (2**20)

      biglist = pipeline | 'oom:create' >> Create(
          ['x' * len_elements] * num_elements)
      for i in range(num_maps):
        biglist = biglist | ('oom:addone-%d' % i) >> Map(lambda x: x + 'y')
      result = biglist | 'oom:check' >> Map(check_memory, memory_threshold)
      assert_that(
          result,
          equal_to(['x' * len_elements + 'y' * num_maps] * num_elements))

  def test_aggregator_empty_input(self):
    actual = [] | CombineGlobally(max).without_defaults()
    self.assertEqual(actual, [])

  def test_pipeline_as_context(self):
    def raise_exception(exn):
      raise exn

    with self.assertRaises(ValueError):
      with Pipeline() as p:
        # pylint: disable=expression-not-assigned
        p | Create([ValueError('msg')]) | Map(raise_exception)

  def test_ptransform_overrides(self):
    class MyParDoOverride(PTransformOverride):
      def matches(self, applied_ptransform):
        return isinstance(applied_ptransform.transform, DoubleParDo)

      def get_replacement_transform_for_applied_ptransform(
          self, applied_ptransform):
        ptransform = applied_ptransform.transform
        if isinstance(ptransform, DoubleParDo):
          return TripleParDo()
        raise ValueError('Unsupported type of transform: %r' % ptransform)

    p = Pipeline()
    pcoll = p | beam.Create([1, 2, 3]) | 'Multiply' >> DoubleParDo()
    assert_that(pcoll, equal_to([3, 6, 9]))

    p.replace_all([MyParDoOverride()])
    p.run()

  def test_ptransform_override_type_hints(self):
    class NoTypeHintOverride(PTransformOverride):
      def matches(self, applied_ptransform):
        return isinstance(applied_ptransform.transform, DoubleParDo)

      def get_replacement_transform_for_applied_ptransform(
          self, applied_ptransform):
        return ToStringParDo()

    class WithTypeHintOverride(PTransformOverride):
      def matches(self, applied_ptransform):
        return isinstance(applied_ptransform.transform, DoubleParDo)

      def get_replacement_transform_for_applied_ptransform(
          self, applied_ptransform):
        return ToStringParDo().with_input_types(int).with_output_types(str)

    for override, expected_type in [(NoTypeHintOverride(), typehints.Any),
                                    (WithTypeHintOverride(), str)]:
      p = TestPipeline()
      pcoll = (
          p
          | beam.Create([1, 2, 3])
          | 'Operate' >> DoubleParDo()
          | 'NoOp' >> beam.Map(lambda x: x))

      p.replace_all([override])
      self.assertEqual(pcoll.producer.inputs[0].element_type, expected_type)

  def test_ptransform_override_multiple_outputs(self):
    class MultiOutputComposite(PTransform):
      def __init__(self):
        self.output_tags = set()

      def expand(self, pcoll):
        def mux_input(x):
          x = x * 2
          if isinstance(x, int):
            yield TaggedOutput('numbers', x)
          else:
            yield TaggedOutput('letters', x)

        multi = pcoll | 'MyReplacement' >> beam.ParDo(mux_input).with_outputs()
        letters = multi.letters | 'LettersComposite' >> beam.Map(
            lambda x: x * 3)
        numbers = multi.numbers | 'NumbersComposite' >> beam.Map(
            lambda x: x * 5)

        return {
            'letters': letters,
            'numbers': numbers,
        }

    class MultiOutputOverride(PTransformOverride):
      def matches(self, applied_ptransform):
        return applied_ptransform.full_label == 'MyMultiOutput'

      def get_replacement_transform_for_applied_ptransform(
          self, applied_ptransform):
        return MultiOutputComposite()

    def mux_input(x):
      if isinstance(x, int):
        yield TaggedOutput('numbers', x)
      else:
        yield TaggedOutput('letters', x)

    with TestPipeline() as p:
      multi = (
          p
          | beam.Create([1, 2, 3, 'a', 'b', 'c'])
          | 'MyMultiOutput' >> beam.ParDo(mux_input).with_outputs())
      letters = multi.letters | 'MyLetters' >> beam.Map(lambda x: x)
      numbers = multi.numbers | 'MyNumbers' >> beam.Map(lambda x: x)

      # Assert that the PCollection replacement worked correctly and that
      # elements are flowing through. The replacement transform first
      # multiples by 2 then the leaf nodes inside the composite multiply by
      # an additional 3 and 5. Use prime numbers to ensure that each
      # transform is getting executed once.
      assert_that(
          letters,
          equal_to(['a' * 2 * 3, 'b' * 2 * 3, 'c' * 2 * 3]),
          label='assert letters')
      assert_that(
          numbers,
          equal_to([1 * 2 * 5, 2 * 2 * 5, 3 * 2 * 5]),
          label='assert numbers')

      # Do the replacement and run the element assertions.
      p.replace_all([MultiOutputOverride()])

    # The following checks the graph to make sure the replacement occurred.
    visitor = PipelineTest.Visitor(visited=[])
    p.visit(visitor)
    pcollections = visitor.visited
    composites = visitor.enter_composite

    # Assert the replacement is in the composite list and retrieve the
    # AppliedPTransform.
    self.assertIn(
        MultiOutputComposite, [t.transform.__class__ for t in composites])
    multi_output_composite = list(
        filter(
            lambda t: t.transform.__class__ == MultiOutputComposite,
            composites))[0]

    # Assert that all of the replacement PCollections are in the graph.
    for output in multi_output_composite.outputs.values():
      self.assertIn(output, pcollections)

    # Assert that all of the "old"/replaced PCollections are not in the graph.
    self.assertNotIn(multi[None], visitor.visited)
    self.assertNotIn(multi.letters, visitor.visited)
    self.assertNotIn(multi.numbers, visitor.visited)

  def test_kv_ptransform_honor_type_hints(self):

    # The return type of this DoFn cannot be inferred by the default
    # Beam type inference
    class StatefulDoFn(DoFn):
      BYTES_STATE = BagStateSpec('bytes', BytesCoder())

      def return_recursive(self, count):
        if count == 0:
          return ["some string"]
        else:
          self.return_recursive(count - 1)

      def process(self, element, counter=DoFn.StateParam(BYTES_STATE)):
        return self.return_recursive(1)

    with TestPipeline() as p:
      pcoll = (
          p
          | beam.Create([(1, 1), (2, 2), (3, 3)])
          | beam.GroupByKey()
          | beam.ParDo(StatefulDoFn()))
    self.assertEqual(pcoll.element_type, typehints.Any)

    with TestPipeline() as p:
      pcoll = (
          p
          | beam.Create([(1, 1), (2, 2), (3, 3)])
          | beam.GroupByKey()
          | beam.ParDo(StatefulDoFn()).with_output_types(str))
    self.assertEqual(pcoll.element_type, str)

  def test_track_pcoll_unbounded(self):
    pipeline = TestPipeline()
    pcoll1 = pipeline | 'read' >> Read(FakeUnboundedSource())
    pcoll2 = pcoll1 | 'do1' >> FlatMap(lambda x: [x + 1])
    pcoll3 = pcoll2 | 'do2' >> FlatMap(lambda x: [x + 1])
    self.assertIs(pcoll1.is_bounded, False)
    self.assertIs(pcoll1.is_bounded, False)
    self.assertIs(pcoll3.is_bounded, False)

  def test_track_pcoll_bounded(self):
    pipeline = TestPipeline()
    pcoll1 = pipeline | 'label1' >> Create([1, 2, 3])
    pcoll2 = pcoll1 | 'do1' >> FlatMap(lambda x: [x + 1])
    pcoll3 = pcoll2 | 'do2' >> FlatMap(lambda x: [x + 1])
    self.assertIs(pcoll1.is_bounded, True)
    self.assertIs(pcoll2.is_bounded, True)
    self.assertIs(pcoll3.is_bounded, True)

  def test_track_pcoll_bounded_flatten(self):
    pipeline = TestPipeline()
    pcoll1_a = pipeline | 'label_a' >> Create([1, 2, 3])
    pcoll2_a = pcoll1_a | 'do_a' >> FlatMap(lambda x: [x + 1])

    pcoll1_b = pipeline | 'label_b' >> Create([1, 2, 3])
    pcoll2_b = pcoll1_b | 'do_b' >> FlatMap(lambda x: [x + 1])

    merged = (pcoll2_a, pcoll2_b) | beam.Flatten()

    self.assertIs(pcoll1_a.is_bounded, True)
    self.assertIs(pcoll2_a.is_bounded, True)
    self.assertIs(pcoll1_b.is_bounded, True)
    self.assertIs(pcoll2_b.is_bounded, True)
    self.assertIs(merged.is_bounded, True)

  def test_track_pcoll_unbounded_flatten(self):
    pipeline = TestPipeline()
    pcoll1_bounded = pipeline | 'label1' >> Create([1, 2, 3])
    pcoll2_bounded = pcoll1_bounded | 'do1' >> FlatMap(lambda x: [x + 1])

    pcoll1_unbounded = pipeline | 'read' >> Read(FakeUnboundedSource())
    pcoll2_unbounded = pcoll1_unbounded | 'do2' >> FlatMap(lambda x: [x + 1])

    merged = (pcoll2_bounded, pcoll2_unbounded) | beam.Flatten()

    self.assertIs(pcoll1_bounded.is_bounded, True)
    self.assertIs(pcoll2_bounded.is_bounded, True)
    self.assertIs(pcoll1_unbounded.is_bounded, False)
    self.assertIs(pcoll2_unbounded.is_bounded, False)
    self.assertIs(merged.is_bounded, False)


class DoFnTest(unittest.TestCase):
  def test_element(self):
    class TestDoFn(DoFn):
      def process(self, element):
        yield element + 10

    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Create' >> Create([1, 2]) | 'Do' >> ParDo(TestDoFn())
      assert_that(pcoll, equal_to([11, 12]))

  def test_side_input_no_tag(self):
    class TestDoFn(DoFn):
      def process(self, element, prefix, suffix):
        return ['%s-%s-%s' % (prefix, element, suffix)]

    with TestPipeline() as pipeline:
      words_list = ['aa', 'bb', 'cc']
      words = pipeline | 'SomeWords' >> Create(words_list)
      prefix = 'zyx'
      suffix = pipeline | 'SomeString' >> Create(['xyz'])  # side in
      result = words | 'DecorateWordsDoFnNoTag' >> ParDo(
          TestDoFn(), prefix, suffix=AsSingleton(suffix))
      assert_that(result, equal_to(['zyx-%s-xyz' % x for x in words_list]))

  def test_side_input_tagged(self):
    class TestDoFn(DoFn):
      def process(self, element, prefix, suffix=DoFn.SideInputParam):
        return ['%s-%s-%s' % (prefix, element, suffix)]

    with TestPipeline() as pipeline:
      words_list = ['aa', 'bb', 'cc']
      words = pipeline | 'SomeWords' >> Create(words_list)
      prefix = 'zyx'
      suffix = pipeline | 'SomeString' >> Create(['xyz'])  # side in
      result = words | 'DecorateWordsDoFnNoTag' >> ParDo(
          TestDoFn(), prefix, suffix=AsSingleton(suffix))
      assert_that(result, equal_to(['zyx-%s-xyz' % x for x in words_list]))

  @attr('ValidatesRunner')
  def test_element_param(self):
    pipeline = TestPipeline()
    input = [1, 2]
    pcoll = (
        pipeline
        | 'Create' >> Create(input)
        | 'Ele param' >> Map(lambda element=DoFn.ElementParam: element))
    assert_that(pcoll, equal_to(input))
    pipeline.run()

  @attr('ValidatesRunner')
  def test_key_param(self):
    pipeline = TestPipeline()
    pcoll = (
        pipeline
        | 'Create' >> Create([('a', 1), ('b', 2)])
        | 'Key param' >> Map(lambda _, key=DoFn.KeyParam: key))
    assert_that(pcoll, equal_to(['a', 'b']))
    pipeline.run()

  def test_window_param(self):
    class TestDoFn(DoFn):
      def process(self, element, window=DoFn.WindowParam):
        yield (element, (float(window.start), float(window.end)))

    with TestPipeline() as pipeline:
      pcoll = (
          pipeline
          | Create([1, 7])
          | Map(lambda x: TimestampedValue(x, x))
          | WindowInto(windowfn=SlidingWindows(10, 5))
          | ParDo(TestDoFn()))
      assert_that(
          pcoll,
          equal_to([(1, (-5, 5)), (1, (0, 10)), (7, (0, 10)), (7, (5, 15))]))
      pcoll2 = pcoll | 'Again' >> ParDo(TestDoFn())
      assert_that(
          pcoll2,
          equal_to([((1, (-5, 5)), (-5, 5)), ((1, (0, 10)), (0, 10)),
                    ((7, (0, 10)), (0, 10)), ((7, (5, 15)), (5, 15))]),
          label='doubled windows')

  def test_timestamp_param(self):
    class TestDoFn(DoFn):
      def process(self, element, timestamp=DoFn.TimestampParam):
        yield timestamp

    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Create' >> Create([1, 2]) | 'Do' >> ParDo(TestDoFn())
      assert_that(pcoll, equal_to([MIN_TIMESTAMP, MIN_TIMESTAMP]))

  def test_timestamp_param_map(self):
    with TestPipeline() as p:
      assert_that(
          p | Create([1, 2]) | beam.Map(lambda _, t=DoFn.TimestampParam: t),
          equal_to([MIN_TIMESTAMP, MIN_TIMESTAMP]))

  def test_pane_info_param(self):
    with TestPipeline() as p:
      pc = p | Create([(None, None)])
      assert_that(
          pc | beam.Map(lambda _, p=DoFn.PaneInfoParam: p),
          equal_to([windowed_value.PANE_INFO_UNKNOWN]),
          label='CheckUngrouped')
      assert_that(
          pc | beam.GroupByKey() | beam.Map(lambda _, p=DoFn.PaneInfoParam: p),
          equal_to([
              windowed_value.PaneInfo(
                  is_first=True,
                  is_last=True,
                  timing=windowed_value.PaneInfoTiming.ON_TIME,
                  index=0,
                  nonspeculative_index=0)
          ]),
          label='CheckGrouped')

  def test_incomparable_default(self):
    class IncomparableType(object):
      def __eq__(self, other):
        raise RuntimeError()

      def __ne__(self, other):
        raise RuntimeError()

      def __hash__(self):
        raise RuntimeError()

    # Ensure that we don't use default values in a context where they must be
    # comparable (see BEAM-8301).
    with TestPipeline() as pipeline:
      pcoll = (
          pipeline
          | beam.Create([None])
          | Map(lambda e, x=IncomparableType(): (e, type(x).__name__)))
      assert_that(pcoll, equal_to([(None, 'IncomparableType')]))


class Bacon(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--slices', type=int)


class Eggs(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--style', default='scrambled')


class Breakfast(Bacon, Eggs):
  pass


class PipelineOptionsTest(unittest.TestCase):
  def test_flag_parsing(self):
    options = Breakfast(['--slices=3', '--style=sunny side up', '--ignored'])
    self.assertEqual(3, options.slices)
    self.assertEqual('sunny side up', options.style)

  def test_keyword_parsing(self):
    options = Breakfast(['--slices=3', '--style=sunny side up', '--ignored'],
                        slices=10)
    self.assertEqual(10, options.slices)
    self.assertEqual('sunny side up', options.style)

  def test_attribute_setting(self):
    options = Breakfast(slices=10)
    self.assertEqual(10, options.slices)
    options.slices = 20
    self.assertEqual(20, options.slices)

  def test_view_as(self):
    generic_options = PipelineOptions(['--slices=3'])
    self.assertEqual(3, generic_options.view_as(Bacon).slices)
    self.assertEqual(3, generic_options.view_as(Breakfast).slices)

    generic_options.view_as(Breakfast).slices = 10
    self.assertEqual(10, generic_options.view_as(Bacon).slices)

    with self.assertRaises(AttributeError):
      generic_options.slices  # pylint: disable=pointless-statement

    with self.assertRaises(AttributeError):
      generic_options.view_as(Eggs).slices  # pylint: disable=expression-not-assigned

  def test_defaults(self):
    options = Breakfast(['--slices=3'])
    self.assertEqual(3, options.slices)
    self.assertEqual('scrambled', options.style)

  def test_dir(self):
    options = Breakfast()
    self.assertEqual({
        'from_dictionary',
        'get_all_options',
        'slices',
        'style',
        'view_as',
        'display_data'
    },
                     {
                         attr
                         for attr in dir(options)
                         if not attr.startswith('_') and attr != 'next'
                     })
    self.assertEqual({
        'from_dictionary',
        'get_all_options',
        'style',
        'view_as',
        'display_data'
    },
                     {
                         attr
                         for attr in dir(options.view_as(Eggs))
                         if not attr.startswith('_') and attr != 'next'
                     })


class RunnerApiTest(unittest.TestCase):
  def test_parent_pointer(self):
    class MyPTransform(beam.PTransform):
      def expand(self, p):
        self.p = p
        return p | beam.Create([None])

    p = beam.Pipeline()
    p | MyPTransform()  # pylint: disable=expression-not-assigned
    p = Pipeline.from_runner_api(
        Pipeline.to_runner_api(p, use_fake_coders=True), None, None)
    self.assertIsNotNone(p.transforms_stack[0].parts[0].parent)
    self.assertEqual(
        p.transforms_stack[0].parts[0].parent, p.transforms_stack[0])

  def test_requirements(self):
    p = beam.Pipeline()
    _ = (
        p | beam.Create([])
        | beam.ParDo(lambda x, finalize=beam.DoFn.BundleFinalizerParam: None))
    proto = p.to_runner_api()
    self.assertTrue(
        common_urns.requirements.REQUIRES_BUNDLE_FINALIZATION.urn,
        proto.requirements)


if __name__ == '__main__':
  unittest.main()
