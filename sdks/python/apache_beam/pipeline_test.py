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

from __future__ import absolute_import

import copy
import logging
import platform
import unittest
from builtins import object
from builtins import range
from collections import defaultdict

import mock

import apache_beam as beam
from apache_beam import typehints
from apache_beam.io import Read
from apache_beam.metrics import Metrics
from apache_beam.pipeline import Pipeline
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import PipelineVisitor
from apache_beam.pipeline import PTransformOverride
from apache_beam.pvalue import AsSingleton
from apache_beam.runners.dataflow.native_io.iobase import NativeSource
from apache_beam.runners.direct.evaluation_context import _ExecutionContext
from apache_beam.runners.direct.transform_evaluator import _GroupByKeyOnlyEvaluator
from apache_beam.runners.direct.transform_evaluator import _TransformEvaluator
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
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.window import TimestampedValue
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
    pipeline = TestPipeline()
    pcoll = pipeline | 'label1' >> Create([1, 2, 3])
    assert_that(pcoll, equal_to([1, 2, 3]))

    # Test if initial value is an iterator object.
    pcoll2 = pipeline | 'label2' >> Create(iter((4, 5, 6)))
    pcoll3 = pcoll2 | 'do' >> FlatMap(lambda x: [x + 10])
    assert_that(pcoll3, equal_to([14, 15, 16]), label='pcoll3')
    pipeline.run()

  def test_flatmap_builtin(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'label1' >> Create([1, 2, 3])
    assert_that(pcoll, equal_to([1, 2, 3]))

    pcoll2 = pcoll | 'do' >> FlatMap(lambda x: [x + 10])
    assert_that(pcoll2, equal_to([11, 12, 13]), label='pcoll2')

    pcoll3 = pcoll2 | 'm1' >> Map(lambda x: [x, 12])
    assert_that(pcoll3,
                equal_to([[11, 12], [12, 12], [13, 12]]), label='pcoll3')

    pcoll4 = pcoll3 | 'do2' >> FlatMap(set)
    assert_that(pcoll4, equal_to([11, 12, 12, 12, 13]), label='pcoll4')
    pipeline.run()

  def test_create_singleton_pcollection(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'label' >> Create([[1, 2, 3]])
    assert_that(pcoll, equal_to([[1, 2, 3]]))
    pipeline.run()

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
    pipeline = TestPipeline()
    pcoll = pipeline | 'read' >> Read(FakeSource([1, 2, 3]))
    assert_that(pcoll, equal_to([1, 2, 3]))
    pipeline.run()

  def test_visit_entire_graph(self):
    pipeline = Pipeline()
    pcoll1 = pipeline | 'pcoll' >> Create([1, 2, 3])
    pcoll2 = pcoll1 | 'do1' >> FlatMap(lambda x: [x + 1])
    pcoll3 = pcoll2 | 'do2' >> FlatMap(lambda x: [x + 1])
    pcoll4 = pcoll2 | 'do3' >> FlatMap(lambda x: [x + 1])
    transform = PipelineTest.CustomTransform()
    pcoll5 = pcoll4 | transform

    visitor = PipelineTest.Visitor(visited=[])
    pipeline.visit(visitor)
    self.assertEqual(set([pcoll1, pcoll2, pcoll3, pcoll4, pcoll5]),
                     set(visitor.visited))
    self.assertEqual(set(visitor.enter_composite),
                     set(visitor.leave_composite))
    self.assertEqual(3, len(visitor.enter_composite))
    self.assertEqual(visitor.enter_composite[2].transform, transform)
    self.assertEqual(visitor.leave_composite[1].transform, transform)

  def test_apply_custom_transform(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'pcoll' >> Create([1, 2, 3])
    result = pcoll | PipelineTest.CustomTransform()
    assert_that(result, equal_to([2, 3, 4]))
    pipeline.run()

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
        'Transform "CustomTransform" does not have a stable unique label. '
        'This will prevent updating of pipelines. '
        'To apply a transform with a specified label write '
        'pvalue | "label" >> transform')

  def test_reuse_cloned_custom_transform_instance(self):
    pipeline = TestPipeline()
    pcoll1 = pipeline | 'pc1' >> Create([1, 2, 3])
    pcoll2 = pipeline | 'pc2' >> Create([4, 5, 6])
    transform = PipelineTest.CustomTransform()
    result1 = pcoll1 | transform
    result2 = pcoll2 | 'new_label' >> transform
    assert_that(result1, equal_to([2, 3, 4]), label='r1')
    assert_that(result2, equal_to([5, 6, 7]), label='r2')
    pipeline.run()

  def test_transform_no_super_init(self):
    class AddSuffix(PTransform):

      def __init__(self, suffix):
        # No call to super(...).__init__
        self.suffix = suffix

      def expand(self, pcoll):
        return pcoll | Map(lambda x: x + self.suffix)

    self.assertEqual(
        ['a-x', 'b-x', 'c-x'],
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
      return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (2 ** 10)

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
    pipeline = TestPipeline(runner='BundleBasedDirectRunner')

    # Consumed memory should not be proportional to the number of maps.
    memory_threshold = (
        get_memory_usage_in_bytes() + (5 * len_elements * num_elements))

    # Plus small additional slack for memory fluctuations during the test.
    memory_threshold += 10 * (2 ** 20)

    biglist = pipeline | 'oom:create' >> Create(
        ['x' * len_elements] * num_elements)
    for i in range(num_maps):
      biglist = biglist | ('oom:addone-%d' % i) >> Map(lambda x: x + 'y')
    result = biglist | 'oom:check' >> Map(check_memory, memory_threshold)
    assert_that(result, equal_to(
        ['x' * len_elements + 'y' * num_maps] * num_elements))

    pipeline.run()

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

  # TODO(BEAM-1894).
  # def test_eager_pipeline(self):
  #   p = Pipeline('EagerRunner')
  #   self.assertEqual([1, 4, 9], p | Create([1, 2, 3]) | Map(lambda x: x*x))

  @mock.patch(
      'apache_beam.runners.direct.direct_runner._get_transform_overrides')
  def test_ptransform_overrides(self, file_system_override_mock):

    class MyParDoOverride(PTransformOverride):

      def matches(self, applied_ptransform):
        return isinstance(applied_ptransform.transform, DoubleParDo)

      def get_replacement_transform(self, ptransform):
        if isinstance(ptransform, DoubleParDo):
          return TripleParDo()
        raise ValueError('Unsupported type of transform: %r' % ptransform)

    def get_overrides(unused_pipeline_options):
      return [MyParDoOverride()]

    file_system_override_mock.side_effect = get_overrides

    # Specify DirectRunner as it's the one patched above.
    with Pipeline(runner='BundleBasedDirectRunner') as p:
      pcoll = p | beam.Create([1, 2, 3]) | 'Multiply' >> DoubleParDo()
      assert_that(pcoll, equal_to([3, 6, 9]))

  def test_ptransform_override_type_hints(self):

    class NoTypeHintOverride(PTransformOverride):

      def matches(self, applied_ptransform):
        return isinstance(applied_ptransform.transform, DoubleParDo)

      def get_replacement_transform(self, ptransform):
        return ToStringParDo()

    class WithTypeHintOverride(PTransformOverride):

      def matches(self, applied_ptransform):
        return isinstance(applied_ptransform.transform, DoubleParDo)

      def get_replacement_transform(self, ptransform):
        return (ToStringParDo()
                .with_input_types(int)
                .with_output_types(str))

    for override, expected_type in [(NoTypeHintOverride(), typehints.Any),
                                    (WithTypeHintOverride(), str)]:
      p = TestPipeline()
      pcoll = (p
               | beam.Create([1, 2, 3])
               | 'Operate' >> DoubleParDo()
               | 'NoOp' >> beam.Map(lambda x: x))

      p.replace_all([override])
      self.assertEquals(pcoll.producer.inputs[0].element_type, expected_type)


class DoFnTest(unittest.TestCase):

  def test_element(self):
    class TestDoFn(DoFn):
      def process(self, element):
        yield element + 10

    pipeline = TestPipeline()
    pcoll = pipeline | 'Create' >> Create([1, 2]) | 'Do' >> ParDo(TestDoFn())
    assert_that(pcoll, equal_to([11, 12]))
    pipeline.run()

  def test_side_input_no_tag(self):
    class TestDoFn(DoFn):
      def process(self, element, prefix, suffix):
        return ['%s-%s-%s' % (prefix, element, suffix)]

    pipeline = TestPipeline()
    words_list = ['aa', 'bb', 'cc']
    words = pipeline | 'SomeWords' >> Create(words_list)
    prefix = 'zyx'
    suffix = pipeline | 'SomeString' >> Create(['xyz'])  # side in
    result = words | 'DecorateWordsDoFnNoTag' >> ParDo(
        TestDoFn(), prefix, suffix=AsSingleton(suffix))
    assert_that(result, equal_to(['zyx-%s-xyz' % x for x in words_list]))
    pipeline.run()

  def test_side_input_tagged(self):
    class TestDoFn(DoFn):
      def process(self, element, prefix, suffix=DoFn.SideInputParam):
        return ['%s-%s-%s' % (prefix, element, suffix)]

    pipeline = TestPipeline()
    words_list = ['aa', 'bb', 'cc']
    words = pipeline | 'SomeWords' >> Create(words_list)
    prefix = 'zyx'
    suffix = pipeline | 'SomeString' >> Create(['xyz'])  # side in
    result = words | 'DecorateWordsDoFnNoTag' >> ParDo(
        TestDoFn(), prefix, suffix=AsSingleton(suffix))
    assert_that(result, equal_to(['zyx-%s-xyz' % x for x in words_list]))
    pipeline.run()

  def test_window_param(self):
    class TestDoFn(DoFn):
      def process(self, element, window=DoFn.WindowParam):
        yield (element, (float(window.start), float(window.end)))

    pipeline = TestPipeline()
    pcoll = (pipeline
             | Create([1, 7])
             | Map(lambda x: TimestampedValue(x, x))
             | WindowInto(windowfn=SlidingWindows(10, 5))
             | ParDo(TestDoFn()))
    assert_that(pcoll, equal_to([(1, (-5, 5)), (1, (0, 10)),
                                 (7, (0, 10)), (7, (5, 15))]))
    pcoll2 = pcoll | 'Again' >> ParDo(TestDoFn())
    assert_that(
        pcoll2,
        equal_to([
            ((1, (-5, 5)), (-5, 5)), ((1, (0, 10)), (0, 10)),
            ((7, (0, 10)), (0, 10)), ((7, (5, 15)), (5, 15))]),
        label='doubled windows')
    pipeline.run()

  def test_timestamp_param(self):
    class TestDoFn(DoFn):
      def process(self, element, timestamp=DoFn.TimestampParam):
        yield timestamp

    pipeline = TestPipeline()
    pcoll = pipeline | 'Create' >> Create([1, 2]) | 'Do' >> ParDo(TestDoFn())
    assert_that(pcoll, equal_to([MIN_TIMESTAMP, MIN_TIMESTAMP]))
    pipeline.run()

  def test_timestamp_param_map(self):
    with TestPipeline() as p:
      assert_that(
          p | Create([1, 2]) | beam.Map(lambda _, t=DoFn.TimestampParam: t),
          equal_to([MIN_TIMESTAMP, MIN_TIMESTAMP]))


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
    self.assertEquals(3, options.slices)
    self.assertEquals('sunny side up', options.style)

  def test_keyword_parsing(self):
    options = Breakfast(
        ['--slices=3', '--style=sunny side up', '--ignored'],
        slices=10)
    self.assertEquals(10, options.slices)
    self.assertEquals('sunny side up', options.style)

  def test_attribute_setting(self):
    options = Breakfast(slices=10)
    self.assertEquals(10, options.slices)
    options.slices = 20
    self.assertEquals(20, options.slices)

  def test_view_as(self):
    generic_options = PipelineOptions(['--slices=3'])
    self.assertEquals(3, generic_options.view_as(Bacon).slices)
    self.assertEquals(3, generic_options.view_as(Breakfast).slices)

    generic_options.view_as(Breakfast).slices = 10
    self.assertEquals(10, generic_options.view_as(Bacon).slices)

    with self.assertRaises(AttributeError):
      generic_options.slices  # pylint: disable=pointless-statement

    with self.assertRaises(AttributeError):
      generic_options.view_as(Eggs).slices  # pylint: disable=expression-not-assigned

  def test_defaults(self):
    options = Breakfast(['--slices=3'])
    self.assertEquals(3, options.slices)
    self.assertEquals('scrambled', options.style)

  def test_dir(self):
    options = Breakfast()
    self.assertEquals(
        set(['from_dictionary', 'get_all_options', 'slices', 'style',
             'view_as', 'display_data']),
        set([attr for attr in dir(options) if not attr.startswith('_') and
             attr != 'next']))
    self.assertEquals(
        set(['from_dictionary', 'get_all_options', 'style', 'view_as',
             'display_data']),
        set([attr for attr in dir(options.view_as(Eggs))
             if not attr.startswith('_') and attr != 'next']))


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
    self.assertEquals(p.transforms_stack[0].parts[0].parent,
                      p.transforms_stack[0])


class DirectRunnerRetryTests(unittest.TestCase):

  def test_retry_fork_graph(self):
    # TODO(BEAM-3642): The FnApiRunner currently does not currently support
    # retries.
    p = beam.Pipeline(runner='BundleBasedDirectRunner')

    # TODO(mariagh): Remove the use of globals from the test.
    global count_b, count_c # pylint: disable=global-variable-undefined
    count_b, count_c = 0, 0

    def f_b(x):
      global count_b  # pylint: disable=global-variable-undefined
      count_b += 1
      raise Exception('exception in f_b')

    def f_c(x):
      global count_c  # pylint: disable=global-variable-undefined
      count_c += 1
      raise Exception('exception in f_c')

    names = p | 'CreateNodeA' >> beam.Create(['Ann', 'Joe'])

    fork_b = names | 'SendToB' >> beam.Map(f_b) # pylint: disable=unused-variable
    fork_c = names | 'SendToC' >> beam.Map(f_c) # pylint: disable=unused-variable

    with self.assertRaises(Exception):
      p.run().wait_until_finish()
    assert count_b == count_c == 4

  def test_no_partial_writeouts(self):

    class TestTransformEvaluator(_TransformEvaluator):

      def __init__(self):
        self._execution_context = _ExecutionContext(None, {})

      def start_bundle(self):
        self.step_context = self._execution_context.get_step_context()

      def process_element(self, element):
        k, v = element
        state = self.step_context.get_keyed_state(k)
        state.add_state(None, _GroupByKeyOnlyEvaluator.ELEMENTS_TAG, v)

    # Create instance and add key/value, key/value2
    evaluator = TestTransformEvaluator()
    evaluator.start_bundle()
    self.assertIsNone(evaluator.step_context.existing_keyed_state.get('key'))
    self.assertIsNone(evaluator.step_context.partial_keyed_state.get('key'))

    evaluator.process_element(['key', 'value'])
    self.assertEqual(
        evaluator.step_context.existing_keyed_state['key'].state,
        defaultdict(lambda: defaultdict(list)))
    self.assertEqual(
        evaluator.step_context.partial_keyed_state['key'].state,
        {None: {'elements':['value']}})

    evaluator.process_element(['key', 'value2'])
    self.assertEqual(
        evaluator.step_context.existing_keyed_state['key'].state,
        defaultdict(lambda: defaultdict(list)))
    self.assertEqual(
        evaluator.step_context.partial_keyed_state['key'].state,
        {None: {'elements':['value', 'value2']}})

    # Simulate an exception (redo key/value)
    evaluator._execution_context.reset()
    evaluator.start_bundle()
    evaluator.process_element(['key', 'value'])
    self.assertEqual(
        evaluator.step_context.existing_keyed_state['key'].state,
        defaultdict(lambda: defaultdict(list)))
    self.assertEqual(
        evaluator.step_context.partial_keyed_state['key'].state,
        {None: {'elements':['value']}})


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
