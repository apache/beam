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

import gc
import logging
import unittest

from apache_beam.io.iobase import NativeSource
from apache_beam.pipeline import Pipeline
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import PipelineVisitor
from apache_beam.pvalue import AsIter
from apache_beam.pvalue import SideOutputValue
from apache_beam.transforms import CombinePerKey
from apache_beam.transforms import Create
from apache_beam.transforms import FlatMap
from apache_beam.transforms import Flatten
from apache_beam.transforms import Map
from apache_beam.transforms import PTransform
from apache_beam.transforms import Read
from apache_beam.transforms.util import assert_that, equal_to


class FakeSource(NativeSource):
  """Fake source returning a fixed list of values."""

  class _Reader(object):

    def __init__(self, vals):
      self._vals = vals

    def __enter__(self):
      return self

    def __exit__(self, exception_type, exception_value, traceback):
      pass

    def __iter__(self):
      for v in self._vals:
        yield v

  def __init__(self, vals):
    self._vals = vals

  def reader(self):
    return FakeSource._Reader(self._vals)


class PipelineTest(unittest.TestCase):

  def setUp(self):
    self.runner_name = 'DirectPipelineRunner'

  @staticmethod
  def custom_callable(pcoll):
    return pcoll | '+1' >> FlatMap(lambda x: [x + 1])

  # Some of these tests designate a runner by name, others supply a runner.
  # This variation is just to verify that both means of runner specification
  # work and is not related to other aspects of the tests.

  class CustomTransform(PTransform):

    def apply(self, pcoll):
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
    pipeline = Pipeline(self.runner_name)
    pcoll = pipeline | 'label1' >> Create([1, 2, 3])
    assert_that(pcoll, equal_to([1, 2, 3]))

    # Test if initial value is an iterator object.
    pcoll2 = pipeline | 'label2' >> Create(iter((4, 5, 6)))
    pcoll3 = pcoll2 | 'do' >> FlatMap(lambda x: [x + 10])
    assert_that(pcoll3, equal_to([14, 15, 16]), label='pcoll3')
    pipeline.run()

  def test_create_singleton_pcollection(self):
    pipeline = Pipeline(self.runner_name)
    pcoll = pipeline | 'label' >> Create([[1, 2, 3]])
    assert_that(pcoll, equal_to([[1, 2, 3]]))
    pipeline.run()

  def test_read(self):
    pipeline = Pipeline(self.runner_name)
    pcoll = pipeline | 'read' >> Read(FakeSource([1, 2, 3]))
    assert_that(pcoll, equal_to([1, 2, 3]))
    pipeline.run()

  def test_visit_entire_graph(self):
    pipeline = Pipeline(self.runner_name)
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
    self.assertEqual(2, len(visitor.enter_composite))
    self.assertEqual(visitor.enter_composite[1].transform, transform)
    self.assertEqual(visitor.leave_composite[0].transform, transform)

  def test_apply_custom_transform(self):
    pipeline = Pipeline(self.runner_name)
    pcoll = pipeline | 'pcoll' >> Create([1, 2, 3])
    result = pcoll | PipelineTest.CustomTransform()
    assert_that(result, equal_to([2, 3, 4]))
    pipeline.run()

  def test_reuse_custom_transform_instance(self):
    pipeline = Pipeline(self.runner_name)
    pcoll1 = pipeline | 'pcoll1' >> Create([1, 2, 3])
    pcoll2 = pipeline | 'pcoll2' >> Create([4, 5, 6])
    transform = PipelineTest.CustomTransform()
    pcoll1 | transform
    with self.assertRaises(RuntimeError) as cm:
      pipeline.apply(transform, pcoll2)
    self.assertEqual(
        cm.exception.message,
        'Transform "CustomTransform" does not have a stable unique label. '
        'This will prevent updating of pipelines. '
        'To apply a transform with a specified label write '
        'pvalue | "label" >> transform')

  def test_reuse_cloned_custom_transform_instance(self):
    pipeline = Pipeline(self.runner_name)
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

      def apply(self, pcoll):
        return pcoll | Map(lambda x: x + self.suffix)

    self.assertEqual(
        ['a-x', 'b-x', 'c-x'],
        sorted(['a', 'b', 'c'] | 'AddSuffix' >> AddSuffix('-x')))

  def test_cached_pvalues_are_refcounted(self):
    """Test that cached PValues are refcounted and deleted.

    The intermediary PValues computed by the workflow below contain
    one million elements so if the refcounting does not work the number of
    objects tracked by the garbage collector will increase by a few millions
    by the time we execute the final Map checking the objects tracked.
    Anything that is much larger than what we started with will fail the test.
    """
    def check_memory(value, count_threshold):
      gc.collect()
      objects_count = len(gc.get_objects())
      if objects_count > count_threshold:
        raise RuntimeError(
            'PValues are not refcounted: %s, %s' % (
                objects_count, count_threshold))
      return value

    def create_dupes(o, _):
      yield o
      yield SideOutputValue('side', o)

    num_elements = 100000

    pipeline = Pipeline('DirectPipelineRunner')

    gc.collect()
    count_threshold = len(gc.get_objects()) + 10000
    biglist = pipeline | 'oom:create' >> Create(['x'] * num_elements)
    dupes = (
        biglist
        | 'oom:addone' >> Map(lambda x: (x, 1))
        | 'oom:dupes' >> FlatMap(
            create_dupes, AsIter(biglist)).with_outputs('side', main='main'))
    result = (
        (dupes.side, dupes.main, dupes.side)
        | 'oom:flatten' >> Flatten()
        | 'oom:combine' >> CombinePerKey(sum)
        | 'oom:check' >> Map(check_memory, count_threshold))

    assert_that(result, equal_to([('x', 3 * num_elements)]))
    pipeline.run()
    self.assertEqual(
        pipeline.runner.debug_counters['element_counts'],
        {
            'oom:flatten': 3 * num_elements,
            ('oom:combine/GroupByKey/reify_windows', None): 3 * num_elements,
            ('oom:dupes/FlatMap(create_dupes)', 'side'): num_elements,
            ('oom:dupes/FlatMap(create_dupes)', None): num_elements,
            'oom:create': num_elements,
            ('oom:addone', None): num_elements,
            'oom:combine/GroupByKey/group_by_key': 1,
            ('oom:check', None): 1,
            'assert_that/singleton': 1,
            ('assert_that/Map(match)', None): 1,
            ('oom:combine/GroupByKey/group_by_window', None): 1,
            ('oom:combine/Combine/ParDo(CombineValuesDoFn)', None): 1})

  def test_pipeline_as_context(self):
    def raise_exception(exn):
      raise exn
    with self.assertRaises(ValueError):
      with Pipeline(self.runner_name) as p:
        # pylint: disable=expression-not-assigned
        p | Create([ValueError]) | Map(raise_exception)

  def test_eager_pipeline(self):
    p = Pipeline('EagerPipelineRunner')
    self.assertEqual([1, 4, 9], p | Create([1, 2, 3]) | Map(lambda x: x*x))


class DiskCachedRunnerPipelineTest(PipelineTest):

  def setUp(self):
    self.runner_name = 'DiskCachedPipelineRunner'

  def test_cached_pvalues_are_refcounted(self):
    # Takes long with disk spilling.
    pass

  def test_eager_pipeline(self):
    # Tests eager runner only
    pass


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
        ['from_dictionary', 'get_all_options', 'slices', 'style', 'view_as'],
        [attr for attr in dir(options) if not attr.startswith('_')])
    self.assertEquals(
        ['from_dictionary', 'get_all_options', 'style', 'view_as'],
        [attr for attr in dir(options.view_as(Eggs))
         if not attr.startswith('_')])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
