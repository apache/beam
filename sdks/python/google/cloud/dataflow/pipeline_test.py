# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the Pipeline class."""

import unittest

from google.cloud.dataflow.io.iobase import Source
from google.cloud.dataflow.pipeline import Pipeline
from google.cloud.dataflow.pipeline import PipelineOptions
from google.cloud.dataflow.pipeline import PipelineVisitor
from google.cloud.dataflow.runners import DirectPipelineRunner
from google.cloud.dataflow.transforms import Create
from google.cloud.dataflow.transforms import FlatMap
from google.cloud.dataflow.transforms import Map
from google.cloud.dataflow.transforms import PTransform
from google.cloud.dataflow.transforms import Read
from google.cloud.dataflow.transforms.util import assert_that, equal_to


class FakeSource(Source):
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

  @staticmethod
  def custom_callable(pcoll):
    return pcoll | FlatMap('+1', lambda x: [x + 1])

  # Some of these tests designate a runner by name, others supply a runner.
  # This variation is just to verify that both means of runner specification
  # work and is not related to other aspects of the tests.

  class CustomTransform(PTransform):

    def apply(self, pcoll):
      return pcoll | FlatMap('+1', lambda x: [x + 1])

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
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | Create('label1', [1, 2, 3])
    assert_that(pcoll, equal_to([1, 2, 3]))

    # Test if initial value is an iterator object.
    pcoll2 = pipeline | Create('label2', iter((4, 5, 6)))
    pcoll3 = pcoll2 | FlatMap('do', lambda x: [x + 10])
    assert_that(pcoll3, equal_to([14, 15, 16]), label='pcoll3')
    pipeline.run()

  def test_create_singleton_pcollection(self):
    pipeline = Pipeline(DirectPipelineRunner())
    pcoll = pipeline | Create('label', [[1, 2, 3]])
    assert_that(pcoll, equal_to([[1, 2, 3]]))
    pipeline.run()

  def test_read(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | Read('read', FakeSource([1, 2, 3]))
    assert_that(pcoll, equal_to([1, 2, 3]))
    pipeline.run()

  def test_visit_entire_graph(self):

    pipeline = Pipeline(DirectPipelineRunner())
    pcoll1 = pipeline | Create('pcoll', [1, 2, 3])
    pcoll2 = pcoll1 | FlatMap('do1', lambda x: [x + 1])
    pcoll3 = pcoll2 | FlatMap('do2', lambda x: [x + 1])
    pcoll4 = pcoll2 | FlatMap('do3', lambda x: [x + 1])
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

  def test_visit_node_sub_graph(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll1 = pipeline | Create('pcoll', [1, 2, 3])
    pcoll2 = pcoll1 | FlatMap('do1', lambda x: [x + 1])
    pcoll3 = pcoll2 | FlatMap('do2', lambda x: [x + 1])
    pcoll4 = pcoll2 | FlatMap('do3', lambda x: [x + 1])

    visitor = PipelineTest.Visitor(visited=[])
    pipeline.visit(visitor, node=pcoll3)
    self.assertFalse(pcoll4 in visitor.visited)
    self.assertEqual(set([pcoll1, pcoll2, pcoll3]), set(visitor.visited))

  def test_apply_custom_transform(self):
    pipeline = Pipeline(DirectPipelineRunner())
    pcoll = pipeline | Create('pcoll', [1, 2, 3])
    result = pcoll | PipelineTest.CustomTransform()
    assert_that(result, equal_to([2, 3, 4]))
    pipeline.run()

  def test_reuse_custom_transform_instance(self):
    pipeline = Pipeline(DirectPipelineRunner())
    pcoll1 = pipeline | Create('pcoll1', [1, 2, 3])
    pcoll2 = pipeline | Create('pcoll2', [4, 5, 6])
    transform = PipelineTest.CustomTransform()
    pcoll1 | transform
    with self.assertRaises(RuntimeError) as cm:
      pipeline.apply(transform, pcoll2)
    self.assertEqual(
        cm.exception.message,
        'Transform "CustomTransform" does not have a stable unique label. '
        'This will prevent updating of pipelines. '
        'To clone a transform with a new label use: '
        'transform.clone("NEW LABEL").')

  def test_reuse_cloned_custom_transform_instance(self):
    pipeline = Pipeline(DirectPipelineRunner())
    pcoll1 = pipeline | Create('pcoll1', [1, 2, 3])
    pcoll2 = pipeline | Create('pcoll2', [4, 5, 6])
    transform = PipelineTest.CustomTransform()
    result1 = pcoll1 | transform
    result2 = pcoll2 | transform.clone('new label')
    assert_that(result1, equal_to([2, 3, 4]), label='r1')
    assert_that(result2, equal_to([5, 6, 7]), label='r2')
    pipeline.run()

  def test_apply_custom_callable(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | Create('pcoll', [1, 2, 3])
    result = pipeline.apply(PipelineTest.custom_callable, pcoll)
    assert_that(result, equal_to([2, 3, 4]))
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
        sorted(['a', 'b', 'c'] | AddSuffix('-x')))


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
        ['slices', 'style', 'view_as'],
        [attr for attr in dir(options) if not attr.startswith('_')])
    self.assertEquals(
        ['style', 'view_as'],
        [attr for attr in dir(options.view_as(Eggs))
         if not attr.startswith('_')])


if __name__ == '__main__':
  unittest.main()
