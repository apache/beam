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

"""Tests for tagged output type hints.

This tests the implementation of type hints for tagged outputs via three styles:

1. Decorator style:
   @with_output_types(int, errors=str, warnings=str)
   class MyDoFn(beam.DoFn):
     ...

2. Method chain style:
   beam.ParDo(MyDoFn()).with_output_types(int, errors=str)

3. Function annotation style:
   def fn(element) -> int | TaggedOutput[Literal['errors'], str]:
     ...
"""

# pytype: skip-file

import unittest
from typing import Iterable
from typing import Literal
from typing import Union

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from apache_beam.typehints import with_output_types
from apache_beam.typehints.decorators import IOTypeHints


class IOTypeHintsTaggedOutputTest(unittest.TestCase):
  """Tests for IOTypeHints.tagged_output_types() accessor."""
  def test_empty_hints_returns_empty_dict(self):
    empty = IOTypeHints.empty()
    self.assertEqual(empty.tagged_output_types(), {})

  def test_with_tagged_types(self):
    hints = IOTypeHints.empty().with_output_types(int, errors=str, warnings=str)
    self.assertEqual(
        hints.tagged_output_types(), {
            'errors': str, 'warnings': str
        })

  def test_simple_output_type_with_tagged_types(self):
    """simple_output_type() should still return main type when tags present."""
    hints = IOTypeHints.empty().with_output_types(int, errors=str, warnings=str)
    self.assertEqual(hints.simple_output_type('test'), int)

    hints = IOTypeHints.empty().with_output_types(
        Union[int, str], errors=str, warnings=str)
    self.assertEqual(hints.simple_output_type('test'), Union[int, str])

  def test_without_tagged_types(self):
    """Without tagged types, tagged_output_types() returns empty dict."""
    hints = IOTypeHints.empty().with_output_types(int)
    self.assertEqual(hints.tagged_output_types(), {})
    self.assertEqual(hints.simple_output_type('test'), int)


class DecoratorStyleTaggedOutputTest(unittest.TestCase):
  """Tests for @with_output_types decorator style across all transforms."""
  def test_pardo_decorator_pipeline(self):
    """Test that tagged types propagate through ParDo pipeline."""
    @with_output_types(int, errors=str)
    class MyDoFn(beam.DoFn):
      def process(self, element):
        if element < 0:
          yield beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
        else:
          yield element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.ParDo(MyDoFn()).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_map_decorator_pipeline(self):
    """Test that tagged types propagate through Map."""
    @with_output_types(int, errors=str)
    def mapfn(element):
      if element < 0:
        return beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
      else:
        return element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.Map(mapfn).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_flatmap_decorator_pipeline(self):
    """Test that tagged types propagate through FlatMap."""
    @with_output_types(Iterable[int], errors=Iterable[str])
    def flatmapfn(element):
      if element < 0:
        yield beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
      else:
        yield element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.FlatMap(flatmapfn).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_maptuple_decorator_pipeline(self):
    """Test that tagged types propagate through MapTuple."""
    @with_output_types(int, errors=str)
    def maptuplefn(key, value):
      if value < 0:
        return beam.pvalue.TaggedOutput('errors', f'Negative: {key}={value}')
      else:
        return value * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([('a', -1), ('b', 2), ('c', 3)])
          | beam.MapTuple(maptuplefn).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_flatmaptuple_decorator_pipeline(self):
    """Test that tagged types propagate through FlatMapTuple."""
    @with_output_types(Iterable[int], errors=Iterable[str])
    def flatmaptuplefn(key, value):
      if value < 0:
        yield beam.pvalue.TaggedOutput('errors', f'Negative: {key}={value}')
      else:
        yield value * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([('a', -1), ('b', 2), ('c', 3)])
          | beam.FlatMapTuple(flatmaptuplefn).with_outputs(
              'errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)


class ChainStyleTaggedOutputTest(unittest.TestCase):
  """Tests for .with_output_types() method chain style across all transforms."""
  def test_pardo_chain_pipeline(self):
    """Test ParDo with chained type hints."""
    class SimpleDoFn(beam.DoFn):
      def process(self, element):
        if element < 0:
          yield beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
        else:
          yield element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.ParDo(SimpleDoFn()).with_output_types(
              int, errors=str).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_map_chain_pipeline(self):
    """Test Map with chained type hints."""
    def mapfn(element):
      if element < 0:
        return beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
      else:
        return element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.Map(mapfn).with_output_types(int, errors=str).with_outputs(
              'errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_flatmap_chain_pipeline(self):
    """Test FlatMap with chained type hints.

    Note: For FlatMap.with_output_types(), specify the element type directly
    (int), not wrapped in Iterable. The transform handles iteration internally.
    """
    def flatmapfn(element):
      if element < 0:
        yield beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
      else:
        yield element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.FlatMap(flatmapfn).with_output_types(
              int, errors=str).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_maptuple_chain_pipeline(self):
    """Test MapTuple with chained type hints."""
    def maptuplefn(key, value):
      if value < 0:
        return beam.pvalue.TaggedOutput('errors', f'Negative: {key}={value}')
      else:
        return value * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([('a', -1), ('b', 2), ('c', 3)])
          | beam.MapTuple(maptuplefn).with_output_types(
              int, errors=str).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_flatmaptuple_chain_pipeline(self):
    """Test FlatMapTuple with chained type hints.

    Note: For FlatMapTuple.with_output_types(), specify the element type
    directly (int), not wrapped in Iterable.
    """
    def flatmaptuplefn(key, value):
      if value < 0:
        yield beam.pvalue.TaggedOutput('errors', f'Negative: {key}={value}')
      else:
        yield value * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([('a', -1), ('b', 2), ('c', 3)])
          | beam.FlatMapTuple(flatmaptuplefn).with_output_types(
              int, errors=str).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)


class AnnotationStyleTaggedOutputTest(unittest.TestCase):
  """Tests for function annotation style across all transforms."""
  def test_map_annotation_union(self):
    """Test Map with Union[int, TaggedOutput[...]] annotation."""
    def mapfn(element: int) -> int | TaggedOutput[Literal['errors'], str]:
      if element < 0:
        return beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
      else:
        return element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.Map(mapfn).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_map_annotation_multiple_tags(self):
    """Test Map with multiple TaggedOutput types in annotation."""
    def mapfn(
        element: int
    ) -> int | TaggedOutput[Literal['errors'],
                            str] | TaggedOutput[Literal['warnings'], str]:
      if element < 0:
        return beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
      elif element == 0:
        return beam.pvalue.TaggedOutput('warnings', 'Zero value')
      else:
        return element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.Map(mapfn).with_outputs('errors', 'warnings', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)
      self.assertEqual(results.warnings.element_type, str)

  def test_flatmap_annotation_iterable(self):
    """Test FlatMap with Iterable[int | TaggedOutput[...]] annotation."""
    def flatmapfn(
        element: int) -> Iterable[int | TaggedOutput[Literal['errors'], str]]:
      if element < 0:
        yield beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
      else:
        yield element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.FlatMap(flatmapfn).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)

  def test_pardo_annotation_process_method(self):
    """Test DoFn with process method annotation."""
    class AnnotatedDoFn(beam.DoFn):
      def process(
          self,
          element: int) -> Iterable[int | TaggedOutput[Literal['errors'], str]]:
        if element < 0:
          yield beam.pvalue.TaggedOutput('errors', f'Negative: {element}')
        else:
          yield element * 2

    with beam.Pipeline() as p:
      results = (
          p
          | beam.Create([-1, 0, 1, 2])
          | beam.ParDo(AnnotatedDoFn()).with_outputs('errors', main='main'))

      self.assertEqual(results.main.element_type, int)
      self.assertEqual(results.errors.element_type, str)


if __name__ == '__main__':
  unittest.main()
