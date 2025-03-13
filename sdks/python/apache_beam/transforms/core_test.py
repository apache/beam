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

"""Unit tests for the core python file."""
# pytype: skip-file

import logging
import os
import tempfile
import unittest

import pytest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import FixedWindows

RETURN_NONE_PARTIAL_WARNING = "No iterator is returned"


class TestDoFn1(beam.DoFn):
  def process(self, element):
    yield element


class TestDoFn2(beam.DoFn):
  def process(self, element):
    def inner_func(x):
      yield x

    return inner_func(element)


class TestDoFn3(beam.DoFn):
  """mixing return and yield is not allowed"""
  def process(self, element):
    if not element:
      return -1
    yield element


class TestDoFn4(beam.DoFn):
  """test the variable name containing return"""
  def process(self, element):
    my_return = element
    yield my_return


class TestDoFn5(beam.DoFn):
  """test the variable name containing yield"""
  def process(self, element):
    my_yield = element
    return my_yield


class TestDoFn6(beam.DoFn):
  """test the variable name containing return"""
  def process(self, element):
    return_test = element
    yield return_test


class TestDoFn7(beam.DoFn):
  """test the variable name containing yield"""
  def process(self, element):
    yield_test = element
    return yield_test


class TestDoFn8(beam.DoFn):
  """test the code containing yield and yield from"""
  def process(self, element):
    if not element:
      yield from [1, 2, 3]
    else:
      yield element


class TestDoFn9(beam.DoFn):
  def process(self, element):
    if len(element) > 3:
      raise ValueError('Not allowed to have long elements')
    yield element


class TestDoFn10(beam.DoFn):
  """test process returning None explicitly"""
  def process(self, element):
    return None


class TestDoFn11(beam.DoFn):
  """test process returning None (no return and no yield)"""
  def process(self, element):
    pass


class TestDoFn12(beam.DoFn):
  """test process returning None (return statement without a value)"""
  def process(self, element):
    return


class CreateTest(unittest.TestCase):
  @pytest.fixture(autouse=True)
  def inject_fixtures(self, caplog):
    self._caplog = caplog

  def test_dofn_with_yield_and_return(self):
    warning_text = 'Using yield and return'

    with self._caplog.at_level(logging.WARNING):
      assert beam.ParDo(sum)
      assert beam.ParDo(TestDoFn1())
      assert beam.ParDo(TestDoFn2())
      assert beam.ParDo(TestDoFn4())
      assert beam.ParDo(TestDoFn5())
      assert beam.ParDo(TestDoFn6())
      assert beam.ParDo(TestDoFn7())
      assert beam.ParDo(TestDoFn8())
      assert warning_text not in self._caplog.text

    with self._caplog.at_level(logging.WARNING):
      beam.ParDo(TestDoFn3())
      assert warning_text in self._caplog.text

  def test_dofn_with_explicit_return_none(self):
    with self._caplog.at_level(logging.WARNING):
      beam.ParDo(TestDoFn10())
      assert RETURN_NONE_PARTIAL_WARNING in self._caplog.text
      assert str(TestDoFn10) in self._caplog.text

  def test_dofn_with_implicit_return_none_missing_return_and_yield(self):
    with self._caplog.at_level(logging.WARNING):
      beam.ParDo(TestDoFn11())
      assert RETURN_NONE_PARTIAL_WARNING in self._caplog.text
      assert str(TestDoFn11) in self._caplog.text

  def test_dofn_with_implicit_return_none_return_without_value(self):
    with self._caplog.at_level(logging.WARNING):
      beam.ParDo(TestDoFn12())
      assert RETURN_NONE_PARTIAL_WARNING in self._caplog.text
      assert str(TestDoFn12) in self._caplog.text


class PartitionTest(unittest.TestCase):
  def test_partition_boundedness(self):
    def partition_fn(val, num_partitions):
      return val % num_partitions

    class UnboundedDoFn(beam.DoFn):
      @beam.DoFn.unbounded_per_element()
      def process(self, element):
        yield element

    with beam.testing.test_pipeline.TestPipeline() as p:
      source = p | beam.Create([1, 2, 3, 4, 5])
      p1, p2, p3 = source | "bounded" >> beam.Partition(partition_fn, 3)

      self.assertEqual(source.is_bounded, True)
      self.assertEqual(p1.is_bounded, True)
      self.assertEqual(p2.is_bounded, True)
      self.assertEqual(p3.is_bounded, True)

      unbounded = source | beam.ParDo(UnboundedDoFn())
      p4, p5, p6 = unbounded | "unbounded" >> beam.Partition(partition_fn, 3)

      self.assertEqual(unbounded.is_bounded, False)
      self.assertEqual(p4.is_bounded, False)
      self.assertEqual(p5.is_bounded, False)
      self.assertEqual(p6.is_bounded, False)


class FlattenTest(unittest.TestCase):
  def test_flatten_identical_windows(self):
    with beam.testing.test_pipeline.TestPipeline() as p:
      source1 = p | "c1" >> beam.Create(
          [1, 2, 3, 4, 5]) | "w1" >> beam.WindowInto(FixedWindows(100))
      source2 = p | "c2" >> beam.Create([6, 7, 8]) | "w2" >> beam.WindowInto(
          FixedWindows(100))
      source3 = p | "c3" >> beam.Create([9, 10]) | "w3" >> beam.WindowInto(
          FixedWindows(100))
      out = (source1, source2, source3) | "flatten" >> beam.Flatten()
      assert_that(out, equal_to([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

  def test_flatten_no_windows(self):
    with beam.testing.test_pipeline.TestPipeline() as p:
      source1 = p | "c1" >> beam.Create([1, 2, 3, 4, 5])
      source2 = p | "c2" >> beam.Create([6, 7, 8])
      source3 = p | "c3" >> beam.Create([9, 10])
      out = (source1, source2, source3) | "flatten" >> beam.Flatten()
      assert_that(out, equal_to([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

  def test_flatten_mismatched_windows(self):
    with beam.testing.test_pipeline.TestPipeline() as p:
      source1 = p | "c1" >> beam.Create(
          [1, 2, 3, 4, 5]) | "w1" >> beam.WindowInto(FixedWindows(25))
      source2 = p | "c2" >> beam.Create([6, 7, 8]) | "w2" >> beam.WindowInto(
          FixedWindows(100))
      source3 = p | "c3" >> beam.Create([9, 10]) | "w3" >> beam.WindowInto(
          FixedWindows(100))
      _ = (source1, source2, source3) | "flatten" >> beam.Flatten()


class ExceptionHandlingTest(unittest.TestCase):
  def test_routes_failures(self):
    with beam.Pipeline() as pipeline:
      good, bad = (
        pipeline | beam.Create(['abc', 'long_word', 'foo', 'bar', 'foobar'])
        | beam.ParDo(TestDoFn9()).with_exception_handling()
      )
      bad_elements = bad | beam.Keys()
      assert_that(good, equal_to(['abc', 'foo', 'bar']), 'good')
      assert_that(bad_elements, equal_to(['long_word', 'foobar']), 'bad')

  def test_handles_callbacks(self):
    with tempfile.TemporaryDirectory() as tmp_dirname:
      tmp_path = os.path.join(tmp_dirname, 'tmp_filename')
      file_contents = 'random content'

      def failure_callback(e, el):
        if type(e) is not ValueError:
          raise Exception(f'Failed to pass in correct exception, received {e}')
        if el != 'foobar':
          raise Exception(f'Failed to pass in correct element, received {el}')
        f = open(tmp_path, "a")
        logging.warning(tmp_path)
        f.write(file_contents)
        f.close()

      with beam.Pipeline() as pipeline:
        good, bad = (
          pipeline | beam.Create(['abc', 'bcd', 'foo', 'bar', 'foobar'])
          | beam.ParDo(TestDoFn9()).with_exception_handling(
            on_failure_callback=failure_callback)
        )
        bad_elements = bad | beam.Keys()
        assert_that(good, equal_to(['abc', 'bcd', 'foo', 'bar']), 'good')
        assert_that(bad_elements, equal_to(['foobar']), 'bad')
      with open(tmp_path) as f:
        s = f.read()
        self.assertEqual(s, file_contents)

  def test_handles_no_callback_triggered(self):
    with tempfile.TemporaryDirectory() as tmp_dirname:
      tmp_path = os.path.join(tmp_dirname, 'tmp_filename')
      file_contents = 'random content'

      def failure_callback(e, el):
        f = open(tmp_path, "a")
        logging.warning(tmp_path)
        f.write(file_contents)
        f.close()

      with beam.Pipeline() as pipeline:
        good, bad = (
          pipeline | beam.Create(['abc', 'bcd', 'foo', 'bar'])
          | beam.ParDo(TestDoFn9()).with_exception_handling(
            on_failure_callback=failure_callback)
        )
        bad_elements = bad | beam.Keys()
        assert_that(good, equal_to(['abc', 'bcd', 'foo', 'bar']), 'good')
        assert_that(bad_elements, equal_to([]), 'bad')
      self.assertFalse(os.path.isfile(tmp_path))


class FlatMapTest(unittest.TestCase):
  def test_default(self):

    with beam.Pipeline() as pipeline:
      letters = (
          pipeline
          | beam.Create(['abc', 'def'], reshuffle=False)
          | beam.FlatMap())
      assert_that(letters, equal_to(['a', 'b', 'c', 'd', 'e', 'f']))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
