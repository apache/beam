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

"""Unit tests for side inputs."""

# pytype: skip-file

import itertools
import logging
import unittest

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms import Map
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Timestamp


class SideInputsTest(unittest.TestCase):
  def create_pipeline(self):
    return TestPipeline()

  def run_windowed_side_inputs(
      self,
      elements,
      main_window_fn,
      side_window_fn=None,
      side_input_type=beam.pvalue.AsList,
      combine_fn=None,
      expected=None):
    with self.create_pipeline() as p:
      pcoll = p | beam.Create(elements) | beam.Map(
          lambda t: window.TimestampedValue(t, t))
      main = pcoll | 'WindowMain' >> beam.WindowInto(main_window_fn)
      side = pcoll | 'WindowSide' >> beam.WindowInto(
          side_window_fn or main_window_fn)
      kw = {}
      if combine_fn is not None:
        side |= beam.CombineGlobally(combine_fn).without_defaults()
        kw['default_value'] = 0
      elif side_input_type == beam.pvalue.AsDict:
        side |= beam.Map(lambda x: ('k%s' % x, 'v%s' % x))
      res = main | beam.Map(lambda x, s: (x, s), side_input_type(side, **kw))
      if side_input_type in (beam.pvalue.AsIter, beam.pvalue.AsList):
        res |= beam.Map(lambda x_s: (x_s[0], sorted(x_s[1])))
      assert_that(res, equal_to(expected))

  def test_global_global_windows(self):
    self.run_windowed_side_inputs([1, 2, 3],
                                  window.GlobalWindows(),
                                  expected=[(1, [1, 2, 3]), (2, [1, 2, 3]),
                                            (3, [1, 2, 3])])

  def test_same_fixed_windows(self):
    self.run_windowed_side_inputs([1, 2, 11],
                                  window.FixedWindows(10),
                                  expected=[(1, [1, 2]), (2, [1, 2]),
                                            (11, [11])])

  def test_different_fixed_windows(self):
    self.run_windowed_side_inputs([1, 2, 11, 21, 31],
                                  window.FixedWindows(10),
                                  window.FixedWindows(20),
                                  expected=[(1, [1, 2, 11]), (2, [1, 2, 11]),
                                            (11, [1, 2, 11]), (21, [21, 31]),
                                            (31, [21, 31])])

  def test_fixed_global_window(self):
    self.run_windowed_side_inputs([1, 2, 11],
                                  window.FixedWindows(10),
                                  window.GlobalWindows(),
                                  expected=[(1, [1, 2, 11]), (2, [1, 2, 11]),
                                            (11, [1, 2, 11])])

  def test_sliding_windows(self):
    self.run_windowed_side_inputs(
        [1, 2, 4],
        window.SlidingWindows(size=6, period=2),
        window.SlidingWindows(size=6, period=2),
        expected=[
            # Element 1 falls in three windows
            (1, [1]),        # [-4, 2)
            (1, [1, 2]),     # [-2, 4)
            (1, [1, 2, 4]),  # [0, 6)
            # as does 2,
            (2, [1, 2]),     # [-2, 4)
            (2, [1, 2, 4]),  # [0, 6)
            (2, [2, 4]),     # [2, 8)
            # and 4.
            (4, [1, 2, 4]),  # [0, 6)
            (4, [2, 4]),     # [2, 8)
            (4, [4]),        # [4, 10)
        ])

  def test_windowed_iter(self):
    self.run_windowed_side_inputs([1, 2, 11],
                                  window.FixedWindows(10),
                                  side_input_type=beam.pvalue.AsIter,
                                  expected=[(1, [1, 2]), (2, [1, 2]),
                                            (11, [11])])

  def test_windowed_singleton(self):
    self.run_windowed_side_inputs([1, 2, 11],
                                  window.FixedWindows(10),
                                  side_input_type=beam.pvalue.AsSingleton,
                                  combine_fn=sum,
                                  expected=[(1, 3), (2, 3), (11, 11)])

  def test_windowed_dict(self):
    self.run_windowed_side_inputs([1, 2, 11],
                                  window.FixedWindows(10),
                                  side_input_type=beam.pvalue.AsDict,
                                  expected=[
                                      (1, {
                                          'k1': 'v1', 'k2': 'v2'
                                      }),
                                      (2, {
                                          'k1': 'v1', 'k2': 'v2'
                                      }),
                                      (11, {
                                          'k11': 'v11'
                                      }),
                                  ])

  @pytest.mark.it_validatesrunner
  def test_empty_singleton_side_input(self):
    pipeline = self.create_pipeline()
    pcol = pipeline | 'start' >> beam.Create([1, 2])
    side = pipeline | 'side' >> beam.Create([])  # Empty side input.

    def my_fn(k, s):
      # TODO(robertwb): Should this be an error as in Java?
      v = ('empty' if isinstance(s, beam.pvalue.EmptySideInput) else 'full')
      return [(k, v)]

    result = pcol | 'compute' >> beam.FlatMap(
        my_fn, beam.pvalue.AsSingleton(side))
    assert_that(result, equal_to([(1, 'empty'), (2, 'empty')]))
    pipeline.run()

  # TODO(BEAM-5025): Disable this test in streaming temporarily.
  # Remove sickbay-streaming tag after it's fixed.
  @pytest.mark.no_sickbay_streaming
  @pytest.mark.it_validatesrunner
  def test_multi_valued_singleton_side_input(self):
    pipeline = self.create_pipeline()
    pcol = pipeline | 'start' >> beam.Create([1, 2])
    side = pipeline | 'side' >> beam.Create([3, 4])  # 2 values in side input.
    pcol | 'compute' >> beam.FlatMap(  # pylint: disable=expression-not-assigned
        lambda x, s: [x * s], beam.pvalue.AsSingleton(side))
    with self.assertRaises(Exception):
      pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_default_value_singleton_side_input(self):
    pipeline = self.create_pipeline()
    pcol = pipeline | 'start' >> beam.Create([1, 2])
    side = pipeline | 'side' >> beam.Create([])  # 0 values in side input.
    result = pcol | beam.FlatMap(
        lambda x, s: [x * s], beam.pvalue.AsSingleton(side, 10))
    assert_that(result, equal_to([10, 20]))
    pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_iterable_side_input(self):
    pipeline = self.create_pipeline()
    pcol = pipeline | 'start' >> beam.Create([1, 2])
    side = pipeline | 'side' >> beam.Create([3, 4])  # 2 values in side input.
    result = pcol | 'compute' >> beam.FlatMap(
        lambda x, s: [x * y for y in s], beam.pvalue.AsIter(side))
    assert_that(result, equal_to([3, 4, 6, 8]))
    pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_reiterable_side_input(self):
    expected_side = frozenset(range(100))

    def check_reiteration(main, side):
      assert expected_side == set(side), side
      # Iterate a second time.
      assert expected_side == set(side), side
      # Iterate over two copies of the input at the same time.
      both = zip(side, side)
      first, second = zip(*both)
      assert expected_side == set(first), first
      assert expected_side == set(second), second
      # This will iterate over two copies of the side input, but offset.
      offset = [None] * (len(expected_side) // 2)
      both = zip(itertools.chain(side, offset), itertools.chain(offset, side))
      first, second = zip(*both)
      expected_and_none = frozenset.union(expected_side, [None])
      assert expected_and_none == set(first), first
      assert expected_and_none == set(second), second

    pipeline = self.create_pipeline()
    pcol = pipeline | 'start' >> beam.Create(['A', 'B'])
    side = pipeline | 'side' >> beam.Create(expected_side)
    _ = pcol | 'check' >> beam.Map(check_reiteration, beam.pvalue.AsIter(side))
    pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_as_list_and_as_dict_side_inputs(self):
    a_list = [5, 1, 3, 2, 9]
    some_pairs = [('crouton', 17), ('supreme', None)]
    pipeline = self.create_pipeline()
    main_input = pipeline | 'main input' >> beam.Create([1])
    side_list = pipeline | 'side list' >> beam.Create(a_list)
    side_pairs = pipeline | 'side pairs' >> beam.Create(some_pairs)
    results = main_input | 'concatenate' >> beam.Map(
        lambda x,
        the_list,
        the_dict: [x, the_list, the_dict],
        beam.pvalue.AsList(side_list),
        beam.pvalue.AsDict(side_pairs))

    def matcher(expected_elem, expected_list, expected_pairs):
      def match(actual):
        [[actual_elem, actual_list, actual_dict]] = actual
        equal_to([expected_elem])([actual_elem])
        equal_to(expected_list)(actual_list)
        equal_to(expected_pairs)(actual_dict.items())

      return match

    assert_that(results, matcher(1, a_list, some_pairs))
    pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_as_singleton_without_unique_labels(self):
    # This should succeed as calling beam.pvalue.AsSingleton on the same
    # PCollection twice with the same defaults will return the same
    # view.
    a_list = [2]
    pipeline = self.create_pipeline()
    main_input = pipeline | 'main input' >> beam.Create([1])
    side_list = pipeline | 'side list' >> beam.Create(a_list)
    results = main_input | beam.Map(
        lambda x,
        s1,
        s2: [x, s1, s2],
        beam.pvalue.AsSingleton(side_list),
        beam.pvalue.AsSingleton(side_list))

    def matcher(expected_elem, expected_singleton):
      def match(actual):
        [[actual_elem, actual_singleton1, actual_singleton2]] = actual
        equal_to([expected_elem])([actual_elem])
        equal_to([expected_singleton])([actual_singleton1])
        equal_to([expected_singleton])([actual_singleton2])

      return match

    assert_that(results, matcher(1, 2))
    pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_as_singleton_with_different_defaults(self):
    a_list = []
    pipeline = self.create_pipeline()
    main_input = pipeline | 'main input' >> beam.Create([1])
    side_list = pipeline | 'side list' >> beam.Create(a_list)
    results = main_input | beam.Map(
        lambda x,
        s1,
        s2: [x, s1, s2],
        beam.pvalue.AsSingleton(side_list, default_value=2),
        beam.pvalue.AsSingleton(side_list, default_value=3))

    def matcher(expected_elem, expected_singleton1, expected_singleton2):
      def match(actual):
        [[actual_elem, actual_singleton1, actual_singleton2]] = actual
        equal_to([expected_elem])([actual_elem])
        equal_to([expected_singleton1])([actual_singleton1])
        equal_to([expected_singleton2])([actual_singleton2])

      return match

    assert_that(results, matcher(1, 2, 3))
    pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_as_list_twice(self):
    # This should succeed as calling beam.pvalue.AsList on the same
    # PCollection twice will return the same view.
    a_list = [1, 2, 3]
    pipeline = self.create_pipeline()
    main_input = pipeline | 'main input' >> beam.Create([1])
    side_list = pipeline | 'side list' >> beam.Create(a_list)
    results = main_input | beam.Map(
        lambda x,
        ls1,
        ls2: [x, ls1, ls2],
        beam.pvalue.AsList(side_list),
        beam.pvalue.AsList(side_list))

    def matcher(expected_elem, expected_list):
      def match(actual):
        [[actual_elem, actual_list1, actual_list2]] = actual
        equal_to([expected_elem])([actual_elem])
        equal_to(expected_list)(actual_list1)
        equal_to(expected_list)(actual_list2)

      return match

    assert_that(results, matcher(1, [1, 2, 3]))
    pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_as_dict_twice(self):
    some_kvs = [('a', 1), ('b', 2)]
    pipeline = self.create_pipeline()
    main_input = pipeline | 'main input' >> beam.Create([1])
    side_kvs = pipeline | 'side kvs' >> beam.Create(some_kvs)
    results = main_input | beam.Map(
        lambda x,
        dct1,
        dct2: [x, dct1, dct2],
        beam.pvalue.AsDict(side_kvs),
        beam.pvalue.AsDict(side_kvs))

    def matcher(expected_elem, expected_kvs):
      def match(actual):
        [[actual_elem, actual_dict1, actual_dict2]] = actual
        equal_to([expected_elem])([actual_elem])
        equal_to(expected_kvs)(actual_dict1.items())
        equal_to(expected_kvs)(actual_dict2.items())

      return match

    assert_that(results, matcher(1, some_kvs))
    pipeline.run()

  @pytest.mark.it_validatesrunner
  def test_flattened_side_input(self):
    pipeline = self.create_pipeline()
    main_input = pipeline | 'main input' >> beam.Create([None])
    side_input = (
        pipeline | 'side1' >> beam.Create(['a']),
        pipeline | 'side2' >> beam.Create(['b'])) | beam.Flatten()
    results = main_input | beam.FlatMap(
        lambda _, ab: ab, beam.pvalue.AsList(side_input))

    assert_that(results, equal_to(['a', 'b']))
    pipeline.run()

  # TODO(BEAM-9499): Disable this test in streaming temporarily.
  @pytest.mark.no_sickbay_batch
  @pytest.mark.no_sickbay_streaming
  @pytest.mark.it_validatesrunner
  def test_multi_triggered_gbk_side_input(self):
    """Test a GBK sideinput, with multiple triggering."""
    # TODO(BEAM-9322): Remove use of this experiment.
    # This flag is only necessary when using the multi-output TestStream b/c
    # it relies on using the PCollection output tags as the PCollection output
    # ids.
    with TestPipeline() as p:

      test_stream = (
          p
          | 'Mixed TestStream' >> TestStream().advance_watermark_to(
              3,
              tag='main').add_elements(['a1'], tag='main').advance_watermark_to(
                  8, tag='main').add_elements(['a2'], tag='main').add_elements(
                      [window.TimestampedValue(('k', 100), 2)], tag='side').
          add_elements([window.TimestampedValue(
              ('k', 400), 7)], tag='side').advance_watermark_to_infinity(
                  tag='main').advance_watermark_to_infinity(tag='side'))

      main_data = (
          test_stream['main']
          | 'Main windowInto' >> beam.WindowInto(
              window.FixedWindows(5),
              accumulation_mode=trigger.AccumulationMode.DISCARDING))

      side_data = (
          test_stream['side']
          | 'Side windowInto' >> beam.WindowInto(
              window.FixedWindows(5),
              trigger=trigger.AfterWatermark(early=trigger.AfterCount(1)),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | beam.CombinePerKey(sum)
          | 'Values' >> Map(lambda k_vs: k_vs[1]))

      class RecordFn(beam.DoFn):
        def process(
            self,
            elm=beam.DoFn.ElementParam,
            ts=beam.DoFn.TimestampParam,
            side=beam.DoFn.SideInputParam):
          yield (elm, ts, side)

      records = (
          main_data
          | beam.ParDo(RecordFn(), beam.pvalue.AsList(side_data)))

      expected_window_to_elements = {
          window.IntervalWindow(0, 5): [
              ('a1', Timestamp(3), [100, 0]),
          ],
          window.IntervalWindow(5, 10): [('a2', Timestamp(8), [400, 0])],
      }

      assert_that(
          records,
          equal_to_per_window(expected_window_to_elements),
          use_global_window=False,
          label='assert per window')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
