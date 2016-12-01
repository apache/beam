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

"""Unit tests for the windowing classes."""

import unittest

from apache_beam.pipeline import Pipeline
from apache_beam.transforms import CombinePerKey
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.transforms import Create
from apache_beam.transforms import GroupByKey
from apache_beam.transforms import Map
from apache_beam.transforms import window
from apache_beam.transforms import WindowInto
from apache_beam.transforms.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowedValue
from apache_beam.transforms.window import WindowFn


def context(element, timestamp, windows):
  return WindowFn.AssignContext(timestamp, element, windows)


sort_values = Map(lambda (k, vs): (k, sorted(vs)))


class ReifyWindowsFn(core.DoFn):
  def process(self, context):
    key, values = context.element
    for window in context.windows:
      yield "%s @ %s" % (key, window), values
reify_windows = core.ParDo(ReifyWindowsFn())


class WindowTest(unittest.TestCase):

  def test_fixed_windows(self):
    # Test windows with offset: 2, 7, 12, 17, ...
    windowfn = window.FixedWindows(size=5, offset=2)
    self.assertEqual([window.IntervalWindow(7, 12)],
                     windowfn.assign(context('v', 7, [])))
    self.assertEqual([window.IntervalWindow(7, 12)],
                     windowfn.assign(context('v', 11, [])))
    self.assertEqual([window.IntervalWindow(12, 17)],
                     windowfn.assign(context('v', 12, [])))

    # Test windows without offset: 0, 5, 10, 15, ...
    windowfn = window.FixedWindows(size=5)
    self.assertEqual([window.IntervalWindow(5, 10)],
                     windowfn.assign(context('v', 5, [])))
    self.assertEqual([window.IntervalWindow(5, 10)],
                     windowfn.assign(context('v', 9, [])))
    self.assertEqual([window.IntervalWindow(10, 15)],
                     windowfn.assign(context('v', 10, [])))

    # Test windows with offset out of range.
    windowfn = window.FixedWindows(size=5, offset=12)
    self.assertEqual([window.IntervalWindow(7, 12)],
                     windowfn.assign(context('v', 11, [])))

  def test_sliding_windows_assignment(self):
    windowfn = SlidingWindows(size=15, period=5, offset=2)
    expected = [IntervalWindow(7, 22),
                IntervalWindow(2, 17),
                IntervalWindow(-3, 12)]
    self.assertEqual(expected, windowfn.assign(context('v', 7, [])))
    self.assertEqual(expected, windowfn.assign(context('v', 8, [])))
    self.assertEqual(expected, windowfn.assign(context('v', 11, [])))

  def test_sessions_merging(self):
    windowfn = Sessions(10)

    def merge(*timestamps):
      windows = [windowfn.assign(context(None, t, [])) for t in timestamps]
      running = set()

      class TestMergeContext(WindowFn.MergeContext):

        def __init__(self):
          super(TestMergeContext, self).__init__(running)

        def merge(self, to_be_merged, merge_result):
          for w in to_be_merged:
            if w in running:
              running.remove(w)
          running.add(merge_result)

      for ws in windows:
        running.update(ws)
        windowfn.merge(TestMergeContext())
      windowfn.merge(TestMergeContext())
      return sorted(running)

    self.assertEqual([IntervalWindow(2, 12)], merge(2))
    self.assertEqual([IntervalWindow(2, 12), IntervalWindow(19, 29)],
                     merge(2, 19))

    self.assertEqual([IntervalWindow(2, 19)], merge(2, 9))
    self.assertEqual([IntervalWindow(2, 19)], merge(9, 2))

    self.assertEqual([IntervalWindow(2, 19), IntervalWindow(19, 29)],
                     merge(2, 9, 19))
    self.assertEqual([IntervalWindow(2, 19), IntervalWindow(19, 29)],
                     merge(19, 9, 2))

    self.assertEqual([IntervalWindow(2, 25)], merge(2, 15, 10))

  def timestamped_key_values(self, pipeline, key, *timestamps):
    return (pipeline | 'start' >> Create(timestamps)
            | Map(lambda x: WindowedValue((key, x), x, [])))

  def test_sliding_windows(self):
    p = Pipeline('DirectPipelineRunner')
    pcoll = self.timestamped_key_values(p, 'key', 1, 2, 3)
    result = (pcoll
              | 'w' >> WindowInto(SlidingWindows(period=2, size=4))
              | GroupByKey()
              | reify_windows)
    expected = [('key @ [-2.0, 2.0)', [1]),
                ('key @ [0.0, 4.0)', [1, 2, 3]),
                ('key @ [2.0, 6.0)', [2, 3])]
    assert_that(result, equal_to(expected))
    p.run()

  def test_sessions(self):
    p = Pipeline('DirectPipelineRunner')
    pcoll = self.timestamped_key_values(p, 'key', 1, 2, 3, 20, 35, 27)
    result = (pcoll
              | 'w' >> WindowInto(Sessions(10))
              | GroupByKey()
              | sort_values
              | reify_windows)
    expected = [('key @ [1.0, 13.0)', [1, 2, 3]),
                ('key @ [20.0, 45.0)', [20, 27, 35])]
    assert_that(result, equal_to(expected))
    p.run()

  def test_timestamped_value(self):
    p = Pipeline('DirectPipelineRunner')
    result = (p
              | 'start' >> Create([(k, k) for k in range(10)])
              | Map(lambda (x, t): TimestampedValue(x, t))
              | 'w' >> WindowInto(FixedWindows(5))
              | Map(lambda v: ('key', v))
              | GroupByKey())
    assert_that(result, equal_to([('key', [0, 1, 2, 3, 4]),
                                  ('key', [5, 6, 7, 8, 9])]))
    p.run()

  def test_timestamped_with_combiners(self):
    p = Pipeline('DirectPipelineRunner')
    result = (p
              # Create some initial test values.
              | 'start' >> Create([(k, k) for k in range(10)])
              # The purpose of the WindowInto transform is to establish a
              # FixedWindows windowing function for the PCollection.
              # It does not bucket elements into windows since the timestamps
              # from Create are not spaced 5 ms apart and very likely they all
              # fall into the same window.
              | 'w' >> WindowInto(FixedWindows(5))
              # Generate timestamped values using the values as timestamps.
              # Now there are values 5 ms apart and since Map propagates the
              # windowing function from input to output the output PCollection
              # will have elements falling into different 5ms windows.
              | Map(lambda (x, t): TimestampedValue(x, t))
              # We add a 'key' to each value representing the index of the
              # window. This is important since there is no guarantee of
              # order for the elements of a PCollection.
              | Map(lambda v: (v / 5, v)))
    # Sum all elements associated with a key and window. Although it
    # is called CombinePerKey it is really CombinePerKeyAndWindow the
    # same way GroupByKey is really GroupByKeyAndWindow.
    sum_per_window = result | CombinePerKey(sum)
    # Compute mean per key and window.
    mean_per_window = result | combiners.Mean.PerKey()
    assert_that(sum_per_window, equal_to([(0, 10), (1, 35)]),
                label='assert:sum')
    assert_that(mean_per_window, equal_to([(0, 2.0), (1, 7.0)]),
                label='assert:mean')
    p.run()


if __name__ == '__main__':
  unittest.main()
