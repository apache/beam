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

import logging
import unittest

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.transforms.util import assert_that, equal_to


class SideInputsTest(unittest.TestCase):

  def create_pipeline(self):
    return beam.Pipeline('DirectPipelineRunner')

  def run_windowed_side_inputs(self, elements, main_window_fn,
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
        res |= beam.Map(lambda (x, s): (x, sorted(s)))
      assert_that(res, equal_to(expected))

  def test_global_global_windows(self):
    self.run_windowed_side_inputs(
        [1, 2, 3],
        window.GlobalWindows(),
        expected=[(1, [1, 2, 3]), (2, [1, 2, 3]), (3, [1, 2, 3])])

  def test_same_fixed_windows(self):
    self.run_windowed_side_inputs(
        [1, 2, 11],
        window.FixedWindows(10),
        expected=[(1, [1, 2]), (2, [1, 2]), (11, [11])])

  def test_different_fixed_windows(self):
    self.run_windowed_side_inputs(
        [1, 2, 11, 21, 31],
        window.FixedWindows(10),
        window.FixedWindows(20),
        expected=[(1, [1, 2, 11]), (2, [1, 2, 11]), (11, [1, 2, 11]),
                  (21, [21, 31]), (31, [21, 31])])

  def test_fixed_global_window(self):
    self.run_windowed_side_inputs(
        [1, 2, 11],
        window.FixedWindows(10),
        window.GlobalWindows(),
        expected=[(1, [1, 2, 11]), (2, [1, 2, 11]), (11, [1, 2, 11])])

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
    self.run_windowed_side_inputs(
        [1, 2, 11],
        window.FixedWindows(10),
        side_input_type=beam.pvalue.AsIter,
        expected=[(1, [1, 2]), (2, [1, 2]), (11, [11])])

  def test_windowed_singleton(self):
    self.run_windowed_side_inputs(
        [1, 2, 11],
        window.FixedWindows(10),
        side_input_type=beam.pvalue.AsSingleton,
        combine_fn=sum,
        expected=[(1, 3), (2, 3), (11, 11)])

  def test_windowed_dict(self):
    self.run_windowed_side_inputs(
        [1, 2, 11],
        window.FixedWindows(10),
        side_input_type=beam.pvalue.AsDict,
        expected=[
            (1, {'k1': 'v1', 'k2': 'v2'}),
            (2, {'k1': 'v1', 'k2': 'v2'}),
            (11, {'k11': 'v11'}),
        ])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
