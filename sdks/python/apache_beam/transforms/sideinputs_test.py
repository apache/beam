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

  # TODO(BEAM-733): Actually support this.
  def test_no_sideinput_windowing(self):
    p = beam.Pipeline('DirectPipelineRunner')
    pc = p | beam.Create([0, 1]) | beam.WindowInto(window.FixedWindows(10))
    with self.assertRaises(ValueError):
      # pylint: disable=expression-not-assigned
      pc | beam.Map(lambda x, side: None, side=beam.pvalue.AsIter(pc))

  def run_windowed_side_inputs(self, elements, main_window_fn,
                               side_window_fn=None,
                               side_input_type=beam.pvalue.AsList,
                               combine_fn=None,
                               expected=None):
    with beam.Pipeline('DirectPipelineRunner') as p:
      pcoll = p | beam.Create(elements) | beam.Map(
          lambda t: window.TimestampedValue(t, t))
      main = pcoll | 'WindowMain' >> beam.WindowInto(main_window_fn)
      side = pcoll | 'WindowSide' >> beam.WindowInto(
          side_window_fn or main_window_fn)
      if combine_fn is not None:
        side |= beam.CombineGlobally(combine_fn)
      res = main | beam.Map(lambda x, s: (x, s), side_input_type(side))
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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
