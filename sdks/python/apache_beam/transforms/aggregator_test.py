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

"""Unit tests for Aggregator class."""

import unittest

import google.cloud.dataflow as df
from google.cloud.dataflow.transforms import combiners
from google.cloud.dataflow.transforms.aggregator import Aggregator


class AggregatorTest(unittest.TestCase):

  def test_str(self):
    basic = Aggregator('a-name')
    self.assertEqual('<Aggregator a-name SumInt64Fn(int)>', str(basic))

    for_max = Aggregator('max-name', max)
    self.assertEqual('<Aggregator max-name MaxInt64Fn(int)>', str(for_max))

    for_float = Aggregator('f-name', sum, float)
    self.assertEqual('<Aggregator f-name SumFloatFn(float)>', str(for_float))

    for_mean = Aggregator('m-name', combiners.MeanCombineFn(), float)
    self.assertEqual('<Aggregator m-name MeanFloatFn(float)>', str(for_mean))

  def test_aggregation(self):

    mean = combiners.MeanCombineFn()
    mean.__name__ = 'mean'
    counter_types = [
        (sum, int, 6),
        (min, int, 0),
        (max, int, 3),
        (mean, int, 1),
        (sum, float, 6.0),
        (min, float, 0.0),
        (max, float, 3.0),
        (mean, float, 1.5),
        (any, int, True),
        (all, float, False),
    ]
    aggeregators = [Aggregator('%s_%s' % (f.__name__, t.__name__), f, t)
                    for f, t, _ in counter_types]

    class UpdateAggregators(df.DoFn):
      def process(self, context):
        for a in aggeregators:
          context.aggregate_to(a, context.element)

    p = df.Pipeline('DirectPipelineRunner')
    p | df.Create([0, 1, 2, 3]) | df.ParDo(UpdateAggregators())
    res = p.run()
    for (_, _, expected), a in zip(counter_types, aggeregators):
      actual = res.aggregated_values(a).values()[0]
      self.assertEqual(expected, actual)
      self.assertEqual(type(expected), type(actual))


if __name__ == '__main__':
  unittest.main()
