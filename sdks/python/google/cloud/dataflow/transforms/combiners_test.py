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

"""Unit tests for our libraries of combine PTransforms."""

import unittest

import google.cloud.dataflow as df
from google.cloud.dataflow.pipeline import Pipeline
from google.cloud.dataflow.transforms import combiners
import google.cloud.dataflow.transforms.combiners as combine
from google.cloud.dataflow.transforms.core import CombineGlobally
from google.cloud.dataflow.transforms.core import Create
from google.cloud.dataflow.transforms.core import Map
from google.cloud.dataflow.transforms.ptransform import PTransform


class CombineTest(unittest.TestCase):

  def test_builtin_combines(self):
    pipeline = Pipeline('DirectPipelineRunner')

    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    mean = sum(vals) / float(len(vals))
    size = len(vals)

    # First for global combines.
    pcoll = pipeline | Create('start', vals)
    result_mean = pcoll | combine.Mean.Globally('mean')
    result_count = pcoll | combine.Count.Globally('count')
    self.assertAlmostEqual([mean], list(result_mean.get()))
    self.assertEqual([size], list(result_count.get()))

    # Again for per-key combines.
    pcoll = pipeline | Create('start-perkey', [('a', x) for x in vals])
    result_mean = pcoll | combine.Mean.PerKey('mean-perkey')
    result_count = pcoll | combine.Count.PerKey('count-perkey')
    self.assertAlmostEqual([('a', mean)], list(result_mean.get()))
    self.assertEqual([('a', size)], list(result_count.get()))

  def test_top(self):
    pipeline = Pipeline('DirectPipelineRunner')

    # A parameter we'll be sharing with a custom comparator.
    names = {0: 'zo',
             1: 'one',
             2: 'twoo',
             3: 'three',
             5: 'fiiive',
             6: 'sssssix',
             9: 'nniiinne'}

    # First for global combines.
    pcoll = pipeline | Create('start', [6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
    result_top = pcoll | combine.Top.Largest('top', 5)
    result_bot = pcoll | combine.Top.Smallest('bot', 4)
    result_cmp = pcoll | combine.Top.Of(
        'cmp',
        6,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names)  # Note parameter passed to comparator.
    self.assertEqual([[9, 6, 6, 5, 3]], list(result_top.get()))
    self.assertEqual([[0, 1, 1, 1]], list(result_bot.get()))
    self.assertEqual([[9, 6, 6, 5, 3, 2]], list(result_cmp.get()))

    # Again for per-key combines.
    pcoll = pipeline | Create(
        'start-perkey', [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_top = pcoll | combine.Top.LargestPerKey('top-perkey', 5)
    result_bot = pcoll | combine.Top.SmallestPerKey('bot-perkey', 4)
    result_cmp = pcoll | combine.Top.PerKey(
        'cmp-perkey',
        6,
        lambda a, b, names: len(names[a]) < len(names[b]),
        names)  # Note parameter passed to comparator.
    self.assertEqual([('a', [9, 6, 6, 5, 3])], list(result_top.get()))
    self.assertEqual([('a', [0, 1, 1, 1])], list(result_bot.get()))
    self.assertEqual([('a', [9, 6, 6, 5, 3, 2])], list(result_cmp.get()))

  def test_top_shorthands(self):
    pipeline = Pipeline('DirectPipelineRunner')

    pcoll = pipeline | Create('start', [6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
    result_top = pcoll | df.CombineGlobally('top', combiners.Largest(5))
    result_bot = pcoll | df.CombineGlobally('bot', combiners.Smallest(4))
    self.assertEqual([[9, 6, 6, 5, 3]], list(result_top.get()))
    self.assertEqual([[0, 1, 1, 1]], list(result_bot.get()))

    pcoll = pipeline | Create(
        'start-perkey', [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
    result_top = pcoll | df.CombinePerKey('top-perkey', combiners.Largest(5))
    result_bot = pcoll | df.CombinePerKey('bot-perkey', combiners.Smallest(4))
    self.assertEqual([('a', [9, 6, 6, 5, 3])], list(result_top.get()))
    self.assertEqual([('a', [0, 1, 1, 1])], list(result_bot.get()))

  def test_sample(self):
    pipeline = Pipeline('DirectPipelineRunner')

    # First test global samples (lots of them).
    pcoll = pipeline | Create('start', [1, 1, 2, 2])
    for ix in xrange(300):
      result = pcoll | combine.Sample.FixedSizeGlobally('sample-%d' % ix, 3)
      result_as_list = list(result.get())
      # There is always exactly one result.
      self.assertEqual(1, len(result_as_list))
      # There are always exactly three samples in the result.
      self.assertEqual(3, len(result_as_list[0]))
      # Sampling is without replacement.
      num_ones = sum(1 for x in result_as_list[0] if x == 1)
      num_twos = sum(1 for x in result_as_list[0] if x == 2)
      self.assertTrue((num_ones == 1 and num_twos == 2) or
                      (num_ones == 2 and num_twos == 1))

    # Now test per-key samples.
    pcoll = pipeline | Create(
        'start-perkey',
        sum(([(i, 1), (i, 1), (i, 2), (i, 2)] for i in xrange(300)), []))
    result = pcoll | combine.Sample.FixedSizePerKey('sample', 3)
    for _, samples in result.get():
      self.assertEqual(3, len(samples))
      num_ones = sum(1 for x in samples if x == 1)
      num_twos = sum(1 for x in samples if x == 2)
      self.assertTrue((num_ones == 1 and num_twos == 2) or
                      (num_ones == 2 and num_twos == 1))

  def test_tuple_combine_fn(self):
    result = (
        [('a', 100, 0.0), ('b', 10, -1), ('c', 1, 100)]
        | df.CombineGlobally(combine.TupleCombineFn(max,
                                                    combine.MeanCombineFn(),
                                                    sum)).without_defaults())
    self.assertEquals(result, [('c', 111.0 / 3, 99.0)])

    result = (
        [1, 1, 2, 3]
        | df.CombineGlobally(combine.TupleCombineFn(min,
                                                    combine.MeanCombineFn(),
                                                    max)
                             .with_common_input()).without_defaults())
    self.assertEquals(result, [(1, 7.0 / 4, 3)])

  def test_to_list_and_to_dict(self):
    pipeline = Pipeline('DirectPipelineRunner')

    the_list = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    pcoll = pipeline | Create('start', the_list)
    result = pcoll | combine.ToList('to list')
    self.assertEqual([sorted(the_list)], [sorted(l) for l in result.get()])

    pairs = [(1, 2), (3, 4), (5, 6)]
    pcoll = pipeline | Create('start-pairs', pairs)
    result = pcoll | combine.ToDict('to dict')
    result_as_list = list(result.get())
    self.assertEqual(1, len(result_as_list))
    self.assertTrue(isinstance(result_as_list[0], dict))
    self.assertEqual(sorted(pairs), sorted(result_as_list[0].iteritems()))

  def test_combine_globally_with_default(self):
    self.assertEqual([0], [] | CombineGlobally(sum))

  def test_combine_globally_without_default(self):
    self.assertEqual([], [] | CombineGlobally(sum).without_defaults())

  def test_combine_globally_with_default_side_input(self):
    class CombineWithSideInput(PTransform):
      def apply(self, pcoll):
        side = pcoll | CombineGlobally(sum).as_singleton_view()
        main = pcoll.pipeline | Create([None])
        return main | Map(lambda _, s: s, side)
    self.assertEqual([0], [] | CombineWithSideInput())
    self.assertEqual([10], [1, 2, 3, 4] | CombineWithSideInput())

if __name__ == '__main__':
  unittest.main()
