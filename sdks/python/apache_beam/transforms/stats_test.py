# -*- coding: utf-8 -*-
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

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import math
import random
import sys
import unittest
from builtins import range
from collections import defaultdict

import hamcrest as hc
from parameterized import parameterized
from parameterized import parameterized_class

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.core import Create
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam.transforms.stats import ApproximateQuantilesCombineFn
from apache_beam.transforms.stats import ApproximateUniqueCombineFn

try:
  import mmh3
  mmh3_options = [(mmh3, ), (None, )]
except ImportError:
  mmh3_options = [(None, )]


@parameterized_class(('mmh3_option', ), mmh3_options)
class ApproximateUniqueTest(unittest.TestCase):
  """Unit tests for ApproximateUnique.Globally and ApproximateUnique.PerKey."""
  random.seed(0)

  def setUp(self):
    sys.modules['mmh3'] = self.mmh3_option

  @parameterized.expand([
      (
          'small_population_by_size',
          list(range(30)),
          32,
          None,
          'assert:global_by_sample_size_with_small_population'),
      (
          'large_population_by_size',
          list(range(100)),
          16,
          None,
          'assert:global_by_sample_size_with_large_population'),
      (
          'with_duplicates_by_size', [10] * 50 + [20] * 50,
          30,
          None,
          'assert:global_by_sample_size_with_duplicates'),
      (
          'small_population_by_error',
          list(range(30)),
          None,
          0.3,
          'assert:global_by_error_with_small_population'),
      (
          'large_population_by_error',
          [random.randint(1, 1000) for _ in range(500)],
          None,
          0.1,
          'assert:global_by_error_with_large_population'),
  ])
  def test_approximate_unique_global(
      self, name, test_input, sample_size, est_error, label):
    # check that only either sample_size or est_error is not None
    assert bool(sample_size) != bool(est_error)
    if sample_size:
      error = 2 / math.sqrt(sample_size)
    else:
      error = est_error
    random.shuffle(test_input)
    actual_count = len(set(test_input))

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.Globally(
              size=sample_size, error=est_error)
          | 'compare' >> beam.FlatMap(
              lambda x: [abs(x - actual_count) * 1.0 / actual_count <= error]))

      assert_that(result, equal_to([True]), label=label)

  @parameterized.expand([
      ('by_size', 20, None, 'assert:unique_perkey_by_sample_size'),
      ('by_error', None, 0.02, 'assert:unique_perkey_by_error')
  ])
  def test_approximate_unique_perkey(self, name, sample_size, est_error, label):
    # check that only either sample_size or est_error is set
    assert bool(sample_size) != bool(est_error)
    if sample_size:
      error = 2 / math.sqrt(sample_size)
    else:
      error = est_error

    test_input = [(8, 73), (6, 724), (7, 70), (1, 576), (10, 120), (2, 662),
                  (7, 115), (3, 731), (6, 340), (6, 623), (1, 74), (9, 280),
                  (8, 298), (6, 440), (10, 243), (1, 125), (9, 754), (8, 833),
                  (9, 751), (4, 818), (6, 176), (9, 253), (2, 721), (8, 936),
                  (3, 691), (10, 685), (1, 69), (3, 155), (8, 86), (5, 693),
                  (2, 809), (4, 723), (8, 102), (9, 707), (8, 558), (4, 537),
                  (5, 371), (7, 432), (2, 51), (10, 397)]
    actual_count_dict = defaultdict(set)
    for (x, y) in test_input:
      actual_count_dict[x].add(y)

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.PerKey(
              size=sample_size, error=est_error)
          | 'compare' >> beam.FlatMap(
              lambda x: [
                  abs(x[1] - len(actual_count_dict[x[0]])) * 1.0 / len(
                      actual_count_dict[x[0]]) <= error
              ]))

      assert_that(
          result, equal_to([True] * len(actual_count_dict)), label=label)

  @parameterized.expand([
      (
          'invalid_input_size',
          list(range(30)),
          10,
          None,
          beam.ApproximateUnique._INPUT_SIZE_ERR_MSG % 10),
      (
          'invalid_type_size',
          list(range(30)),
          100.0,
          None,
          beam.ApproximateUnique._INPUT_SIZE_ERR_MSG % 100.0),
      (
          'invalid_small_error',
          list(range(30)),
          None,
          0.0,
          beam.ApproximateUnique._INPUT_ERROR_ERR_MSG % 0.0),
      (
          'invalid_big_error',
          list(range(30)),
          None,
          0.6,
          beam.ApproximateUnique._INPUT_ERROR_ERR_MSG % 0.6),
      (
          'no_input',
          list(range(30)),
          None,
          None,
          beam.ApproximateUnique._NO_VALUE_ERR_MSG),
      (
          'both_input',
          list(range(30)),
          30,
          0.2,
          beam.ApproximateUnique._MULTI_VALUE_ERR_MSG % (30, 0.2)),
  ])
  def test_approximate_unique_global_value_error(
      self, name, test_input, sample_size, est_error, expected_msg):
    with self.assertRaises(ValueError) as e:
      with TestPipeline() as pipeline:
        _ = (
            pipeline
            | 'create' >> beam.Create(test_input)
            | 'get_estimate' >> beam.ApproximateUnique.Globally(
                size=sample_size, error=est_error))

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_combine_fn_requires_nondeterministic_coder(self):
    sample_size = 30
    coder = coders.Base64PickleCoder()

    with self.assertRaises(ValueError) as e:
      _ = ApproximateUniqueCombineFn(sample_size, coder)

    self.assertRegex(
        e.exception.args[0],
        'The key coder "Base64PickleCoder" '
        'for ApproximateUniqueCombineFn is not deterministic.')

  def test_approximate_unique_combine_fn_requires_compatible_coder(self):
    test_input = 'a'
    sample_size = 30
    coder = coders.FloatCoder()
    combine_fn = ApproximateUniqueCombineFn(sample_size, coder)
    accumulator = combine_fn.create_accumulator()
    with self.assertRaises(RuntimeError) as e:
      accumulator = combine_fn.add_input(accumulator, test_input)

    self.assertRegex(e.exception.args[0], 'Runtime exception')

  def test_get_sample_size_from_est_error(self):
    # test if get correct sample size from input error.
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.5) == 16
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.4) == 25
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.2) == 100
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.1) == 400
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.05) == 1600
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.01) == 40000


class ApproximateQuantilesTest(unittest.TestCase):
  _kv_data = [("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 10), ("b", 10),
              ("b", 100)]
  _kv_str_data = [("a", "a"), ("a", "a" * 2), ("a", "a" * 3), ("b", "b"),
                  ("b", "b" * 10), ("b", "b" * 10), ("b", "b" * 100)]

  @staticmethod
  def _quantiles_matcher(expected):
    l = len(expected)

    def assert_true(exp):
      if not exp:
        raise BeamAssertException('%s Failed assert True' % repr(exp))

    def match(actual):
      actual = actual[0]
      for i in range(l):
        if isinstance(expected[i], list):
          assert_true(expected[i][0] <= actual[i] <= expected[i][1])
        else:
          equal_to([expected[i]])([actual[i]])

    return match

  @staticmethod
  def _approx_quantile_generator(size, num_of_quantiles, absoluteError):
    quantiles = [0]
    k = 1
    while k < num_of_quantiles - 1:
      expected = (size - 1) * k / (num_of_quantiles - 1)
      quantiles.append([expected - absoluteError, expected + absoluteError])
      k = k + 1
    quantiles.append(size - 1)
    return quantiles

  def test_quantiles_globaly(self):
    with TestPipeline() as p:
      pc = p | Create(list(range(101)))

      quantiles = pc | 'Quantiles globally' >> \
                  beam.ApproximateQuantiles.Globally(5)
      quantiles_reversed = pc | 'Quantiles globally reversed' >> \
                           beam.ApproximateQuantiles.Globally(5, reverse=True)

      assert_that(
          quantiles,
          equal_to([[0, 25, 50, 75, 100]]),
          label='checkQuantilesGlobally')
      assert_that(
          quantiles_reversed,
          equal_to([[100, 75, 50, 25, 0]]),
          label='checkReversedQuantiles')

  def test_quantiles_globally_weighted(self):
    num_inputs = 1e3
    a = -3
    b = 3

    # Weighting function coincides with the pdf of the standard normal
    # distribution up to a constant. Since 99.7% of the probability mass for
    # this pdf is concentrated in the interval [a, b] = [-3, 3], the quantiles
    # for a sample from this interval with the given weight function are
    # expected to be close to the quantiles of the standard normal distribution.
    def weight(x):
      return math.exp(-(x**2) / 2)

    input_data = [
        (a + (b - a) * i / num_inputs, weight(a + (b - a) * i / num_inputs))
        for i in range(int(num_inputs) + 1)
    ]
    with TestPipeline() as p:
      pc = p | Create(input_data)

      weighted_quantiles = pc | "Quantiles globally weighted" >> \
                           beam.ApproximateQuantiles.Globally(5, weighted=True)
      reversed_weighted_quantiles = (
          pc | 'Quantiles globally weighted reversed' >>
          beam.ApproximateQuantiles.Globally(5, reverse=True, weighted=True))

      assert_that(
          weighted_quantiles,
          equal_to([[-3., -0.6720000000000002, 0., 0.6720000000000002, 3.]]),
          label="checkWeightedQuantilesGlobally")
      assert_that(
          reversed_weighted_quantiles,
          equal_to([[3., 0.6720000000000002, 0., -0.6720000000000002, -3.]]),
          label="checkWeightedReversedQuantilesGlobally")

  def test_quantiles_per_key(self):
    with TestPipeline() as p:
      data = self._kv_data
      pc = p | Create(data)

      per_key = pc | 'Quantiles PerKey' >> beam.ApproximateQuantiles.PerKey(2)
      per_key_reversed = (
          pc | 'Quantiles PerKey Reversed' >> beam.ApproximateQuantiles.PerKey(
              2, reverse=True))

      assert_that(
          per_key,
          equal_to([('a', [1, 3]), ('b', [1, 100])]),
          label='checkQuantilePerKey')
      assert_that(
          per_key_reversed,
          equal_to([('a', [3, 1]), ('b', [100, 1])]),
          label='checkReversedQuantilesPerKey')

  def test_quantiles_per_key_weighted(self):
    with TestPipeline() as p:
      data = [(k, (v, 2.)) for k, v in self._kv_data]
      pc = p | Create(data)

      per_key = pc | 'Weighted Quantiles PerKey' >> \
                beam.ApproximateQuantiles.PerKey(2, weighted=True)
      per_key_reversed = pc | 'Weighted Quantiles PerKey Reversed' >> \
                         beam.ApproximateQuantiles.PerKey(
                           2, reverse=True, weighted=True)

      assert_that(
          per_key,
          equal_to([('a', [1, 3]), ('b', [1, 100])]),
          label='checkWeightedQuantilesPerKey')
      assert_that(
          per_key_reversed,
          equal_to([('a', [3, 1]), ('b', [100, 1])]),
          label='checkWeightedReversedQuantilesPerKey')

  def test_quantiles_per_key_with_key_argument(self):
    with TestPipeline() as p:
      data = self._kv_str_data
      pc = p | Create(data)

      per_key = pc | 'Per Key' >> beam.ApproximateQuantiles.PerKey(2, key=len)
      per_key_reversed = (
          pc | 'Per Key Reversed' >> beam.ApproximateQuantiles.PerKey(
              2, key=len, reverse=True))

      assert_that(
          per_key,
          equal_to([('a', ['a', 'a' * 3]), ('b', ['b', 'b' * 100])]),
          label='checkPerKey')
      assert_that(
          per_key_reversed,
          equal_to([('a', ['a' * 3, 'a']), ('b', ['b' * 100, 'b'])]),
          label='checkPerKeyReversed')

  def test_singleton(self):
    with TestPipeline() as p:
      data = [389]
      pc = p | Create(data)
      quantiles = pc | beam.ApproximateQuantiles.Globally(5)
      assert_that(quantiles, equal_to([[389, 389, 389, 389, 389]]))

  def test_uneven_quantiles(self):
    with TestPipeline() as p:
      pc = p | Create(list(range(5000)))
      quantiles = pc | beam.ApproximateQuantiles.Globally(37)
      approx_quantiles = self._approx_quantile_generator(
          size=5000, num_of_quantiles=37, absoluteError=20)
      assert_that(quantiles, self._quantiles_matcher(approx_quantiles))

  def test_large_quantiles(self):
    with TestPipeline() as p:
      pc = p | Create(list(range(10001)))
      quantiles = pc | beam.ApproximateQuantiles.Globally(50)
      approx_quantiles = self._approx_quantile_generator(
          size=10001, num_of_quantiles=50, absoluteError=20)
      assert_that(quantiles, self._quantiles_matcher(approx_quantiles))

  def test_random_quantiles(self):
    with TestPipeline() as p:
      data = list(range(101))
      random.shuffle(data)
      pc = p | Create(data)
      quantiles = pc | beam.ApproximateQuantiles.Globally(5)
      assert_that(quantiles, equal_to([[0, 25, 50, 75, 100]]))

  def test_duplicates(self):
    y = list(range(101))
    data = []
    for _ in range(10):
      data.extend(y)

    with TestPipeline() as p:
      pc = p | Create(data)
      quantiles = (
          pc | 'Quantiles Globally' >> beam.ApproximateQuantiles.Globally(5))
      quantiles_reversed = (
          pc | 'Quantiles Reversed' >> beam.ApproximateQuantiles.Globally(
              5, reverse=True))

      assert_that(
          quantiles,
          equal_to([[0, 25, 50, 75, 100]]),
          label="checkQuantilesGlobally")
      assert_that(
          quantiles_reversed,
          equal_to([[100, 75, 50, 25, 0]]),
          label="checkQuantileReversed")

  def test_lots_of_duplicates(self):
    with TestPipeline() as p:
      data = [1]
      data.extend([2 for _ in range(299)])
      data.extend([3 for _ in range(799)])
      pc = p | Create(data)
      quantiles = pc | beam.ApproximateQuantiles.Globally(5)
      assert_that(quantiles, equal_to([[1, 2, 3, 3, 3]]))

  def test_log_distribution(self):
    with TestPipeline() as p:
      data = [int(math.log(x)) for x in range(1, 1000)]
      pc = p | Create(data)
      quantiles = pc | beam.ApproximateQuantiles.Globally(5)
      assert_that(quantiles, equal_to([[0, 5, 6, 6, 6]]))

  def test_zipfian_distribution(self):
    with TestPipeline() as p:
      data = []
      for i in range(1, 1000):
        data.append(int(1000 / i))
      pc = p | Create(data)
      quantiles = pc | beam.ApproximateQuantiles.Globally(5)
      assert_that(quantiles, equal_to([[1, 1, 2, 4, 1000]]))

  def test_alternate_quantiles(self):
    data = ["aa", "aaa", "aaaa", "b", "ccccc", "dddd", "zz"]
    with TestPipeline() as p:
      pc = p | Create(data)

      globally = pc | 'Globally' >> beam.ApproximateQuantiles.Globally(3)
      with_key = (
          pc |
          'Globally with key' >> beam.ApproximateQuantiles.Globally(3, key=len))
      key_with_reversed = (
          pc | 'Globally with key and reversed' >>
          beam.ApproximateQuantiles.Globally(3, key=len, reverse=True))

      assert_that(
          globally, equal_to([["aa", "b", "zz"]]), label='checkGlobally')
      assert_that(
          with_key,
          equal_to([["b", "aaa", "ccccc"]]),
          label='checkGloballyWithKey')
      assert_that(
          key_with_reversed,
          equal_to([["ccccc", "aaa", "b"]]),
          label='checkWithKeyAndReversed')

  @staticmethod
  def _display_data_matcher(instance):
    expected_items = [
        DisplayDataItemMatcher('num_quantiles', instance._num_quantiles),
        DisplayDataItemMatcher('weighted', str(instance._weighted)),
        DisplayDataItemMatcher('key', str(instance._key.__name__)),
        DisplayDataItemMatcher('reverse', str(instance._reverse))
    ]
    return expected_items

  def test_global_display_data(self):
    transform = beam.ApproximateQuantiles.Globally(
        3, weighted=True, key=len, reverse=True)
    data = DisplayData.create_from(transform)
    expected_items = self._display_data_matcher(transform)
    hc.assert_that(data.items, hc.contains_inanyorder(*expected_items))

  def test_perkey_display_data(self):
    transform = beam.ApproximateQuantiles.PerKey(
        3, weighted=True, key=len, reverse=True)
    data = DisplayData.create_from(transform)
    expected_items = self._display_data_matcher(transform)
    hc.assert_that(data.items, hc.contains_inanyorder(*expected_items))


def _build_quantilebuffer_test_data():
  """
  Test data taken from "Munro-Paterson Algorithm" reference values table of
  "Approximate Medians and other Quantiles in One Pass and with Limited Memory"
  paper. See ApproximateQuantilesCombineFn for paper reference.
  """
  epsilons = [0.1, 0.05, 0.01, 0.005, 0.001]
  maxElementExponents = [5, 6, 7, 8, 9]
  expectedNumBuffersValues = [[11, 14, 17, 21, 24], [11, 14, 17, 20, 23],
                              [9, 11, 14, 17, 21], [8, 11, 14, 17,
                                                    20], [6, 9, 11, 14, 17]]
  expectedBufferSizeValues = [[98, 123, 153, 96, 120], [98, 123, 153, 191, 239],
                              [391, 977, 1221, 1526,
                               954], [782, 977, 1221, 1526,
                                      1908], [3125, 3907, 9766, 12208, 15259]]
  test_data = list()
  i = 0
  for epsilon in epsilons:
    j = 0
    for maxElementExponent in maxElementExponents:
      test_data.append([
          epsilon, (10**maxElementExponent),
          expectedNumBuffersValues[i][j],
          expectedBufferSizeValues[i][j]
      ])
      j += 1
    i += 1
  return test_data


class ApproximateQuantilesBufferTest(unittest.TestCase):
  """ Approximate Quantiles Buffer Tests to ensure we are calculating the
  optimal buffers."""
  @parameterized.expand(_build_quantilebuffer_test_data)
  def test_efficiency(
      self, epsilon, maxInputSize, expectedNumBuffers, expectedBufferSize):
    """
    Verify the buffers are efficiently calculated according to the reference
    table values.
    """

    combine_fn = ApproximateQuantilesCombineFn.create(
        num_quantiles=10, max_num_elements=maxInputSize, epsilon=epsilon)
    self.assertEqual(
        expectedNumBuffers, combine_fn._num_buffers, "Number of buffers")
    self.assertEqual(expectedBufferSize, combine_fn._buffer_size, "Buffer size")

  @parameterized.expand(_build_quantilebuffer_test_data)
  def test_correctness(self, epsilon, maxInputSize, *args):
    """
    Verify that buffers are correct according to the two constraint equations.
    """
    combine_fn = ApproximateQuantilesCombineFn.create(
        num_quantiles=10, max_num_elements=maxInputSize, epsilon=epsilon)
    b = combine_fn._num_buffers
    k = combine_fn._buffer_size
    n = maxInputSize
    self.assertLessEqual((b - 2) * (1 << (b - 2)) + 0.5, (epsilon * n),
                         '(b-2)2^(b-2) + 1/2 <= eN')
    self.assertGreaterEqual((k * 2)**(b - 1), n, 'k2^(b-1) >= N')


if __name__ == '__main__':
  unittest.main()
