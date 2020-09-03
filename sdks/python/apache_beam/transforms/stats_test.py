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
import unittest
from builtins import range
from collections import defaultdict

import hamcrest as hc
from parameterized import parameterized

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


class ApproximateUniqueTest(unittest.TestCase):
  """Unit tests for ApproximateUnique.Globally, ApproximateUnique.PerKey,
  and ApproximateUniqueCombineFn.
  """
  def test_approximate_unique_global_by_invalid_size(self):
    # test if the transformation throws an error as expected with an invalid
    # small input size (< 16).
    sample_size = 10
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      with TestPipeline() as pipeline:
        _ = (
            pipeline
            | 'create' >> beam.Create(test_input)
            |
            'get_estimate' >> beam.ApproximateUnique.Globally(size=sample_size))

    expected_msg = beam.ApproximateUnique._INPUT_SIZE_ERR_MSG % (sample_size)

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_type_size(self):
    # test if the transformation throws an error as expected with an invalid
    # type of input size (not int).
    sample_size = 100.0
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      with TestPipeline() as pipeline:
        _ = (
            pipeline
            | 'create' >> beam.Create(test_input)
            |
            'get_estimate' >> beam.ApproximateUnique.Globally(size=sample_size))

    expected_msg = beam.ApproximateUnique._INPUT_SIZE_ERR_MSG % (sample_size)

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_small_error(self):
    # test if the transformation throws an error as expected with an invalid
    # small input error (< 0.01).
    est_err = 0.0
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      with TestPipeline() as pipeline:
        _ = (
            pipeline
            | 'create' >> beam.Create(test_input)
            | 'get_estimate' >> beam.ApproximateUnique.Globally(error=est_err))

    expected_msg = beam.ApproximateUnique._INPUT_ERROR_ERR_MSG % (est_err)

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_big_error(self):
    # test if the transformation throws an error as expected with an invalid
    # big input error (> 0.50).
    est_err = 0.6
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      with TestPipeline() as pipeline:
        _ = (
            pipeline
            | 'create' >> beam.Create(test_input)
            | 'get_estimate' >> beam.ApproximateUnique.Globally(error=est_err))

    expected_msg = beam.ApproximateUnique._INPUT_ERROR_ERR_MSG % (est_err)

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_no_input(self):
    # test if the transformation throws an error as expected with no input.
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      with TestPipeline() as pipeline:
        _ = (
            pipeline
            | 'create' >> beam.Create(test_input)
            | 'get_estimate' >> beam.ApproximateUnique.Globally())

    expected_msg = beam.ApproximateUnique._NO_VALUE_ERR_MSG
    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_both_input(self):
    # test if the transformation throws an error as expected with multi input.
    test_input = [random.randint(0, 1000) for _ in range(100)]
    est_err = 0.2
    sample_size = 30

    with self.assertRaises(ValueError) as e:
      with TestPipeline() as pipeline:
        _ = (
            pipeline
            | 'create' >> beam.Create(test_input)
            | 'get_estimate' >> beam.ApproximateUnique.Globally(
                size=sample_size, error=est_err))

    expected_msg = beam.ApproximateUnique._MULTI_VALUE_ERR_MSG % (
        sample_size, est_err)

    assert e.exception.args[0] == expected_msg

  def test_get_sample_size_from_est_error(self):
    # test if get correct sample size from input error.
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.5) == 16
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.4) == 25
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.2) == 100
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.1) == 400
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.05) == 1600
    assert beam.ApproximateUnique._get_sample_size_from_est_error(0.01) == 40000

  def test_approximate_unique_global_by_sample_size(self):
    # test if estimation error with a given sample size is not greater than
    # expected max error.
    sample_size = 16
    max_err = 2 / math.sqrt(sample_size)
    test_input = [
        4,
        34,
        29,
        46,
        80,
        66,
        51,
        81,
        31,
        9,
        26,
        36,
        10,
        41,
        90,
        35,
        33,
        19,
        88,
        86,
        28,
        93,
        38,
        76,
        15,
        87,
        12,
        39,
        84,
        13,
        32,
        49,
        65,
        100,
        16,
        27,
        23,
        30,
        96,
        54
    ]

    actual_count = len(set(test_input))

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.Globally(size=sample_size)
          | 'compare' >> beam.FlatMap(
              lambda x: [abs(x - actual_count) * 1.0 / actual_count <= max_err])
      )

      assert_that(result, equal_to([True]), label='assert:global_by_size')

  def test_approximate_unique_global_by_sample_size_with_duplicates(self):
    # test if estimation error with a given sample size is not greater than
    # expected max error with duplicated input.
    sample_size = 30
    max_err = 2 / math.sqrt(sample_size)
    test_input = [10] * 50 + [20] * 50
    actual_count = len(set(test_input))

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.Globally(size=sample_size)
          | 'compare' >> beam.FlatMap(
              lambda x: [abs(x - actual_count) * 1.0 / actual_count <= max_err])
      )

      assert_that(
          result,
          equal_to([True]),
          label='assert:global_by_size_with_duplicates')

  def test_approximate_unique_global_by_sample_size_with_small_population(self):
    # test if estimation is exactly same to actual value when sample size is
    # not smaller than population size (sample size > 100% of population).
    sample_size = 31
    test_input = [
        144,
        160,
        229,
        923,
        390,
        756,
        674,
        769,
        145,
        888,
        809,
        159,
        222,
        101,
        943,
        901,
        876,
        194,
        232,
        631,
        221,
        829,
        965,
        729,
        35,
        33,
        115,
        894,
        827,
        364
    ]
    actual_count = len(set(test_input))

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.Globally(size=sample_size))

      assert_that(
          result,
          equal_to([actual_count]),
          label='assert:global_by_sample_size_with_small_population')

  def test_approximate_unique_global_by_error(self):
    # test if estimation error from input error is not greater than input error.
    est_err = 0.3
    test_input = [
        291,
        371,
        271,
        126,
        762,
        391,
        222,
        565,
        428,
        786,
        801,
        867,
        337,
        690,
        261,
        436,
        311,
        568,
        946,
        722,
        973,
        386,
        506,
        546,
        991,
        450,
        226,
        889,
        514,
        693
    ]
    actual_count = len(set(test_input))

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.Globally(error=est_err)
          | 'compare' >> beam.FlatMap(
              lambda x: [abs(x - actual_count) * 1.0 / actual_count <= est_err])
      )

      assert_that(result, equal_to([True]), label='assert:global_by_error')

  def test_approximate_unique_global_by_error_with_small_population(self):
    # test if estimation error from input error of a small dataset is not
    # greater than input error. Sample size is always not smaller than 16, so
    # when population size is smaller than 16, estimation should be exactly
    # same to actual value.
    est_err = 0.01
    test_input = [
        585,
        104,
        613,
        503,
        658,
        640,
        118,
        492,
        189,
        798,
        756,
        755,
        839,
        79,
        393
    ]
    actual_count = len(set(test_input))

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.Globally(error=est_err))

      assert_that(
          result,
          equal_to([actual_count]),
          label='assert:global_by_error_with_small_population')

  def test_approximate_unique_perkey_by_size(self):
    # test if est error per key from sample size is in a expected range.
    sample_size = 20
    max_err = 2 / math.sqrt(sample_size)
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
          | 'get_estimate' >> beam.ApproximateUnique.PerKey(size=sample_size)
          | 'compare' >> beam.FlatMap(
              lambda x: [
                  abs(x[1] - len(actual_count_dict[x[0]])) * 1.0 / len(
                      actual_count_dict[x[0]]) <= max_err
              ]))

      assert_that(
          result,
          equal_to([True] * len(actual_count_dict)),
          label='assert:perkey_by_size')

  def test_approximate_unique_perkey_by_error(self):
    # test if estimation error per key from input err is in the expected range.
    est_err = 0.01
    test_input = [(9, 6), (5, 5), (6, 9), (2, 4), (8, 3), (9, 0), (6, 10),
                  (8, 8), (9, 7), (2, 0), (9, 2), (1, 3), (4, 0), (7, 6),
                  (10, 6), (4, 7), (5, 8), (7, 2), (7, 10), (5, 10)]
    actual_count_dict = defaultdict(set)
    for (x, y) in test_input:
      actual_count_dict[x].add(y)

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.PerKey(error=est_err)
          | 'compare' >> beam.FlatMap(
              lambda x: [
                  abs(x[1] - len(actual_count_dict[x[0]])) * 1.0 / len(
                      actual_count_dict[x[0]]) <= est_err
              ]))

      assert_that(
          result,
          equal_to([True] * len(actual_count_dict)),
          label='assert:perkey_by_error')

  def test_approximate_unique_globally_by_error_with_skewed_data(self):
    # test if estimation error is within the expected range with skewed data.
    est_err = 0.01
    test_input = [
        19,
        21,
        32,
        29,
        5,
        31,
        52,
        50,
        59,
        80,
        7,
        3,
        34,
        19,
        13,
        6,
        55,
        1,
        13,
        90,
        4,
        18,
        52,
        33,
        0,
        77,
        21,
        26,
        5,
        18
    ]
    actual_count = len(set(test_input))

    with TestPipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(test_input)
          | 'get_estimate' >> beam.ApproximateUnique.Globally(error=est_err)
          | 'compare' >> beam.FlatMap(
              lambda x: [abs(x - actual_count) * 1.0 / actual_count <= est_err])
      )

      assert_that(
          result,
          equal_to([True]),
          label='assert:globally_by_error_with_skewed_data')

  def test_approximate_unique_combine_fn_by_nondeterministic_coder(self):
    # test if the combiner throws an error with a nondeterministic coder.
    sample_size = 30
    coder = coders.Base64PickleCoder()

    with self.assertRaises(ValueError) as e:
      _ = ApproximateUniqueCombineFn(sample_size, coder)

    self.assertRegex(
        e.exception.args[0],
        'The key coder "Base64PickleCoder" '
        'for ApproximateUniqueCombineFn is not deterministic.')

  def test_approximate_unique_combine_fn_by_wrong_coder(self):
    # test if the combiner throws an error with a wrong coder.
    test_input = 'a'
    sample_size = 30
    coder = coders.FloatCoder()
    combine_fn = ApproximateUniqueCombineFn(sample_size, coder)
    accumulator = combine_fn.create_accumulator()
    with self.assertRaises(RuntimeError) as e:
      accumulator = combine_fn.add_input(accumulator, test_input)

    expected_msg = 'Runtime exception: required argument is not a float'
    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_combine_fn_adds_values_correctly(self):
    test_input = [['a', 'b', 'c'], ['b', 'd']]
    sample_size = 30
    coder = coders.StrUtf8Coder()
    combine_fn = ApproximateUniqueCombineFn(sample_size, coder)
    accumulator = combine_fn.create_accumulator()
    for batch in test_input:
      for value in batch:
        accumulator = combine_fn.add_input(accumulator, value)

    result = 4
    self.assertEqual(combine_fn.extract_output(accumulator), result)

  def test_approximate_unique_combine_fn_merges_values_correctly(self):
    test_input = [['a', 'b', 'c'], ['b', 'd']]
    sample_size = 30
    coder = coders.StrUtf8Coder()
    combine_fn = ApproximateUniqueCombineFn(sample_size, coder)

    accumulators = []
    for batch in test_input:
      accumulator = combine_fn.create_accumulator()
      for value in batch:
        combine_fn.add_input(accumulator, value)
      accumulators.append(accumulator)
    merged_accumulator = combine_fn.merge_accumulators(accumulators)

    result = 4
    self.assertEqual(combine_fn.extract_output(merged_accumulator), result)


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
