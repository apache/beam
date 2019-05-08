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

from __future__ import absolute_import

import unittest
import random
import math
from collections import defaultdict
import numpy as np

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class ApproximateUniqueTest(unittest.TestCase):
  """Unit tests for ApproximateUniqueGlobally and ApproximateUniquePerKey."""

  def _get_error(self, est_count, actual_count):
    # return estimation error.
    return abs(est_count - actual_count) * 1.0 / actual_count

  def _assert_error(self, est_count, actual_count, max_error, p=False):
    # return whether estimation error is smaller or equal to max error.
    est_error = self._get_error(est_count, actual_count)
    if p:
      print est_count, actual_count, max_error, est_error
    return [est_error <= max_error]

  def test_approximate_unique_global_by_invalid_size(self):
    # test if the transformation throws an error as expected with an invalid
    # small input size (< 16).
    sample_size = 10
    input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      (pipeline
       | 'create' >> beam.Create(input)
       | 'get_estimate'
       >> beam.ApproximateUniqueGlobally(size=sample_size))
      pipeline.run()

    expected_msg = beam.ApproximateUniqueGlobally._INPUT_SIZE_ERR_MSG % (
      sample_size)

    assert expected_msg == e.exception.args[0]

  def test_approximate_unique_global_by_invalid_type_size(self):
    # test if the transformation throws an error as expected with an invalid
    # type of input size (not int).
    sample_size = 100.0
    input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      (pipeline
       | 'create' >> beam.Create(input)
       | 'get_estimate'
       >> beam.ApproximateUniqueGlobally(size=sample_size))
      pipeline.run()

    expected_msg = beam.ApproximateUniqueGlobally._INPUT_SIZE_ERR_MSG % (
      sample_size)

    assert expected_msg == e.exception.args[0]

  def test_approximate_unique_global_by_invalid_small_error(self):
    # test if the transformation throws an error as expected with an invalid
    # small input error (< 0.01).
    est_error = 0.0
    input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      (pipeline
       | 'create' >> beam.Create(input)
       | 'get_estimate'
       >> beam.ApproximateUniqueGlobally(error=est_error))
      pipeline.run()

    expected_msg = beam.ApproximateUniqueGlobally._INPUT_ERROR_ERR_MSG % (
      est_error)

    assert expected_msg == e.exception.args[0]

  def test_approximate_unique_global_by_invalid_big_error(self):
    # test if the transformation throws an error as expected with an invalid
    # big input error (> 0.50).
    est_error = 0.6
    input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      (pipeline
       | 'create' >> beam.Create(input)
       | 'get_estimate'
       >> beam.ApproximateUniqueGlobally(error=est_error))
      pipeline.run()

    expected_msg = beam.ApproximateUniqueGlobally._INPUT_ERROR_ERR_MSG % (
      est_error)

    assert expected_msg == e.exception.args[0]

  def test_approximate_unique_global_by_invalid_no_input(self):
    # test if the transformation throws an error as expected with no input.
    input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      (pipeline
       | 'create' >> beam.Create(input)
       | 'get_estimate'
       >> beam.ApproximateUniqueGlobally())
      pipeline.run()

    expected_msg = beam.ApproximateUniqueGlobally._NO_VALUE_ERR_MSG
    assert expected_msg == e.exception.args[0]

  def test_approximate_unique_global_by_invalid_both_input(self):
    # test if the transformation throws an error as expected with multi input.
    input = [random.randint(0, 1000) for _ in range(100)]
    est_error = 0.2
    sample_size = 30

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      (pipeline
       | 'create' >> beam.Create(input)
       | 'get_estimate'
       >> beam.ApproximateUniqueGlobally(size=sample_size, error=est_error))
      pipeline.run()

    expected_msg = beam.ApproximateUniqueGlobally._MULTI_VALUE_ERR_MSG % (
    sample_size, est_error)

    assert expected_msg == e.exception.args[0]

  def test_sampleSizeFromEstimationError(self):
    # test if get correct sample size from input error.
    assert 16 == beam.ApproximateUniqueGlobally.sampleSizeFromEstimationError(
      0.5)
    assert 25 == beam.ApproximateUniqueGlobally.sampleSizeFromEstimationError(
      0.4)
    assert 100 == beam.ApproximateUniqueGlobally.sampleSizeFromEstimationError(
      0.2)
    assert 400 == beam.ApproximateUniqueGlobally.sampleSizeFromEstimationError(
      0.1)
    assert 1600 == beam.ApproximateUniqueGlobally.sampleSizeFromEstimationError(
      0.05)
    assert 40000 == beam.ApproximateUniqueGlobally.sampleSizeFromEstimationError(
      0.01)

  def test_approximate_unique_global_by_sample_size(self):
    # test if estimation error with a given sample size is not greater than
    # expected max error (sample size = 50% of population).
    sample_size = 50
    input = [random.randint(0, 1000) for _ in range(100)]
    # input = [8, 19, 30, 38, 41, 45, 58, 59, 64, 74, 82, 82, 101, 113, 117, 123, 143, 148, 149, 169, 177, 256, 259, 266, 288, 296, 296, 300, 300, 337, 343, 347, 348, 365, 373, 373, 373, 377, 382, 423, 423, 430, 432, 442, 477, 479, 484, 486, 512, 523, 524, 542, 568, 570, 597, 600, 614, 618, 626, 629, 630, 651, 661, 684, 685, 690, 692, 693, 693, 705, 710, 715, 724, 743, 757, 767, 782, 789, 795, 802, 804, 807, 818, 843, 844, 859, 863, 875, 879, 881, 894, 911, 912, 920, 928, 931, 946, 964, 979, 980]
    actual_count = len(set(input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniqueGlobally(size=sample_size)
              | 'compare'
              >> beam.ParDo(lambda x: self._assert_error(
            x, actual_count, 2 / math.sqrt(sample_size))))

    assert_that(result, equal_to([True]),
                label='assert:global_by_size')
    pipeline.run()

  def test_approximate_unique_global_by_sample_size_with_duplicates(self):
    # test if estimation error with a given sample size is not greater than
    # expected max error with duplicated input.
    sample_size = 30
    input = [10] * 50 + [20] * 50
    actual_count = len(set(input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniqueGlobally(size=sample_size)
              | 'compare'
              >> beam.ParDo(lambda x: self._assert_error(
            x, actual_count, 2 / math.sqrt(sample_size))))

    assert_that(result, equal_to([True]),
                label='assert:global_by_size_with_duplicates')
    pipeline.run()

  def test_approximate_unique_global_by_sample_size_with_small_population(self):
    # test if estimation is exactly same to actual value when sample size is
    # not smaller than population size (sample size > 100% of population).
    sample_size = 31
    input = [random.randint(0, 1000) for _ in range(30)]
    actual_count = len(set(input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniqueGlobally(size=sample_size))

    assert_that(result, equal_to([actual_count]),
                label='assert:global_by_sample_size_with_small_population')
    pipeline.run()

  def test_approximate_unique_global_by__sample_size_with_big_population(self):
    # test if estimation error is smaller than expected max error with a small
    # sample and a big population (sample size = 0.03% of population).
    sample_size = 30
    input = [random.randint(0, 1000) for _ in range(100000)]
    actual_count = len(set(input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniqueGlobally(size=sample_size)
              | 'compare'
              >> beam.ParDo(lambda x: self._assert_error(
            x, actual_count, 2 / math.sqrt(sample_size))))

    assert_that(result, equal_to([True]),
                label='assert:global_by_sample_size_with_big_population')
    pipeline.run()

  def test_approximate_unique_global_by_error(self):
    # test if estimation error from input error is not greater than input error.
    est_error = 0.3
    input = [random.randint(0, 1000) for _ in range(100)]
    actual_count = len(set(input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniqueGlobally(error=est_error)
              | 'compare'
              >> beam.ParDo(
            lambda x: self._assert_error(x, actual_count, est_error))
              )

    assert_that(result, equal_to([True]),
                label='assert:global_by_error')
    pipeline.run()

  def test_approximate_unique_global_by_error_with_samll_population(self):
    # test if estimation error from input error of a small dataset is not
    # greater than input error. Sample size is always not smaller than 16, so
    # when population size is smaller than 16, estimation should be exactly
    # same to actual value.
    est_error = 0.01
    input = [random.randint(0, 1000) for _ in range(15)]
    actual_count = len(set(input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniqueGlobally(error=est_error))

    assert_that(result, equal_to([actual_count]),
                label='assert:global_by_error_with_samll_population')
    pipeline.run()

  def test_approximate_unique_global_by_error_with_big_population(self):
    # test if estimation error from input error is with in expected range with
    # a big population.
    est_error = 0.3
    input = [random.randint(0, 1000) for _ in range(100000)]
    actual_count = len(set(input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniqueGlobally(error=est_error)
              | 'compare'
              >> beam.ParDo(
            lambda x: self._assert_error(x, actual_count, est_error))
              )

    assert_that(result, equal_to([True]),
                label='assert:global_by_error_with_big_population')
    pipeline.run()

  def test_approximate_unique_perkey_by_size(self):
    # test if estimation error per key from sample size is in the expected range.
    sample_size = 50
    number_of_keys = 10
    input = [(random.randint(1, number_of_keys), random.randint(0, 1000))
                    for _ in range(100)]
    actual_count = defaultdict(set)
    for (x, y) in input:
      actual_count[x].add(y)

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniquePerKey(size=sample_size)
              | 'compare'
              >> beam.ParDo(lambda (k, v): self._assert_error(
            v, len(actual_count[k]), 2 / math.sqrt(sample_size))))

    assert_that(result, equal_to([True] * number_of_keys),
                label='assert:perkey_by_size')
    pipeline.run()

  def test_approximate_unique_perkey_by_error(self):
    # test if estimation error per key from input err is in the expected range.
    est_error = 0.01
    number_of_keys = 10
    input = [(random.randint(1, number_of_keys), random.randint(0, 1000))
                    for _ in range(100)]
    actual_count = defaultdict(set)
    for (x, y) in input:
      actual_count[x].add(y)

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniquePerKey(error=est_error)
              | 'compare'
              >> beam.ParDo(lambda (k, v): self._assert_error(
            v, len(actual_count[k]), est_error)))

    assert_that(result, equal_to([True] * number_of_keys),
                label='assert:perkey_by_error')
    pipeline.run()

  def test_approximate_unique_globally_by_error_with_skewed_data(self):
    # test if estimation error is within the expected range with skewed data.
    est_error = 0.01

    # generate skewed dataset
    values = range(0, 200)
    probs = [1.0 / 200] * 200

    for idx, prob in enumerate(probs):
      if idx > 3 and idx < 20:
        probs[idx] = probs[idx] * (1 + math.log(idx + 1))
      if idx > 20 and idx < 40:
        probs[idx] = probs[idx] * (1 + math.log((40 - idx) + 1))

    probs = [p / sum(probs) for p in probs]
    input = np.random.choice(values, 1000, p=probs)
    actual_count = len(set(input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(input)
              | 'get_estimate'
              >> beam.ApproximateUniqueGlobally(error=est_error)
              | 'compare'
              >> beam.ParDo(lambda x: self._assert_error(
            x, actual_count, est_error)))

    assert_that(result, equal_to([True]),
                label='assert:globally_by_error_with_skewed_data')
    pipeline.run()


if __name__ == '__main__':
  unittest.main()
