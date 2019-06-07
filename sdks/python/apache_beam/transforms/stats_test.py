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
from __future__ import division

import math
import random
import unittest
from collections import defaultdict

import numpy as np

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class ApproximateUniqueTest(unittest.TestCase):
  """Unit tests for ApproximateUnique.Globally and ApproximateUnique.PerKey."""

  def test_approximate_unique_global_by_invalid_size(self):
    # test if the transformation throws an error as expected with an invalid
    # small input size (< 16).
    sample_size = 10
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      _ = (pipeline
           | 'create'
           >> beam.Create(test_input)
           | 'get_estimate'
           >> beam.ApproximateUnique.Globally(size=sample_size))
      pipeline.run()

    expected_msg = beam.ApproximateUnique._INPUT_SIZE_ERR_MSG % (sample_size)

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_type_size(self):
    # test if the transformation throws an error as expected with an invalid
    # type of input size (not int).
    sample_size = 100.0
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      _ = (pipeline
           | 'create' >> beam.Create(test_input)
           | 'get_estimate'
           >> beam.ApproximateUnique.Globally(size=sample_size))
      pipeline.run()

    expected_msg = beam.ApproximateUnique._INPUT_SIZE_ERR_MSG % (sample_size)

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_small_error(self):
    # test if the transformation throws an error as expected with an invalid
    # small input error (< 0.01).
    est_err = 0.0
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      _ = (pipeline
           | 'create' >> beam.Create(test_input)
           | 'get_estimate'
           >> beam.ApproximateUnique.Globally(error=est_err))
      pipeline.run()

    expected_msg = beam.ApproximateUnique._INPUT_ERROR_ERR_MSG % (est_err)

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_big_error(self):
    # test if the transformation throws an error as expected with an invalid
    # big input error (> 0.50).
    est_err = 0.6
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      _ = (pipeline
           | 'create' >> beam.Create(test_input)
           | 'get_estimate'
           >> beam.ApproximateUnique.Globally(error=est_err))
      pipeline.run()

    expected_msg = beam.ApproximateUnique._INPUT_ERROR_ERR_MSG % (est_err)

    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_no_input(self):
    # test if the transformation throws an error as expected with no input.
    test_input = [random.randint(0, 1000) for _ in range(100)]

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      _ = (pipeline
           | 'create' >> beam.Create(test_input)
           | 'get_estimate'
           >> beam.ApproximateUnique.Globally())
      pipeline.run()

    expected_msg = beam.ApproximateUnique._NO_VALUE_ERR_MSG
    assert e.exception.args[0] == expected_msg

  def test_approximate_unique_global_by_invalid_both_input(self):
    # test if the transformation throws an error as expected with multi input.
    test_input = [random.randint(0, 1000) for _ in range(100)]
    est_err = 0.2
    sample_size = 30

    with self.assertRaises(ValueError) as e:
      pipeline = TestPipeline()
      _ = (pipeline
           | 'create' >> beam.Create(test_input)
           | 'get_estimate'
           >> beam.ApproximateUnique.Globally(size=sample_size, error=est_err))
      pipeline.run()

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
    # expected max error (sample size = 50% of population).
    sample_size = 50
    max_err = 2 / math.sqrt(sample_size)
    random.seed(1)
    test_input = [random.randint(0, 1000) for _ in range(100)]
    actual_count = len(set(test_input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(test_input)
              | 'get_estimate'
              >> beam.ApproximateUnique.Globally(size=sample_size)
              | 'compare'
              >> beam.FlatMap(lambda x: [abs(x - actual_count) * 1.0
                                         / actual_count <= max_err]))

    assert_that(result, equal_to([True]),
                label='assert:global_by_size')
    pipeline.run()

  def test_approximate_unique_global_by_sample_size_with_duplicates(self):
    # test if estimation error with a given sample size is not greater than
    # expected max error with duplicated input.
    sample_size = 30
    max_err = 2 / math.sqrt(sample_size)
    test_input = [10] * 50 + [20] * 50
    actual_count = len(set(test_input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(test_input)
              | 'get_estimate'
              >> beam.ApproximateUnique.Globally(size=sample_size)
              | 'compare'
              >> beam.FlatMap(lambda x: [abs(x - actual_count) * 1.0
                                         / actual_count <= max_err]))

    assert_that(result, equal_to([True]),
                label='assert:global_by_size_with_duplicates')
    pipeline.run()

  def test_approximate_unique_global_by_sample_size_with_small_population(self):
    # test if estimation is exactly same to actual value when sample size is
    # not smaller than population size (sample size > 100% of population).
    sample_size = 31
    test_input = [random.randint(0, 1000) for _ in range(30)]
    actual_count = len(set(test_input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(test_input)
              | 'get_estimate'
              >> beam.ApproximateUnique.Globally(size=sample_size))

    assert_that(result, equal_to([actual_count]),
                label='assert:global_by_sample_size_with_small_population')
    pipeline.run()

  def test_approximate_unique_global_by_error(self):
    # test if estimation error from input error is not greater than input error.
    est_err = 0.3
    random.seed(1)
    test_input = [random.randint(0, 1000) for _ in range(100)]
    actual_count = len(set(test_input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(test_input)
              | 'get_estimate'
              >> beam.ApproximateUnique.Globally(error=est_err)
              | 'compare'
              >> beam.FlatMap(lambda x: [abs(x - actual_count) * 1.0
                                         / actual_count <= est_err]))

    assert_that(result, equal_to([True]), label='assert:global_by_error')
    pipeline.run()

  def test_approximate_unique_global_by_error_with_small_population(self):
    # test if estimation error from input error of a small dataset is not
    # greater than input error. Sample size is always not smaller than 16, so
    # when population size is smaller than 16, estimation should be exactly
    # same to actual value.
    est_err = 0.01
    test_input = [random.randint(0, 1000) for _ in range(15)]
    actual_count = len(set(test_input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(test_input)
              | 'get_estimate'
              >> beam.ApproximateUnique.Globally(error=est_err))

    assert_that(result, equal_to([actual_count]),
                label='assert:global_by_error_with_small_population')
    pipeline.run()

  def test_approximate_unique_perkey_by_size(self):
    # test if est error per key from sample size is in a expected range.
    sample_size = 50
    max_err = 2 / math.sqrt(sample_size)
    number_of_keys = 10
    test_input = [(random.randint(1, number_of_keys), random.randint(0, 1000))
                  for _ in range(100)]
    actual_count_dict = defaultdict(set)
    for (x, y) in test_input:
      actual_count_dict[x].add(y)

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(test_input)
              | 'get_estimate'
              >> beam.ApproximateUnique.PerKey(size=sample_size)
              | 'compare'
              >> beam.FlatMap(lambda x: [abs(x[1]
                                             - len(actual_count_dict[x[0]]))
                                         * 1.0 / len(actual_count_dict[x[0]])
                                         <= max_err]))

    assert_that(result, equal_to([True] * number_of_keys),
                label='assert:perkey_by_size')
    pipeline.run()

  def test_approximate_unique_perkey_by_error(self):
    # test if estimation error per key from input err is in the expected range.
    est_err = 0.01
    number_of_keys = 10
    test_input = [(random.randint(1, number_of_keys), random.randint(0, 1000))
                  for _ in range(100)]
    actual_count_dict = defaultdict(set)
    for (x, y) in test_input:
      actual_count_dict[x].add(y)

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(test_input)
              | 'get_estimate'
              >> beam.ApproximateUnique.PerKey(error=est_err)
              | 'compare'
              >> beam.FlatMap(lambda x: [abs(x[1]
                                             - len(actual_count_dict[x[0]]))
                                         * 1.0 / len(actual_count_dict[x[0]])
                                         <= est_err]))

    assert_that(result, equal_to([True] * number_of_keys),
                label='assert:perkey_by_error')
    pipeline.run()

  def test_approximate_unique_globally_by_error_with_skewed_data(self):
    # test if estimation error is within the expected range with skewed data.
    est_err = 0.01

    # generate skewed dataset
    values = [i for i in range(200)]
    probs = [1.0 / 200] * 200

    for idx, _ in enumerate(probs):
      if idx > 3 and idx < 20:
        probs[idx] = probs[idx] * (1 + math.log(idx + 1))
      if idx > 20 and idx < 40:
        probs[idx] = probs[idx] * (1 + math.log((40 - idx) + 1))

    probs = [p / sum(probs) for p in probs]
    test_input = np.random.choice(values, 1000, p=probs)
    actual_count = len(set(test_input))

    pipeline = TestPipeline()
    result = (pipeline
              | 'create' >> beam.Create(test_input)
              | 'get_estimate'
              >> beam.ApproximateUnique.Globally(error=est_err)
              | 'compare'
              >> beam.FlatMap(lambda x: [abs(x - actual_count) * 1.0
                                         / actual_count <= est_err]))

    assert_that(result, equal_to([True]),
                label='assert:globally_by_error_with_skewed_data')
    pipeline.run()


if __name__ == '__main__':
  unittest.main()
