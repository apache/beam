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

"""Unit tests for batched type converters."""

import contextlib
import random
import typing
import unittest

import numpy as np
from parameterized import parameterized
from parameterized import parameterized_class

from apache_beam.typehints import typehints
from apache_beam.typehints.batch import BatchConverter
from apache_beam.typehints.batch import N
from apache_beam.typehints.batch import NumpyArray


@parameterized_class([
    {
        'batch_typehint': np.ndarray,
        'element_typehint': np.int32,
        'batch': np.array(range(100), np.int32)
    },
    {
        'batch_typehint': NumpyArray[np.int64, (N, 10)],
        'element_typehint': NumpyArray[np.int64, (10, )],
        'batch': np.array([list(range(i, i + 10)) for i in range(100)],
                          np.int64),
    },
    {
        'batch_typehint': typehints.List[str],
        'element_typehint': str,
        'batch': ["foo" * (i % 5) + str(i) for i in range(1000)],
    },
    {
        'batch_typehint': typing.List[str],
        'element_typehint': str,
        'batch': ["foo" * (i % 5) + str(i) for i in range(1000)],
    },
])
class BatchConverterTest(unittest.TestCase):
  def create_batch_converter(self):
    return BatchConverter.from_typehints(
        element_type=self.element_typehint, batch_type=self.batch_typehint)

  def setUp(self):
    self.converter = self.create_batch_converter()
    self.normalized_batch_typehint = typehints.normalize(self.batch_typehint)
    self.normalized_element_typehint = typehints.normalize(
        self.element_typehint)

  def equality_check(self, left, right):
    if isinstance(left, np.ndarray) and isinstance(right, np.ndarray):
      return np.array_equal(left, right)
    else:
      return left == right

  def test_typehint_validates(self):
    typehints.validate_composite_type_param(self.batch_typehint, '')
    typehints.validate_composite_type_param(self.element_typehint, '')

  def test_type_check(self):
    typehints.check_constraint(self.normalized_batch_typehint, self.batch)

  def test_type_check_element(self):
    for element in self.converter.explode_batch(self.batch):
      typehints.check_constraint(self.normalized_element_typehint, element)

  def test_explode_rebatch(self):
    exploded = list(self.converter.explode_batch(self.batch))
    rebatched = self.converter.produce_batch(exploded)

    typehints.check_constraint(self.normalized_batch_typehint, rebatched)
    self.assertTrue(self.equality_check(self.batch, rebatched))

  def test_estimate_byte_size_implemented(self):
    # Just verify that we can call byte size
    self.assertGreater(self.converter.estimate_byte_size(self.batch), 0)

  @parameterized.expand([
      (2, ),
      (3, ),
      (10, ),
  ])
  def test_estimate_byte_size_partitions(self, N):
    elements = list(self.converter.explode_batch(self.batch))

    # Split elements into N contiguous partitions, create a batch out of each
    batches = [
        self.converter.produce_batch(
            elements[len(elements) * i // N:len(elements) * (i + 1) // N])
        for i in range(N)
    ]

    # Some estimate_byte_size implementations use random samples,
    # set a seed temporarily to make this test deterministic
    with temp_seed(12345):
      partitioned_size_estimate = sum(
          self.converter.estimate_byte_size(batch) for batch in batches)
      size_estimate = self.converter.estimate_byte_size(self.batch)

    # Assert that size estimate for partitions is within 10% of size estimate
    # for the whole partition.
    self.assertLessEqual(
        abs(partitioned_size_estimate / size_estimate - 1), 0.1)

  @parameterized.expand([
      (2, ),
      (3, ),
      (10, ),
  ])
  def test_combine_batches(self, N):
    elements = list(self.converter.explode_batch(self.batch))

    # Split elements into N contiguous partitions, create a batch out of each
    batches = [
        self.converter.produce_batch(
            elements[len(elements) * i // N:len(elements) * (i + 1) // N])
        for i in range(N)
    ]

    # Combine the batches, output should be equivalent to the original batch
    combined = self.converter.combine_batches(batches)

    self.assertTrue(self.equality_check(self.batch, combined))

  def test_equals(self):
    self.assertTrue(self.converter == self.create_batch_converter())
    self.assertTrue(self.create_batch_converter() == self.converter)

  def test_hash(self):
    self.assertEqual(hash(self.create_batch_converter()), hash(self.converter))


@contextlib.contextmanager
def temp_seed(seed):
  state = random.getstate()
  random.seed(seed)
  try:
    yield
  finally:
    random.setstate(state)


if __name__ == '__main__':
  unittest.main()
