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
        'batch': ["foo" * (i % 5) + str(i) for i in range(100)],
    },
    {
        'batch_typehint': typing.List[str],
        'element_typehint': str,
        'batch': ["foo" * (i % 5) + str(i) for i in range(100)],
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


if __name__ == '__main__':
  unittest.main()
