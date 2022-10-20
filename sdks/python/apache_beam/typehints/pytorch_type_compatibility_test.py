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

"""Unit tests for pytorch_type_compabitility."""

import unittest

import pytest
from parameterized import parameterized
from parameterized import parameterized_class

from apache_beam.typehints import typehints
from apache_beam.typehints.batch import BatchConverter
from apache_beam.typehints.batch import N

# Protect against environments where pytorch library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import torch
  from apache_beam.typehints.pytorch_type_compatibility import PytorchTensor
except ImportError:
  raise unittest.SkipTest('PyTorch dependencies are not installed')


@parameterized_class([
    {
        'batch_typehint': torch.Tensor,
        'element_typehint': PytorchTensor[torch.int32, ()],
        'batch': torch.tensor(range(100), dtype=torch.int32)
    },
    {
        'batch_typehint': PytorchTensor[torch.int64, (N, 10)],
        'element_typehint': PytorchTensor[torch.int64, (10, )],
        'batch': torch.tensor([list(range(i, i + 10)) for i in range(100)],
                              dtype=torch.int64),
    },
])
@pytest.mark.uses_pytorch
class PytorchBatchConverterTest(unittest.TestCase):
  def create_batch_converter(self):
    return BatchConverter.from_typehints(
        element_type=self.element_typehint, batch_type=self.batch_typehint)

  def setUp(self):
    self.converter = self.create_batch_converter()
    self.normalized_batch_typehint = typehints.normalize(self.batch_typehint)
    self.normalized_element_typehint = typehints.normalize(
        self.element_typehint)

  def equality_check(self, left, right):
    if isinstance(left, torch.Tensor):
      self.assertTrue(torch.equal(left, right))
    else:
      raise TypeError(f"Encountered unexpected type, left is a {type(left)!r}")

  def test_typehint_validates(self):
    typehints.validate_composite_type_param(self.batch_typehint, '')
    typehints.validate_composite_type_param(self.element_typehint, '')

  def test_type_check_batch(self):
    typehints.check_constraint(self.normalized_batch_typehint, self.batch)

  def test_type_check_element(self):
    for element in self.converter.explode_batch(self.batch):
      typehints.check_constraint(self.normalized_element_typehint, element)

  def test_explode_rebatch(self):
    exploded = list(self.converter.explode_batch(self.batch))
    rebatched = self.converter.produce_batch(exploded)

    typehints.check_constraint(self.normalized_batch_typehint, rebatched)
    self.equality_check(self.batch, rebatched)

  def _split_batch_into_n_partitions(self, N):
    elements = list(self.converter.explode_batch(self.batch))

    # Split elements into N contiguous partitions
    element_batches = [
        elements[len(elements) * i // N:len(elements) * (i + 1) // N]
        for i in range(N)
    ]

    lengths = [len(element_batch) for element_batch in element_batches]
    batches = [
        self.converter.produce_batch(element_batch)
        for element_batch in element_batches
    ]

    return batches, lengths

  @parameterized.expand([
      (2, ),
      (3, ),
      (10, ),
  ])
  def test_combine_batches(self, N):
    batches, _ = self._split_batch_into_n_partitions(N)

    # Combine the batches, output should be equivalent to the original batch
    combined = self.converter.combine_batches(batches)

    self.equality_check(self.batch, combined)

  @parameterized.expand([
      (2, ),
      (3, ),
      (10, ),
  ])
  def test_get_length(self, N):
    batches, lengths = self._split_batch_into_n_partitions(N)

    for batch, expected_length in zip(batches, lengths):
      self.assertEqual(self.converter.get_length(batch), expected_length)

  def test_equals(self):
    self.assertTrue(self.converter == self.create_batch_converter())
    self.assertTrue(self.create_batch_converter() == self.converter)

  def test_hash(self):
    self.assertEqual(hash(self.create_batch_converter()), hash(self.converter))


if __name__ == '__main__':
  unittest.main()
