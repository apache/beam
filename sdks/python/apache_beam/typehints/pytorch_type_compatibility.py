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

from typing import Optional

import torch
from apache_beam.typehints import typehints
from apache_beam.typehints.batch import BatchConverter
from apache_beam.typehints.batch import N


class PytorchBatchConverter(BatchConverter):
  def __init__(
      self,
      batch_type,
      element_type,
      dtype,
      element_shape=(),
      partition_dimension=0):
    super().__init__(batch_type, element_type)
    self.dtype = dtype
    self.element_shape = element_shape
    self.partition_dimension = partition_dimension

  @staticmethod
  @BatchConverter.register
  def from_typehints(element_type,
                     batch_type) -> Optional['PytorchBatchConverter']:
    if not isinstance(element_type, PytorchTypeHint.PytorchTypeConstraint):
      try:
        element_type = PytorchTensor[element_type, ()]
      except TypeError:
        # TODO: Is there a better way to detect if element_type is a dtype?
        return None

    if not isinstance(batch_type, PytorchTypeHint.PytorchTypeConstraint):
      if not batch_type == torch.Tensor:
        # TODO: Include explanation for mismatch?
        return None
      batch_type = PytorchTensor[element_type.dtype, (N, )]

    if not batch_type.dtype == element_type.dtype:
      return None
    batch_shape = list(batch_type.shape)
    partition_dimension = batch_shape.index(N)
    batch_shape.pop(partition_dimension)
    if not tuple(batch_shape) == element_type.shape:
      return None

    return PytorchBatchConverter(
        batch_type,
        element_type,
        batch_type.dtype,
        element_type.shape,
        partition_dimension)

  def produce_batch(self, elements):
    return torch.stack(elements, dim=self.partition_dimension)

  def explode_batch(self, batch):
    """Convert an instance of B to Generator[E]."""
    yield from torch.swapaxes(batch, self.partition_dimension, 0)

  def combine_batches(self, batches):
    return torch.cat(batches, dim=self.partition_dimension)

  def get_length(self, batch):
    return batch.size(dim=self.partition_dimension)

  def estimate_byte_size(self, batch):
    return batch.nelement() * batch.element_size()


class PytorchTypeHint():
  class PytorchTypeConstraint(typehints.TypeConstraint):
    def __init__(self, dtype, shape=()):
      self.dtype = dtype
      self.shape = shape

    def type_check(self, batch):
      if not isinstance(batch, torch.Tensor):
        raise TypeError(f"Batch {batch!r} is not an instance of torch.Tensor")
      if not batch.dtype == self.dtype:
        raise TypeError(
            f"Batch {batch!r} does not have expected dtype: {self.dtype!r}")

      for dim in range(len(self.shape)):
        if not self.shape[dim] == N and not batch.shape[dim] == self.shape[dim]:
          raise TypeError(
              f"Batch {batch!r} does not have expected shape: {self.shape!r}")

    def _consistent_with_check_(self, sub):
      # TODO Check sub against batch type, and element type
      return True

    def __key(self):
      return (self.dtype, self.shape)

    def __eq__(self, other) -> bool:
      if isinstance(other, PytorchTypeHint.PytorchTypeConstraint):
        return self.__key() == other.__key()

      return NotImplemented

    def __hash__(self) -> int:
      return hash(self.__key())

    def __repr__(self):
      if self.shape == (N, ):
        return f'PytorchTensor[{self.dtype!r}]'
      else:
        return f'PytorchTensor[{self.dtype!r}, {self.shape!r}]'

  def __getitem__(self, value):
    if isinstance(value, tuple):
      if len(value) == 2:
        dtype, shape = value
        return self.PytorchTypeConstraint(dtype, shape=shape)
      else:
        raise ValueError
    else:
      dtype = value
      return self.PytorchTypeConstraint(dtype, shape=(N, ))


PytorchTensor = PytorchTypeHint()
