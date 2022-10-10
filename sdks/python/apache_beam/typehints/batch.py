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

"""Utilities for working with batched types in the Beam SDK.

A batched type is a type B that is logically equivalent to Sequence[E], where E
is some other type. Typically B has a different physical representation than
Sequence[E] for performance reasons.

A trivial example is B=np.array(dtype=np.int64), E=int.
"""

import random
from math import ceil
from typing import Callable
from typing import Generic
from typing import Iterator
from typing import List
from typing import Optional
from typing import Sequence
from typing import TypeVar

import numpy as np

from apache_beam import coders
from apache_beam.typehints import typehints

B = TypeVar('B')
E = TypeVar('E')

BATCH_CONVERTER_REGISTRY: List[Callable[[type, type], 'BatchConverter']] = []

__all__ = ['BatchConverter']


class BatchConverter(Generic[B, E]):
  def __init__(self, batch_type, element_type):
    self._batch_type = batch_type
    self._element_type = element_type

  def produce_batch(self, elements: Sequence[E]) -> B:
    """Convert an instance of List[E] to a single instance of B."""
    raise NotImplementedError

  def explode_batch(self, batch: B) -> Iterator[E]:
    """Convert an instance of B to Iterator[E]."""
    raise NotImplementedError

  def combine_batches(self, batches: Sequence[B]) -> B:
    raise NotImplementedError

  def get_length(self, batch: B) -> int:
    raise NotImplementedError

  def estimate_byte_size(self, batch):
    raise NotImplementedError

  @staticmethod
  def register(
      batch_converter_constructor: Callable[[type, type], 'BatchConverter']):
    BATCH_CONVERTER_REGISTRY.append(batch_converter_constructor)
    return batch_converter_constructor

  @staticmethod
  def from_typehints(*, element_type, batch_type) -> 'BatchConverter':
    element_type = typehints.normalize(element_type)
    batch_type = typehints.normalize(batch_type)
    for constructor in BATCH_CONVERTER_REGISTRY:
      result = constructor(element_type, batch_type)
      if result is not None:
        return result

    # TODO(https://github.com/apache/beam/issues/21654): Aggregate error
    # information from the failed BatchConverter matches instead of this
    # generic error.
    raise TypeError(
        f"Unable to find BatchConverter for element_type {element_type!r} and "
        f"batch_type {batch_type!r}")

  @property
  def batch_type(self):
    return self._batch_type

  @property
  def element_type(self):
    return self._element_type

  def __key(self):
    return (self._element_type, self._batch_type)

  def __eq__(self, other: 'BatchConverter') -> bool:
    if isinstance(other, BatchConverter):
      return self.__key() == other.__key()

    return NotImplemented

  def __hash__(self) -> int:
    return hash(self.__key())


class ListBatchConverter(BatchConverter):
  SAMPLE_FRACTION = 0.2
  MAX_SAMPLES = 100
  SAMPLED_BATCH_SIZE = MAX_SAMPLES / SAMPLE_FRACTION

  def __init__(self, batch_type, element_type):
    super().__init__(batch_type, element_type)
    self.element_coder = coders.registry.get_coder(element_type)

  @staticmethod
  @BatchConverter.register
  def from_typehints(element_type, batch_type):
    if (isinstance(batch_type, typehints.ListConstraint) and
        batch_type.inner_type == element_type):
      return ListBatchConverter(batch_type, element_type)
    else:
      return None

  def produce_batch(self, elements):
    return list(elements)

  def explode_batch(self, batch):
    return iter(batch)

  def combine_batches(self, batches):
    return sum(batches, [])

  def get_length(self, batch):
    return len(batch)

  def estimate_byte_size(self, batch):
    # randomly sample a fraction of the elements and use the element_coder to
    # estimate the size of each
    nsampled = (
        ceil(len(batch) * self.SAMPLE_FRACTION)
        if len(batch) < self.SAMPLED_BATCH_SIZE else self.MAX_SAMPLES)
    mean_byte_size = sum(
        self.element_coder.estimate_size(element)
        for element in random.sample(batch, nsampled)) / nsampled
    return ceil(mean_byte_size * len(batch))


N = "ARBITRARY LENGTH DIMENSION"


class NumpyBatchConverter(BatchConverter):
  def __init__(
      self,
      batch_type,
      element_type,
      dtype,
      element_shape=(),
      partition_dimension=0):
    super().__init__(batch_type, element_type)
    self.dtype = np.dtype(dtype)
    self.element_shape = element_shape
    self.partition_dimension = partition_dimension

  @staticmethod
  @BatchConverter.register
  def from_typehints(element_type,
                     batch_type) -> Optional['NumpyBatchConverter']:
    if not isinstance(element_type, NumpyTypeHint.NumpyTypeConstraint):
      try:
        element_type = NumpyArray[element_type, ()]
      except TypeError:
        # TODO: Is there a better way to detect if element_type is a dtype?
        return None

    if not isinstance(batch_type, NumpyTypeHint.NumpyTypeConstraint):
      if not batch_type == np.ndarray:
        # TODO: Include explanation for mismatch?
        return None
      batch_type = NumpyArray[element_type.dtype, (N, )]

    if not batch_type.dtype == element_type.dtype:
      return None
    batch_shape = list(batch_type.shape)
    partition_dimension = batch_shape.index(N)
    batch_shape.pop(partition_dimension)
    if not tuple(batch_shape) == element_type.shape:
      return None

    return NumpyBatchConverter(
        batch_type,
        element_type,
        batch_type.dtype,
        element_type.shape,
        partition_dimension)

  def produce_batch(self, elements):
    return np.stack(elements, axis=self.partition_dimension)

  def explode_batch(self, batch):
    """Convert an instance of B to Generator[E]."""
    yield from batch.swapaxes(self.partition_dimension, 0)

  def combine_batches(self, batches):
    return np.concatenate(batches, axis=self.partition_dimension)

  def get_length(self, batch):
    return np.size(batch, axis=self.partition_dimension)

  def estimate_byte_size(self, batch):
    return batch.nbytes


# numpy is starting to add typehints, which we should support
# https://numpy.org/doc/stable/reference/typing.html for now they don't allow
# specifying shape, seems to be coming after
# https://www.python.org/dev/peps/pep-0646/
class NumpyTypeHint():
  class NumpyTypeConstraint(typehints.TypeConstraint):
    def __init__(self, dtype, shape=()):
      self.dtype = np.dtype(dtype)
      self.shape = shape

    def type_check(self, batch):
      if not isinstance(batch, np.ndarray):
        raise TypeError(f"Batch {batch!r} is not an instance of ndarray")
      if not np.issubdtype(batch.dtype, self.dtype):
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
      if isinstance(other, NumpyTypeHint.NumpyTypeConstraint):
        return self.__key() == other.__key()

      return NotImplemented

    def __hash__(self) -> int:
      return hash(self.__key())

    def __repr__(self):
      if self.shape == (N, ):
        return f'NumpyArray[{self.dtype!r}]'
      else:
        return f'NumpyArray[{self.dtype!r}, {self.shape!r}]'

  def __getitem__(self, value):
    if isinstance(value, tuple):
      if len(value) == 2:
        dtype, shape = value
        return self.NumpyTypeConstraint(dtype, shape=shape)
      else:
        raise ValueError
    else:
      dtype = value
      return self.NumpyTypeConstraint(dtype, shape=(N, ))


NumpyArray = NumpyTypeHint()
