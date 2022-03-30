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

"""Utilities for type-hinting batched types for use in the Beam SDK.

A batched type is a type B that is logically equivalent to Sequence[E], where E
is some other type. Typically B has a different physical representation than
Sequence[E] for performance reasons.

A trivial example is B=np.array(dtype=np.int64), E=int.

Batched type hints are used to enable more efficient processing of
a PCollection[E], by allowing users to write DoFns that operate on
multi-element partitions of the PCollection represented with type B."""

from typing import Generic
from typing import Iterator
from typing import Optional
from typing import Sequence
from typing import TypeVar

import numpy as np

import apache_beam as beam
from apache_beam.typehints import row_type
from apache_beam.typehints.typehints import TypeConstraint

try:
  import pandas as pd
except ImportError:
  pd = None


B = TypeVar('B')
E = TypeVar('E')

BATCH_CONVERTER_REGISTRY = []


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

  @staticmethod
  def register(batching_util_fn):
    BATCH_CONVERTER_REGISTRY.append(batching_util_fn)
    return batching_util_fn

  # TODO: Consider required kwargs here. Easy to mix up element/batch_type
  @staticmethod
  def from_typehints(*, element_type, batch_type) -> 'BatchConverter':
    for constructor in BATCH_CONVERTER_REGISTRY:
      result = constructor(element_type, batch_type)
      if result is not None:
        return result

    # TODO: Include explanations for realistic candidate BatchConverter
    raise TypeError(
        f"Unable to find BatchConverter for element_type {element_type!r} and "
        f"batch_type {batch_type!r}")

  @property
  def batch_type(self):
    return self._batch_type

  @property
  def element_type(self):
    return self._element_type


N = "ARBITRARY LENGTH DIMENSION"


class BatchTypeConstraint(TypeConstraint):
  def type_check(self, instance):
    """Determines if the type of 'instance' satisfies this type constraint.

    Note that `type_check` verifies that a *batch* instance has type B. See
    :meth:`type_check_element` for verifying *batch* instances.

    Args:
      instance: An instance of a Python object.

    Raises:
      :class:`TypeError`: The passed **instance** doesn't satisfy
        this :class:`TypeConstraint`. Subclasses of
        :class:`TypeConstraint` are free to raise any of the subclasses of
        :class:`TypeError` defined above, depending on
        the manner of the type hint error.

    All :class:`TypeConstraint` sub-classes must define this method in other
    for the class object to be created.
    """
    raise NotImplementedError

  def type_check_element(self, instance):
    """Determines if the type of 'instance' is an element of type ``E``.
    """
    raise NotImplementedError()


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


if pd is not None:

  class DataFrameRowBatchConverter(BatchConverter):
    """
    NOTE: We assume the index is not part of the data for this mapping
    """
    def __init__(self, element_type):
      super().__init__(pd.DataFrame, element_type)
      # TODO: Merge this with apache_beam.dataframe.schemas to avoid circular
      # import
      from apache_beam.dataframe.schemas import BEAM_TO_PANDAS
      self._element_type = element_type
      self._column_names = [name for name, _ in self._element_type._fields]
      self._column_dtypes = [
          BEAM_TO_PANDAS.get(typ, object) for _,
          typ in self._element_type._fields
      ]

    # TODO: Static list instead of registry (not ready for users to make their
    # own)
    @staticmethod
    @BatchConverter.register
    def from_typehints(element_type,
                       batch_type) -> Optional['DataFrameRowBatchConverter']:
      if not batch_type == pd.DataFrame or not isinstance(
          element_type, row_type.RowTypeConstraint):
        return None

      return DataFrameRowBatchConverter(element_type)

    def produce_batch(self, elements):
      return pd.DataFrame({
          name: pd.Series((getattr(element, name) for element in elements),
                          dtype=dtype)
          for name,
          dtype in zip(self._column_names, self._column_dtypes)
      })

    def explode_batch(self, batch):
      # TODO: null checking, like in apache_beam.dataframe.schemas
      all_series = [batch[name] for name in self._column_names]
      for value in zip(*all_series):
        yield beam.Row(**dict(zip(self._column_names, value)))

    def combine_batches(self, batches):
      return pd.concat(batches)

    def get_length(self, batch):
      return len(batch)


# numpy is starting to add typehints, which we should support
# https://numpy.org/doc/stable/reference/typing.html for now they don't allow
# specifying shape, seems to be coming after
# https://www.python.org/dev/peps/pep-0646/
class NumpyTypeHint():
  class NumpyTypeConstraint(BatchTypeConstraint):
    def __init__(self, dtype, shape=(), partition_dimension=0):
      self.dtype = np.dtype(dtype)
      self.shape = shape
      self.partition_dimension = partition_dimension

    def type_check_element(self, element):
      # TODO: Should BatchTypeConstraint store a TypeConstraint for the elements
      # instead
      if self.shape == ():
        if not np.issubdtype(type(element), self.dtype):
          raise TypeError(
              f"Element {element!r} with type={type(element)!r} is not an "
              f"instance of dtype {self.dtype!r}")
      else:  # Each element should itself be an ndarray
        if not isinstance(element, np.ndarray):
          raise TypeError(f"Element {element!r} is not an instance of ndarray")
        if not np.issubdtype(element.dtype, self.dtype):
          raise TypeError(
              f"Element {element!r} does not have expected dtype: "
              f"{self.dtype!r}")
        if not element.shape == self.shape:
          raise TypeError(
              f"Element {element!r} does not have expected shape: "
              f"{self.shape!r}")
      # looks good!

    def type_check(self, batch):
      if not isinstance(batch, np.ndarray):
        raise TypeError(f"Batch {batch!r} is not an instance of ndarray")
      if not np.issubdtype(batch.dtype, self.dtype):
        raise TypeError(
            f"Batch {batch!r} does not have expected dtype: {self.dtype!r}")

      for dim in range(len(self.shape)):
        if not dim == self.partition_dimension:
          if batch.shape[dim] != self.shape[dim]:
            raise TypeError(
                f"Batch {batch!r} does not have expected shape: {self.shape!r}")

    def _consistent_with_check_(self, sub):
      # TODO Check sub against batch type, and element type
      return True

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
