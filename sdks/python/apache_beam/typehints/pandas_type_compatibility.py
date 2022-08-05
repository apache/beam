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

"""Utilities for converting between Beam schemas and pandas DataFrames.

For internal use only, no backward compatibility guarantees.
"""

from apache_beam.typehints.batch import BatchConverter
from apache_beam.typehints.typehints import is_nullable
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.dataframe.schemas import dtype_from_typehint
from apache_beam.dataframe.schemas import generate_proxy

import pandas as pd
import numpy as np

from typing import Any
from typing import Optional
from typing import List
from typing import Tuple
from typing import Dict

# Name for a valueless field-level option which, when present, indicates that
# a field should map to an index in the Beam DataFrame API.
INDEX_OPTION_NAME = 'beam:dataframe:index'


class DataFrameBatchConverter(BatchConverter):
  def __init__(
      self,
      element_type: RowTypeConstraint,
  ):
    super().__init__(pd.DataFrame, element_type)
    self._columns = [name for name, _ in element_type._fields]

  @staticmethod
  @BatchConverter.register
  def from_typehints(element_type,
                     batch_type) -> Optional['DataFrameBatchConverter']:
    if not batch_type == pd.DataFrame:
      return None

    if not isinstance(element_type, RowTypeConstraint):
      element_type = RowTypeConstraint.from_user_type(element_type)
      if element_type is None:
        return None

    index_columns = [
        field_name
        for (field_name, field_options) in element_type._field_options.items()
        if any(key == INDEX_OPTION_NAME for key, value in field_options)
    ]

    if index_columns:
      return DataFrameBatchConverterKeepIndex(element_type, index_columns)
    else:
      return DataFrameBatchConverterDropIndex(element_type)

  def _get_series(self, batch: pd.DataFrame):
    raise NotImplementedError

  def explode_batch(self, batch: pd.DataFrame):
    # TODO: Only do null checks for nullable types
    def make_null_checking_generator(series):
      nulls = pd.isnull(series)
      return (None if isnull else value for isnull, value in zip(nulls, series))

    all_series = self._get_series(batch)
    iterators = [make_null_checking_generator(series) for series in all_series]

    for values in zip(*iterators):
      yield self._element_type.user_type(
          **{column: value
             for column, value in zip(self._columns, values)})

  def combine_batches(self, batches: List[pd.DataFrame]):
    return pd.concat(batches)

  def estimate_byte_size(self, batch: pd.DataFrame):
    return batch.memory_usage().sum()

  def get_length(self, batch: pd.DataFrame):
    return len(batch)


class DataFrameBatchConverterDropIndex(DataFrameBatchConverter):
  """A DataFrameBatchConverter that assumes the DataFrame index has no meaning.

  When producing a DataFrame from Rows, a meaningless index will be generated.
  When exploding a DataFrame into Rows, the index will be dropped.
  """
  def _get_series(self, batch: pd.DataFrame):
    return [batch[column] for column in batch.columns]

  def produce_batch(self, elements):
    # Note from_records has an index= parameter
    batch = pd.DataFrame.from_records(elements, columns=self._columns)

    for column, typehint in self._element_type._fields:
      batch[column] = batch[column].astype(dtype_from_typehint(typehint))

    return batch


class DataFrameBatchConverterKeepIndex(DataFrameBatchConverter):
  """A DataFrameBatchConverter that preserves the DataFrame index.

  This is tracked via options on the Beam schema. Each field in the schema that
  should map to the index is tagged in an option with name 'dataframe:index'.
  """
  def __init__(self, element_type: RowTypeConstraint, index_columns: List[Any]):
    super().__init__(element_type)
    self._index_columns = index_columns

  def _get_series(self, batch: pd.DataFrame):
    assert list(batch.index.names) == self._index_columns
    return [
        batch.index.get_level_values(i) for i in range(len(batch.index.names))
    ] + [batch[column] for column in batch.columns]

  def produce_batch(self, elements):
    # Note from_records has an index= parameter
    batch = pd.DataFrame.from_records(elements, columns=self._columns)

    for column, typehint in self._element_type._fields:
      batch[column] = batch[column].astype(dtype_from_typehint(typehint))

    return batch.set_index(self._index_columns)


class SeriesBatchConverter(BatchConverter):
  def __init__(
      self,
      element_type: type,
      dtype,
  ):
    super().__init__(pd.DataFrame, element_type)
    self._dtype = dtype

    if is_nullable(element_type):

      def unbatch(series):
        for isnull, value in zip(pd.isnull(series), series):
          yield None if isnull else value
    else:

      def unbatch(series):
        yield from series

    self.explode_batch = unbatch

  @staticmethod
  @BatchConverter.register
  def from_typehints(element_type,
                     batch_type) -> Optional['SeriesBatchConverter']:
    if not batch_type == pd.Series:
      return None

    dtype = dtype_from_typehint(element_type)
    if dtype == np.object:
      # Don't create Any <-> Series[np.object] mapping
      return None

    return SeriesBatchConverter(element_type, dtype)

  def produce_batch(self, elements: List[Any]) -> pd.Series:
    return pd.Series(elements, dtype=self._dtype)

  def explode_batch(self, batch: pd.Series):
    raise NotImplementedError(
        "explode_batch should be generated in SeriesBatchConverter.__init__")

  def combine_batches(self, batches: List[pd.Series]):
    return pd.concat(batches)

  def estimate_byte_size(self, batch: pd.Series):
    return batch.memory_usage()

  def get_length(self, batch: pd.Series):
    return len(batch)
