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

"""Utilities for relating schema-aware PCollections and DataFrame transforms.

The utilities here enforce the type mapping defined in
:mod:`apache_beam.typehints.pandas_type_compatibility`.
"""

# pytype: skip-file

import warnings
from typing import Any
from typing import Dict
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union

import pandas as pd

import apache_beam as beam
from apache_beam import typehints
from apache_beam.transforms.util import BatchElements
from apache_beam.typehints.pandas_type_compatibility import INDEX_OPTION_NAME
from apache_beam.typehints.pandas_type_compatibility import create_pandas_batch_converter
from apache_beam.typehints.pandas_type_compatibility import dtype_from_typehint
from apache_beam.typehints.pandas_type_compatibility import dtype_to_fieldtype
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.typehints.typehints import normalize

__all__ = (
    'BatchRowsAsDataFrame',
    'generate_proxy',
    'UnbatchPandas',
    'element_type_from_dataframe')

T = TypeVar('T', bound=NamedTuple)


@typehints.with_input_types(T)
@typehints.with_output_types(pd.DataFrame)
class BatchRowsAsDataFrame(beam.PTransform):
  """A transform that batches schema-aware PCollection elements into DataFrames

  Batching parameters are inherited from
  :class:`~apache_beam.transforms.util.BatchElements`.
  """
  def __init__(self, *args, proxy=None, **kwargs):
    self._batch_elements_transform = BatchElements(*args, **kwargs)
    self._proxy = proxy

  def expand(self, pcoll):
    if self._proxy is not None:
      # Generate typehint
      proxy = self._proxy
      element_typehint = _element_typehint_from_proxy(proxy)
    else:
      # Generate proxy
      proxy = generate_proxy(pcoll.element_type)
      element_typehint = pcoll.element_type

    converter = create_pandas_batch_converter(
        element_type=element_typehint, batch_type=type(proxy))

    return (
        pcoll | self._batch_elements_transform
        | beam.Map(converter.produce_batch))


def generate_proxy(element_type: type) -> pd.DataFrame:
  """Generate a proxy pandas object for the given PCollection element_type.

  Currently only supports generating a DataFrame proxy from a schema-aware
  PCollection or a Series proxy from a primitively typed PCollection.
  """
  dtype = dtype_from_typehint(element_type)
  if dtype is not object:
    return pd.Series(dtype=dtype)
  else:
    fields = named_fields_from_element_type(element_type)
    proxy = pd.DataFrame(columns=[name for name, _ in fields])
    for name, typehint in fields:
      dtype = dtype_from_typehint(typehint)
      proxy[name] = proxy[name].astype(dtype)

    return proxy


def element_type_from_dataframe(
    proxy: pd.DataFrame, include_indexes: bool = False) -> type:
  """Generate an element_type for an element-wise PCollection from a proxy
  pandas object. Currently only supports converting the element_type for
  a schema-aware PCollection to a proxy DataFrame.

  Currently only supports generating a DataFrame proxy from a schema-aware
  PCollection.
  """
  return element_typehint_from_dataframe_proxy(proxy, include_indexes).user_type


def _element_typehint_from_proxy(
    proxy: pd.core.generic.NDFrame, include_indexes: bool = False):
  if isinstance(proxy, pd.DataFrame):
    return element_typehint_from_dataframe_proxy(
        proxy, include_indexes=include_indexes)
  elif isinstance(proxy, pd.Series):
    if include_indexes:
      warnings.warn(
          "include_indexes=True for a Series input. Note that this "
          "parameter is _not_ respected for DeferredSeries "
          "conversion.")
    return dtype_to_fieldtype(proxy.dtype)
  else:
    raise TypeError(f"Proxy '{proxy}' has unsupported type '{type(proxy)}'")


def element_typehint_from_dataframe_proxy(
    proxy: pd.DataFrame, include_indexes: bool = False) -> RowTypeConstraint:

  output_columns = []
  if include_indexes:
    remaining_index_names = list(proxy.index.names)
    i = 0
    while len(remaining_index_names):
      index_name = remaining_index_names.pop(0)
      if index_name is None:
        raise ValueError(
            "Encountered an unnamed index. Cannot convert to a "
            "schema-aware PCollection with include_indexes=True. "
            "Please name all indexes or consider not including "
            "indexes.")
      elif index_name in remaining_index_names:
        raise ValueError(
            "Encountered multiple indexes with the name '%s'. "
            "Cannot convert to a schema-aware PCollection with "
            "include_indexes=True. Please ensure all indexes have "
            "unique names or consider not including indexes." % index_name)
      elif index_name in proxy.columns:
        raise ValueError(
            "Encountered an index that has the same name as one "
            "of the columns, '%s'. Cannot convert to a "
            "schema-aware PCollection with include_indexes=True. "
            "Please ensure all indexes have unique names or "
            "consider not including indexes." % index_name)
      else:
        # its ok!
        output_columns.append(
            (index_name, proxy.index.get_level_values(i).dtype))
        i += 1

  output_columns.extend(zip(proxy.columns, proxy.dtypes))

  fields = [(column, dtype_to_fieldtype(dtype))
            for (column, dtype) in output_columns]
  field_options: Optional[Dict[str, Sequence[Tuple[str, Any]]]]
  if include_indexes:
    field_options = {
        index_name: [(INDEX_OPTION_NAME, None)]
        for index_name in proxy.index.names
    }
  else:
    field_options = None

  return RowTypeConstraint.from_fields(fields, field_options=field_options)


def _unbatch_transform(proxy, include_indexes):
  element_typehint = normalize(
      _element_typehint_from_proxy(proxy, include_indexes=include_indexes))

  converter = create_pandas_batch_converter(
      element_type=element_typehint, batch_type=type(proxy))

  return beam.FlatMap(
      converter.explode_batch).with_output_types(element_typehint)


@typehints.with_input_types(Union[pd.DataFrame, pd.Series])
class UnbatchPandas(beam.PTransform):
  """A transform that explodes a PCollection of DataFrame or Series. DataFrame
  is converterd to a schema-aware PCollection, while Series is converted to its
  underlying type.

  Args:
    include_indexes: (optional, default: False) When unbatching a DataFrame
        if include_indexes=True, attempt to include index columns in the output
        schema for expanded DataFrames. Raises an error if any of the index
        levels are unnamed (name=None), or if any of the names are not unique
        among all column and index names.
  """
  def __init__(self, proxy, include_indexes=False):
    self._proxy = proxy
    self._include_indexes = include_indexes

  def expand(self, pcoll):
    return pcoll | _unbatch_transform(self._proxy, self._include_indexes)
