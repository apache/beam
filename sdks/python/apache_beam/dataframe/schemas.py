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

r"""Utilities for relating schema-aware PCollections and dataframe transforms.

Imposes a mapping between native Python typings (specifically those compatible
with apache_beam.typehints.schemas), and common pandas dtypes.

  pandas dtype                    Python typing
  np.int{8,16,32,64}      <-----> np.int{8,16,32,64}*
  pd.Int{8,16,32,64}Dtype <-----> Optional[np.int{8,16,32,64}]*
  np.float{32,64}         <-----> Optional[np.float{32,64}]
                             \--- np.float{32,64}
  Not supported           <------ Optional[bytes]
  np.bool                 <-----> np.bool

* int, float, bool are treated the same as np.int64, np.float64, np.bool

Any unknown or unsupported types are trested as Any and shunted to
np.object:

  np.object               <-----> Any

bytes, unicode strings and nullable Booleans are handled differently when using
pandas 0.x vs. 1.x. pandas 0.x has no mapping for these types, so they are
shunted to np.object.

pandas 1.x Only:

  np.dtype('S')     <-----> bytes
  pd.BooleanDType() <-----> Optional[bool]
  pd.StringDType()  <-----> Optional[str]
                       \--- str

Pandas does not support hierarchical data natively. Currently, all structured
types (Sequence, Mapping, nested NamedTuple types), will be shunted lossily to
np.object/Any like all other unknown types. In the future these types may be
given special consideration.

TODO: Mapping for date/time types
https://pandas.pydata.org/docs/user_guide/timeseries.html#overview
"""

# pytype: skip-file

from __future__ import absolute_import

from typing import Any
from typing import NamedTuple
from typing import Optional
from typing import TypeVar
from typing import Union

import numpy as np
import pandas as pd

import apache_beam as beam
from apache_beam import typehints
from apache_beam.portability.api import schema_pb2
from apache_beam.transforms.util import BatchElements
from apache_beam.typehints.native_type_compatibility import _match_is_optional
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.typehints.schemas import named_fields_to_schema
from apache_beam.typehints.schemas import named_tuple_from_schema
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.utils import proto_utils

__all__ = (
    'BatchRowsAsDataFrame',
    'generate_proxy',
    'UnbatchPandas',
    'element_type_from_dataframe')

T = TypeVar('T', bound=NamedTuple)

PD_MAJOR = int(pd.__version__.split('.')[0])

# Generate type map (presented visually in the docstring)
_BIDIRECTIONAL = [
    (np.bool, np.bool),
    (np.int8, np.int8),
    (np.int16, np.int16),
    (np.int32, np.int32),
    (np.int64, np.int64),
    (pd.Int8Dtype(), Optional[np.int8]),
    (pd.Int16Dtype(), Optional[np.int16]),
    (pd.Int32Dtype(), Optional[np.int32]),
    (pd.Int64Dtype(), Optional[np.int64]),
    (np.float32, Optional[np.float32]),
    (np.float64, Optional[np.float64]),
    (np.object, Any),
]

if PD_MAJOR >= 1:
  _BIDIRECTIONAL.extend([
      (pd.StringDtype(), Optional[str]),
      (pd.BooleanDtype(), Optional[np.bool]),
  ])

PANDAS_TO_BEAM = {
    pd.Series([], dtype=dtype).dtype: fieldtype
    for dtype,
    fieldtype in _BIDIRECTIONAL
}
BEAM_TO_PANDAS = {fieldtype: dtype for dtype, fieldtype in _BIDIRECTIONAL}

# Shunt non-nullable Beam types to the same pandas types as their non-nullable
# equivalents for FLOATs, DOUBLEs, and STRINGs. pandas has no non-nullable dtype
# for these.
OPTIONAL_SHUNTS = [np.float32, np.float64]

if PD_MAJOR >= 1:
  OPTIONAL_SHUNTS.append(str)

for typehint in OPTIONAL_SHUNTS:
  BEAM_TO_PANDAS[typehint] = BEAM_TO_PANDAS[Optional[typehint]]

# int, float -> int64, np.float64
BEAM_TO_PANDAS[int] = BEAM_TO_PANDAS[np.int64]
BEAM_TO_PANDAS[Optional[int]] = BEAM_TO_PANDAS[Optional[np.int64]]
BEAM_TO_PANDAS[float] = BEAM_TO_PANDAS[np.float64]
BEAM_TO_PANDAS[Optional[float]] = BEAM_TO_PANDAS[Optional[np.float64]]

BEAM_TO_PANDAS[bytes] = 'bytes'


@typehints.with_input_types(T)
@typehints.with_output_types(pd.DataFrame)
class BatchRowsAsDataFrame(beam.PTransform):
  """A transform that batches schema-aware PCollection elements into DataFrames

  Batching parameters are inherited from
  :class:`~apache_beam.transforms.util.BatchElements`.
  """
  def __init__(self, *args, **kwargs):
    self._batch_elements_transform = BatchElements(*args, **kwargs)

  def expand(self, pcoll):
    columns = [
        name for name, _ in named_fields_from_element_type(pcoll.element_type)
    ]
    return pcoll | self._batch_elements_transform | beam.Map(
        lambda batch: pd.DataFrame.from_records(batch, columns=columns))


def generate_proxy(element_type):
  # type: (type) -> pd.DataFrame

  """Generate a proxy pandas object for the given PCollection element_type.

  Currently only supports generating a DataFrame proxy from a schema-aware
  PCollection.
  """
  fields = named_fields_from_element_type(element_type)
  proxy = pd.DataFrame(columns=[name for name, _ in fields])

  for name, typehint in fields:
    # Default to np.object. This is lossy, we won't be able to recover the type
    # at the output.
    dtype = BEAM_TO_PANDAS.get(typehint, np.object)
    proxy[name] = proxy[name].astype(dtype)

  return proxy


def element_type_from_dataframe(proxy, include_indexes=False):
  # type: (pd.DataFrame, bool) -> type

  """Generate an element_type for an element-wise PCollection from a proxy
  pandas object. Currently only supports converting the element_type for
  a schema-aware PCollection to a proxy DataFrame.

  Currently only supports generating a DataFrame proxy from a schema-aware
  PCollection.
  """
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

  return named_tuple_from_schema(
      named_fields_to_schema([(column, _dtype_to_fieldtype(dtype))
                              for (column, dtype) in output_columns]))


class _BaseDataframeUnbatchDoFn(beam.DoFn):
  def __init__(self, namedtuple_ctor):
    self._namedtuple_ctor = namedtuple_ctor

  def _get_series(self, df):
    raise NotImplementedError()

  def process(self, df):
    # TODO: Only do null checks for nullable types
    def make_null_checking_generator(series):
      nulls = pd.isnull(series)
      return (None if isnull else value for isnull, value in zip(nulls, series))

    all_series = self._get_series(df)
    iterators = [
        make_null_checking_generator(series) for series,
        typehint in zip(all_series, self._namedtuple_ctor._field_types)
    ]

    # TODO: Avoid materializing the rows. Produce an object that references the
    # underlying dataframe
    for values in zip(*iterators):
      yield self._namedtuple_ctor(*values)

  def infer_output_type(self, input_type):
    return self._namedtuple_ctor

  @classmethod
  def _from_serialized_schema(cls, schema_str):
    return cls(
        named_tuple_from_schema(
            proto_utils.parse_Bytes(schema_str, schema_pb2.Schema)))

  def __reduce__(self):
    # when pickling, use bytes representation of the schema.
    return (
        self._from_serialized_schema,
        (named_tuple_to_schema(self._namedtuple_ctor).SerializeToString(), ))


class _UnbatchNoIndex(_BaseDataframeUnbatchDoFn):
  def _get_series(self, df):
    return [df[column] for column in df.columns]


class _UnbatchWithIndex(_BaseDataframeUnbatchDoFn):
  def _get_series(self, df):
    return [df.index.get_level_values(i) for i in range(len(df.index.names))
            ] + [df[column] for column in df.columns]


def _unbatch_transform(proxy, include_indexes):
  if isinstance(proxy, pd.DataFrame):
    ctor = element_type_from_dataframe(proxy, include_indexes=include_indexes)

    return beam.ParDo(
        _UnbatchWithIndex(ctor) if include_indexes else _UnbatchNoIndex(ctor))
  elif isinstance(proxy, pd.Series):
    # Raise a TypeError if proxy has an unknown type
    output_type = _dtype_to_fieldtype(proxy.dtype)
    # TODO: Should the index ever be included for a Series?
    if _match_is_optional(output_type):

      def unbatch(series):
        for isnull, value in zip(pd.isnull(series), series):
          yield None if isnull else value
    else:

      def unbatch(series):
        yield from series

    return beam.FlatMap(unbatch).with_output_types(output_type)
  # TODO: What about scalar inputs?
  else:
    raise TypeError(
        "Proxy '%s' has unsupported type '%s'" % (proxy, type(proxy)))


def _dtype_to_fieldtype(dtype):
  fieldtype = PANDAS_TO_BEAM.get(dtype)

  if fieldtype is not None:
    return fieldtype
  elif dtype.kind == 'S':
    return bytes
  else:
    raise TypeError("Unsupported dtype in proxy: '%s'" % dtype)


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
