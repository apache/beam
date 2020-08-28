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

"""Utilities for relating schema-aware PCollections and dataframe transforms.
"""

# pytype: skip-file

from __future__ import absolute_import

import typing

import pandas as pd

import apache_beam as beam
from apache_beam import typehints
from apache_beam.transforms.util import BatchElements
from apache_beam.typehints.schemas import named_fields_from_element_type

__all__ = ('BatchRowsAsDataFrame', 'generate_proxy')

T = typing.TypeVar('T', bound=typing.NamedTuple)


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


def _make_empty_series(name, typ):
  try:
    return pd.Series(name=name, dtype=typ)
  except TypeError:
    raise TypeError("Unable to convert type '%s' for field '%s'" % (name, typ))


def generate_proxy(element_type):
  # type: (type) -> pd.DataFrame
  return pd.DataFrame({
      name: _make_empty_series(name, typ)
      for name,
      typ in named_fields_from_element_type(element_type)
  })
