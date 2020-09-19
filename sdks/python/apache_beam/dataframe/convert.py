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

from __future__ import absolute_import

import inspect
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Tuple
from typing import Union

from apache_beam import pvalue
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import schemas
from apache_beam.dataframe import transforms

if TYPE_CHECKING:
  # pylint: disable=ungrouped-imports
  import pandas


# TODO: Or should this be called as_dataframe?
def to_dataframe(
    pcoll,  # type: pvalue.PCollection
    proxy=None,  # type: pandas.core.generic.NDFrame
):
  # type: (...) -> frame_base.DeferredFrame

  """Convers a PCollection to a deferred dataframe-like object, which can
  manipulated with pandas methods like `filter` and `groupby`.

  For example, one might write::

    pcoll = ...
    df = to_dataframe(pcoll, proxy=...)
    result = df.groupby('col').sum()
    pcoll_result = to_pcollection(result)

  A proxy object must be given if the schema for the PCollection is not known.
  """
  if proxy is None:
    if pcoll.element_type is None:
      raise ValueError(
          "Cannot infer a proxy because the input PCollection does not have a "
          "schema defined. Please make sure a schema type is specified for "
          "the input PCollection, or provide a proxy.")
    # If no proxy is given, assume this is an element-wise schema-aware
    # PCollection that needs to be batched.
    proxy = schemas.generate_proxy(pcoll.element_type)
    pcoll = pcoll | 'BatchElements' >> schemas.BatchRowsAsDataFrame()
  return frame_base.DeferredFrame.wrap(
      expressions.PlaceholderExpression(proxy, pcoll))


# TODO: Or should this be called from_dataframe?
def to_pcollection(
    *dataframes,  # type: frame_base.DeferredFrame
    **kwargs):
  # type: (...) -> Union[pvalue.PCollection, Tuple[pvalue.PCollection, ...]]

  """Converts one or more deferred dataframe-like objects back to a PCollection.

  This method creates and applies the actual Beam operations that compute
  the given deferred dataframes, returning a PCollection of their results.

  If more than one (related) result is desired, it can be more efficient to
  pass them all at the same time to this method.
  """
  label = kwargs.pop('label', None)
  always_return_tuple = kwargs.pop('always_return_tuple', False)
  yield_dataframes = kwargs.pop('yield_dataframes', False)
  assert not kwargs  # TODO(BEAM-7372): Use PEP 3102
  if label is None:
    # Attempt to come up with a reasonable, stable label by retrieving the name
    # of these variables in the calling context.
    current_frame = inspect.currentframe()
    if current_frame is None:
      label = 'ToDataframe(...)'

    else:
      previous_frame = current_frame.f_back

      def name(obj):
        for key, value in previous_frame.f_locals.items():
          if obj is value:
            return key
        for key, value in previous_frame.f_globals.items():
          if obj is value:
            return key
        return '...'

      label = 'ToDataframe(%s)' % ', '.join(name(e) for e in dataframes)

  def extract_input(placeholder):
    if not isinstance(placeholder._reference, pvalue.PCollection):
      raise TypeError(
          'Expression roots must have been created with to_dataframe.')
    return placeholder._reference

  placeholders = frozenset.union(
      frozenset(), *[df._expr.placeholders() for df in dataframes])
  results = {p: extract_input(p)
             for p in placeholders
             } | label >> transforms._DataframeExpressionsTransform(
                 dict((ix, df._expr) for ix, df in enumerate(
                     dataframes)))  # type: Dict[Any, pvalue.PCollection]

  if not yield_dataframes:
    results = {
        key: pc | "Unbatch '%s'" % dataframes[key]._expr._id >>
        schemas.UnbatchPandas(dataframes[key]._expr.proxy())
        for key,
        pc in results.items()
    }

  if len(results) == 1 and not always_return_tuple:
    return results[0]
  else:
    return tuple(value for key, value in sorted(results.items()))
