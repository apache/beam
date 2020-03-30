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

from apache_beam import pvalue
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import transforms


def to_dataframe(pc):
  pass


# TODO: Or should this be called as_dataframe?
def to_dataframe(pcoll, proxy):
  return frame_base.DeferredFrame.wrap(
      expressions.PlaceholderExpression(proxy, pcoll))


# TODO: Or should this be called from_dataframe?
def to_pcollection(*dataframes, **kwargs):
  label = kwargs.pop('label', None)
  assert not kwargs  # TODO(Py3): Use PEP 3102
  if label is None:
    # Attempt to come up with a reasonable, stable label.
    previous_frame = inspect.currentframe().f_back

    def name(obj):
      for key, value in previous_frame.f_locals.items():
        if obj is value:
          return key
      for key, value in previous_frame.f_globals.items():
        if obj is value:
          return key
      else:
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
             } | label >> transforms.DataframeExpressionsTransform(
                 dict((ix, df._expr) for ix, df in enumerate(dataframes)))
  if len(results) == 1:
    return results[0]
  else:
    return tuple(value for key, value in sorted(results.items()))
