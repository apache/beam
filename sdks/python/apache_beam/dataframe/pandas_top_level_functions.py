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

"""A module providing various functionality from the top-level pandas namespace.
"""

import re
from typing import Mapping

import pandas as pd

from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import partitionings


def _call_on_first_arg(name):
  def wrapper(target, *args, **kwargs):
    if isinstance(target, frame_base.DeferredBase):
      return getattr(target, name)(*args, **kwargs)
    else:
      return getattr(pd, name)(target, *args, **kwargs)

  return staticmethod(wrapper)


def _defer_to_pandas(name):
  def wrapper(*args, **kwargs):
    res = getattr(pd, name)(*args, **kwargs)
    if type(res) in frame_base.DeferredBase._pandas_type_map.keys():
      return frame_base.DeferredBase.wrap(
          expressions.ConstantExpression(res, res[0:0]))
    else:
      return res

  return staticmethod(wrapper)


def _is_top_level_function(o):
  return (
      callable(o) and not isinstance(o, type) and hasattr(o, '__name__') and
      re.match('[a-z].*', o.__name__))


class DeferredPandasModule(object):
  array = _defer_to_pandas('array')
  bdate_range = _defer_to_pandas('bdate_range')

  @staticmethod
  @frame_base.args_to_kwargs(pd)
  @frame_base.populate_defaults(pd)
  def concat(
      objs,
      axis,
      join,
      ignore_index,
      keys,
      levels,
      names,
      verify_integrity,
      sort,
      copy):

    if ignore_index:
      raise NotImplementedError('concat(ignore_index)')
    if levels:
      raise NotImplementedError('concat(levels)')

    if isinstance(objs, Mapping):
      if keys is None:
        keys = list(objs.keys())
      objs = [objs[k] for k in keys]
    else:
      objs = list(objs)
    deferred_none = expressions.ConstantExpression(None)
    exprs = [deferred_none if o is None else o._expr for o in objs]

    if axis in (1, 'columns'):
      required_partitioning = partitionings.Index()
    elif verify_integrity:
      required_partitioning = partitionings.Index()
    else:
      required_partitioning = partitionings.Nothing()

    return frame_base.DeferredBase.wrap(
        expressions.ComputedExpression(
            'concat',
            lambda *objs: pd.concat(
                objs,
                axis=axis,
                join=join,
                ignore_index=ignore_index,
                keys=keys,
                levels=levels,
                names=names,
                verify_integrity=verify_integrity),  # yapf break
            exprs,
            requires_partition_by=required_partitioning,
            preserves_partition_by=partitionings.Index()))

  date_range = _defer_to_pandas('date_range')
  describe_option = _defer_to_pandas('describe_option')
  factorize = _call_on_first_arg('factorize')
  get_option = _defer_to_pandas('get_option')
  interval_range = _defer_to_pandas('interval_range')
  isna = _call_on_first_arg('isna')
  isnull = _call_on_first_arg('isnull')
  json_normalize = _defer_to_pandas('json_normalize')
  melt = _call_on_first_arg('melt')
  merge = _call_on_first_arg('merge')
  melt = _call_on_first_arg('melt')
  merge_ordered = frame_base.wont_implement_method('order-sensitive')
  notna = _call_on_first_arg('notna')
  notnull = _call_on_first_arg('notnull')
  option_context = _defer_to_pandas('option_context')
  period_range = _defer_to_pandas('period_range')
  pivot = _call_on_first_arg('pivot')
  pivot_table = _call_on_first_arg('pivot_table')
  show_versions = _defer_to_pandas('show_versions')
  test = frame_base.wont_implement_method('test')
  timedelta_range = _defer_to_pandas('timedelta_range')
  to_pickle = frame_base.wont_implement_method('order-sensitive')
  notna = _call_on_first_arg('notna')

  def __getattr__(self, name):
    if name.startswith('read_'):
      return frame_base.wont_implement_method(
          'Use p | apache_beam.dataframe.io.%s' % name)
    res = getattr(pd, name)
    if _is_top_level_function(res):
      return frame_base.not_implemented_method(name)
    else:
      return res


pd_wrapper = DeferredPandasModule()
