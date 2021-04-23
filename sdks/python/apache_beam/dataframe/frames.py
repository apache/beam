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

"""Analogs for :class:`pandas.DataFrame` and :class:`pandas.Series`:
:class:`DeferredDataFrame` and :class:`DeferredSeries`.

These classes are effectively wrappers around a `schema-aware`_
:class:`~apache_beam.pvalue.PCollection` that provide a set of operations
compatible with the `pandas`_ API.

Note that we aim for the Beam DataFrame API to be completely compatible with
the pandas API, but there are some features that are currently unimplemented
for various reasons. Pay particular attention to the **'Differences from
pandas'** section for each operation to understand where we diverge.

.. _schema-aware:
  https://beam.apache.org/documentation/programming-guide/#what-is-a-schema
.. _pandas:
  https://pandas.pydata.org/
"""

import collections
import inspect
import itertools
import math
import re
from typing import List
from typing import Optional

import numpy as np
import pandas as pd
from pandas.core.groupby.generic import DataFrameGroupBy

from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import io
from apache_beam.dataframe import partitionings

__all__ = [
    'DeferredSeries',
    'DeferredDataFrame',
]


def populate_not_implemented(pd_type):
  def wrapper(deferred_type):
    for attr in dir(pd_type):
      # Don't auto-define hidden methods or dunders
      if attr.startswith('_'):
        continue
      if not hasattr(deferred_type, attr):
        pd_value = getattr(pd_type, attr)
        if isinstance(pd_value, property) or inspect.isclass(pd_value):
          # Some of the properties on pandas types (cat, dt, sparse), are
          # actually attributes with class values, not properties
          setattr(
              deferred_type,
              attr,
              property(frame_base.not_implemented_method(attr)))
        elif callable(pd_value):
          setattr(deferred_type, attr, frame_base.not_implemented_method(attr))
    return deferred_type

  return wrapper


class DeferredDataFrameOrSeries(frame_base.DeferredFrame):

  __array__ = frame_base.wont_implement_method(
      pd.Series, '__array__', reason="non-deferred-result")

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def drop(self, labels, axis, index, columns, errors, **kwargs):
    if labels is not None:
      if index is not None or columns is not None:
        raise ValueError("Cannot specify both 'labels' and 'index'/'columns'")
      if axis in (0, 'index'):
        index = labels
        columns = None
      elif axis in (1, 'columns'):
        index = None
        columns = labels
      else:
        raise ValueError(
            "axis must be one of (0, 1, 'index', 'columns'), "
            "got '%s'" % axis)

    if columns is not None:
      # Compute the proxy based on just the columns that are dropped.
      proxy = self._expr.proxy().drop(columns=columns, errors=errors)
    else:
      proxy = self._expr.proxy()

    if index is not None and errors == 'raise':
      # In order to raise an error about missing index values, we'll
      # need to collect the entire dataframe.
      requires = partitionings.Singleton()
    else:
      requires = partitionings.Arbitrary()

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'drop',
            lambda df: df.drop(
                axis=axis,
                index=index,
                columns=columns,
                errors=errors,
                **kwargs), [self._expr],
            proxy=proxy,
            requires_partition_by=requires))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def droplevel(self, level, axis):
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'droplevel',
            lambda df: df.droplevel(level, axis=axis), [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()
            if axis in (1, 'column') else partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def fillna(self, value, method, axis, limit, **kwargs):
    # Default value is None, but is overriden with index.
    axis = axis or 'index'
    if method is not None and axis in (0, 'index'):
      raise frame_base.WontImplementError(
          f"fillna(method={method!r}) is not supported because it is "
          "order-sensitive. Only fillna(method=None) is supported.",
          reason="order-sensitive")
    if isinstance(value, frame_base.DeferredBase):
      value_expr = value._expr
    else:
      value_expr = expressions.ConstantExpression(value)

    if limit is not None and method is None:
      # If method is not None (and axis is 'columns'), we can do limit in
      # a distributed way. Otherwise the limit is global, so it requires
      # Singleton partitioning.
      requires = partitionings.Singleton()
    else:
      requires = partitionings.Arbitrary()

    return frame_base.DeferredFrame.wrap(
        # yapf: disable
        expressions.ComputedExpression(
            'fillna',
            lambda df,
            value: df.fillna(
                value, method=method, axis=axis, limit=limit, **kwargs),
            [self._expr, value_expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=requires))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def ffill(self, **kwargs):
    return self.fillna(method='ffill', **kwargs)

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def bfill(self, **kwargs):
    return self.fillna(method='bfill', **kwargs)

  pad = ffill
  backfill = bfill

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def groupby(self, by, level, axis, as_index, group_keys, **kwargs):
    if not as_index:
      raise NotImplementedError('groupby(as_index=False)')
    if not group_keys:
      raise NotImplementedError('groupby(group_keys=False)')

    if axis in (1, 'columns'):
      return _DeferredGroupByCols(
          expressions.ComputedExpression(
              'groupbycols',
              lambda df: df.groupby(by, axis=axis, **kwargs), [self._expr],
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))

    if level is None and by is None:
      raise TypeError("You have to supply one of 'by' and 'level'")

    elif level is not None:
      if isinstance(level, (list, tuple)):
        grouping_indexes = level
      else:
        grouping_indexes = [level]

      grouping_columns = []

      index = self._expr.proxy().index

      # Translate to level numbers only
      grouping_indexes = [
          l if isinstance(l, int) else index.names.index(l)
          for l in grouping_indexes
      ]

      if index.nlevels == 1:
        to_group_with_index = self._expr
        to_group = self._expr
      else:
        levels_to_drop = [
            i for i in range(index.nlevels) if i not in grouping_indexes
        ]

        # Reorder so the grouped indexes are first
        to_group_with_index = self.reorder_levels(
            grouping_indexes + levels_to_drop)

        grouping_indexes = list(range(len(grouping_indexes)))
        levels_to_drop = list(range(len(grouping_indexes), index.nlevels))
        if levels_to_drop:
          to_group = to_group_with_index.droplevel(levels_to_drop)._expr
        else:
          to_group = to_group_with_index._expr
        to_group_with_index = to_group_with_index._expr

    elif callable(by):

      def map_index(df):
        df = df.copy()
        df.index = df.index.map(by)
        return df

      to_group = expressions.ComputedExpression(
          'map_index',
          map_index, [self._expr],
          requires_partition_by=partitionings.Arbitrary(),
          preserves_partition_by=partitionings.Singleton())

      orig_nlevels = self._expr.proxy().index.nlevels
      to_group_with_index = expressions.ComputedExpression(
          'map_index_keep_orig',
          lambda df: df.set_index([df.index.map(by), df.index]),
          [self._expr],
          requires_partition_by=partitionings.Arbitrary(),
          # Partitioning by the original indexes is preserved
          preserves_partition_by=partitionings.Index(
              list(range(1, orig_nlevels + 1))))

      grouping_columns = []
      # The index we need to group by is the last one
      grouping_indexes = [0]

    elif isinstance(by, DeferredSeries):

      raise NotImplementedError(
          "grouping by a Series is not yet implemented. You can group by a "
          "DataFrame column by specifying its name.")

    elif isinstance(by, np.ndarray):
      raise frame_base.WontImplementError(
          "Grouping by a concrete ndarray is order sensitive.",
          reason="order-sensitive")

    elif isinstance(self, DeferredDataFrame):
      if not isinstance(by, list):
        by = [by]
      # Find the columns that we need to move into the index so we can group by
      # them
      column_names = self._expr.proxy().columns
      grouping_columns = list(set(by).intersection(column_names))
      index_names = self._expr.proxy().index.names
      for label in by:
        if label not in index_names and label not in self._expr.proxy().columns:
          raise KeyError(label)
      grouping_indexes = list(set(by).intersection(index_names))

      if grouping_indexes:
        if set(by) == set(index_names):
          to_group = self._expr
        elif set(by).issubset(index_names):
          to_group = self.droplevel(index_names.difference(by))._expr
        else:
          to_group = self.reset_index(grouping_indexes).set_index(by)._expr
      else:
        to_group = self.set_index(by)._expr

      if grouping_columns:
        # TODO(BEAM-11711): It should be possible to do this without creating an
        # expression manually, by using DeferredDataFrame.set_index, i.e.:
        #   to_group_with_index = self.set_index([self.index] +
        #                                        grouping_columns)._expr
        to_group_with_index = expressions.ComputedExpression(
            'move_grouped_columns_to_index',
            lambda df: df.set_index([df.index] + grouping_columns),
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Index(
                list(range(self._expr.proxy().index.nlevels))))
      else:
        to_group_with_index = self._expr

    else:
      raise NotImplementedError(by)

    return DeferredGroupBy(
        expressions.ComputedExpression(
            'groupbyindex',
            lambda df: df.groupby(
                level=list(range(df.index.nlevels)), **kwargs), [to_group],
            requires_partition_by=partitionings.Index(),
            preserves_partition_by=partitionings.Arbitrary()),
        kwargs,
        to_group,
        to_group_with_index,
        grouping_columns=grouping_columns,
        grouping_indexes=grouping_indexes)

  abs = frame_base._elementwise_method('abs', base=pd.core.generic.NDFrame)
  astype = frame_base._elementwise_method(
      'astype', base=pd.core.generic.NDFrame)
  copy = frame_base._elementwise_method('copy', base=pd.core.generic.NDFrame)

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def tz_localize(self, ambiguous, **kwargs):
    if isinstance(ambiguous, np.ndarray):
      raise frame_base.WontImplementError(
          "tz_localize(ambiguous=ndarray) is not supported because it makes "
          "this operation sensitive to the order of the data. Please use a "
          "DeferredSeries instead.",
          reason="order-sensitive")
    elif isinstance(ambiguous, frame_base.DeferredFrame):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'tz_localize',
              lambda df,
              ambiguous: df.tz_localize(ambiguous=ambiguous, **kwargs),
              [self._expr, ambiguous._expr],
              requires_partition_by=partitionings.Index(),
              preserves_partition_by=partitionings.Singleton()))
    elif ambiguous == 'infer':
      # infer attempts to infer based on the order of the timestamps
      raise frame_base.WontImplementError(
          f"tz_localize(ambiguous={ambiguous!r}) is not allowed because it "
          "makes this operation sensitive to the order of the data.",
          reason="order-sensitive")

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'tz_localize',
            lambda df: df.tz_localize(ambiguous=ambiguous, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Singleton()))

  @property
  def size(self):
    sizes = expressions.ComputedExpression(
        'get_sizes',
        # Wrap scalar results in a Series for easier concatenation later
        lambda df: pd.Series(df.size),
        [self._expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Singleton())

    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'sum_sizes',
              lambda sizes: sizes.sum(), [sizes],
              requires_partition_by=partitionings.Singleton(),
              preserves_partition_by=partitionings.Singleton()))

  @property
  def empty(self):
    empties = expressions.ComputedExpression(
        'get_empties',
        # Wrap scalar results in a Series for easier concatenation later
        lambda df: pd.Series(df.empty),
        [self._expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Singleton())

    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'check_all_empty',
              lambda empties: empties.all(), [empties],
              requires_partition_by=partitionings.Singleton(),
              preserves_partition_by=partitionings.Singleton()))

  def bool(self):
    # Will throw if any partition has >1 element
    bools = expressions.ComputedExpression(
        'get_bools',
        # Wrap scalar results in a Series for easier concatenation later
        lambda df: pd.Series([], dtype=bool)
        if df.empty else pd.Series([df.bool()]),
        [self._expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Singleton())

    with expressions.allow_non_parallel_operations(True):
      # Will throw if overall dataset has != 1 element
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'combine_all_bools',
              lambda bools: bools.bool(), [bools],
              proxy=bool(),
              requires_partition_by=partitionings.Singleton(),
              preserves_partition_by=partitionings.Singleton()))

  def equals(self, other):
    intermediate = expressions.ComputedExpression(
        'equals_partitioned',
        # Wrap scalar results in a Series for easier concatenation later
        lambda df,
        other: pd.Series(df.equals(other)),
        [self._expr, other._expr],
        requires_partition_by=partitionings.Index(),
        preserves_partition_by=partitionings.Singleton())

    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'aggregate_equals',
              lambda df: df.all(), [intermediate],
              requires_partition_by=partitionings.Singleton(),
              preserves_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def sort_values(self, axis, **kwargs):
    """``sort_values`` is not implemented.

    It is not implemented for ``axis=index`` because it imposes an ordering on
    the dataset, and we cannot guarantee it will be maintained (see
    https://s.apache.org/dataframe-order-sensitive-operations).

    It is not implemented for ``axis=columns`` because it makes the order of
    the columns depend on the data (see
    https://s.apache.org/dataframe-non-deferred-column-names)."""
    if axis in (0, 'index'):
      # axis=index imposes an ordering on the DataFrame rows which we do not
      # support
      raise frame_base.WontImplementError(
          "sort_values(axis=index) is not supported because it imposes an "
          "ordering on the dataset which we cannot guarantee will be "
          "preserved.",
          reason="order-sensitive")
    else:
      # axis=columns will reorder the columns based on the data
      raise frame_base.WontImplementError(
          "sort_values(axis=columns) is not supported because the order of the "
          "columns in the result depends on the data.",
          reason="non-deferred-columns")

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def sort_index(self, axis, **kwargs):
    """``axis=index`` is not allowed because it imposes an ordering on the
    dataset, and we cannot guarantee it will be maintained (see
    https://s.apache.org/dataframe-order-sensitive-operations). Only
    ``axis=columns`` is allowed."""
    if axis in (0, 'index'):
      # axis=rows imposes an ordering on the DataFrame which we do not support
      raise frame_base.WontImplementError(
          "sort_index(axis=index) is not supported because it imposes an "
          "ordering on the dataset which we cannot guarantee will be "
          "preserved.",
          reason="order-sensitive")

    # axis=columns reorders the columns by name
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'sort_index',
            lambda df: df.sort_index(axis, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary(),
        ))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def where(self, cond, other, errors, **kwargs):
    requires = partitionings.Arbitrary()
    deferred_args = {}
    actual_args = {}

    # TODO(bhulette): This is very similar to the logic in
    # frame_base.elementwise_method, can we unify it?
    if isinstance(cond, frame_base.DeferredFrame):
      deferred_args['cond'] = cond
      requires = partitionings.Index()
    else:
      actual_args['cond'] = cond

    if isinstance(other, frame_base.DeferredFrame):
      deferred_args['other'] = other
      requires = partitionings.Index()
    else:
      actual_args['other'] = other

    if errors == "ignore":
      # We need all data in order to ignore errors and propagate the original
      # data.
      requires = partitionings.Singleton()

    actual_args['errors'] = errors

    def where_execution(df, *args):
      runtime_values = {
          name: value
          for (name, value) in zip(deferred_args.keys(), args)
      }
      return df.where(**runtime_values, **actual_args, **kwargs)

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            "where",
            where_execution,
            [self._expr] + [df._expr for df in deferred_args.values()],
            requires_partition_by=requires,
            preserves_partition_by=partitionings.Index(),
        ))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def mask(self, cond, **kwargs):
    return self.where(~cond, **kwargs)

  @property
  def dtype(self):
    return self._expr.proxy().dtype

  isin = frame_base._elementwise_method('isin', base=pd.DataFrame)

  @property
  def ndim(self):
    return self._expr.proxy().ndim

  def _get_index(self):
    return _DeferredIndex(self)

  index = property(
      _get_index, frame_base.not_implemented_method('index (setter)'))

  hist = frame_base.wont_implement_method(
      pd.DataFrame, 'hist', reason="plotting-tools")

  attrs = property(
      frame_base.wont_implement_method(
          pd.DataFrame, 'attrs', reason='experimental'))

  reorder_levels = frame_base._proxy_method(
      'reorder_levels',
      base=pd.DataFrame,
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Singleton())


@populate_not_implemented(pd.Series)
@frame_base.DeferredFrame._register_for(pd.Series)
class DeferredSeries(DeferredDataFrameOrSeries):
  @property
  def name(self):
    return self._expr.proxy().name

  @name.setter
  def name(self, value):
    def fn(s):
      s = s.copy()
      s.name = value
      return s

    self._expr = expressions.ComputedExpression(
        'series_set_name',
        fn, [self._expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Arbitrary())

  @property
  def dtype(self):
    return self._expr.proxy().dtype

  dtypes = dtype

  def __getitem__(self, key):
    if _is_null_slice(key) or key is Ellipsis:
      return self

    elif (isinstance(key, int) or _is_integer_slice(key)
          ) and self._expr.proxy().index._should_fallback_to_positional():
      raise frame_base.WontImplementError(
          "Accessing an item by an integer key is order sensitive for this "
          "Series.",
          reason="order-sensitive")

    elif isinstance(key, slice) or callable(key):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              # yapf: disable
              'getitem',
              lambda df: df[key],
              [self._expr],
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))

    elif isinstance(key, DeferredSeries) and key._expr.proxy().dtype == bool:
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              # yapf: disable
              'getitem',
              lambda df,
              indexer: df[indexer],
              [self._expr, key._expr],
              requires_partition_by=partitionings.Index(),
              preserves_partition_by=partitionings.Arbitrary()))

    elif pd.core.series.is_iterator(key) or pd.core.common.is_bool_indexer(key):
      raise frame_base.WontImplementError(
          "Accessing a DeferredSeries with an iterator is sensitive to the "
          "order of the data.",
          reason="order-sensitive")

    else:
      # We could consider returning a deferred scalar, but that might
      # be more surprising than a clear error.
      raise frame_base.WontImplementError(
          f"Indexing a series with key of type {type(key)} is not supported "
          "because it produces a non-deferred result.",
          reason="non-deferred-result")

  def keys(self):
    return self.index

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def append(self, to_append, ignore_index, verify_integrity, **kwargs):
    if not isinstance(to_append, DeferredSeries):
      raise frame_base.WontImplementError(
          "append() only accepts DeferredSeries instances, received " +
          str(type(to_append)))
    if ignore_index:
      raise frame_base.WontImplementError(
          "append(ignore_index=True) is order sensitive because it requires "
          "generating a new index based on the order of the data.",
          reason="order-sensitive")

    if verify_integrity:
      # verifying output has a unique index requires global index.
      # TODO(BEAM-11839): Attach an explanation to the Singleton partitioning
      # requirement, and include it in raised errors.
      requires = partitionings.Singleton()
    else:
      requires = partitionings.Arbitrary()

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'append',
            lambda s,
            to_append: s.append(
                to_append, verify_integrity=verify_integrity, **kwargs),
            [self._expr, to_append._expr],
            requires_partition_by=requires,
            preserves_partition_by=partitionings.Arbitrary()))

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def align(self, other, join, axis, level, method, **kwargs):
    """Aligning per-level is not yet supported. Only the default,
    ``level=None``, is allowed.

    Filling NaN values via ``method`` is not supported, because it is
    sensitive to the order of the data
    (see https://s.apache.org/dataframe-order-sensitive-operations). Only the
    default, ``method=None``, is allowed.
    """
    if level is not None:
      raise NotImplementedError('per-level align')
    if method is not None:
      raise frame_base.WontImplementError(
          f"align(method={method!r}) is not supported because it is "
          "order sensitive. Only align(method=None) is supported.",
          reason="order-sensitive")
    # We're using pd.concat here as expressions don't yet support
    # multiple return values.
    aligned = frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'align',
            lambda x,
            y: pd.concat([x, y], axis=1, join='inner'),
            [self._expr, other._expr],
            requires_partition_by=partitionings.Index(),
            preserves_partition_by=partitionings.Arbitrary()))
    return aligned.iloc[:, 0], aligned.iloc[:, 1]

  array = property(
      frame_base.wont_implement_method(
          pd.Series, 'array', reason="non-deferred-result"))

  ravel = frame_base.wont_implement_method(
      pd.Series, 'ravel', reason="non-deferred-result")

  rename = frame_base._elementwise_method('rename', base=pd.Series)
  between = frame_base._elementwise_method('between', base=pd.Series)

  add_suffix = frame_base._proxy_method(
      'add_suffix',
      base=pd.DataFrame,
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Singleton())
  add_prefix = frame_base._proxy_method(
      'add_prefix',
      base=pd.DataFrame,
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Singleton())

  def dot(self, other):
    left = self._expr
    if isinstance(other, DeferredSeries):
      right = expressions.ComputedExpression(
          'to_dataframe',
          pd.DataFrame, [other._expr],
          requires_partition_by=partitionings.Arbitrary(),
          preserves_partition_by=partitionings.Arbitrary())
      right_is_series = True
    elif isinstance(other, DeferredDataFrame):
      right = other._expr
      right_is_series = False
    else:
      raise frame_base.WontImplementError('non-deferred result')

    dots = expressions.ComputedExpression(
        'dot',
        # Transpose so we can sum across rows.
        (lambda left, right: pd.DataFrame(left @ right).T),
        [left, right],
        requires_partition_by=partitionings.Index())
    with expressions.allow_non_parallel_operations(True):
      sums = expressions.ComputedExpression(
          'sum',
          lambda dots: dots.sum(),  #
          [dots],
          requires_partition_by=partitionings.Singleton())

      if right_is_series:
        result = expressions.ComputedExpression(
            'extract',
            lambda df: df[0], [sums],
            requires_partition_by=partitionings.Singleton())
      else:
        result = sums
      return frame_base.DeferredFrame.wrap(result)

  __matmul__ = dot

  @frame_base.with_docs_from(pd.Series)
  def std(self, *args, **kwargs):
    # Compute variance (deferred scalar) with same args, then sqrt it
    return self.var(*args, **kwargs).apply(lambda var: math.sqrt(var))

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def var(self, axis, skipna, level, ddof, **kwargs):
    """Per-level aggregation is not yet supported (BEAM-11777). Only the
    default, ``level=None``, is allowed."""
    if level is not None:
      raise NotImplementedError("per-level aggregation")
    if skipna is None or skipna:
      self = self.dropna()  # pylint: disable=self-cls-assignment

    # See the online, numerically stable formulae at
    # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    # and
    # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
    def compute_moments(x):
      n = len(x)
      m = x.std(ddof=0)**2 * n
      s = x.sum()
      return pd.DataFrame(dict(m=[m], s=[s], n=[n]))

    def combine_moments(data):
      m = s = n = 0.0
      for datum in data.itertuples():
        if datum.n == 0:
          continue
        elif n == 0:
          m, s, n = datum.m, datum.s, datum.n
        else:
          delta = s / n - datum.s / datum.n
          m += datum.m + delta**2 * n * datum.n / (n + datum.n)
          s += datum.s
          n += datum.n
      if n <= ddof:
        return float('nan')
      else:
        return m / (n - ddof)

    moments = expressions.ComputedExpression(
        'compute_moments',
        compute_moments, [self._expr],
        requires_partition_by=partitionings.Arbitrary())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'combine_moments',
              combine_moments, [moments],
              requires_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def corr(self, other, method, min_periods):
    if method == 'pearson':  # Note that this is the default.
      x, y = self.dropna().align(other.dropna(), 'inner')
      return x._corr_aligned(y, min_periods)

    else:
      # The rank-based correlations are not obviously parallelizable, though
      # perhaps an approximation could be done with a knowledge of quantiles
      # and custom partitioning.
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'corr',
              lambda df,
              other: df.corr(other, method=method, min_periods=min_periods),
              [self._expr, other._expr],
              # TODO(BEAM-11839): Attach an explanation to the Singleton
              # partitioning requirement, and include it in raised errors.
              requires_partition_by=partitionings.Singleton()))

  def _corr_aligned(self, other, min_periods):
    std_x = self.std()
    std_y = other.std()
    cov = self._cov_aligned(other, min_periods)
    return cov.apply(
        lambda cov, std_x, std_y: cov / (std_x * std_y), args=[std_x, std_y])

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def cov(self, other, min_periods, ddof):
    x, y = self.dropna().align(other.dropna(), 'inner')
    return x._cov_aligned(y, min_periods, ddof)

  def _cov_aligned(self, other, min_periods, ddof=1):
    # Use the formulae from
    # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance
    def compute_co_moments(x, y):
      n = len(x)
      if n <= 1:
        c = 0
      else:
        c = x.cov(y) * (n - 1)
      sx = x.sum()
      sy = y.sum()
      return pd.DataFrame(dict(c=[c], sx=[sx], sy=[sy], n=[n]))

    def combine_co_moments(data):
      c = sx = sy = n = 0.0
      for datum in data.itertuples():
        if datum.n == 0:
          continue
        elif n == 0:
          c, sx, sy, n = datum.c, datum.sx, datum.sy, datum.n
        else:
          c += (
              datum.c + (sx / n - datum.sx / datum.n) *
              (sy / n - datum.sy / datum.n) * n * datum.n / (n + datum.n))
          sx += datum.sx
          sy += datum.sy
          n += datum.n
      if n < max(2, ddof, min_periods or 0):
        return float('nan')
      else:
        return c / (n - ddof)

    moments = expressions.ComputedExpression(
        'compute_co_moments',
        compute_co_moments, [self._expr, other._expr],
        requires_partition_by=partitionings.Index())

    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'combine_co_moments',
              combine_co_moments, [moments],
              requires_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  @frame_base.maybe_inplace
  def dropna(self, **kwargs):
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'dropna',
            lambda df: df.dropna(**kwargs), [self._expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=partitionings.Arbitrary()))

  isnull = isna = frame_base._elementwise_method('isna', base=pd.Series)
  notnull = notna = frame_base._elementwise_method('notna', base=pd.Series)

  items = frame_base.wont_implement_method(
      pd.Series, 'items', reason="non-deferred-result")
  iteritems = frame_base.wont_implement_method(
      pd.Series, 'iteritems', reason="non-deferred-result")
  tolist = frame_base.wont_implement_method(
      pd.Series, 'tolist', reason="non-deferred-result")
  to_numpy = frame_base.wont_implement_method(
      pd.Series, 'to_numpy', reason="non-deferred-result")
  to_string = frame_base.wont_implement_method(
      pd.Series, 'to_string', reason="non-deferred-result")

  def aggregate(self, func, axis=0, *args, **kwargs):
    if isinstance(func, list) and len(func) > 1:
      # Aggregate each column separately, then stick them all together.
      rows = [self.agg([f], *args, **kwargs) for f in func]
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'join_aggregate',
              lambda *rows: pd.concat(rows), [row._expr for row in rows]))
    else:
      # We're only handling a single column.
      base_func = func[0] if isinstance(func, list) else func
      if _is_associative(base_func) and not args and not kwargs:
        intermediate = expressions.ComputedExpression(
            'pre_aggregate',
            lambda s: s.agg([base_func], *args, **kwargs), [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Singleton())
        allow_nonparallel_final = True
      else:
        intermediate = self._expr
        allow_nonparallel_final = None  # i.e. don't change the value
      with expressions.allow_non_parallel_operations(allow_nonparallel_final):
        return frame_base.DeferredFrame.wrap(
            expressions.ComputedExpression(
                'aggregate',
                lambda s: s.agg(func, *args, **kwargs), [intermediate],
                preserves_partition_by=partitionings.Arbitrary(),
                requires_partition_by=partitionings.Singleton()))

  agg = aggregate

  @property
  def axes(self):
    return [self.index]

  clip = frame_base._elementwise_method('clip', base=pd.Series)

  all = frame_base._agg_method('all')
  any = frame_base._agg_method('any')
  count = frame_base._agg_method('count')
  min = frame_base._agg_method('min')
  max = frame_base._agg_method('max')
  prod = product = frame_base._agg_method('prod')
  sum = frame_base._agg_method('sum')
  mean = frame_base._agg_method('mean')
  median = frame_base._agg_method('median')

  argmax = frame_base.wont_implement_method(
      pd.Series, 'argmax', reason='order-sensitive')
  argmin = frame_base.wont_implement_method(
      pd.Series, 'argmin', reason='order-sensitive')
  cummax = frame_base.wont_implement_method(
      pd.Series, 'cummax', reason='order-sensitive')
  cummin = frame_base.wont_implement_method(
      pd.Series, 'cummin', reason='order-sensitive')
  cumprod = frame_base.wont_implement_method(
      pd.Series, 'cumprod', reason='order-sensitive')
  cumsum = frame_base.wont_implement_method(
      pd.Series, 'cumsum', reason='order-sensitive')
  diff = frame_base.wont_implement_method(
      pd.Series, 'diff', reason='order-sensitive')
  first = frame_base.wont_implement_method(
      pd.Series, 'first', reason='order-sensitive')
  head = frame_base.wont_implement_method(
      pd.Series, 'head', reason='order-sensitive')
  interpolate = frame_base.wont_implement_method(
      pd.Series, 'interpolate', reason='order-sensitive')
  last = frame_base.wont_implement_method(
      pd.Series, 'last', reason='order-sensitive')
  searchsorted = frame_base.wont_implement_method(
      pd.Series, 'searchsorted', reason='order-sensitive')
  shift = frame_base.wont_implement_method(
      pd.Series, 'shift', reason='order-sensitive')
  tail = frame_base.wont_implement_method(
      pd.Series, 'tail', reason='order-sensitive')

  filter = frame_base._elementwise_method('filter', base=pd.Series)

  memory_usage = frame_base.wont_implement_method(
      pd.Series, 'memory_usage', reason="non-deferred-result")

  # In Series __contains__ checks the index
  __contains__ = frame_base.wont_implement_method(
      pd.Series, '__contains__', reason="non-deferred-result")

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def nlargest(self, keep, **kwargs):
    # TODO(robertwb): Document 'any' option.
    # TODO(robertwb): Consider (conditionally) defaulting to 'any' if no
    # explicit keep parameter is requested.
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError(
          "nlargest(keep={keep!r}) is not supported because it is "
          "order sensitive. Only keep=\"all\" is supported.",
          reason="order-sensitive")
    kwargs['keep'] = keep
    per_partition = expressions.ComputedExpression(
        'nlargest-per-partition',
        lambda df: df.nlargest(**kwargs), [self._expr],
        preserves_partition_by=partitionings.Arbitrary(),
        requires_partition_by=partitionings.Arbitrary())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'nlargest',
              lambda df: df.nlargest(**kwargs), [per_partition],
              preserves_partition_by=partitionings.Arbitrary(),
              requires_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def nsmallest(self, keep, **kwargs):
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError(
          "nsmallest(keep={keep!r}) is not supported because it is "
          "order sensitive. Only keep=\"all\" is supported.",
          reason="order-sensitive")
    kwargs['keep'] = keep
    per_partition = expressions.ComputedExpression(
        'nsmallest-per-partition',
        lambda df: df.nsmallest(**kwargs), [self._expr],
        preserves_partition_by=partitionings.Arbitrary(),
        requires_partition_by=partitionings.Arbitrary())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'nsmallest',
              lambda df: df.nsmallest(**kwargs), [per_partition],
              preserves_partition_by=partitionings.Arbitrary(),
              requires_partition_by=partitionings.Singleton()))

  @property
  def is_unique(self):
    def set_index(s):
      s = s[:]
      s.index = s
      return s

    self_index = expressions.ComputedExpression(
        'set_index',
        set_index, [self._expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Singleton())

    is_unique_distributed = expressions.ComputedExpression(
        'is_unique_distributed',
        lambda s: pd.Series(s.is_unique), [self_index],
        requires_partition_by=partitionings.Index(),
        preserves_partition_by=partitionings.Singleton())

    with expressions.allow_non_parallel_operations():
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'combine',
              lambda s: s.all(), [is_unique_distributed],
              requires_partition_by=partitionings.Singleton(),
              preserves_partition_by=partitionings.Singleton()))

  plot = frame_base.wont_implement_method(
      pd.Series, 'plot', reason="plotting-tools")
  pop = frame_base.wont_implement_method(
      pd.Series, 'pop', reason="non-deferred-result")

  rename_axis = frame_base._elementwise_method('rename_axis', base=pd.Series)

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  @frame_base.maybe_inplace
  def replace(self, to_replace, value, limit, method, **kwargs):
    if method is not None and not isinstance(to_replace,
                                             dict) and value is None:
      # pandas only relies on method if to_replace is not a dictionary, and
      # value is None
      raise frame_base.WontImplementError(
          f"replace(method={method!r}) is not supported because it is "
          "order sensitive. Only replace(method=None) is supported.",
          reason="order-sensitive")

    if limit is None:
      requires_partition_by = partitionings.Arbitrary()
    else:
      requires_partition_by = partitionings.Singleton()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'replace',
            lambda df: df.replace(
                to_replace=to_replace,
                value=value,
                limit=limit,
                method=method,
                **kwargs), [self._expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=requires_partition_by))

  round = frame_base._elementwise_method('round', base=pd.Series)

  take = frame_base.wont_implement_method(
      pd.Series, 'take', reason='deprecated')

  to_dict = frame_base.wont_implement_method(
      pd.Series, 'to_dict', reason="non-deferred-result")

  to_frame = frame_base._elementwise_method('to_frame', base=pd.Series)

  def unique(self, as_series=False):
    if not as_series:
      raise frame_base.WontImplementError(
          "unique() is not supported by default because it produces a "
          "non-deferred result, a numpy array. You may call it with "
          "unique(as_series=True) to get the result as a DeferredSeries",
          reason="non-deferred-result")
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'unique',
            lambda df: pd.Series(df.unique()), [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=partitionings.Singleton()))

  def update(self, other):
    self._expr = expressions.ComputedExpression(
        'update',
        lambda df,
        other: df.update(other) or df, [self._expr, other._expr],
        preserves_partition_by=partitionings.Arbitrary(),
        requires_partition_by=partitionings.Index())

  unstack = frame_base.wont_implement_method(
      pd.Series, 'unstack', reason='non-deferred-columns')

  values = property(
      frame_base.wont_implement_method(
          pd.Series, 'values', reason="non-deferred-result"))

  view = frame_base.wont_implement_method(
      pd.Series,
      'view',
      explanation=(
          "because it relies on memory-sharing semantics that are "
          "not compatible with the Beam model"))

  @property
  def str(self):
    return _DeferredStringMethods(self._expr)

  apply = frame_base._elementwise_method('apply', base=pd.Series)
  map = frame_base._elementwise_method('map', base=pd.Series)
  # TODO(BEAM-11636): Implement transform using type inference to determine the
  # proxy
  #transform = frame_base._elementwise_method('transform', base=pd.Series)


@populate_not_implemented(pd.DataFrame)
@frame_base.DeferredFrame._register_for(pd.DataFrame)
class DeferredDataFrame(DeferredDataFrameOrSeries):
  @property
  def T(self):
    return self.transpose()

  @property
  def columns(self):
    return self._expr.proxy().columns

  @columns.setter
  def columns(self, columns):
    def set_columns(df):
      df = df.copy()
      df.columns = columns
      return df

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'set_columns',
            set_columns, [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()))

  def keys(self):
    return self.columns

  def __getattr__(self, name):
    # Column attribute access.
    if name in self._expr.proxy().columns:
      return self[name]
    else:
      return object.__getattribute__(self, name)

  def __getitem__(self, key):
    # TODO: Replicate pd.DataFrame.__getitem__ logic
    if isinstance(key, DeferredSeries) and key._expr.proxy().dtype == bool:
      return self.loc[key]

    elif isinstance(key, frame_base.DeferredBase):
      # Fail early if key is a DeferredBase as it interacts surprisingly with
      # key in self._expr.proxy().columns
      raise NotImplementedError(
          "Indexing with a non-bool deferred frame is not yet supported. "
          "Consider using df.loc[...]")

    elif isinstance(key, slice):
      if _is_null_slice(key):
        return self
      elif _is_integer_slice(key):
        # This depends on the contents of the index.
        raise frame_base.WontImplementError(
            'Use iloc or loc with integer slices.')
      else:
        return self.loc[key]

    elif (
        (isinstance(key, list) and all(key_column in self._expr.proxy().columns
                                       for key_column in key)) or
        key in self._expr.proxy().columns):
      return self._elementwise(lambda df: df[key], 'get_column')

    else:
      raise NotImplementedError(key)

  def __contains__(self, key):
    # Checks if proxy has the given column
    return self._expr.proxy().__contains__(key)

  def __setitem__(self, key, value):
    if isinstance(
        key, str) or (isinstance(key, list) and
                      all(isinstance(c, str)
                          for c in key)) or (isinstance(key, DeferredSeries) and
                                             key._expr.proxy().dtype == bool):
      # yapf: disable
      return self._elementwise(
          lambda df, key, value: df.__setitem__(key, value),
          'set_column',
          (key, value),
          inplace=True)
    else:
      raise NotImplementedError(key)

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def align(self, other, join, axis, copy, level, method, **kwargs):
    if not copy:
      raise frame_base.WontImplementError('align(copy=False)')
    if method is not None:
      raise frame_base.WontImplementError(
          f"align(method={method!r}) is not supported because it is "
          "order sensitive. Only align(method=None) is supported.",
          reason="order-sensitive")
    if kwargs:
      raise NotImplementedError('align(%s)' % ', '.join(kwargs.keys()))

    if level is not None:
      # Could probably get by partitioning on the used levels.
      requires_partition_by = partitionings.Singleton()
    elif axis in ('columns', 1):
      requires_partition_by = partitionings.Arbitrary()
    else:
      requires_partition_by = partitionings.Index()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'align',
            lambda df, other: df.align(other, join=join, axis=axis),
            [self._expr, other._expr],
            requires_partition_by=requires_partition_by,
            preserves_partition_by=partitionings.Arbitrary()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def append(self, other, ignore_index, verify_integrity, sort, **kwargs):
    if not isinstance(other, DeferredDataFrame):
      raise frame_base.WontImplementError(
          "append() only accepts DeferredDataFrame instances, received " +
          str(type(other)))
    if ignore_index:
      raise frame_base.WontImplementError(
          "append(ignore_index=True) is order sensitive because it requires "
          "generating a new index based on the order of the data.",
          reason="order-sensitive")
    if verify_integrity:
      raise frame_base.WontImplementError(
          "append(verify_integrity=True) produces an execution time error")

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'append',
            lambda s, other: s.append(other, sort=sort, **kwargs),
            [self._expr, other._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()
        )
    )

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def set_index(self, keys, **kwargs):
    if isinstance(keys, str):
      keys = [keys]

    if any(isinstance(k, (_DeferredIndex, frame_base.DeferredFrame))
           for k in keys):
      raise NotImplementedError("set_index with Index or Series instances is "
                                "not yet supported (BEAM-11711)")

    return frame_base.DeferredFrame.wrap(
      expressions.ComputedExpression(
          'set_index',
          lambda df: df.set_index(keys, **kwargs),
          [self._expr],
          requires_partition_by=partitionings.Arbitrary(),
          preserves_partition_by=partitionings.Singleton()))

  @property
  def loc(self):
    return _DeferredLoc(self)

  @property
  def iloc(self):
    return _DeferredILoc(self)

  @property
  def axes(self):
    return (self.index, self.columns)

  @property
  def dtypes(self):
    return self._expr.proxy().dtypes

  def assign(self, **kwargs):
    for name, value in kwargs.items():
      if not callable(value) and not isinstance(value, DeferredSeries):
        raise frame_base.WontImplementError(
            f"Unsupported value for new column '{name}': '{value}'. Only "
            "callables and DeferredSeries instances are supported. Other types "
            "make this operation sensitive to the order of the data",
            reason="order-sensitive")
    return self._elementwise(
        lambda df, *args, **kwargs: df.assign(*args, **kwargs),
        'assign',
        other_kwargs=kwargs)

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def explode(self, column, ignore_index):
    # ignoring the index will not preserve it
    preserves = (partitionings.Singleton() if ignore_index
                 else partitionings.Index())
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'explode',
            lambda df: df.explode(column, ignore_index),
            [self._expr],
            preserves_partition_by=preserves,
            requires_partition_by=partitionings.Arbitrary()))



  def aggregate(self, func, axis=0, *args, **kwargs):
    if axis is None:
      # Aggregate across all elements by first aggregating across columns,
      # then across rows.
      return self.agg(func, *args, **dict(kwargs, axis=1)).agg(
          func, *args, **dict(kwargs, axis=0))
    elif axis in (1, 'columns'):
      # This is an easy elementwise aggregation.
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'aggregate',
              lambda df: df.agg(func, axis=1, *args, **kwargs),
              [self._expr],
              requires_partition_by=partitionings.Arbitrary()))
    elif len(self._expr.proxy().columns) == 0 or args or kwargs:
      # For these corner cases, just colocate everything.
      return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'aggregate',
            lambda df: df.agg(func, *args, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Singleton()))
    else:
      # In the general case, compute the aggregation of each column separately,
      # then recombine.
      if not isinstance(func, dict):
        col_names = list(self._expr.proxy().columns)
        func = {col: func for col in col_names}
      else:
        col_names = list(func.keys())
      aggregated_cols = []
      for col in col_names:
        funcs = func[col]
        if not isinstance(funcs, list):
          funcs = [funcs]
        aggregated_cols.append(self[col].agg(funcs, *args, **kwargs))
      # The final shape is different depending on whether any of the columns
      # were aggregated by a list of aggregators.
      with expressions.allow_non_parallel_operations():
        if any(isinstance(funcs, list) for funcs in func.values()):
          return frame_base.DeferredFrame.wrap(
              expressions.ComputedExpression(
                  'join_aggregate',
                  lambda *cols: pd.DataFrame(
                      {col: value for col, value in zip(col_names, cols)}),
                  [col._expr for col in aggregated_cols],
                  requires_partition_by=partitionings.Singleton()))
        else:
          return frame_base.DeferredFrame.wrap(
            expressions.ComputedExpression(
                'join_aggregate',
                  lambda *cols: pd.Series(
                      {col: value[0] for col, value in zip(col_names, cols)}),
                [col._expr for col in aggregated_cols],
                requires_partition_by=partitionings.Singleton(),
                proxy=self._expr.proxy().agg(func, *args, **kwargs)))

  agg = aggregate

  applymap = frame_base._elementwise_method('applymap', base=pd.DataFrame)
  add_prefix = frame_base._elementwise_method('add_prefix', base=pd.DataFrame)
  add_suffix = frame_base._elementwise_method('add_suffix', base=pd.DataFrame)

  memory_usage = frame_base.wont_implement_method(
      pd.DataFrame, 'memory_usage', reason="non-deferred-result")
  info = frame_base.wont_implement_method(
      pd.DataFrame, 'info', reason="non-deferred-result")

  clip = frame_base._elementwise_method(
      'clip', restrictions={'axis': lambda axis: axis in (0, 'index')},
      base=pd.DataFrame)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def corr(self, method, min_periods):
    """Only ``method="pearson"`` can be parallelized. Other methods require
    collecting all data on a single worker (see
    https://s.apache.org/dataframe-non-parallelizable-operations for details).
    """
    if method == 'pearson':
      proxy = self._expr.proxy().corr()
      columns = list(proxy.columns)
      args = []
      arg_indices = []
      for col1, col2 in itertools.combinations(columns, 2):
        arg_indices.append((col1, col2))
        args.append(self[col1].corr(self[col2], method=method,
                                    min_periods=min_periods))
      def fill_matrix(*args):
        data = collections.defaultdict(dict)
        for col in columns:
          data[col][col] = 1.0
        for ix, (col1, col2) in enumerate(arg_indices):
          data[col1][col2] = data[col2][col1] = args[ix]
        return pd.DataFrame(data, columns=columns, index=columns)
      with expressions.allow_non_parallel_operations(True):
        return frame_base.DeferredFrame.wrap(
            expressions.ComputedExpression(
                'fill_matrix',
                fill_matrix,
                [arg._expr for arg in args],
                requires_partition_by=partitionings.Singleton(),
                proxy=proxy))

    else:
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'corr',
              lambda df: df.corr(method=method, min_periods=min_periods),
              [self._expr],
              requires_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def cov(self, min_periods, ddof):
    proxy = self._expr.proxy().corr()
    columns = list(proxy.columns)
    args = []
    arg_indices = []
    for col in columns:
      arg_indices.append((col, col))
      std = self[col].std(ddof)
      args.append(std.apply(lambda x: x*x, 'square'))
    for ix, col1 in enumerate(columns):
      for col2 in columns[ix+1:]:
        arg_indices.append((col1, col2))
        # Note that this set may be different for each pair.
        no_na = self.loc[self[col1].notna() & self[col2].notna()]
        args.append(no_na[col1]._cov_aligned(no_na[col2], min_periods, ddof))
    def fill_matrix(*args):
      data = collections.defaultdict(dict)
      for ix, (col1, col2) in enumerate(arg_indices):
        data[col1][col2] = data[col2][col1] = args[ix]
      return pd.DataFrame(data, columns=columns, index=columns)
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'fill_matrix',
              fill_matrix,
              [arg._expr for arg in args],
              requires_partition_by=partitionings.Singleton(),
              proxy=proxy))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def corrwith(self, other, axis, drop, method):
    if axis not in (0, 'index'):
      raise NotImplementedError('corrwith(axis=%r)' % axis)
    if not isinstance(other, frame_base.DeferredFrame):
      other = frame_base.DeferredFrame.wrap(
          expressions.ConstantExpression(other))

    if isinstance(other, DeferredSeries):
      proxy = self._expr.proxy().corrwith(other._expr.proxy(), method=method)
      self, other = self.align(other, axis=0, join='inner')
      col_names = proxy.index
      other_cols = [other] * len(col_names)
    elif isinstance(other, DeferredDataFrame):
      proxy = self._expr.proxy().corrwith(
          other._expr.proxy(), method=method, drop=drop)
      self, other = self.align(other, axis=0, join='inner')
      col_names = list(
          set(self.columns)
          .intersection(other.columns)
          .intersection(proxy.index))
      other_cols = [other[col_name] for col_name in col_names]
    else:
      # Raise the right error.
      self._expr.proxy().corrwith(other._expr.proxy())
      # Just in case something else becomes valid.
      raise NotImplementedError('corrwith(%s)' % type(other._expr.proxy))

    # Generate expressions to compute the actual correlations.
    corrs = [
        self[col_name].corr(other_col, method)
        for col_name, other_col in zip(col_names, other_cols)]

    # Combine the results
    def fill_dataframe(*args):
      result = proxy.copy(deep=True)
      for col, value in zip(proxy.index, args):
        result[col] = value
      return result
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
          'fill_dataframe',
          fill_dataframe,
          [corr._expr for corr in corrs],
          requires_partition_by=partitionings.Singleton(),
          proxy=proxy))

  cummax = frame_base.wont_implement_method(pd.DataFrame, 'cummax',
                                            reason='order-sensitive')
  cummin = frame_base.wont_implement_method(pd.DataFrame, 'cummin',
                                            reason='order-sensitive')
  cumprod = frame_base.wont_implement_method(pd.DataFrame, 'cumprod',
                                             reason='order-sensitive')
  cumsum = frame_base.wont_implement_method(pd.DataFrame, 'cumsum',
                                            reason='order-sensitive')
  # TODO(BEAM-12071): Consider adding an order-insensitive implementation for
  # diff that relies on the index
  diff = frame_base.wont_implement_method(pd.DataFrame, 'diff',
                                          reason='order-sensitive')
  first = frame_base.wont_implement_method(pd.DataFrame, 'first',
                                           reason='order-sensitive')
  head = frame_base.wont_implement_method(pd.DataFrame, 'head',
                                          reason='order-sensitive')
  interpolate = frame_base.wont_implement_method(pd.DataFrame, 'interpolate',
                                                 reason='order-sensitive')
  last = frame_base.wont_implement_method(pd.DataFrame, 'last',
                                          reason='order-sensitive')
  tail = frame_base.wont_implement_method(pd.DataFrame, 'tail',
                                          reason='order-sensitive')

  def dot(self, other):
    # We want to broadcast the right hand side to all partitions of the left.
    # This is OK, as its index must be the same size as the columns set of self,
    # so cannot be too large.
    class AsScalar(object):
      def __init__(self, value):
        self.value = value

    if isinstance(other, frame_base.DeferredFrame):
      proxy = other._expr.proxy()
      with expressions.allow_non_parallel_operations():
        side = expressions.ComputedExpression(
            'as_scalar',
            lambda df: AsScalar(df),
            [other._expr],
            requires_partition_by=partitionings.Singleton())
    else:
      proxy = pd.DataFrame(columns=range(len(other[0])))
      side = expressions.ConstantExpression(AsScalar(other))

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'dot',
            lambda left, right: left @ right.value,
            [self._expr, side],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary(),
            proxy=proxy))

  __matmul__ = dot

  def mode(self, axis=0, *args, **kwargs):
    if axis == 1 or axis == 'columns':
      # Number of columns is max(number mode values for each row), so we can't
      # determine how many there will be before looking at the data.
      raise frame_base.WontImplementError(
          "mode(axis=columns) is not supported because it produces a variable "
          "number of columns depending on the data.",
          reason="non-deferred-columns")
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'mode',
            lambda df: df.mode(*args, **kwargs),
            [self._expr],
            #TODO(robertwb): Approximate?
            requires_partition_by=partitionings.Singleton(),
            preserves_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def dropna(self, axis, **kwargs):
    # TODO(robertwb): This is a common pattern. Generalize?
    if axis == 1 or axis == 'columns':
      requires_partition_by = partitionings.Singleton()
    else:
      requires_partition_by = partitionings.Arbitrary()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'dropna',
            lambda df: df.dropna(axis=axis, **kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=requires_partition_by))

  def _eval_or_query(self, name, expr, inplace, **kwargs):
    for key in ('local_dict', 'global_dict', 'level', 'target', 'resolvers'):
      if key in kwargs:
        raise NotImplementedError(f"Setting '{key}' is not yet supported")

    # look for '@<py identifier>'
    if re.search(r'\@[^\d\W]\w*', expr, re.UNICODE):
      raise NotImplementedError("Accessing locals with @ is not yet supported "
                                "(BEAM-11202)")

    result_expr = expressions.ComputedExpression(
        name,
        lambda df: getattr(df, name)(expr, **kwargs),
        [self._expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Arbitrary())

    if inplace:
      self._expr = result_expr
    else:
      return frame_base.DeferredFrame.wrap(result_expr)


  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def eval(self, expr, inplace, **kwargs):
    return self._eval_or_query('eval', expr, inplace, **kwargs)

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def query(self, expr, inplace, **kwargs):
    return self._eval_or_query('query', expr, inplace, **kwargs)

  isnull = isna = frame_base._elementwise_method('isna', base=pd.DataFrame)
  notnull = notna = frame_base._elementwise_method('notna', base=pd.DataFrame)

  items = frame_base.wont_implement_method(pd.DataFrame, 'items',
                                           reason="non-deferred-result")
  itertuples = frame_base.wont_implement_method(pd.DataFrame, 'itertuples',
                                                reason="non-deferred-result")
  iterrows = frame_base.wont_implement_method(pd.DataFrame, 'iterrows',
                                              reason="non-deferred-result")
  iteritems = frame_base.wont_implement_method(pd.DataFrame, 'iteritems',
                                               reason="non-deferred-result")

  def _cols_as_temporary_index(self, cols, suffix=''):
    original_index_names = list(self._expr.proxy().index.names)
    new_index_names = [
        '__apache_beam_temp_%d_%s' % (ix, suffix)
        for (ix, _) in enumerate(original_index_names)]
    def reindex(df):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'reindex',
              lambda df:
                  df.rename_axis(index=new_index_names, copy=False)
                  .reset_index().set_index(cols),
              [df._expr],
              preserves_partition_by=partitionings.Singleton(),
              requires_partition_by=partitionings.Arbitrary()))
    def revert(df):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'join_restoreindex',
              lambda df:
                  df.reset_index().set_index(new_index_names)
                  .rename_axis(index=original_index_names, copy=False),
              [df._expr],
              preserves_partition_by=partitionings.Singleton(),
              requires_partition_by=partitionings.Arbitrary()))
    return reindex, revert

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def join(self, other, on, **kwargs):
    if on is not None:
      reindex, revert = self._cols_as_temporary_index(on)
      return revert(reindex(self).join(other, **kwargs))
    if isinstance(other, list):
      other_is_list = True
    else:
      other = [other]
      other_is_list = False
    placeholder = object()
    other_exprs = [
        df._expr for df in other if isinstance(df, frame_base.DeferredFrame)]
    const_others = [
        placeholder if isinstance(df, frame_base.DeferredFrame) else df
        for df in other]
    def fill_placeholders(values):
      values = iter(values)
      filled = [
          next(values) if df is placeholder else df for df in const_others]
      if other_is_list:
        return filled
      else:
        return filled[0]
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'join',
            lambda df, *deferred_others: df.join(
                fill_placeholders(deferred_others), **kwargs),
            [self._expr] + other_exprs,
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=partitionings.Index()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def merge(
      self,
      right,
      on,
      left_on,
      right_on,
      left_index,
      right_index,
      suffixes,
      **kwargs):
    self_proxy = self._expr.proxy()
    right_proxy = right._expr.proxy()
    # Validate with a pandas call.
    _ = self_proxy.merge(
        right_proxy,
        on=on,
        left_on=left_on,
        right_on=right_on,
        left_index=left_index,
        right_index=right_index,
        **kwargs)
    if kwargs.get('how', None) == 'cross':
      raise NotImplementedError("cross join is not yet implemented (BEAM-9547)")
    if not any([on, left_on, right_on, left_index, right_index]):
      on = [col for col in self_proxy.columns if col in right_proxy.columns]
    if not left_on:
      left_on = on
    if left_on and not isinstance(left_on, list):
      left_on = [left_on]
    if not right_on:
      right_on = on
    if right_on and not isinstance(right_on, list):
      right_on = [right_on]

    if left_index:
      indexed_left = self
    else:
      indexed_left = self.set_index(left_on, drop=False)

    if right_index:
      indexed_right = right
    else:
      indexed_right = right.set_index(right_on, drop=False)

    if left_on and right_on:
      common_cols = set(left_on).intersection(right_on)
      if len(common_cols):
        # When merging on the same column name from both dfs, we need to make
        # sure only one df has the column. Otherwise we end up with
        # two duplicate columns, one with lsuffix and one with rsuffix.
        # It's safe to drop from either because the data has already been duped
        # to the index.
        indexed_right = indexed_right.drop(columns=common_cols)


    merged = frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'merge',
            lambda left, right: left.merge(right,
                                           left_index=True,
                                           right_index=True,
                                           suffixes=suffixes,
                                           **kwargs),
            [indexed_left._expr, indexed_right._expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=partitionings.Index()))

    if left_index or right_index:
      return merged
    else:

      return merged.reset_index(drop=True)

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def nlargest(self, keep, **kwargs):
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError(
          "nlargest(keep={keep!r}) is not supported because it is "
          "order sensitive. Only keep=\"all\" is supported.",
          reason="order-sensitive")
    kwargs['keep'] = keep
    per_partition = expressions.ComputedExpression(
            'nlargest-per-partition',
            lambda df: df.nlargest(**kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=partitionings.Arbitrary())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'nlargest',
              lambda df: df.nlargest(**kwargs),
              [per_partition],
              preserves_partition_by=partitionings.Singleton(),
              requires_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def nsmallest(self, keep, **kwargs):
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError(
          "nsmallest(keep={keep!r}) is not supported because it is "
          "order sensitive. Only keep=\"all\" is supported.",
          reason="order-sensitive")
    kwargs['keep'] = keep
    per_partition = expressions.ComputedExpression(
            'nsmallest-per-partition',
            lambda df: df.nsmallest(**kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=partitionings.Arbitrary())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'nsmallest',
              lambda df: df.nsmallest(**kwargs),
              [per_partition],
              preserves_partition_by=partitionings.Singleton(),
              requires_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  def nunique(self, **kwargs):
    if kwargs.get('axis', None) in (1, 'columns'):
      requires_partition_by = partitionings.Arbitrary()
      preserves_partition_by = partitionings.Index()
    else:
      # TODO: This could be implemented in a distributed fashion
      requires_partition_by = partitionings.Singleton()
      preserves_partition_by = partitionings.Singleton()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'nunique',
            lambda df: df.nunique(**kwargs),
            [self._expr],
            preserves_partition_by=preserves_partition_by,
            requires_partition_by=requires_partition_by))

  plot = frame_base.wont_implement_method(pd.DataFrame, 'plot',
                                                      reason="plotting-tools")

  def pop(self, item):
    result = self[item]

    self._expr = expressions.ComputedExpression(
            'popped',
            lambda df: df.drop(columns=[item]),
            [self._expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=partitionings.Arbitrary())
    return result

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def quantile(self, q, axis, **kwargs):
    if axis in (1, 'columns') and isinstance(q, list):
      raise frame_base.WontImplementError(
          "quantile(axis=columns) with multiple q values is not supported "
          "because it transposes the input DataFrame. Note computing "
          "an individual quantile across columns (e.g. "
          f"df.quantile(q={q[0]!r}, axis={axis!r}) is supported.",
          reason="non-deferred-columns")

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'quantile',
            lambda df: df.quantile(q=q, axis=axis, **kwargs),
            [self._expr],
            # TODO(BEAM-12167): Provide an option for approximate distributed
            # quantiles
            requires_partition_by=partitionings.Singleton(),
            preserves_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.maybe_inplace
  def rename(self, **kwargs):
    rename_index = (
        'index' in kwargs
        or kwargs.get('axis', None) in (0, 'index')
        or ('columns' not in kwargs and 'axis' not in kwargs))
    rename_columns = (
        'columns' in kwargs
        or kwargs.get('axis', None) in (1, 'columns'))

    if rename_index:
      # Technically, it's still partitioned by index, but it's no longer
      # partitioned by the hash of the index.
      preserves_partition_by = partitionings.Singleton()
    else:
      preserves_partition_by = partitionings.Index()

    if kwargs.get('errors', None) == 'raise' and rename_index:
      # Renaming index with checking requires global index.
      requires_partition_by = partitionings.Singleton()
    else:
      requires_partition_by = partitionings.Arbitrary()

    proxy = None
    if rename_index:
      # The proxy can't be computed by executing rename, it will error
      # renaming the index.
      if rename_columns:
        # Note if both are being renamed, index and columns must be specified
        # (not axis)
        proxy = self._expr.proxy().rename(**{k: v for (k, v) in kwargs.items()
                                             if not k == 'index'})
      else:
        # No change in columns, reuse proxy
        proxy = self._expr.proxy()

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'rename',
            lambda df: df.rename(**kwargs),
            [self._expr],
            proxy=proxy,
            preserves_partition_by=preserves_partition_by,
            requires_partition_by=requires_partition_by))

  rename_axis = frame_base._elementwise_method('rename_axis', base=pd.DataFrame)

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def replace(self, limit, **kwargs):
    if limit is None:
      requires_partition_by = partitionings.Arbitrary()
    else:
      requires_partition_by = partitionings.Singleton()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'replace',
            lambda df: df.replace(limit=limit, **kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def reset_index(self, level=None, **kwargs):
    # TODO: Docs should note that the index is not in the same order as it would
    # be with pandas. Technically an order sensitive operation
    if level is not None and not isinstance(level, (tuple, list)):
      level = [level]
    if level is None or len(level) == self._expr.proxy().index.nlevels:
      # TODO: Could do distributed re-index with offsets.
      requires_partition_by = partitionings.Singleton()
    else:
      requires_partition_by = partitionings.Arbitrary()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'reset_index',
            lambda df: df.reset_index(level=level, **kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def round(self, decimals, *args, **kwargs):

    if isinstance(decimals, frame_base.DeferredFrame):
      # Disallow passing a deferred Series in, our current partitioning model
      # prevents us from using it correctly.
      raise NotImplementedError("Passing a deferred series to round() is not "
                                "supported, please use a concrete pd.Series "
                                "instance or a dictionary")

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'round',
            lambda df: df.round(decimals, *args, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Index()
        )
    )

  select_dtypes = frame_base._elementwise_method('select_dtypes',
                                                 base=pd.DataFrame)

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def shift(self, axis, **kwargs):
    if 'freq' in kwargs:
      raise frame_base.WontImplementError('data-dependent')
    if axis == 1 or axis == 'columns':
      requires_partition_by = partitionings.Arbitrary()
    else:
      requires_partition_by = partitionings.Singleton()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'shift',
            lambda df: df.shift(axis=axis, **kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  shape = property(frame_base.wont_implement_method(
      pd.DataFrame, 'shape', reason="non-deferred-result"))

  stack = frame_base._elementwise_method('stack', base=pd.DataFrame)

  all = frame_base._agg_method('all')
  any = frame_base._agg_method('any')
  count = frame_base._agg_method('count')
  max = frame_base._agg_method('max')
  min = frame_base._agg_method('min')
  prod = product = frame_base._agg_method('prod')
  sum = frame_base._agg_method('sum')
  mean = frame_base._agg_method('mean')
  median = frame_base._agg_method('median')

  take = frame_base.wont_implement_method(pd.DataFrame, 'take',
                                          reason='deprecated')

  to_records = frame_base.wont_implement_method(pd.DataFrame, 'to_records',
                                                reason="non-deferred-result")
  to_dict = frame_base.wont_implement_method(pd.DataFrame, 'to_dict',
                                             reason="non-deferred-result")
  to_numpy = frame_base.wont_implement_method(pd.DataFrame, 'to_numpy',
                                              reason="non-deferred-result")
  to_string = frame_base.wont_implement_method(pd.DataFrame, 'to_string',
                                               reason="non-deferred-result")

  to_sparse = frame_base.wont_implement_method(pd.DataFrame, 'to_sparse',
                                               reason="non-deferred-result")

  transpose = frame_base.wont_implement_method(
      pd.DataFrame, 'transpose', reason='non-deferred-columns')

  def unstack(self, *args, **kwargs):
    if self._expr.proxy().index.nlevels == 1:
      return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'unstack',
            lambda df: df.unstack(*args, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Index()))
    else:
      raise frame_base.WontImplementError(
          "unstack() is not supported on DataFrames with a multiple indexes, "
          "because the columns in the output depend on the input data.",
          reason="non-deferred-columns")

  update = frame_base._proxy_method(
      'update',
      inplace=True,
      base=pd.DataFrame,
      requires_partition_by=partitionings.Index(),
      preserves_partition_by=partitionings.Arbitrary())

  values = property(frame_base.wont_implement_method(
      pd.DataFrame, 'values', reason="non-deferred-result"))


for io_func in dir(io):
  if io_func.startswith('to_'):
    setattr(DeferredDataFrame, io_func, getattr(io, io_func))
    setattr(DeferredSeries, io_func, getattr(io, io_func))


for meth in ('filter', ):
  setattr(DeferredDataFrame, meth,
          frame_base._elementwise_method(meth, base=pd.DataFrame))


@populate_not_implemented(DataFrameGroupBy)
class DeferredGroupBy(frame_base.DeferredFrame):
  def __init__(self, expr, kwargs,
               ungrouped: expressions.Expression,
               ungrouped_with_index: expressions.Expression,
               grouping_columns,
               grouping_indexes,
               projection=None):
    """This object represents the result of::

        ungrouped.groupby(level=[grouping_indexes + grouping_columns],
                          **kwargs)[projection]

    :param expr: An expression to compute a pandas GroupBy object. Convenient
        for unliftable aggregations.
    :param ungrouped: An expression to compute the DataFrame pre-grouping, the
        (Multi)Index contains only the grouping columns/indexes.
    :param ungrouped_with_index: Same as ungrouped, except the index includes
        all of the original indexes as well as any grouping columns. This is
        important for operations that expose the original index, e.g. .apply(),
        but we only use it when necessary to avoid unnessary data transfer and
        GBKs.
    :param grouping_columns: list of column labels that were in the original
        groupby(..) ``by`` parameter. Only relevant for grouped DataFrames.
    :param grouping_indexes: list of index names (or index level numbers) to be
        grouped.
    :param kwargs: Keywords args passed to the original groupby(..) call."""
    super(DeferredGroupBy, self).__init__(expr)
    self._ungrouped = ungrouped
    self._ungrouped_with_index = ungrouped_with_index
    self._projection = projection
    self._grouping_columns = grouping_columns
    self._grouping_indexes = grouping_indexes
    self._kwargs = kwargs

  def __getattr__(self, name):
    return DeferredGroupBy(
        expressions.ComputedExpression(
            'groupby_project',
            lambda gb: getattr(gb, name), [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()),
        self._kwargs,
        self._ungrouped,
        self._ungrouped_with_index,
        self._grouping_columns,
        self._grouping_indexes,
        projection=name)

  def __getitem__(self, name):
    return DeferredGroupBy(
        expressions.ComputedExpression(
            'groupby_project',
            lambda gb: gb[name], [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()),
        self._kwargs,
        self._ungrouped,
        self._ungrouped_with_index,
        self._grouping_columns,
        self._grouping_indexes,
        projection=name)

  def agg(self, fn):
    if not callable(fn):
      # TODO: Add support for strings in (UN)LIFTABLE_AGGREGATIONS. Test by
      # running doctests for pandas.core.groupby.generic
      raise NotImplementedError('GroupBy.agg currently only supports callable '
                                'arguments')
    return DeferredDataFrame(
        expressions.ComputedExpression(
            'agg',
            lambda gb: gb.agg(fn), [self._expr],
            requires_partition_by=partitionings.Index(),
            preserves_partition_by=partitionings.Singleton()))

  def apply(self, fn, *args, **kwargs):
    if self._grouping_columns and not self._projection:
      grouping_columns = self._grouping_columns
      def fn_wrapper(x, *args, **kwargs):
        # TODO(BEAM-11710): Moving a column to an index and back is lossy
        # since indexes dont support as many dtypes. We should keep the original
        # column in groupby() instead. We need it anyway in case the grouping
        # column is projected, which is allowed.

        # Move the columns back to columns
        x = x.assign(**{col: x.index.get_level_values(col)
                        for col in grouping_columns})
        x = x.droplevel(grouping_columns)
        return fn(x, *args, **kwargs)
    else:
      fn_wrapper = fn

    project = _maybe_project_func(self._projection)

    # Unfortunately pandas does not execute fn to determine the right proxy.
    # We run user fn on a proxy here to detect the return type and generate the
    # proxy.
    result = fn_wrapper(project(self._ungrouped_with_index.proxy()))
    if isinstance(result, pd.core.generic.NDFrame):
      proxy = result[:0]

      def index_to_arrays(index):
        return [index.get_level_values(level)
                for level in range(index.nlevels)]

      # The final result will have the grouped indexes + the indexes from the
      # result
      proxy.index = pd.MultiIndex.from_arrays(
          index_to_arrays(self._ungrouped.proxy().index) +
          index_to_arrays(proxy.index),
          names=self._ungrouped.proxy().index.names + proxy.index.names)
    else:
      # The user fn returns some non-pandas type. The expected result is a
      # Series where each element is the result of one user fn call.
      dtype = pd.Series([result]).dtype
      proxy = pd.Series([], dtype=dtype, index=self._ungrouped.proxy().index)

    levels = self._grouping_indexes + self._grouping_columns

    return DeferredDataFrame(
        expressions.ComputedExpression(
            'apply',
            lambda df: project(df.groupby(level=levels)).apply(
                fn_wrapper,
                *args,
                **kwargs),
            [self._ungrouped_with_index],
            proxy=proxy,
            requires_partition_by=partitionings.Index(levels),
            preserves_partition_by=partitionings.Index(levels)))

  aggregate = agg

  hist = frame_base.wont_implement_method(DataFrameGroupBy, 'plot',
                                          reason="plotting-tools")
  plot = frame_base.wont_implement_method(DataFrameGroupBy, 'hist',
                                          reason="plotting-tools")

  first = frame_base.wont_implement_method(
      DataFrameGroupBy, 'first', reason='order-sensitive')
  last = frame_base.wont_implement_method(
      DataFrameGroupBy, 'last', reason='order-sensitive')
  head = frame_base.wont_implement_method(
      DataFrameGroupBy, 'head', reason='order-sensitive')
  tail = frame_base.wont_implement_method(
      DataFrameGroupBy, 'tail', reason='order-sensitive')
  nth = frame_base.wont_implement_method(
      DataFrameGroupBy, 'nth', reason='order-sensitive')
  cumcount = frame_base.wont_implement_method(
      DataFrameGroupBy, 'cumcount', reason='order-sensitive')
  cummax = frame_base.wont_implement_method(
      DataFrameGroupBy, 'cummax', reason='order-sensitive')
  cummin = frame_base.wont_implement_method(
      DataFrameGroupBy, 'cummin', reason='order-sensitive')
  cumsum = frame_base.wont_implement_method(
      DataFrameGroupBy, 'cumsum', reason='order-sensitive')
  cumprod = frame_base.wont_implement_method(
      DataFrameGroupBy, 'cumprod', reason='order-sensitive')

  # TODO(BEAM-12169): Consider allowing this for categorical keys.
  __len__ = frame_base.wont_implement_method(
      DataFrameGroupBy, '__len__', reason="non-deferred-result")
  groups = property(frame_base.wont_implement_method(
      DataFrameGroupBy, 'groups', reason="non-deferred-result"))

def _maybe_project_func(projection: Optional[List[str]]):
  """ Returns identity func if projection is empty or None, else returns
  a function that projects the specified columns. """
  if projection:
    return lambda df: df[projection]
  else:
    return lambda x: x


def _liftable_agg(meth, postagg_meth=None):
  name, agg_func = frame_base.name_and_func(meth)

  if postagg_meth is None:
    post_agg_name, post_agg_func = name, agg_func
  else:
    post_agg_name, post_agg_func = frame_base.name_and_func(postagg_meth)

  def wrapper(self, *args, **kwargs):
    assert isinstance(self, DeferredGroupBy)

    to_group = self._ungrouped.proxy().index
    is_categorical_grouping = any(to_group.get_level_values(i).is_categorical()
                                  for i in self._grouping_indexes)
    groupby_kwargs = self._kwargs

    # Don't include un-observed categorical values in the preagg
    preagg_groupby_kwargs = groupby_kwargs.copy()
    preagg_groupby_kwargs['observed'] = True

    project = _maybe_project_func(self._projection)
    pre_agg = expressions.ComputedExpression(
        'pre_combine_' + name,
        lambda df: agg_func(project(
            df.groupby(level=list(range(df.index.nlevels)),
                   **preagg_groupby_kwargs),
        ), **kwargs),
        [self._ungrouped],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Arbitrary())

    post_agg = expressions.ComputedExpression(
        'post_combine_' + post_agg_name,
        lambda df: post_agg_func(
            df.groupby(level=list(range(df.index.nlevels)), **groupby_kwargs),
            **kwargs),
        [pre_agg],
        requires_partition_by=(partitionings.Singleton()
                               if is_categorical_grouping
                               else partitionings.Index()),
        preserves_partition_by=partitionings.Arbitrary())
    return frame_base.DeferredFrame.wrap(post_agg)

  return wrapper


def _unliftable_agg(meth):
  name, agg_func = frame_base.name_and_func(meth)

  def wrapper(self, *args, **kwargs):
    assert isinstance(self, DeferredGroupBy)

    to_group = self._ungrouped.proxy().index
    is_categorical_grouping = any(to_group.get_level_values(i).is_categorical()
                                  for i in self._grouping_indexes)

    groupby_kwargs = self._kwargs
    project = _maybe_project_func(self._projection)
    post_agg = expressions.ComputedExpression(
        name,
        lambda df: agg_func(project(
            df.groupby(level=list(range(df.index.nlevels)),
                       **groupby_kwargs),
            ), **kwargs),
        [self._ungrouped],
        requires_partition_by=(partitionings.Singleton()
                               if is_categorical_grouping
                               else partitionings.Index()),
        preserves_partition_by=partitionings.Arbitrary())
    return frame_base.DeferredFrame.wrap(post_agg)

  return wrapper

LIFTABLE_AGGREGATIONS = ['all', 'any', 'max', 'min', 'prod', 'sum']
LIFTABLE_WITH_SUM_AGGREGATIONS = ['size', 'count']
UNLIFTABLE_AGGREGATIONS = ['mean', 'median', 'std', 'var']

for meth in LIFTABLE_AGGREGATIONS:
  setattr(DeferredGroupBy, meth, _liftable_agg(meth))
for meth in LIFTABLE_WITH_SUM_AGGREGATIONS:
  setattr(DeferredGroupBy, meth, _liftable_agg(meth, postagg_meth='sum'))
for meth in UNLIFTABLE_AGGREGATIONS:
  setattr(DeferredGroupBy, meth, _unliftable_agg(meth))


def _is_associative(agg_func):
  return agg_func in LIFTABLE_AGGREGATIONS or (
      getattr(agg_func, '__name__', None) in LIFTABLE_AGGREGATIONS
      and agg_func.__module__ in ('numpy', 'builtins'))



@populate_not_implemented(DataFrameGroupBy)
class _DeferredGroupByCols(frame_base.DeferredFrame):
  # It's not clear that all of these make sense in Pandas either...
  agg = aggregate = frame_base._elementwise_method('agg', base=DataFrameGroupBy)
  any = frame_base._elementwise_method('any', base=DataFrameGroupBy)
  all = frame_base._elementwise_method('all', base=DataFrameGroupBy)
  boxplot = frame_base.wont_implement_method(
      DataFrameGroupBy, 'boxplot', reason="plotting-tools")
  describe = frame_base.not_implemented_method('describe')
  diff = frame_base._elementwise_method('diff', base=DataFrameGroupBy)
  fillna = frame_base._elementwise_method('fillna', base=DataFrameGroupBy)
  filter = frame_base._elementwise_method('filter', base=DataFrameGroupBy)
  first = frame_base.wont_implement_method(
      DataFrameGroupBy, 'first', reason="order-sensitive")
  get_group = frame_base._elementwise_method('get_group', base=DataFrameGroupBy)
  head = frame_base.wont_implement_method(
      DataFrameGroupBy, 'head', reason="order-sensitive")
  hist = frame_base.wont_implement_method(
      DataFrameGroupBy, 'hist', reason="plotting-tools")
  idxmax = frame_base._elementwise_method('idxmax', base=DataFrameGroupBy)
  idxmin = frame_base._elementwise_method('idxmin', base=DataFrameGroupBy)
  last = frame_base.wont_implement_method(
      DataFrameGroupBy, 'last', reason="order-sensitive")
  mad = frame_base._elementwise_method('mad', base=DataFrameGroupBy)
  max = frame_base._elementwise_method('max', base=DataFrameGroupBy)
  mean = frame_base._elementwise_method('mean', base=DataFrameGroupBy)
  median = frame_base._elementwise_method('median', base=DataFrameGroupBy)
  min = frame_base._elementwise_method('min', base=DataFrameGroupBy)
  nunique = frame_base._elementwise_method('nunique', base=DataFrameGroupBy)
  plot = frame_base.wont_implement_method(
      DataFrameGroupBy, 'plot', reason="plotting-tools")
  prod = frame_base._elementwise_method('prod', base=DataFrameGroupBy)
  quantile = frame_base._elementwise_method('quantile', base=DataFrameGroupBy)
  shift = frame_base._elementwise_method('shift', base=DataFrameGroupBy)
  size = frame_base._elementwise_method('size', base=DataFrameGroupBy)
  skew = frame_base._elementwise_method('skew', base=DataFrameGroupBy)
  std = frame_base._elementwise_method('std', base=DataFrameGroupBy)
  sum = frame_base._elementwise_method('sum', base=DataFrameGroupBy)
  tail = frame_base.wont_implement_method(
      DataFrameGroupBy, 'tail', reason="order-sensitive")
  take = frame_base.wont_implement_method(
      DataFrameGroupBy, 'take', reason='deprecated')
  tshift = frame_base._elementwise_method('tshift', base=DataFrameGroupBy)
  var = frame_base._elementwise_method('var', base=DataFrameGroupBy)

  @property
  def groups(self):
    return self._expr.proxy().groups

  @property
  def indices(self):
    return self._expr.proxy().indices

  @property
  def ndim(self):
    return self._expr.proxy().ndim

  @property
  def ngroups(self):
    return self._expr.proxy().ngroups


@populate_not_implemented(pd.core.indexes.base.Index)
class _DeferredIndex(object):
  def __init__(self, frame):
    self._frame = frame

  @property
  def names(self):
    return self._frame._expr.proxy().index.names

  @names.setter
  def names(self, value):
    def set_index_names(df):
      df = df.copy()
      df.index.names = value
      return df

    self._frame._expr = expressions.ComputedExpression(
      'set_index_names',
      set_index_names,
      [self._frame._expr],
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Arbitrary())

  @property
  def ndim(self):
    return self._frame._expr.proxy().index.ndim

  @property
  def nlevels(self):
    return self._frame._expr.proxy().index.nlevels

  def __getattr__(self, name):
    raise NotImplementedError('index.%s' % name)


@populate_not_implemented(pd.core.indexing._LocIndexer)
class _DeferredLoc(object):
  def __init__(self, frame):
    self._frame = frame

  def __getitem__(self, index):
    if isinstance(index, tuple):
      rows, cols = index
      return self[rows][cols]
    elif isinstance(index, list) and index and isinstance(index[0], bool):
      # Aligned by numerical index.
      raise NotImplementedError(type(index))
    elif isinstance(index, list):
      # Select rows, but behaves poorly on missing values.
      raise NotImplementedError(type(index))
    elif isinstance(index, slice):
      args = [self._frame._expr]
      func = lambda df: df.loc[index]
    elif isinstance(index, frame_base.DeferredFrame):
      args = [self._frame._expr, index._expr]
      func = lambda df, index: df.loc[index]
    elif callable(index):

      def checked_callable_index(df):
        computed_index = index(df)
        if isinstance(computed_index, tuple):
          row_index, _ = computed_index
        else:
          row_index = computed_index
        if isinstance(row_index, list) and row_index and isinstance(
            row_index[0], bool):
          raise NotImplementedError(type(row_index))
        elif not isinstance(row_index, (slice, pd.Series)):
          raise NotImplementedError(type(row_index))
        return computed_index

      args = [self._frame._expr]
      func = lambda df: df.loc[checked_callable_index]
    else:
      raise NotImplementedError(type(index))

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'loc',
            func,
            args,
            requires_partition_by=(
                partitionings.Index()
                if len(args) > 1
                else partitionings.Arbitrary()),
            preserves_partition_by=partitionings.Arbitrary()))

  __setitem__ = frame_base.not_implemented_method('loc.setitem')

@populate_not_implemented(pd.core.indexing._iLocIndexer)
class _DeferredILoc(object):
  def __init__(self, frame):
    self._frame = frame

  def __getitem__(self, index):
    if isinstance(index, tuple):
      rows, _ = index
      if rows != slice(None, None, None):
        raise frame_base.WontImplementError(
            "Using iloc to select rows is not supported because it's "
            "position-based indexing is sensitive to the order of the data.",
            reason="order-sensitive")
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'iloc',
              lambda df: df.iloc[index],
              [self._frame._expr],
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))
    else:
      raise frame_base.WontImplementError(
          "Using iloc to select rows is not supported because it's "
          "position-based indexing is sensitive to the order of the data.",
          reason="order-sensitive")

  def __setitem__(self, index, value):
    raise frame_base.WontImplementError(
        "Using iloc to mutate a frame is not supported because it's "
        "position-based indexing is sensitive to the order of the data.",
        reason="order-sensitive")


class _DeferredStringMethods(frame_base.DeferredBase):
  @frame_base.args_to_kwargs(pd.core.strings.StringMethods)
  @frame_base.populate_defaults(pd.core.strings.StringMethods)
  def cat(self, others, join, **kwargs):
    if others is None:
      # Concatenate series into a single String
      requires = partitionings.Singleton()
      func = lambda df: df.str.cat(join=join, **kwargs)
      args = [self._expr]

    elif (isinstance(others, frame_base.DeferredBase) or
         (isinstance(others, list) and
          all(isinstance(other, frame_base.DeferredBase) for other in others))):

      if isinstance(others, frame_base.DeferredBase):
        others = [others]

      requires = partitionings.Index()
      def func(*args):
        return args[0].str.cat(others=args[1:], join=join, **kwargs)
      args = [self._expr] + [other._expr for other in others]

    else:
      raise frame_base.WontImplementError(
          "others must be None, DeferredSeries, or List[DeferredSeries] "
          f"(encountered {type(others)}). Other types are not supported "
          "because they make this operation sensitive to the order of the "
          "data.", reason="order-sensitive")

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'cat',
            func,
            args,
            requires_partition_by=requires,
            preserves_partition_by=partitionings.Arbitrary()))

  @frame_base.args_to_kwargs(pd.core.strings.StringMethods)
  def repeat(self, repeats):
    if isinstance(repeats, int):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'repeat',
              lambda series: series.str.repeat(repeats),
              [self._expr],
              # TODO(BEAM-11155): Defer to pandas to compute this proxy.
              # Currently it incorrectly infers dtype bool, may require upstream
              # fix.
              proxy=self._expr.proxy(),
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))
    elif isinstance(repeats, frame_base.DeferredBase):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'repeat',
              lambda series, repeats_series: series.str.repeat(repeats_series),
              [self._expr, repeats._expr],
              # TODO(BEAM-11155): Defer to pandas to compute this proxy.
              # Currently it incorrectly infers dtype bool, may require upstream
              # fix.
              proxy=self._expr.proxy(),
              requires_partition_by=partitionings.Index(),
              preserves_partition_by=partitionings.Arbitrary()))
    elif isinstance(repeats, list):
      raise frame_base.WontImplementError(
          "str.repeat(repeats=) repeats must be an int or a DeferredSeries. "
          "Lists are not supported because they make this operation sensitive "
          "to the order of the data.", reason="order-sensitive")
    else:
      raise TypeError("str.repeat(repeats=) value must be an int or a "
                      f"DeferredSeries (encountered {type(repeats)}).")

  get_dummies = frame_base.wont_implement_method(
      pd.core.strings.StringMethods, 'get_dummies',
      reason='non-deferred-columns')


ELEMENTWISE_STRING_METHODS = [
            'capitalize',
            'casefold',
            'contains',
            'count',
            'endswith',
            'extract',
            'extractall',
            'findall',
            'fullmatch',
            'get',
            'isalnum',
            'isalpha',
            'isdecimal',
            'isdigit',
            'islower',
            'isnumeric',
            'isspace',
            'istitle',
            'isupper',
            'join',
            'len',
            'lower',
            'lstrip',
            'match',
            'pad',
            'partition',
            'replace',
            'rpartition',
            'rsplit',
            'rstrip',
            'slice',
            'slice_replace',
            'split',
            'startswith',
            'strip',
            'swapcase',
            'title',
            'upper',
            'wrap',
            'zfill',
            '__getitem__',
]

def make_str_func(method):
  def func(df, *args, **kwargs):
    try:
      df_str = df.str
    except AttributeError:
      # If there's a non-string value in a Series passed to .str method, pandas
      # will generally just replace it with NaN in the result. However if
      # there are _only_ non-string values, pandas will raise:
      #
      #   AttributeError: Can only use .str accessor with string values!
      #
      # This can happen to us at execution time if we split a partition that is
      # only non-strings. This branch just replaces all those values with NaN
      # in that case.
      return df.map(lambda _: np.nan)
    else:
      return getattr(df_str, method)(*args, **kwargs)

  return func

for method in ELEMENTWISE_STRING_METHODS:
  setattr(_DeferredStringMethods,
          method,
          frame_base._elementwise_method(make_str_func(method),
                                         name=method,
                                         base=pd.core.strings.StringMethods))

for base in ['add',
             'sub',
             'mul',
             'div',
             'truediv',
             'floordiv',
             'mod',
             'divmod',
             'pow',
             'and',
             'or']:
  for p in ['%s', 'r%s', '__%s__', '__r%s__']:
    # TODO: non-trivial level?
    name = p % base
    if hasattr(pd.Series, name):
      setattr(
          DeferredSeries,
          name,
          frame_base._elementwise_method(name, restrictions={'level': None},
                                         base=pd.Series))
    if hasattr(pd.DataFrame, name):
      setattr(
          DeferredDataFrame,
          name,
          frame_base._elementwise_method(name, restrictions={'level': None},
                                         base=pd.DataFrame))
  inplace_name = '__i%s__' % base
  if hasattr(pd.Series, inplace_name):
    setattr(
        DeferredSeries,
        inplace_name,
        frame_base._elementwise_method(inplace_name, inplace=True,
                                       base=pd.Series))
  if hasattr(pd.DataFrame, inplace_name):
    setattr(
        DeferredDataFrame,
        inplace_name,
        frame_base._elementwise_method(inplace_name, inplace=True,
                                       base=pd.DataFrame))

for name in ['lt', 'le', 'gt', 'ge', 'eq', 'ne']:
  for p in '%s', '__%s__':
    # Note that non-underscore name is used for both as the __xxx__ methods are
    # order-sensitive.
    setattr(DeferredSeries, p % name,
            frame_base._elementwise_method(name, base=pd.Series))
    setattr(DeferredDataFrame, p % name,
            frame_base._elementwise_method(name, base=pd.DataFrame))

for name in ['__neg__', '__pos__', '__invert__']:
  setattr(DeferredSeries, name,
          frame_base._elementwise_method(name, base=pd.Series))
  setattr(DeferredDataFrame, name,
          frame_base._elementwise_method(name, base=pd.DataFrame))

DeferredSeries.multiply = DeferredSeries.mul  # type: ignore
DeferredDataFrame.multiply = DeferredDataFrame.mul  # type: ignore


def _slice_parts(s):
  yield s.start
  yield s.stop
  yield s.step

def _is_null_slice(s):
  return isinstance(s, slice) and all(x is None for x in _slice_parts(s))

def _is_integer_slice(s):
  return isinstance(s, slice) and all(
      x is None or isinstance(x, int)
      for x in _slice_parts(s)) and not _is_null_slice(s)
