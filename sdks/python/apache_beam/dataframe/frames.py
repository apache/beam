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
import warnings
from typing import Optional

import numpy as np
import pandas as pd
from pandas._libs import lib
from pandas.api.types import is_float_dtype
from pandas.api.types import is_int64_dtype
from pandas.api.types import is_list_like
from pandas.core.groupby.generic import DataFrameGroupBy

from apache_beam.dataframe import convert
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import io
from apache_beam.dataframe import partitionings
from apache_beam.transforms import PTransform

__all__ = [
    'DeferredSeries',
    'DeferredDataFrame',
]

# Get major, minor version
PD_VERSION = tuple(map(int, pd.__version__.split('.')[0:2]))


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
              property(
                  frame_base.not_implemented_method(attr, base_type=pd_type)))
        elif callable(pd_value):
          setattr(
              deferred_type,
              attr,
              frame_base.not_implemented_method(attr, base_type=pd_type))
    return deferred_type

  return wrapper


def _fillna_alias(method):
  def wrapper(self, *args, **kwargs):
    return self.fillna(*args, method=method, **kwargs)

  wrapper.__name__ = method
  wrapper.__doc__ = (
      f'{method} is only supported for axis="columns". '
      'axis="index" is order-sensitive.')

  return frame_base.with_docs_from(pd.DataFrame)(
      frame_base.args_to_kwargs(pd.DataFrame)(
          frame_base.populate_defaults(pd.DataFrame)(wrapper)))


# These aggregations are commutative and associative, they can be trivially
# "lifted" (i.e. we can pre-aggregate on partitions, group, then post-aggregate)
LIFTABLE_AGGREGATIONS = ['all', 'any', 'max', 'min', 'prod', 'sum']
# These aggregations can be lifted if post-aggregated with "sum"
LIFTABLE_WITH_SUM_AGGREGATIONS = ['size', 'count']
UNLIFTABLE_AGGREGATIONS = [
    'mean',
    'median',
    'quantile',
    'describe',
    'sem',
    'skew',
    'kurt',
    'kurtosis',
    'std',
    'var',
    'corr',
    'cov',
    'nunique',
]
# mad was removed in Pandas 2.0.
if PD_VERSION < (2, 0):
  UNLIFTABLE_AGGREGATIONS.append('mad')

ALL_AGGREGATIONS = (
    LIFTABLE_AGGREGATIONS + LIFTABLE_WITH_SUM_AGGREGATIONS +
    UNLIFTABLE_AGGREGATIONS)

# These aggregations have specialized distributed implementations on
# DeferredSeries, which are re-used in DeferredFrame. Note they are *not* used
# for grouped aggregations, since they generally require tracking multiple
# intermediate series, which is difficult to lift in groupby.
HAND_IMPLEMENTED_GLOBAL_AGGREGATIONS = {
    'quantile',
    'std',
    'var',
    'mean',
    'nunique',
    'corr',
    'cov',
    'skew',
    'kurt',
    'kurtosis'
}
UNLIFTABLE_GLOBAL_AGGREGATIONS = (
    set(UNLIFTABLE_AGGREGATIONS) - set(HAND_IMPLEMENTED_GLOBAL_AGGREGATIONS))


def _agg_method(base, func):
  def wrapper(self, *args, **kwargs):
    return self.agg(func, *args, **kwargs)

  if func in UNLIFTABLE_GLOBAL_AGGREGATIONS:
    wrapper.__doc__ = (
        f"``{func}`` cannot currently be parallelized. It will "
        "require collecting all data on a single node.")
  wrapper.__name__ = func

  return frame_base.with_docs_from(base)(wrapper)


# Docstring to use for head and tail (commonly used to peek at datasets)
_PEEK_METHOD_EXPLANATION = (
    "because it is `order-sensitive "
    "<https://s.apache.org/dataframe-order-sensitive-operations>`_.\n\n"
    "If you want to peek at a large dataset consider using interactive Beam's "
    ":func:`ib.collect "
    "<apache_beam.runners.interactive.interactive_beam.collect>` "
    "with ``n`` specified, or :meth:`sample`. If you want to find the "
    "N largest elements, consider using :meth:`DeferredDataFrame.nlargest`.")


class DeferredDataFrameOrSeries(frame_base.DeferredFrame):
  def _render_indexes(self):
    if self.index.nlevels == 1:
      return 'index=' + (
          '<unnamed>' if self.index.name is None else repr(self.index.name))
    else:
      return 'indexes=[' + ', '.join(
          '<unnamed>' if ix is None else repr(ix)
          for ix in self.index.names) + ']'

  __array__ = frame_base.wont_implement_method(
      pd.Series, '__array__', reason="non-deferred-result")

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def drop(self, labels, axis, index, columns, errors, **kwargs):
    """drop is not parallelizable when dropping from the index and
    ``errors="raise"`` is specified. It requires collecting all data on a single
    node in order to detect if one of the index values is missing."""
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
      # TODO: This could be parallelized by putting index values in a
      # ConstantExpression and partitioning by index.
      requires = partitionings.Singleton(
          reason=(
              "drop(errors='raise', axis='index') is not currently "
              "parallelizable. This requires collecting all data on a single "
              f"node in order to detect if one of {index!r} is missing."))
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

  @frame_base.with_docs_from(pd.DataFrame)
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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  def swaplevel(self, **kwargs):
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'swaplevel',
            lambda df: df.swaplevel(**kwargs), [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def fillna(self, value, method, axis, limit, **kwargs):
    """When ``axis="index"``, both ``method`` and ``limit`` must be ``None``.
    otherwise this operation is order-sensitive."""
    # Default value is None, but is overriden with index.
    axis = axis or 'index'

    if axis in (0, 'index'):
      if method is not None:
        raise frame_base.WontImplementError(
            f"fillna(method={method!r}, axis={axis!r}) is not supported "
            "because it is order-sensitive. Only fillna(method=None) is "
            f"supported with axis={axis!r}.",
            reason="order-sensitive")
      if limit is not None:
        raise frame_base.WontImplementError(
            f"fillna(limit={method!r}, axis={axis!r}) is not supported because "
            "it is order-sensitive. Only fillna(limit=None) is supported with "
            f"axis={axis!r}.",
            reason="order-sensitive")

    if isinstance(self, DeferredDataFrame) and isinstance(value,
                                                          DeferredSeries):
      # If self is a DataFrame and value is a Series we want to broadcast value
      # to all partitions of self.
      # This is OK, as its index must be the same size as the columns set of
      # self, so cannot be too large.
      class AsScalar(object):
        def __init__(self, value):
          self.value = value

      with expressions.allow_non_parallel_operations():
        value_expr = expressions.ComputedExpression(
            'as_scalar',
            lambda df: AsScalar(df), [value._expr],
            requires_partition_by=partitionings.Singleton())

      get_value = lambda x: x.value
      requires = partitionings.Arbitrary()
    elif isinstance(value, frame_base.DeferredBase):
      # For other DeferredBase combinations, use Index partitioning to
      # co-locate on the Index
      value_expr = value._expr
      get_value = lambda x: x
      requires = partitionings.Index()
    else:
      # Default case, pass value through as a constant, no particular
      # partitioning requirement
      value_expr = expressions.ConstantExpression(value)
      get_value = lambda x: x
      requires = partitionings.Arbitrary()

    return frame_base.DeferredFrame.wrap(
        # yapf: disable
        expressions.ComputedExpression(
            'fillna',
            lambda df,
            value: df.fillna(
                get_value(value),
                method=method,
                axis=axis,
                limit=limit,
                **kwargs), [self._expr, value_expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=requires))

  if hasattr(pd.DataFrame, 'ffill'):
    ffill = _fillna_alias('ffill')
  if hasattr(pd.DataFrame, 'bfill'):
    bfill = _fillna_alias('bfill')
  if hasattr(pd.DataFrame, 'backfill'):
    backfill = _fillna_alias('backfill')
  if hasattr(pd.DataFrame, 'pad'):
    pad = _fillna_alias('pad')

  @frame_base.with_docs_from(pd.DataFrame)
  def first(self, offset):
    per_partition = expressions.ComputedExpression(
        'first-per-partition',
        lambda df: df.sort_index().first(offset=offset), [self._expr],
        preserves_partition_by=partitionings.Arbitrary(),
        requires_partition_by=partitionings.Arbitrary())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'first',
              lambda df: df.sort_index().first(offset=offset), [per_partition],
              preserves_partition_by=partitionings.Arbitrary(),
              requires_partition_by=partitionings.Singleton()))

  @frame_base.with_docs_from(pd.DataFrame)
  def last(self, offset):
    per_partition = expressions.ComputedExpression(
        'last-per-partition',
        lambda df: df.sort_index().last(offset=offset), [self._expr],
        preserves_partition_by=partitionings.Arbitrary(),
        requires_partition_by=partitionings.Arbitrary())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'last',
              lambda df: df.sort_index().last(offset=offset), [per_partition],
              preserves_partition_by=partitionings.Arbitrary(),
              requires_partition_by=partitionings.Singleton()))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def groupby(self, by, level, axis, as_index, group_keys, **kwargs):
    """``as_index`` must be ``True``.

    Aggregations grouping by a categorical column with ``observed=False`` set
    are not currently parallelizable
    (`Issue 21827 <https://github.com/apache/beam/issues/21827>`_).
    """
    if not as_index:
      raise NotImplementedError('groupby(as_index=False)')

    if axis in (1, 'columns'):
      return _DeferredGroupByCols(
          expressions.ComputedExpression(
              'groupbycols',
              lambda df: df.groupby(
                  by, axis=axis, group_keys=group_keys, **kwargs), [self._expr],
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()),
          group_keys=group_keys)

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

      def prepend_mapped_index(df):
        df = df.copy()

        index = df.index.to_frame()
        index.insert(0, None, df.index.map(by))

        df.index = pd.MultiIndex.from_frame(
            index, names=[None] + list(df.index.names))
        return df

      to_group_with_index = expressions.ComputedExpression(
          'map_index_keep_orig',
          prepend_mapped_index,
          [self._expr],
          requires_partition_by=partitionings.Arbitrary(),
          # Partitioning by the original indexes is preserved
          preserves_partition_by=partitionings.Index(
              list(range(1, orig_nlevels + 1))))

      grouping_columns = []
      # The index we need to group by is the last one
      grouping_indexes = [0]

    elif isinstance(by, DeferredSeries):
      if isinstance(self, DeferredSeries):

        def set_index(s, by):
          df = pd.DataFrame(s)
          df, by = df.align(by, axis=0, join='inner')
          return df.set_index(by).iloc[:, 0]

        def prepend_index(s, by):
          df = pd.DataFrame(s)
          df, by = df.align(by, axis=0, join='inner')
          return df.set_index([by, df.index]).iloc[:, 0]

      else:

        def set_index(df, by):  # type: ignore
          df, by = df.align(by, axis=0, join='inner')
          return df.set_index(by)

        def prepend_index(df, by):  # type: ignore
          df, by = df.align(by, axis=0, join='inner')
          return df.set_index([by, df.index])

      to_group = expressions.ComputedExpression(
          'set_index',
          set_index, [self._expr, by._expr],
          requires_partition_by=partitionings.Index(),
          preserves_partition_by=partitionings.Singleton())

      orig_nlevels = self._expr.proxy().index.nlevels
      to_group_with_index = expressions.ComputedExpression(
          'prependindex',
          prepend_index, [self._expr, by._expr],
          requires_partition_by=partitionings.Index(),
          preserves_partition_by=partitionings.Index(
              list(range(1, orig_nlevels + 1))))

      grouping_columns = []
      grouping_indexes = [0]

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
        # TODO(https://github.com/apache/beam/issues/20759):
        # It should be possible to do this without creating
        # an expression manually, by using DeferredDataFrame.set_index, i.e.:
        #   to_group_with_index = self.set_index([self.index] +
        #                                        grouping_columns)._expr
        to_group_with_index = expressions.ComputedExpression(
            'move_grouped_columns_to_index',
            lambda df: df.set_index([df.index] + grouping_columns, drop=False),
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
                level=list(range(df.index.nlevels)),
                group_keys=group_keys,
                **kwargs), [to_group],
            requires_partition_by=partitionings.Index(),
            preserves_partition_by=partitionings.Arbitrary()),
        kwargs,
        to_group,
        to_group_with_index,
        grouping_columns=grouping_columns,
        grouping_indexes=grouping_indexes,
        group_keys=group_keys)

  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
  def loc(self):
    return _DeferredLoc(self)

  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
  def iloc(self):
    """Position-based indexing with `iloc` is order-sensitive in almost every
    case. Beam DataFrame users should prefer label-based indexing with `loc`.
    """
    return _DeferredILoc(self)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def reset_index(self, level=None, **kwargs):
    """Dropping the entire index (e.g. with ``reset_index(level=None)``) is
    not parallelizable. It is also only guaranteed that the newly generated
    index values will be unique. The Beam DataFrame API makes no guarantee
    that the same index values as the equivalent pandas operation will be
    generated, because that implementation is order-sensitive."""
    if level is not None and not isinstance(level, (tuple, list)):
      level = [level]
    if level is None or len(level) == self._expr.proxy().index.nlevels:
      # TODO(https://github.com/apache/beam/issues/20859):
      # Could do distributed re-index with offsets.
      requires_partition_by = partitionings.Singleton(
          reason=(
              f"reset_index(level={level!r}) drops the entire index and "
              "creates a new one, so it cannot currently be parallelized "
              "(https://github.com/apache/beam/issues/20859)."))
    else:
      requires_partition_by = partitionings.Arbitrary()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'reset_index',
            lambda df: df.reset_index(level=level, **kwargs), [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  abs = frame_base._elementwise_method('abs', base=pd.core.generic.NDFrame)

  @frame_base.with_docs_from(pd.core.generic.NDFrame)
  @frame_base.args_to_kwargs(pd.core.generic.NDFrame)
  @frame_base.populate_defaults(pd.core.generic.NDFrame)
  def astype(self, dtype, copy, errors):
    """astype is not parallelizable when ``errors="ignore"`` is specified.

    ``copy=False`` is not supported because it relies on memory-sharing
    semantics.

    ``dtype="category`` is not supported because the type of the output column
    depends on the data. Please use ``pd.CategoricalDtype`` with explicit
    categories instead.
    """
    requires = partitionings.Arbitrary()

    if errors == "ignore":
      # We need all data in order to ignore errors and propagate the original
      # data.
      requires = partitionings.Singleton(
          reason=(
              f"astype(errors={errors!r}) is currently not parallelizable, "
              "because all data must be collected on one node to determine if "
              "the original data should be propagated instead."))

    if not copy:
      raise frame_base.WontImplementError(
          f"astype(copy={copy!r}) is not supported because it relies on "
          "memory-sharing semantics that are not compatible with the Beam "
          "model.")

    # An instance of CategoricalDtype is actualy considered equal to the string
    # 'category', so we have to explicitly check if dtype is an instance of
    # CategoricalDtype, and allow it.
    # See https://github.com/apache/beam/issues/23276
    if dtype == 'category' and not isinstance(dtype, pd.CategoricalDtype):
      raise frame_base.WontImplementError(
          "astype(dtype='category') is not supported because the type of the "
          "output column depends on the data. Please use pd.CategoricalDtype "
          "with explicit categories instead.",
          reason="non-deferred-columns")

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'astype',
            lambda df: df.astype(dtype=dtype, copy=copy, errors=errors),
            [self._expr],
            requires_partition_by=requires,
            preserves_partition_by=partitionings.Arbitrary()))

  at_time = frame_base._elementwise_method(
      'at_time', base=pd.core.generic.NDFrame)
  between_time = frame_base._elementwise_method(
      'between_time', base=pd.core.generic.NDFrame)
  copy = frame_base._elementwise_method('copy', base=pd.core.generic.NDFrame)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def replace(self, to_replace, value, limit, method, **kwargs):
    """``method`` is not supported in the Beam DataFrame API because it is
    order-sensitive. It cannot be specified.

    If ``limit`` is specified this operation is not parallelizable."""
    # pylint: disable-next=c-extension-no-member
    value_compare = None if PD_VERSION < (1, 4) else lib.no_default
    if method is not None and not isinstance(to_replace,
                                             dict) and value is value_compare:
      # pandas only relies on method if to_replace is not a dictionary, and
      # value is the <no_default> value. This is different than
      # if ``None`` is explicitly passed for ``value``. In this case, it will be
      # respected
      raise frame_base.WontImplementError(
          f"replace(method={method!r}) is not supported because it is "
          "order sensitive. Only replace(method=None) is supported.",
          reason="order-sensitive")

    if limit is None:
      requires_partition_by = partitionings.Arbitrary()
    else:
      requires_partition_by = partitionings.Singleton(
          reason=(
              f"replace(limit={limit!r}) cannot currently be parallelized. It "
              "requires collecting all data on a single node."))
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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def tz_localize(self, ambiguous, **kwargs):
    """``ambiguous`` cannot be set to ``"infer"`` as its semantics are
    order-sensitive. Similarly, specifying ``ambiguous`` as an
    :class:`~numpy.ndarray` is order-sensitive, but you can achieve similar
    functionality by specifying ``ambiguous`` as a Series."""
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

  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
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

  def length(self):
    """Alternative to ``len(df)`` which returns a deferred result that can be
    used in arithmetic with :class:`DeferredSeries` or
    :class:`DeferredDataFrame` instances."""
    lengths = expressions.ComputedExpression(
        'get_lengths',
        # Wrap scalar results in a Series for easier concatenation later
        lambda df: pd.Series(len(df)),
        [self._expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Singleton())

    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'sum_lengths',
              lambda lengths: lengths.sum(), [lengths],
              requires_partition_by=partitionings.Singleton(),
              preserves_partition_by=partitionings.Singleton()))

  def __len__(self):
    raise frame_base.WontImplementError(
        "len(df) is not currently supported because it produces a non-deferred "
        "result. Consider using df.length() instead.",
        reason="non-deferred-result")

  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
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

  @frame_base.with_docs_from(pd.DataFrame)
  def bool(self):
    # TODO: Documentation about DeferredScalar
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

  @frame_base.with_docs_from(pd.DataFrame)
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
    the dataset, and it likely will not be maintained (see
    https://s.apache.org/dataframe-order-sensitive-operations).

    It is not implemented for ``axis=columns`` because it makes the order of
    the columns depend on the data (see
    https://s.apache.org/dataframe-non-deferred-columns)."""
    if axis in (0, 'index'):
      # axis=index imposes an ordering on the DataFrame rows which we do not
      # support
      raise frame_base.WontImplementError(
          "sort_values(axis=index) is not supported because it imposes an "
          "ordering on the dataset which likely will not be preserved.",
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
            lambda df: df.sort_index(axis=axis, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary(),
        ))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(
      pd.DataFrame, removed_args=["errors"] if PD_VERSION >= (2, 0) else None)
  @frame_base.populate_defaults(
      pd.DataFrame, removed_args=["errors"] if PD_VERSION >= (2, 0) else None)
  @frame_base.maybe_inplace
  def where(self, cond, other, errors, **kwargs):
    """where is not parallelizable when ``errors="ignore"`` is specified."""
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

    # For Pandas 2.0, errors was removed as an argument.
    if PD_VERSION < (2, 0):
      if "errors" in kwargs and kwargs['errors'] == "ignore":
        # We need all data in order to ignore errors and propagate the original
        # data.
        requires = partitionings.Singleton(
            reason=(
                f"where(errors={kwargs['errors']!r}) is currently not "
                "parallelizable, because all data must be collected on one "
                "node to determine if the original data should be propagated "
                "instead."))

      actual_args['errors'] = kwargs['errors'] if 'errors' in kwargs else None

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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def mask(self, cond, **kwargs):
    """mask is not parallelizable when ``errors="ignore"`` is specified."""
    return self.where(~cond, **kwargs)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def truncate(self, before, after, axis):

    if axis in (None, 0, 'index'):

      def truncate(df):
        return df.sort_index().truncate(before=before, after=after, axis=axis)
    else:

      def truncate(df):
        return df.truncate(before=before, after=after, axis=axis)

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'truncate',
            truncate, [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def unstack(self, **kwargs):
    level = kwargs.get('level', -1)

    if self._expr.proxy().index.nlevels == 1:
      if PD_VERSION < (1, 2):
        raise frame_base.WontImplementError(
            "unstack() is not supported when using pandas < 1.2.0\n"
            "Please upgrade to pandas 1.2.0 or higher to use this operation.")
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'unstack',
              lambda s: s.unstack(**kwargs), [self._expr],
              requires_partition_by=partitionings.Index()))
    else:
      # Unstacking MultiIndex objects
      idx = self._expr.proxy().index

      # Converting level (int, str, or combination) to a list of number levels
      level_list = level if isinstance(level, list) else [level]
      level_number_list = [idx._get_level_number(l) for l in level_list]

      # Checking if levels provided are of CategoricalDtype
      if not all(isinstance(idx.levels[l].dtype, (pd.CategoricalDtype,
                                                  pd.BooleanDtype))
                 for l in level_number_list):
        raise frame_base.WontImplementError(
            "unstack() is only supported on DataFrames if unstacked level "
            "is a categorical or boolean column",
            reason="non-deferred-columns")
      else:
        tmp = self._expr.proxy().unstack(**kwargs)
        if isinstance(tmp.columns, pd.MultiIndex):
          levels = []
          for i in range(tmp.columns.nlevels):
            level = tmp.columns.levels[i]
            levels.append(level)
          col_idx = pd.MultiIndex.from_product(levels)
        else:
          if tmp.columns.dtype == 'boolean':
            col_idx = pd.Index([False, True], dtype='boolean')
          else:
            col_idx = pd.CategoricalIndex(tmp.columns.categories)

        if isinstance(self._expr.proxy(), pd.Series):
          proxy_dtype = self._expr.proxy().dtypes
        else:
          # Set dtype to object if more than one value
          dtypes = [d for d in self._expr.proxy().dtypes]
          proxy_dtype = object
          if np.int64 in dtypes:
            proxy_dtype = np.int64
          if np.float64 in dtypes:
            proxy_dtype = np.float64
          if object in dtypes:
            proxy_dtype = object

        proxy = pd.DataFrame(
            columns=col_idx, dtype=proxy_dtype, index=tmp.index)

        with expressions.allow_non_parallel_operations(True):
          return frame_base.DeferredFrame.wrap(
              expressions.ComputedExpression(
                  'unstack',
                  lambda s: pd.concat([proxy, s.unstack(**kwargs)]),
                  [self._expr],
                  proxy=proxy,
                  requires_partition_by=partitionings.Singleton()))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def xs(self, key, axis, level, **kwargs):
    """Note that ``xs(axis='index')`` will raise a ``KeyError`` at execution
    time if the key does not exist in the index."""

    if axis in ('columns', 1):
      # Special case for axis=columns. This is a simple project that raises a
      # KeyError at construction time for missing columns.
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'xs',
              lambda df: df.xs(key, axis=axis, **kwargs), [self._expr],
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))
    elif axis not in ('index', 0):
      # Make sure that user's axis is valid
      raise ValueError(
          "axis must be one of ('index', 0, 'columns', 1). "
          f"got {axis!r}.")

    if not isinstance(key, tuple):
      key_size = 1
      key_series = pd.Series([key], index=[key])
    else:
      key_size = len(key)
      key_series = pd.Series([key], pd.MultiIndex.from_tuples([key]))

    key_expr = expressions.ConstantExpression(
        key_series, proxy=key_series.iloc[:0])

    if level is None:
      reindexed = self
    else:
      if not isinstance(level, list):
        level = [level]

      # If user specifed levels, reindex so those levels are at the beginning.
      # Keep the others and preserve their order.
      level = [
          l if isinstance(l, int) else list(self.index.names).index(l)
          for l in level
      ]

      reindexed = self.reorder_levels(
          level + [i for i in range(self.index.nlevels) if i not in level])

    def xs_partitioned(frame, key):
      if not len(key):
        # key is not in this partition, return empty dataframe
        result = frame.iloc[:0]
        if key_size < frame.index.nlevels:
          return result.droplevel(list(range(key_size)))
        else:
          return result

      # key should be in this partition, call xs. Will raise KeyError if not
      # present.
      return frame.xs(key.item())

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'xs',
            xs_partitioned,
            [reindexed._expr, key_expr],
            requires_partition_by=partitionings.Index(list(range(key_size))),
            # Drops index levels, so partitioning is not preserved
            preserves_partition_by=partitionings.Singleton()))

  @property
  def dtype(self):
    return self._expr.proxy().dtype

  isin = frame_base._elementwise_method('isin', base=pd.DataFrame)
  combine_first = frame_base._elementwise_method(
      'combine_first', base=pd.DataFrame)

  combine = frame_base._proxy_method(
      'combine',
      base=pd.DataFrame,
      requires_partition_by=expressions.partitionings.Singleton(
          reason="combine() is not parallelizable because func might operate "
          "on the full dataset."),
      preserves_partition_by=expressions.partitionings.Singleton())

  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
  def ndim(self):
    return self._expr.proxy().ndim

  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
  def index(self):
    return _DeferredIndex(self)

  @index.setter
  def _set_index(self, value):
    # TODO: assigning the index is generally order-sensitive, but we could
    # support it in some rare cases, e.g. when assigning the index from one
    # of a DataFrame's columns
    raise NotImplementedError(
        "Assigning an index is not yet supported. "
        "Consider using set_index() instead.")

  reindex = frame_base.wont_implement_method(
      pd.DataFrame, 'reindex', reason="order-sensitive")

  hist = frame_base.wont_implement_method(
      pd.DataFrame, 'hist', reason="plotting-tools")

  attrs = property(
      fget=frame_base.wont_implement_method(
          pd.DataFrame, 'attrs', reason='experimental'),
      fset=frame_base.wont_implement_method(
          pd.DataFrame, 'attrs', reason='experimental'),
  )

  reorder_levels = frame_base._proxy_method(
      'reorder_levels',
      base=pd.DataFrame,
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Singleton())

  resample = frame_base.wont_implement_method(
      pd.DataFrame, 'resample', reason='event-time-semantics')

  rolling = frame_base.wont_implement_method(
      pd.DataFrame, 'rolling', reason='event-time-semantics')

  to_xarray = frame_base.wont_implement_method(
      pd.DataFrame, 'to_xarray', reason='non-deferred-result')
  to_clipboard = frame_base.wont_implement_method(
      pd.DataFrame, 'to_clipboard', reason="non-deferred-result")

  swapaxes = frame_base.wont_implement_method(
      pd.Series, 'swapaxes', reason="non-deferred-columns")
  infer_object = frame_base.wont_implement_method(
      pd.Series, 'infer_objects', reason="non-deferred-columns")

  ewm = frame_base.wont_implement_method(
      pd.Series, 'ewm', reason="event-time-semantics")
  expanding = frame_base.wont_implement_method(
      pd.Series, 'expanding', reason="event-time-semantics")

  sparse = property(
      frame_base.not_implemented_method(
          'sparse', '20902', base_type=pd.DataFrame))

  transform = frame_base._elementwise_method('transform', base=pd.DataFrame)

  tz_convert = frame_base._proxy_method(
      'tz_convert',
      base=pd.DataFrame,
      requires_partition_by=partitionings.Arbitrary(),
      # Manipulates index, partitioning is not preserved
      preserves_partition_by=partitionings.Singleton())

  @frame_base.with_docs_from(pd.DataFrame)
  def pipe(self, func, *args, **kwargs):
    if isinstance(func, tuple):
      func, data = func
      kwargs[data] = self
      return func(*args, **kwargs)

    return func(self, *args, **kwargs)


@populate_not_implemented(pd.Series)
@frame_base.DeferredFrame._register_for(pd.Series)
class DeferredSeries(DeferredDataFrameOrSeries):
  def __repr__(self):
    return (
        f'DeferredSeries(name={self.name!r}, dtype={self.dtype}, '
        f'{self._render_indexes()})')

  @property  # type: ignore
  @frame_base.with_docs_from(pd.Series)
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

  @property  # type: ignore
  @frame_base.with_docs_from(pd.Series)
  def hasnans(self):
    has_nans = expressions.ComputedExpression(
        'hasnans',
        lambda s: pd.Series(s.hasnans), [self._expr],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Singleton())

    with expressions.allow_non_parallel_operations():
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'combine_hasnans',
              lambda s: s.any(), [has_nans],
              requires_partition_by=partitionings.Singleton(),
              preserves_partition_by=partitionings.Singleton()))

  @property  # type: ignore
  @frame_base.with_docs_from(pd.Series)
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

  @frame_base.with_docs_from(pd.Series)
  def keys(self):
    return self.index

  # Series.T == transpose. Both are a no-op
  T = frame_base._elementwise_method('T', base=pd.Series)
  transpose = frame_base._elementwise_method('transpose', base=pd.Series)
  shape = property(
      frame_base.wont_implement_method(
          pd.Series, 'shape', reason="non-deferred-result"))

  @frame_base.with_docs_from(pd.Series, removed_method=PD_VERSION >= (2, 0))
  @frame_base.args_to_kwargs(pd.Series, removed_method=PD_VERSION >= (2, 0))
  @frame_base.populate_defaults(pd.Series, removed_method=PD_VERSION >= (2, 0))
  def append(self, to_append, ignore_index, verify_integrity, **kwargs):
    """``ignore_index=True`` is not supported, because it requires generating an
    order-sensitive index."""
    if PD_VERSION >= (2, 0):
      raise frame_base.WontImplementError('append() was removed in Pandas 2.0.')
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
      # We can verify the index is non-unique within index partitioned data.
      requires = partitionings.Index()
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
    `order-sensitive
    <https://s.apache.org/dataframe-order-sensitive-operations>`_.
    Only the default, ``method=None``, is allowed."""
    if level is not None:
      raise NotImplementedError('per-level align')
    if method is not None and method != lib.no_default:
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

  argsort = frame_base.wont_implement_method(
      pd.Series, 'argsort', reason="order-sensitive")

  array = property(
      frame_base.wont_implement_method(
          pd.Series, 'array', reason="non-deferred-result"))

  # We can't reliably predict the output type, it depends on whether `key` is:
  # - not in the index (default_value)
  # - in the index once (constant)
  # - in the index multiple times (Series)
  get = frame_base.wont_implement_method(
      pd.Series, 'get', reason="non-deferred-columns")

  ravel = frame_base.wont_implement_method(
      pd.Series, 'ravel', reason="non-deferred-result")

  slice_shift = frame_base.wont_implement_method(
      pd.Series, 'slice_shift', reason="deprecated")
  tshift = frame_base.wont_implement_method(
      pd.Series, 'tshift', reason="deprecated")

  rename = frame_base._proxy_method(
      'rename',
      base=pd.Series,
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Singleton())

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

  info = frame_base.wont_implement_method(
      pd.Series, 'info', reason="non-deferred-result")

  def _idxmaxmin_helper(self, op, **kwargs):
    if op == 'idxmax':
      func = pd.Series.idxmax
    elif op == 'idxmin':
      func = pd.Series.idxmin
    else:
      raise ValueError(
          "op must be one of ('idxmax', 'idxmin'). "
          f"got {op!r}.")

    def compute_idx(s):
      index = func(s, **kwargs)
      if pd.isna(index):
        return s
      else:
        return s.loc[[index]]

    # Avoids empty Series error when evaluating proxy
    index_dtype = self._expr.proxy().index.dtype
    index = pd.Index([], dtype=index_dtype)
    proxy = self._expr.proxy().copy()
    proxy.index = index
    proxy = pd.concat([
        proxy,
        pd.Series([1], index=np.asarray(['0']).astype(proxy.index.dtype))
    ])

    idx_func = expressions.ComputedExpression(
        'idx_func',
        compute_idx, [self._expr],
        proxy=proxy,
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Arbitrary())

    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'idx_combine',
              lambda s: func(s, **kwargs), [idx_func],
              requires_partition_by=partitionings.Singleton(),
              preserves_partition_by=partitionings.Singleton()))

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def idxmin(self, **kwargs):
    return self._idxmaxmin_helper('idxmin', **kwargs)

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def idxmax(self, **kwargs):
    return self._idxmaxmin_helper('idxmax', **kwargs)

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def explode(self, ignore_index):
    # ignoring the index will not preserve it
    preserves = (
        partitionings.Singleton() if ignore_index else partitionings.Index())
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'explode',
            lambda s: s.explode(ignore_index), [self._expr],
            preserves_partition_by=preserves,
            requires_partition_by=partitionings.Arbitrary()))

  @frame_base.with_docs_from(pd.DataFrame)
  def dot(self, other):
    """``other`` must be a :class:`DeferredDataFrame` or :class:`DeferredSeries`
    instance. Computing the dot product with an array-like is not supported
    because it is order-sensitive."""
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
      raise frame_base.WontImplementError(
          "other must be a DeferredDataFrame or DeferredSeries instance. "
          "Passing a concrete list or numpy array is not supported. Those "
          "types have no index and must be joined based on the order of the "
          "data.",
          reason="order-sensitive")

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
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def nunique(self, **kwargs):
    return self.drop_duplicates(keep="any").size

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def quantile(self, q, **kwargs):
    """quantile is not parallelizable. See
    `Issue 20933 <https://github.com/apache/beam/issues/20933>`_ tracking
    the possible addition of an approximate, parallelizable implementation of
    quantile."""
    # TODO(https://github.com/apache/beam/issues/20933): Provide an option for
    #  approximate distributed quantiles
    requires = partitionings.Singleton(
        reason=(
            "Computing quantiles across index cannot currently be "
            "parallelized. See https://github.com/apache/beam/issues/20933 "
            "tracking the possible addition of an approximate, parallelizable "
            "implementation of quantile."))

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'quantile',
            lambda df: df.quantile(q=q, **kwargs), [self._expr],
            requires_partition_by=requires,
            preserves_partition_by=partitionings.Singleton()))

  @frame_base.with_docs_from(pd.Series)
  def std(self, *args, **kwargs):
    # Compute variance (deferred scalar) with same args, then sqrt it
    return self.var(*args, **kwargs).apply(lambda var: math.sqrt(var))

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def mean(self, skipna, **kwargs):
    if skipna:
      size = self.count()
    else:
      size = self.length()

    return self.sum(skipna=skipna, **kwargs) / size

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(
      pd.Series, removed_args=["level"] if PD_VERSION >= (2, 0) else None)
  @frame_base.populate_defaults(
      pd.Series, removed_args=["level"] if PD_VERSION >= (2, 0) else None)
  def var(self, axis, skipna, level, ddof, **kwargs):
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

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def corr(self, other, method, min_periods):
    """Only ``method='pearson'`` is currently parallelizable."""
    if method == 'pearson':  # Note that this is the default.
      x, y = self.dropna().align(other.dropna(), 'inner')
      return x._corr_aligned(y, min_periods)

    else:
      reason = (
          f"Encountered corr(method={method!r}) which cannot be "
          "parallelized. Only corr(method='pearson') is currently "
          "parallelizable.")
      # The rank-based correlations are not obviously parallelizable, though
      # perhaps an approximation could be done with a knowledge of quantiles
      # and custom partitioning.
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'corr',
              lambda df,
              other: df.corr(other, method=method, min_periods=min_periods),
              [self._expr, other._expr],
              requires_partition_by=partitionings.Singleton(reason=reason)))

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(
      pd.Series, removed_args=["level"] if PD_VERSION >= (2, 0) else None)
  @frame_base.populate_defaults(
      pd.Series, removed_args=["level"] if PD_VERSION >= (2, 0) else None)
  def skew(self, axis, skipna, level, numeric_only, **kwargs):
    if skipna is None or skipna:
      self = self.dropna()  # pylint: disable=self-cls-assignment
    # See the online, numerically stable formulae at
    # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
    # Note that we are calculating the unbias (sample) version of skew here.
    # See https://en.wikipedia.org/wiki/Skewness#Sample_skewness
    # for more details.
    def compute_moments(x):
      n = len(x)
      if n == 0:
        m2, sum, m3 = 0, 0, 0
      else:
        m2 = x.std(ddof=0)**2 * n
        sum = x.sum()
        m3 = (((x - x.mean())**3).sum())
      return pd.DataFrame(dict(m2=[m2], sum=[sum], n=[n], m3=[m3]))

    def combine_moments(data):
      m2 = sum = n = m3 = 0.0
      for datum in data.itertuples():
        if datum.n == 0:
          continue
        elif n == 0:
          m2, sum, n, m3 = datum.m2, datum.sum, datum.n, datum.m3
        else:
          n_a, n_b = datum.n, n
          sum_a, sum_b = datum.sum, sum
          m2_a, m2_b = datum.m2, m2
          mean_a, mean_b = sum_a / n_a, sum_b / n_b
          delta = mean_b - mean_a
          combined_n = n_a + n_b
          m3 += datum.m3 + (
              (delta**3 * ((n_a * n_b) * (n_a - n_b)) / ((combined_n)**2)) +
              ((3 * delta) * ((n_a * m2_b) - (n_b * m2_a)) / (combined_n)))
          m2 += datum.m2 + delta**2 * n_b * n_a / combined_n
          sum += datum.sum
          n += datum.n

      if n < 3:
        return float('nan')
      elif m2 == 0:
        return float(0)
      else:
        return combined_n * math.sqrt(combined_n - 1) / (combined_n -
                                                         2) * m3 / (
                                                             m2**(3 / 2))

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

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(
      pd.Series, removed_args=["level"] if PD_VERSION >= (2, 0) else None)
  @frame_base.populate_defaults(
      pd.Series, removed_args=["level"] if PD_VERSION >= (2, 0) else None)
  def kurtosis(self, axis, skipna, level, numeric_only, **kwargs):
    if skipna is None or skipna:
      self = self.dropna()  # pylint: disable=self-cls-assignment

    # See the online, numerically stable formulae at
    # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
    # kurtosis here calculated as sample kurtosis
    # https://en.wikipedia.org/wiki/Kurtosis#Sample_kurtosis
    def compute_moments(x):
      n = len(x)
      if n == 0:
        m2, sum, m3, m4 = 0, 0, 0, 0
      else:
        m2 = x.std(ddof=0)**2 * n
        sum = x.sum()
        m3 = (((x - x.mean())**3).sum())
        m4 = (((x - x.mean())**4).sum())
      return pd.DataFrame(dict(m2=[m2], sum=[sum], n=[n], m3=[m3], m4=[m4]))

    def combine_moments(data):
      m2 = sum = n = m3 = m4 = 0.0
      for datum in data.itertuples():
        if datum.n == 0:
          continue
        elif n == 0:
          m2, sum, n, m3, m4 = datum.m2, datum.sum, datum.n, datum.m3, datum.m4
        else:
          n_a, n_b = datum.n, n
          m2_a, m2_b = datum.m2, m2
          m3_a, m3_b = datum.m3, m3
          sum_a, sum_b = datum.sum, sum
          mean_a, mean_b = sum_a / n_a, sum_b / n_b
          delta = mean_b - mean_a
          combined_n = n_a + n_b
          m4 += datum.m4 + ((delta**4) * (n_a * n_b) * (
              (n_a**2) - (n_a * n_b) +
              (n_b**2)) / combined_n**3) + ((6 * delta**2) * ((n_a**2 * m2_b) +
                                                              (n_b**2 * m2_a)) /
                                            (combined_n**2)) + ((4 * delta) *
                                                                ((n_a * m3_b) -
                                                                 (n_b * m3_a)) /
                                                                (combined_n))
          m3 += datum.m3 + (
              (delta**3 * ((n_a * n_b) * (n_a - n_b)) / ((combined_n)**2)) +
              ((3 * delta) * ((n_a * m2_b) - (n_b * m2_a)) / (combined_n)))
          m2 += datum.m2 + delta**2 * n_b * n_a / combined_n
          sum += datum.sum
          n += datum.n

      if n < 4:
        return float('nan')
      elif m2 == 0:
        return float(0)
      else:
        return (((combined_n + 1) * (combined_n) * (combined_n - 1)) /
                ((combined_n - 2) *
                 (combined_n - 3))) * (m4 /
                                       (m2)**2) - ((3 * (combined_n - 1)**2) /
                                                   ((combined_n - 2) *
                                                    (combined_n - 3)))

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

  @frame_base.with_docs_from(pd.Series)
  def kurt(self, *args, **kwargs):
    # Compute Kurtosis as kurt is an alias for kurtosis.
    return self.kurtosis(*args, **kwargs)

  def _corr_aligned(self, other, min_periods):
    std_x = self.std()
    std_y = other.std()
    cov = self._cov_aligned(other, min_periods)
    return cov.apply(
        lambda cov, std_x, std_y: cov / (std_x * std_y), args=[std_x, std_y])

  @frame_base.with_docs_from(pd.Series)
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

  @frame_base.with_docs_from(pd.Series)
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

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(
      pd.Series, removed_args=['inplace'] if PD_VERSION >= (2, 0) else None)
  @frame_base.maybe_inplace
  def set_axis(self, labels, **kwargs):
    # TODO: assigning the index is generally order-sensitive, but we could
    # support it in some rare cases, e.g. when assigning the index from one
    # of a DataFrame's columns
    raise NotImplementedError(
        "Assigning an index is not yet supported. "
        "Consider using set_index() instead.")

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

  def _wrap_in_df(self):
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'wrap_in_df',
            lambda s: pd.DataFrame(s),
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary(),
        ))

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  @frame_base.maybe_inplace
  def duplicated(self, keep):
    """Only ``keep=False`` and ``keep="any"`` are supported. Other values of
    ``keep`` make this an order-sensitive operation. Note ``keep="any"`` is
    a Beam-specific option that guarantees only one duplicate will be kept, but
    unlike ``"first"`` and ``"last"`` it makes no guarantees about _which_
    duplicate element is kept."""
    # Re-use the DataFrame based duplcated, extract the series back out
    df = self._wrap_in_df()

    return df.duplicated(keep=keep)[df.columns[0]]

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  @frame_base.maybe_inplace
  def drop_duplicates(self, keep):
    """Only ``keep=False`` and ``keep="any"`` are supported. Other values of
    ``keep`` make this an order-sensitive operation. Note ``keep="any"`` is
    a Beam-specific option that guarantees only one duplicate will be kept, but
    unlike ``"first"`` and ``"last"`` it makes no guarantees about _which_
    duplicate element is kept."""
    # Re-use the DataFrame based drop_duplicates, extract the series back out
    df = self._wrap_in_df()

    return df.drop_duplicates(keep=keep)[df.columns[0]]

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  @frame_base.maybe_inplace
  def sample(self, **kwargs):
    """Only ``n`` and/or ``weights`` may be specified.  ``frac``,
    ``random_state``, and ``replace=True`` are not yet supported.
    See `Issue 21010 <https://github.com/apache/beam/issues/21010>`_.

    Note that pandas will raise an error if ``n`` is larger than the length
    of the dataset, while the Beam DataFrame API will simply return the full
    dataset in that case."""

    # Re-use the DataFrame based sample, extract the series back out
    df = self._wrap_in_df()

    return df.sample(**kwargs)[df.columns[0]]

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def aggregate(self, func, axis, *args, **kwargs):
    """Some aggregation methods cannot be parallelized, and computing
    them will require collecting all data on a single machine."""
    if kwargs.get('skipna', False):
      # Eagerly generate a proxy to make sure skipna is a valid argument
      # for this aggregation method
      _ = self._expr.proxy().aggregate(func, axis, *args, **kwargs)
      kwargs.pop('skipna')
      return self.dropna().aggregate(func, axis, *args, **kwargs)
    if isinstance(func, list) and len(func) > 1:
      # level arg is ignored for multiple aggregations
      _ = kwargs.pop('level', None)

      # Aggregate with each method separately, then stick them all together.
      rows = [self.agg([f], *args, **kwargs) for f in func]
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'join_aggregate',
              lambda *rows: pd.concat(rows), [row._expr for row in rows]))
    else:
      # We're only handling a single column. It could be 'func' or ['func'],
      # which produce different results. 'func' produces a scalar, ['func']
      # produces a single element Series.
      base_func = func[0] if isinstance(func, list) else func

      if (_is_numeric(base_func) and
          not pd.core.dtypes.common.is_numeric_dtype(self.dtype)):
        warnings.warn(
            f"Performing a numeric aggregation, {base_func!r}, on "
            f"Series {self._expr.proxy().name!r} with non-numeric type "
            f"{self.dtype!r}. This can result in runtime errors or surprising "
            "results.")

      if 'level' in kwargs:
        # Defer to groupby.agg for level= mode
        return self.groupby(
            level=kwargs.pop('level'), axis=axis).agg(func, *args, **kwargs)

      singleton_reason = None
      if 'min_count' in kwargs:
        # Eagerly generate a proxy to make sure min_count is a valid argument
        # for this aggregation method
        _ = self._expr.proxy().agg(func, axis, *args, **kwargs)

        singleton_reason = (
            "Aggregation with min_count= requires collecting all data on a "
            "single node.")

      # We have specialized distributed implementations for these
      if base_func in HAND_IMPLEMENTED_GLOBAL_AGGREGATIONS:
        result = getattr(self, base_func)(*args, **kwargs)
        if isinstance(func, list):
          with expressions.allow_non_parallel_operations(True):
            return frame_base.DeferredFrame.wrap(
                expressions.ComputedExpression(
                    f'wrap_aggregate_{base_func}',
                    lambda x: pd.Series(x, index=[base_func]), [result._expr],
                    requires_partition_by=partitionings.Singleton(),
                    preserves_partition_by=partitionings.Singleton()))
        else:
          return result

      agg_kwargs = kwargs.copy()
      if ((_is_associative(base_func) or _is_liftable_with_sum(base_func)) and
          singleton_reason is None):
        intermediate = expressions.ComputedExpression(
            f'pre_aggregate_{base_func}',
            # Coerce to a Series, if the result is scalar we still want a Series
            # so we can combine and do the final aggregation next.
            lambda s: pd.Series(s.agg(func, *args, **kwargs)),
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Singleton())
        allow_nonparallel_final = True
        if _is_associative(base_func):
          agg_func = func
        else:
          agg_func = ['sum'] if isinstance(func, list) else 'sum'
      else:
        intermediate = self._expr
        allow_nonparallel_final = None  # i.e. don't change the value
        agg_func = func
        singleton_reason = (
            f"Aggregation function {func!r} cannot currently be "
            "parallelized. It requires collecting all data for "
            "this Series on a single node.")
      with expressions.allow_non_parallel_operations(allow_nonparallel_final):
        return frame_base.DeferredFrame.wrap(
            expressions.ComputedExpression(
                f'post_aggregate_{base_func}',
                lambda s: s.agg(agg_func, *args, **agg_kwargs), [intermediate],
                preserves_partition_by=partitionings.Singleton(),
                requires_partition_by=partitionings.Singleton(
                    reason=singleton_reason)))

  agg = aggregate

  @property  # type: ignore
  @frame_base.with_docs_from(pd.Series)
  def axes(self):
    return [self.index]

  clip = frame_base._elementwise_method('clip', base=pd.Series)

  all = _agg_method(pd.Series, 'all')
  any = _agg_method(pd.Series, 'any')
  # TODO(BEAM-12074): Document that Series.count(level=) will drop NaN's
  count = _agg_method(pd.Series, 'count')
  describe = _agg_method(pd.Series, 'describe')
  min = _agg_method(pd.Series, 'min')
  max = _agg_method(pd.Series, 'max')
  prod = product = _agg_method(pd.Series, 'prod')
  sum = _agg_method(pd.Series, 'sum')
  median = _agg_method(pd.Series, 'median')
  sem = _agg_method(pd.Series, 'sem')
  # mad was removed in Pandas 2.0.
  if PD_VERSION < (2, 0):
    mad = _agg_method(pd.Series, 'mad')

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
  interpolate = frame_base.wont_implement_method(
      pd.Series, 'interpolate', reason='order-sensitive')
  searchsorted = frame_base.wont_implement_method(
      pd.Series, 'searchsorted', reason='order-sensitive')
  shift = frame_base.wont_implement_method(
      pd.Series, 'shift', reason='order-sensitive')
  pct_change = frame_base.wont_implement_method(
      pd.Series, 'pct_change', reason='order-sensitive')
  is_monotonic = frame_base.wont_implement_method(
      pd.Series, 'is_monotonic', reason='order-sensitive')
  is_monotonic_increasing = frame_base.wont_implement_method(
      pd.Series, 'is_monotonic_increasing', reason='order-sensitive')
  is_monotonic_decreasing = frame_base.wont_implement_method(
      pd.Series, 'is_monotonic_decreasing', reason='order-sensitive')
  asof = frame_base.wont_implement_method(
      pd.Series, 'asof', reason='order-sensitive')
  first_valid_index = frame_base.wont_implement_method(
      pd.Series, 'first_valid_index', reason='order-sensitive')
  last_valid_index = frame_base.wont_implement_method(
      pd.Series, 'last_valid_index', reason='order-sensitive')
  autocorr = frame_base.wont_implement_method(
      pd.Series, 'autocorr', reason='order-sensitive')
  iat = property(
      frame_base.wont_implement_method(
          pd.Series, 'iat', reason='order-sensitive'))

  head = frame_base.wont_implement_method(
      pd.Series, 'head', explanation=_PEEK_METHOD_EXPLANATION)
  tail = frame_base.wont_implement_method(
      pd.Series, 'tail', explanation=_PEEK_METHOD_EXPLANATION)

  filter = frame_base._elementwise_method('filter', base=pd.Series)

  memory_usage = frame_base.wont_implement_method(
      pd.Series, 'memory_usage', reason="non-deferred-result")
  nbytes = frame_base.wont_implement_method(
      pd.Series, 'nbytes', reason="non-deferred-result")
  to_list = frame_base.wont_implement_method(
      pd.Series, 'to_list', reason="non-deferred-result")

  factorize = frame_base.wont_implement_method(
      pd.Series, 'factorize', reason="non-deferred-columns")

  # In Series __contains__ checks the index
  __contains__ = frame_base.wont_implement_method(
      pd.Series, '__contains__', reason="non-deferred-result")

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def nlargest(self, keep, **kwargs):
    """Only ``keep=False`` and ``keep="any"`` are supported. Other values of
    ``keep`` make this an order-sensitive operation. Note ``keep="any"`` is
    a Beam-specific option that guarantees only one duplicate will be kept, but
    unlike ``"first"`` and ``"last"`` it makes no guarantees about _which_
    duplicate element is kept."""
    # TODO(robertwb): Document 'any' option.
    # TODO(robertwb): Consider (conditionally) defaulting to 'any' if no
    # explicit keep parameter is requested.
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError(
          f"nlargest(keep={keep!r}) is not supported because it is "
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

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def nsmallest(self, keep, **kwargs):
    """Only ``keep=False`` and ``keep="any"`` are supported. Other values of
    ``keep`` make this an order-sensitive operation. Note ``keep="any"`` is
    a Beam-specific option that guarantees only one duplicate will be kept, but
    unlike ``"first"`` and ``"last"`` it makes no guarantees about _which_
    duplicate element is kept."""
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError(
          f"nsmallest(keep={keep!r}) is not supported because it is "
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

  @property  # type: ignore
  @frame_base.with_docs_from(pd.Series)
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

  round = frame_base._elementwise_method('round', base=pd.Series)

  take = frame_base.wont_implement_method(
      pd.Series, 'take', reason='deprecated')

  to_dict = frame_base.wont_implement_method(
      pd.Series, 'to_dict', reason="non-deferred-result")

  to_frame = frame_base._elementwise_method('to_frame', base=pd.Series)

  @frame_base.with_docs_from(pd.Series)
  def unique(self, as_series=False):
    """unique is not supported by default because it produces a
    non-deferred result: an :class:`~numpy.ndarray`. You can use the
    Beam-specific argument ``unique(as_series=True)`` to get the result as
    a :class:`DeferredSeries`"""

    if not as_series:
      raise frame_base.WontImplementError(
          "unique() is not supported by default because it produces a "
          "non-deferred result: a numpy array. You can use the Beam-specific "
          "argument unique(as_series=True) to get the result as a "
          "DeferredSeries",
          reason="non-deferred-result")
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'unique',
            lambda df: pd.Series(df.unique()), [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=partitionings.Singleton(
                reason="unique() cannot currently be parallelized.")))

  @frame_base.with_docs_from(pd.Series)
  def update(self, other):
    self._expr = expressions.ComputedExpression(
        'update',
        lambda df,
        other: df.update(other) or df, [self._expr, other._expr],
        preserves_partition_by=partitionings.Arbitrary(),
        requires_partition_by=partitionings.Index())

  @frame_base.with_docs_from(pd.Series)
  def value_counts(
      self,
      sort=False,
      normalize=False,
      ascending=False,
      bins=None,
      dropna=True):
    """``sort`` is ``False`` by default, and ``sort=True`` is not supported
    because it imposes an ordering on the dataset which likely will not be
    preserved.

    When ``bin`` is specified this operation is not parallelizable. See
    [Issue 20903](https://github.com/apache/beam/issues/20903) tracking the
    possible addition of a distributed implementation."""

    if sort:
      raise frame_base.WontImplementError(
          "value_counts(sort=True) is not supported because it imposes an "
          "ordering on the dataset which likely will not be preserved.",
          reason="order-sensitive")

    if bins is not None:
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'value_counts',
              lambda s: s.value_counts(
                  normalize=normalize, bins=bins, dropna=dropna)[self._expr],
              requires_partition_by=partitionings.Singleton(
                  reason=(
                      "value_counts with bin specified requires collecting "
                      "the entire dataset to identify the range.")),
              preserves_partition_by=partitionings.Singleton(),
          ))

    if dropna:
      column = self.dropna()
    else:
      column = self

    result = column.groupby(column, dropna=dropna).size()

    # Pandas 2 introduces new naming for the results.
    if PD_VERSION >= (2, 0):
      result.index.name = getattr(self, "name", None)
      result.name = "proportion" if normalize else "count"
    else:
      # groupby.size() names the index, which we don't need
      result.index.name = None

    if normalize:
      return result / column.length()
    else:
      return result

  values = property(
      frame_base.wont_implement_method(
          pd.Series, 'values', reason="non-deferred-result"))

  view = frame_base.wont_implement_method(
      pd.Series,
      'view',
      explanation=(
          "because it relies on memory-sharing semantics that are "
          "not compatible with the Beam model."))

  @property  # type: ignore
  @frame_base.with_docs_from(pd.Series)
  def str(self):
    return _DeferredStringMethods(self._expr)

  @property  # type: ignore
  @frame_base.with_docs_from(pd.Series)
  def cat(self):
    return _DeferredCategoricalMethods(self._expr)

  @property  # type: ignore
  @frame_base.with_docs_from(pd.Series)
  def dt(self):
    return _DeferredDatetimeMethods(self._expr)

  @frame_base.with_docs_from(pd.Series)
  def mode(self, *args, **kwargs):
    """mode is not currently parallelizable. An approximate,
    parallelizable implementation of mode may be added in the future
    (`Issue 20946 <https://github.com/apache/beam/issues/20946>`_)."""
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'mode',
            lambda df: df.mode(*args, **kwargs),
            [self._expr],
            #TODO(https://github.com/apache/beam/issues/20946):
            # Can we add an approximate implementation?
            requires_partition_by=partitionings.Singleton(
                reason=(
                    "mode cannot currently be parallelized. See "
                    "https://github.com/apache/beam/issues/20946 tracking the "
                    "possble addition of an approximate, parallelizable "
                    "implementation of mode.")),
            preserves_partition_by=partitionings.Singleton()))

  apply = frame_base._elementwise_method('apply', base=pd.Series)
  map = frame_base._elementwise_method('map', base=pd.Series)
  # TODO(https://github.com/apache/beam/issues/20764): Implement transform
  # using type inference to determine the proxy
  #transform = frame_base._elementwise_method('transform', base=pd.Series)

  @frame_base.with_docs_from(pd.Series)
  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def repeat(self, repeats, axis):
    """``repeats`` must be an ``int`` or a :class:`DeferredSeries`. Lists are
    not supported because they make this operation order-sensitive."""
    if isinstance(repeats, int):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'repeat',
              lambda series: series.repeat(repeats), [self._expr],
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))
    elif isinstance(repeats, frame_base.DeferredBase):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'repeat',
              lambda series,
              repeats_series: series.repeat(repeats_series),
              [self._expr, repeats._expr],
              requires_partition_by=partitionings.Index(),
              preserves_partition_by=partitionings.Arbitrary()))
    elif isinstance(repeats, list):
      raise frame_base.WontImplementError(
          "repeat(repeats=) repeats must be an int or a DeferredSeries. "
          "Lists are not supported because they make this operation sensitive "
          "to the order of the data.",
          reason="order-sensitive")
    else:
      raise TypeError(
          "repeat(repeats=) value must be an int or a "
          f"DeferredSeries (encountered {type(repeats)}).")

  if hasattr(pd.Series, 'compare'):

    @frame_base.with_docs_from(pd.Series)
    @frame_base.args_to_kwargs(pd.Series)
    @frame_base.populate_defaults(pd.Series)
    def compare(self, other, align_axis, **kwargs):

      if align_axis in ('index', 0):
        preserves_partition = partitionings.Singleton()
      elif align_axis in ('columns', 1):
        preserves_partition = partitionings.Arbitrary()
      else:
        raise ValueError(
            "align_axis must be one of ('index', 0, 'columns', 1). "
            f"got {align_axis!r}.")

      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'compare',
              lambda s,
              other: s.compare(other, align_axis, **kwargs),
              [self._expr, other._expr],
              requires_partition_by=partitionings.Index(),
              preserves_partition_by=preserves_partition))


@populate_not_implemented(pd.DataFrame)
@frame_base.DeferredFrame._register_for(pd.DataFrame)
class DeferredDataFrame(DeferredDataFrameOrSeries):
  def __repr__(self):
    return (
        f'DeferredDataFrame(columns={list(self.columns)}, '
        f'{self._render_indexes()})')

  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
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

  @frame_base.with_docs_from(pd.DataFrame)
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
            "Integer slices are not supported as they are ambiguous. Please "
            "use iloc or loc with integer slices.")
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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def align(self, other, join, axis, copy, level, method, **kwargs):
    """Aligning per level is not yet supported. Only the default,
    ``level=None``, is allowed.

    Filling NaN values via ``method`` is not supported, because it is
    `order-sensitive
    <https://s.apache.org/dataframe-order-sensitive-operations>`_. Only the
    default, ``method=None``, is allowed.

    ``copy=False`` is not supported because its behavior (whether or not it is
    an inplace operation) depends on the data."""
    if not copy:
      raise frame_base.WontImplementError(
          "align(copy=False) is not supported because it might be an inplace "
          "operation depending on the data. Please prefer the default "
          "align(copy=True).")
    if method is not None and method != lib.no_default:
      raise frame_base.WontImplementError(
          f"align(method={method!r}) is not supported because it is "
          "order sensitive. Only align(method=None) is supported.",
          reason="order-sensitive")
    if kwargs:
      raise NotImplementedError('align(%s)' % ', '.join(kwargs.keys()))

    # In Pandas 2.0, all aggregations lost the level keyword.
    if PD_VERSION < (2, 0) and level is not None:
      # Could probably get by partitioning on the used levels.
      requires_partition_by = partitionings.Singleton(reason=(
          f"align(level={level}) is not currently parallelizable. Only "
          "align(level=None) can be parallelized."))
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

  @frame_base.with_docs_from(pd.DataFrame, removed_method=PD_VERSION >= (2, 0))
  @frame_base.args_to_kwargs(pd.DataFrame, removed_method=PD_VERSION >= (2, 0))
  @frame_base.populate_defaults(pd.DataFrame,
                                removed_method=PD_VERSION >= (2, 0))
  def append(self, other, ignore_index, verify_integrity, sort, **kwargs):
    """``ignore_index=True`` is not supported, because it requires generating an
    order-sensitive index."""
    if PD_VERSION >= (2, 0):
      raise frame_base.WontImplementError('append() was removed in Pandas 2.0.')
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
      # We can verify the index is non-unique within index partitioned data.
      requires = partitionings.Index()
    else:
      requires = partitionings.Arbitrary()

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'append',
            lambda s, other: s.append(other, sort=sort,
                                      verify_integrity=verify_integrity,
                                      **kwargs),
            [self._expr, other._expr],
            requires_partition_by=requires,
            preserves_partition_by=partitionings.Arbitrary()
        )
    )

  # If column name exists this is a simple project, otherwise it is a constant
  # (default_value)
  @frame_base.with_docs_from(pd.DataFrame)
  def get(self, key, default_value=None):
    if key in self.columns:
      return self[key]
    else:
      return default_value

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def set_index(self, keys, **kwargs):
    """``keys`` must be a ``str`` or ``list[str]``. Passing an Index or Series
    is not yet supported (`Issue 20759
    <https://github.com/apache/beam/issues/20759>`_)."""
    if isinstance(keys, str):
      keys = [keys]

    if any(isinstance(k, (_DeferredIndex, frame_base.DeferredFrame))
           for k in keys):
      raise NotImplementedError("set_index with Index or Series instances is "
                                "not yet supported "
                                "(https://github.com/apache/beam/issues/20759)"
                                ".")

    return frame_base.DeferredFrame.wrap(
      expressions.ComputedExpression(
          'set_index',
          lambda df: df.set_index(keys, **kwargs),
          [self._expr],
          requires_partition_by=partitionings.Arbitrary(),
          preserves_partition_by=partitionings.Singleton()))


  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(
      pd.DataFrame,
      removed_args=['inplace'] if PD_VERSION >= (2, 0) else None)
  @frame_base.maybe_inplace
  def set_axis(self, labels, axis, **kwargs):
    if axis in ('index', 0):
      # TODO: assigning the index is generally order-sensitive, but we could
      # support it in some rare cases, e.g. when assigning the index from one
      # of a DataFrame's columns
      raise NotImplementedError(
          "Assigning an index is not yet supported. "
          "Consider using set_index() instead.")
    else:
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'set_axis',
              lambda df: df.set_axis(labels, axis=axis, **kwargs),
              [self._expr],
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))


  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
  def axes(self):
    return (self.index, self.columns)

  @property  # type: ignore
  @frame_base.with_docs_from(pd.DataFrame)
  def dtypes(self):
    return self._expr.proxy().dtypes

  @frame_base.with_docs_from(pd.DataFrame)
  def assign(self, **kwargs):
    """``value`` must be a ``callable`` or :class:`DeferredSeries`. Other types
    make this operation order-sensitive."""
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

  @frame_base.with_docs_from(pd.DataFrame)
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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def insert(self, value, **kwargs):
    """``value`` cannot be a ``List`` because aligning it with this
    DeferredDataFrame is order-sensitive."""
    if isinstance(value, list):
      raise frame_base.WontImplementError(
          "insert(value=list) is not supported because it joins the input "
          "list to the deferred DataFrame based on the order of the data.",
          reason="order-sensitive")

    if isinstance(value, pd.core.generic.NDFrame):
      value = frame_base.DeferredFrame.wrap(
          expressions.ConstantExpression(value))

    if isinstance(value, frame_base.DeferredFrame):
      def func_zip(df, value):
        df = df.copy()
        df.insert(value=value, **kwargs)
        return df

      inserted = frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'insert',
              func_zip,
              [self._expr, value._expr],
              requires_partition_by=partitionings.Index(),
              preserves_partition_by=partitionings.Arbitrary()))
    else:
      def func_elementwise(df):
        df = df.copy()
        df.insert(value=value, **kwargs)
        return df
      inserted = frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'insert',
              func_elementwise,
              [self._expr],
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))

    self._expr = inserted._expr

  @staticmethod
  @frame_base.with_docs_from(pd.DataFrame)
  def from_dict(*args, **kwargs):
    return frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(pd.DataFrame.from_dict(*args, **kwargs)))

  @staticmethod
  @frame_base.with_docs_from(pd.DataFrame)
  def from_records(*args, **kwargs):
    return frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(pd.DataFrame.from_records(*args,
                                                                 **kwargs)))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def duplicated(self, keep, subset):
    """Only ``keep=False`` and ``keep="any"`` are supported. Other values of
    ``keep`` make this an order-sensitive operation. Note ``keep="any"`` is
    a Beam-specific option that guarantees only one duplicate will be kept, but
    unlike ``"first"`` and ``"last"`` it makes no guarantees about _which_
    duplicate element is kept."""
    # TODO(BEAM-12074): Document keep="any"
    if keep == 'any':
      keep = 'first'
    elif keep is not False:
      raise frame_base.WontImplementError(
          f"duplicated(keep={keep!r}) is not supported because it is "
          "sensitive to the order of the data. Only keep=False and "
          "keep=\"any\" are supported.",
          reason="order-sensitive")

    by = subset or list(self.columns)

    return self.groupby(by).apply(
        lambda df: pd.DataFrame(df.duplicated(keep=keep, subset=subset),
                                columns=[None]))[None].droplevel(by)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def drop_duplicates(self, keep, subset, ignore_index):
    """Only ``keep=False`` and ``keep="any"`` are supported. Other values of
    ``keep`` make this an order-sensitive operation. Note ``keep="any"`` is
    a Beam-specific option that guarantees only one duplicate will be kept, but
    unlike ``"first"`` and ``"last"`` it makes no guarantees about _which_
    duplicate element is kept."""
    # TODO(BEAM-12074): Document keep="any"
    if keep == 'any':
      keep = 'first'
    elif keep is not False:
      raise frame_base.WontImplementError(
          f"drop_duplicates(keep={keep!r}) is not supported because it is "
          "sensitive to the order of the data. Only keep=False and "
          "keep=\"any\" are supported.",
          reason="order-sensitive")

    if ignore_index is not False:
      raise frame_base.WontImplementError(
          "drop_duplicates(ignore_index=False) is not supported because it "
          "requires generating a new index that is sensitive to the order of "
          "the data.",
          reason="order-sensitive")

    by = subset or list(self.columns)

    return self.groupby(by).apply(
        lambda df: df.drop_duplicates(keep=keep, subset=subset)).droplevel(by)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def aggregate(self, func, axis, *args, **kwargs):
    # We have specialized implementations for these.
    if func in ('quantile',):
      return getattr(self, func)(*args, axis=axis, **kwargs)

    # In pandas<1.3.0, maps to a property, args are ignored
    if func in ('size',) and PD_VERSION < (1, 3):
      return getattr(self, func)

    # We also have specialized distributed implementations for these. They only
    # support axis=0 (implicitly) though. axis=1 should fall through
    if func in ('corr', 'cov') and axis in (0, 'index'):
      return getattr(self, func)(*args, **kwargs)

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
    elif len(self._expr.proxy().columns) == 0:
      # For this corner case, just colocate everything.
      return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'aggregate',
            lambda df: df.agg(func, *args, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Singleton()))
    else:
      # In the general case, we will compute the aggregation of each column
      # separately, then recombine.

      # First, handle any kwargs that cause a projection, by eagerly generating
      # the proxy, and only including the columns that are in the output.
      PROJECT_KWARGS = ('numeric_only', 'bool_only', 'include', 'exclude')
      proxy = self._expr.proxy().agg(func, axis, *args, **kwargs)

      if isinstance(proxy, pd.DataFrame):
        projected = self[list(proxy.columns)]
      elif isinstance(proxy, pd.Series):
        projected = self[list(proxy.index)]
      else:
        projected = self

      nonnumeric_columns = [name for (name, dtype) in projected.dtypes.items()
                            if not
                            pd.core.dtypes.common.is_numeric_dtype(dtype)]

      if _is_numeric(func) and nonnumeric_columns:
        if 'numeric_only' in kwargs and kwargs['numeric_only'] is False:
          # User has opted in to execution with non-numeric columns, they
          # will accept runtime errors
          pass
        else:
          raise frame_base.WontImplementError(
              f"Numeric aggregation ({func!r}) on a DataFrame containing "
              f"non-numeric columns ({*nonnumeric_columns,!r} is not "
              "supported, unless `numeric_only=` is specified.\n"
              "Use `numeric_only=True` to only aggregate over numeric "
              "columns.\nUse `numeric_only=False` to aggregate over all "
              "columns. Note this is not recommended, as it could result in "
              "execution time errors.")

      for key in PROJECT_KWARGS:
        if key in kwargs:
          kwargs.pop(key)

      if not isinstance(func, dict):
        col_names = list(projected._expr.proxy().columns)
        func_by_col = {col: func for col in col_names}
      else:
        func_by_col = func
        col_names = list(func.keys())
      aggregated_cols = []
      has_lists = any(isinstance(f, list) for f in func_by_col.values())
      for col in col_names:
        funcs = func_by_col[col]
        if has_lists and not isinstance(funcs, list):
          # If any of the columns do multiple aggregations, they all must use
          # "list" style output
          funcs = [funcs]
        aggregated_cols.append(projected[col].agg(funcs, *args, **kwargs))
      # The final shape is different depending on whether any of the columns
      # were aggregated by a list of aggregators.
      with expressions.allow_non_parallel_operations():
        if isinstance(proxy, pd.Series):
          return frame_base.DeferredFrame.wrap(
            expressions.ComputedExpression(
                'join_aggregate',
                  lambda *cols: pd.Series(
                      {col: value for col, value in zip(col_names, cols)}),
                [col._expr for col in aggregated_cols],
                requires_partition_by=partitionings.Singleton()))
        elif isinstance(proxy, pd.DataFrame):
          return frame_base.DeferredFrame.wrap(
              expressions.ComputedExpression(
                  'join_aggregate',
                  lambda *cols: pd.DataFrame(
                      {col: value for col, value in zip(col_names, cols)}),
                  [col._expr for col in aggregated_cols],
                  requires_partition_by=partitionings.Singleton()))
        else:
          raise AssertionError("Unexpected proxy type for "
                               f"DataFrame.aggregate!: proxy={proxy!r}, "
                               f"type(proxy)={type(proxy)!r}")

  agg = aggregate

  applymap = frame_base._elementwise_method('applymap', base=pd.DataFrame)
  if PD_VERSION >= (2, 1):
    map = frame_base._elementwise_method('map', base=pd.DataFrame)
  add_prefix = frame_base._elementwise_method('add_prefix', base=pd.DataFrame)
  add_suffix = frame_base._elementwise_method('add_suffix', base=pd.DataFrame)

  memory_usage = frame_base.wont_implement_method(
      pd.DataFrame, 'memory_usage', reason="non-deferred-result")
  info = frame_base.wont_implement_method(
      pd.DataFrame, 'info', reason="non-deferred-result")


  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def clip(self, axis, **kwargs):
    """``lower`` and ``upper`` must be :class:`DeferredSeries` instances, or
    constants.  Array-like arguments are not supported because they are
    order-sensitive."""

    if any(isinstance(kwargs.get(arg, None), frame_base.DeferredFrame)
           for arg in ('upper', 'lower')) and axis not in (0, 'index'):
      raise frame_base.WontImplementError(
          "axis must be 'index' when upper and/or lower are a DeferredFrame",
          reason='order-sensitive')

    return frame_base._elementwise_method('clip', base=pd.DataFrame)(self,
                                                                     axis=axis,
                                                                     **kwargs)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def corr(self, method, min_periods):
    """Only ``method="pearson"`` can be parallelized. Other methods require
    collecting all data on a single worker (see
    https://s.apache.org/dataframe-non-parallel-operations for details).
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
      reason = (f"Encountered corr(method={method!r}) which cannot be "
                "parallelized. Only corr(method='pearson') is currently "
                "parallelizable.")
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'corr',
              lambda df: df.corr(method=method, min_periods=min_periods),
              [self._expr],
              requires_partition_by=partitionings.Singleton(reason=reason)))

  @frame_base.with_docs_from(pd.DataFrame)
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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def corrwith(self, other, axis, drop, method):
    if axis in (1, 'columns'):
      return self._elementwise(
          lambda df, other: df.corrwith(other, axis=axis, drop=drop,
                                        method=method),
          'corrwith',
          other_args=(other,))


    if not isinstance(other, frame_base.DeferredFrame):
      other = frame_base.DeferredFrame.wrap(
          expressions.ConstantExpression(other))

    if isinstance(other, DeferredSeries):
      proxy = self._expr.proxy().corrwith(other._expr.proxy(), axis=axis,
                                          drop=drop, method=method)
      self, other = self.align(other, axis=0, join='inner')
      col_names = proxy.index
      other_cols = [other] * len(col_names)
    elif isinstance(other, DeferredDataFrame):
      proxy = self._expr.proxy().corrwith(
          other._expr.proxy(), axis=axis, method=method, drop=drop)
      self, other = self.align(other, axis=0, join='inner')
      col_names = list(
          set(self.columns)
          .intersection(other.columns)
          .intersection(proxy.index))
      other_cols = [other[col_name] for col_name in col_names]
    else:
      # Raise the right error.
      self._expr.proxy().corrwith(other._expr.proxy(), axis=axis, drop=drop,
                                  method=method)

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
  interpolate = frame_base.wont_implement_method(pd.DataFrame, 'interpolate',
                                                 reason='order-sensitive')

  pct_change = frame_base.wont_implement_method(
      pd.DataFrame, 'pct_change', reason='order-sensitive')
  asof = frame_base.wont_implement_method(
      pd.DataFrame, 'asof', reason='order-sensitive')
  first_valid_index = frame_base.wont_implement_method(
      pd.DataFrame, 'first_valid_index', reason='order-sensitive')
  last_valid_index = frame_base.wont_implement_method(
      pd.DataFrame, 'last_valid_index', reason='order-sensitive')
  iat = property(frame_base.wont_implement_method(
      pd.DataFrame, 'iat', reason='order-sensitive'))

  lookup = frame_base.wont_implement_method(
      pd.DataFrame, 'lookup', reason='deprecated')

  head = frame_base.wont_implement_method(pd.DataFrame, 'head',
      explanation=_PEEK_METHOD_EXPLANATION)
  tail = frame_base.wont_implement_method(pd.DataFrame, 'tail',
      explanation=_PEEK_METHOD_EXPLANATION)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def sample(self, n, frac, replace, weights, random_state, axis):
    """When ``axis='index'``, only ``n`` and/or ``weights`` may be specified.
    ``frac``, ``random_state``, and ``replace=True`` are not yet supported.
    See `Issue 21010 <https://github.com/apache/beam/issues/21010>`_.

    Note that pandas will raise an error if ``n`` is larger than the length
    of the dataset, while the Beam DataFrame API will simply return the full
    dataset in that case.

    sample is fully supported for axis='columns'."""
    if axis in (1, 'columns'):
      # Sampling on axis=columns just means projecting random columns
      # Eagerly generate proxy to determine the set of columns at construction
      # time
      proxy = self._expr.proxy().sample(n=n, frac=frac, replace=replace,
                                        weights=weights,
                                        random_state=random_state, axis=axis)
      # Then do the projection
      return self[list(proxy.columns)]

    # axis='index'
    if frac is not None or random_state is not None or replace:
      raise NotImplementedError(
          f"When axis={axis!r}, only n and/or weights may be specified. "
          "frac, random_state, and replace=True are not yet supported "
          f"(got frac={frac!r}, random_state={random_state!r}, "
          f"replace={replace!r}). See "
          "https://github.com/apache/beam/issues/21010.")

    if n is None:
      n = 1

    if isinstance(weights, str):
      weights = self[weights]

    tmp_weight_column_name = "___Beam_DataFrame_weights___"

    if weights is None:
      self_with_randomized_weights = frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
          'randomized_weights',
          lambda df: df.assign(**{tmp_weight_column_name:
                                  np.random.rand(len(df))}),
          [self._expr],
          requires_partition_by=partitionings.Arbitrary(),
          preserves_partition_by=partitionings.Arbitrary()))
    else:
      # See "Fast Parallel Weighted Random Sampling" by Efraimidis and Spirakis
      # https://www.cti.gr/images_gr/reports/99-06-02.ps
      def assign_randomized_weights(df, weights):
        non_zero_weights = (weights > 0) | pd.Series(dtype=bool, index=df.index)
        df = df.loc[non_zero_weights]
        weights = weights.loc[non_zero_weights]
        random_weights = np.log(np.random.rand(len(weights))) / weights
        return df.assign(**{tmp_weight_column_name: random_weights})
      self_with_randomized_weights = frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
          'randomized_weights',
          assign_randomized_weights,
          [self._expr, weights._expr],
          requires_partition_by=partitionings.Index(),
          preserves_partition_by=partitionings.Arbitrary()))

    return self_with_randomized_weights.nlargest(
        n=n, columns=tmp_weight_column_name, keep='any').drop(
            tmp_weight_column_name, axis=1)

  @frame_base.with_docs_from(pd.DataFrame)
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

  @frame_base.with_docs_from(pd.DataFrame)
  def mode(self, axis=0, *args, **kwargs):
    """mode with axis="columns" is not implemented because it produces
    non-deferred columns.

    mode with axis="index" is not currently parallelizable. An approximate,
    parallelizable implementation of mode may be added in the future
    (`Issue 20946 <https://github.com/apache/beam/issues/20946>`_)."""

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
            #TODO(https://github.com/apache/beam/issues/20946):
            # Can we add an approximate implementation?
            requires_partition_by=partitionings.Singleton(reason=(
                "mode(axis='index') cannot currently be parallelized. See "
                "https://github.com/apache/beam/issues/20946 tracking the "
                "possble addition of an approximate, parallelizable "
                "implementation of mode."
            )),
            preserves_partition_by=partitionings.Singleton()))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def dropna(self, axis, **kwargs):
    """dropna with axis="columns" specified cannot be parallelized."""
    # TODO(robertwb): This is a common pattern. Generalize?
    if axis in (1, 'columns'):
      requires_partition_by = partitionings.Singleton(reason=(
          "dropna(axis=1) cannot currently be parallelized. It requires "
          "checking all values in each column for NaN values, to determine "
          "if that column should be dropped."
      ))
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
                                "(https://github.com/apache/beam/issues/20626)"
                                )

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


  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def eval(self, expr, inplace, **kwargs):
    """Accessing local variables with ``@<varname>`` is not yet supported
    (`Issue 20626 <https://github.com/apache/beam/issues/20626>`_).

    Arguments ``local_dict``, ``global_dict``, ``level``, ``target``, and
    ``resolvers`` are not yet supported."""
    return self._eval_or_query('eval', expr, inplace, **kwargs)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def query(self, expr, inplace, **kwargs):
    """Accessing local variables with ``@<varname>`` is not yet supported
    (`Issue 20626 <https://github.com/apache/beam/issues/20626>`_).

    Arguments ``local_dict``, ``global_dict``, ``level``, ``target``, and
    ``resolvers`` are not yet supported."""
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

  @frame_base.with_docs_from(pd.DataFrame)
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

  @frame_base.with_docs_from(pd.DataFrame)
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
    """merge is not parallelizable unless ``left_index`` or ``right_index`` is
    ``True`, because it requires generating an entirely new unique index.
    See notes on :meth:`DeferredDataFrame.reset_index`. It is recommended to
    move the join key for one of your columns to the index to avoid this issue.
    For an example see the enrich pipeline in
    :mod:`apache_beam.examples.dataframe.taxiride`.

    ``how="cross"`` is not yet supported.
    """
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
      raise NotImplementedError(
        "cross join is not yet implemented "
        "(https://github.com/apache/beam/issues/20318)")
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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def nlargest(self, keep, **kwargs):
    """Only ``keep=False`` and ``keep="any"`` are supported. Other values of
    ``keep`` make this an order-sensitive operation. Note ``keep="any"`` is
    a Beam-specific option that guarantees only one duplicate will be kept, but
    unlike ``"first"`` and ``"last"`` it makes no guarantees about _which_
    duplicate element is kept."""
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError(
          f"nlargest(keep={keep!r}) is not supported because it is "
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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def nsmallest(self, keep, **kwargs):
    """Only ``keep=False`` and ``keep="any"`` are supported. Other values of
    ``keep`` make this an order-sensitive operation. Note ``keep="any"`` is
    a Beam-specific option that guarantees only one duplicate will be kept, but
    unlike ``"first"`` and ``"last"`` it makes no guarantees about _which_
    duplicate element is kept."""
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError(
          f"nsmallest(keep={keep!r}) is not supported because it is "
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

  plot = frame_base.wont_implement_method(pd.DataFrame, 'plot',
                                                      reason="plotting-tools")

  @frame_base.with_docs_from(pd.DataFrame)
  def pop(self, item):
    result = self[item]

    self._expr = expressions.ComputedExpression(
            'popped',
            lambda df: df.drop(columns=[item]),
            [self._expr],
            preserves_partition_by=partitionings.Arbitrary(),
            requires_partition_by=partitionings.Arbitrary())
    return result

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def quantile(self, q, axis, **kwargs):
    """``quantile(axis="index")`` is not parallelizable. See
    `Issue 20933 <https://github.com/apache/beam/issues/20933>`_ tracking
    the possible addition of an approximate, parallelizable implementation of
    quantile.

    When using quantile with ``axis="columns"`` only a single ``q`` value can be
    specified."""
    if axis in (1, 'columns'):
      if isinstance(q, list):
        raise frame_base.WontImplementError(
            "quantile(axis=columns) with multiple q values is not supported "
            "because it transposes the input DataFrame. Note computing "
            "an individual quantile across columns (e.g. "
            f"df.quantile(q={q[0]!r}, axis={axis!r}) is supported.",
            reason="non-deferred-columns")
      else:
        requires = partitionings.Arbitrary()
    else: # axis='index'
      # TODO(https://github.com/apache/beam/issues/20933): Provide an option
      # for approximate distributed quantiles
      requires = partitionings.Singleton(reason=(
          "Computing quantiles across index cannot currently be parallelized. "
          "See https://github.com/apache/beam/issues/20933 tracking the "
          "possible addition of an approximate, parallelizable implementation "
          "of quantile."
      ))

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'quantile',
            lambda df: df.quantile(q=q, axis=axis, **kwargs),
            [self._expr],
            requires_partition_by=requires,
            preserves_partition_by=partitionings.Singleton()))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.maybe_inplace
  def rename(self, **kwargs):
    """rename is not parallelizable when ``axis="index"`` and
    ``errors="raise"``. It requires collecting all data on a single
    node in order to detect if one of the index values is missing."""
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
      # TODO: We could do this in parallel by creating a ConstantExpression
      # with a series created from the mapper dict. Then Index() partitioning
      # would co-locate the necessary index values and we could raise
      # individually within each partition. Execution time errors are
      # discouraged anyway so probably not worth the effort.
      requires_partition_by = partitionings.Singleton(reason=(
          "rename(errors='raise', axis='index') requires collecting all "
          "data on a single node in order to detect missing index values."
      ))
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

  @frame_base.with_docs_from(pd.DataFrame)
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

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def shift(self, axis, freq, **kwargs):
    """shift with ``axis="index" is only supported with ``freq`` specified and
    ``fill_value`` undefined. Other configurations make this operation
    order-sensitive."""
    if axis in (1, 'columns'):
      preserves = partitionings.Arbitrary()
      proxy = None
    else:
      if freq is None or 'fill_value' in kwargs:
        fill_value = kwargs.get('fill_value', 'NOT SET')
        raise frame_base.WontImplementError(
            f"shift(axis={axis!r}) is only supported with freq defined, and "
            f"fill_value undefined (got freq={freq!r},"
            f"fill_value={fill_value!r}). Other configurations are sensitive "
            "to the order of the data because they require populating shifted "
            "rows with `fill_value`.",
            reason="order-sensitive")
      # proxy generation fails in pandas <1.2
      # Seems due to https://github.com/pandas-dev/pandas/issues/14811,
      # bug with shift on empty indexes.
      # Fortunately the proxy should be identical to the input.
      proxy = self._expr.proxy().copy()


      # index is modified, so no partitioning is preserved.
      preserves = partitionings.Singleton()

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'shift',
            lambda df: df.shift(axis=axis, freq=freq, **kwargs),
            [self._expr],
            proxy=proxy,
            preserves_partition_by=preserves,
            requires_partition_by=partitionings.Arbitrary()))


  shape = property(frame_base.wont_implement_method(
      pd.DataFrame, 'shape', reason="non-deferred-result"))

  stack = frame_base._proxy_method(
      'stack',
      base=pd.DataFrame,
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Singleton())

  all = _agg_method(pd.DataFrame, 'all')
  any = _agg_method(pd.DataFrame, 'any')
  count = _agg_method(pd.DataFrame, 'count')
  describe = _agg_method(pd.DataFrame, 'describe')
  max = _agg_method(pd.DataFrame, 'max')
  min = _agg_method(pd.DataFrame, 'min')

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def pivot(self, index=None, columns=None, values=None, **kwargs):
    """Because pivot is a non-deferred method, any columns specified in
    ``columns`` must be CategoricalDType so we can determine the output column
    names."""

    def verify_all_categorical(all_cols_are_categorical):
      if not all_cols_are_categorical:
        message = "pivot() of non-categorical type is not supported because " \
            "the type of the output column depends on the data. Please use " \
            "pd.CategoricalDtype with explicit categories."
        raise frame_base.WontImplementError(
          message, reason="non-deferred-columns")

    # If values not provided, take all remaining columns of dataframe
    if not values:
      tmp = self._expr.proxy()
      if index:
        tmp = tmp.drop(index, axis=1)
      if columns:
        tmp = tmp.drop(columns, axis=1)
      values = tmp.columns.values

    # Construct column index
    if is_list_like(columns) and len(columns) <= 1:
      columns = columns[0]
    selected_cols = self._expr.proxy()[columns]
    if isinstance(selected_cols, pd.Series):
      all_cols_are_categorical = isinstance(
        selected_cols.dtype, pd.CategoricalDtype
      )
      verify_all_categorical(all_cols_are_categorical)

      if is_list_like(values) and len(values) > 1:
        # If more than one value provided, don't create a None level
        values_in_col_index = values
        names = [None, columns]
        col_index = pd.MultiIndex.from_product(
          [values_in_col_index,
          selected_cols.dtypes.categories.astype('category')],
          names=names
        )
      else:
        col_index = pd.CategoricalIndex(
          selected_cols.dtype.categories,
          name=columns
        )
    else:
      all_cols_are_categorical = all(
        isinstance(c, pd.CategoricalDtype) for c in selected_cols.dtypes
      )
      verify_all_categorical(all_cols_are_categorical)

      if is_list_like(values) and len(values) > 1:
        # If more than one value provided, don't create a None level
        values_in_col_index = values
        names = [None, *columns]
        categories = [
          c.categories.astype('category') for c in selected_cols.dtypes
        ]
        col_index = pd.MultiIndex.from_product(
          [values_in_col_index, *categories],
          names=names
        )
      else:
        # If one value provided, don't create a None level
        names = columns
        categories = [
          c.categories.astype('category') for c in selected_cols.dtypes
        ]
        col_index = pd.MultiIndex.from_product(
          categories,
          names=names
        )

    # Construct row index
    if index:
      if PD_VERSION < (1, 4) and is_list_like(index) and len(index) > 1:
        raise frame_base.WontImplementError(
          "pivot() is not supported when pandas<1.4 and index is a MultiIndex")
      per_partition = expressions.ComputedExpression(
          'pivot-per-partition',
          lambda df: df.set_index(keys=index), [self._expr],
          preserves_partition_by=partitionings.Singleton(),
          requires_partition_by=partitionings.Arbitrary()
      )
      tmp = per_partition.proxy().pivot(
        columns=columns, values=values, **kwargs)
      row_index = tmp.index
    else:
      per_partition = self._expr
      row_index = self._expr.proxy().index
    if PD_VERSION < (1, 4) and isinstance(row_index, pd.MultiIndex):
      raise frame_base.WontImplementError(
        "pivot() is not supported when pandas<1.4 and index is a MultiIndex")

    selected_values = self._expr.proxy()[values]
    if isinstance(selected_values, pd.Series):
      value_dtype = selected_values.dtype
    else:
      # Set dtype to object if more than one value
      dtypes = [d for d in selected_values.dtypes]
      value_dtype = object
      if any((is_int64_dtype(x) for x in dtypes)):
        value_dtype = np.int64
      if any((is_float_dtype(x) for x in dtypes)):
        value_dtype = np.float64
      if object in dtypes:
        value_dtype = object

    # Construct proxy
    proxy = pd.DataFrame(
      columns=col_index, dtype=value_dtype, index=row_index
    )

    def pivot_helper(df):
      result = pd.concat(
        [proxy, df.pivot(columns=columns, values=values, **kwargs)]
      )
      result.columns = col_index
      result = result.astype(value_dtype)
      return result

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'pivot',
            pivot_helper,
            [per_partition],
            proxy=proxy,
            preserves_partition_by=partitionings.Index(),
            requires_partition_by=partitionings.Index()))

  prod = product = _agg_method(pd.DataFrame, 'prod')
  sum = _agg_method(pd.DataFrame, 'sum')
  mean = _agg_method(pd.DataFrame, 'mean')
  median = _agg_method(pd.DataFrame, 'median')
  nunique = _agg_method(pd.DataFrame, 'nunique')
  std = _agg_method(pd.DataFrame, 'std')
  var = _agg_method(pd.DataFrame, 'var')
  sem = _agg_method(pd.DataFrame, 'sem')
  skew = _agg_method(pd.DataFrame, 'skew')
  kurt = _agg_method(pd.DataFrame, 'kurt')
  kurtosis = _agg_method(pd.DataFrame, 'kurtosis')
  # mad was removed in Pandas 2.0.
  if PD_VERSION < (2, 0):
    mad = _agg_method(pd.DataFrame, 'mad')

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
  T = property(frame_base.wont_implement_method(
      pd.DataFrame, 'T', reason='non-deferred-columns'))

  update = frame_base._proxy_method(
      'update',
      inplace=True,
      base=pd.DataFrame,
      requires_partition_by=partitionings.Index(),
      preserves_partition_by=partitionings.Arbitrary())

  values = property(frame_base.wont_implement_method(
      pd.DataFrame, 'values', reason="non-deferred-result"))

  style = property(frame_base.wont_implement_method(
      pd.DataFrame, 'style', reason="non-deferred-result"))

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def melt(self, ignore_index, **kwargs):
    """``ignore_index=True`` is not supported, because it requires generating an
    order-sensitive index."""
    if ignore_index:
      raise frame_base.WontImplementError(
          "melt(ignore_index=True) is order sensitive because it requires "
          "generating a new index based on the order of the data.",
          reason="order-sensitive")

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'melt',
            lambda df: df.melt(ignore_index=False, **kwargs), [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Singleton()))

  if hasattr(pd.DataFrame, 'value_counts'):
    @frame_base.with_docs_from(pd.DataFrame)
    def value_counts(self, subset=None, sort=False, normalize=False,
                     ascending=False, dropna=True):
      """``sort`` is ``False`` by default, and ``sort=True`` is not supported
      because it imposes an ordering on the dataset which likely will not be
      preserved."""

      if sort:
        raise frame_base.WontImplementError(
            "value_counts(sort=True) is not supported because it imposes an "
            "ordering on the dataset which likely will not be preserved.",
            reason="order-sensitive")
      columns = subset or list(self.columns)

      if dropna:
        # Must include subset here because otherwise we spuriously drop NAs due
        # to columns outside our subset.
        dropped = self.dropna(subset=subset)
      else:
        dropped = self

      result = dropped.groupby(columns, dropna=dropna).size()

      # Pandas 2 introduces new naming for the results.
      if PD_VERSION >= (2,0):
        result.name = "proportion" if normalize else "count"

      if normalize:
        return result/dropped.length()
      else:
        return result

  if hasattr(pd.DataFrame, 'compare'):

    @frame_base.with_docs_from(pd.DataFrame)
    @frame_base.args_to_kwargs(pd.DataFrame)
    @frame_base.populate_defaults(pd.DataFrame)
    def compare(self, other, align_axis, keep_shape, **kwargs):
      """The default values ``align_axis=1 and ``keep_shape=False``
       are not supported, because the output columns depend on the data.
       To use ``align_axis=1``, please specify ``keep_shape=True``."""

      preserve_partition = None

      if align_axis in (1, 'columns') and not keep_shape:
        raise frame_base.WontImplementError(
          f"compare(align_axis={align_axis!r}, keep_shape={keep_shape!r}) "
          "is not allowed because the output columns depend on the data, "
          "please specify keep_shape=True.",
        reason='non-deferred-columns'
        )

      if align_axis in (1, 'columns'):
        preserve_partition = partitionings.Arbitrary()
      elif align_axis in (0, 'index'):
        preserve_partition = partitionings.Singleton()
      else:
        raise ValueError(
          "align_axis must be one of ('index', 0, 'columns', 1). "
          f"got {align_axis!r}.")


      return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
          'compare',
          lambda df, other: df.compare(other, align_axis, keep_shape, **kwargs),
          [self._expr, other._expr],
          requires_partition_by=partitionings.Index(),
          preserves_partition_by=preserve_partition
        )
      )

  def _idxmaxmin_helper(self, op, **kwargs):
    if op == 'idxmax':
      func = pd.DataFrame.idxmax
    elif op == 'idxmin':
      func = pd.DataFrame.idxmin
    else:
      raise ValueError("op must be one of ('idxmax', 'idxmin'). "
                       f"got {op!r}.")

    axis = kwargs.get('axis', 0)

    index_dtype = self._expr.proxy().index.dtype
    columns_dtype = self._expr.proxy().columns.dtype

    def compute_idx(df):
      indexes = func(df, **kwargs).unique()
      if pd.isna(indexes).any():
        return df
      else:
        return df.loc[indexes]

    if axis in ('index', 0):
      requires_partition = partitionings.Singleton()

      proxy_index = pd.Index([], dtype=columns_dtype)
      proxy = pd.Series([], index=proxy_index, dtype=index_dtype)
      partition_proxy = self._expr.proxy().copy()

      idx_per_partition = expressions.ComputedExpression(
        'idx-per-partition',
        compute_idx, [self._expr],
        proxy=partition_proxy,
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Arbitrary()
      )

    elif axis in ('columns', 1):
      requires_partition = partitionings.Index()

      proxy_index = pd.Index([], dtype=index_dtype)
      proxy = pd.Series([], index=proxy_index, dtype=columns_dtype)

      idx_per_partition = self._expr

    else:
      raise ValueError("axis must be one of ('index', 0, 'columns', 1). "
                       f"got {axis!r}.")

    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
          'idx',
          lambda df: func(df, **kwargs), [idx_per_partition],
          proxy=proxy,
          requires_partition_by=requires_partition,
          preserves_partition_by=partitionings.Singleton()
        )
      )


  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def idxmin(self, **kwargs):
    return self._idxmaxmin_helper('idxmin', **kwargs)

  @frame_base.with_docs_from(pd.DataFrame)
  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def idxmax(self, **kwargs):
    return self._idxmaxmin_helper('idxmax', **kwargs)


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
               ungrouped: expressions.Expression[pd.core.generic.NDFrame],
               ungrouped_with_index: expressions.Expression[pd.core.generic.NDFrame], # pylint: disable=line-too-long
               grouping_columns,
               grouping_indexes,
               group_keys,
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
    super().__init__(expr)
    self._ungrouped = ungrouped
    self._ungrouped_with_index = ungrouped_with_index
    self._projection = projection
    self._grouping_columns = grouping_columns
    self._grouping_indexes = grouping_indexes
    self._group_keys = group_keys
    self._kwargs = kwargs

    if (self._kwargs.get('dropna', True) is False and
        self._ungrouped.proxy().index.nlevels > 1):
      raise NotImplementedError(
          "dropna=False does not work as intended in the Beam DataFrame API "
          "when grouping on multiple columns or indexes (See "
          "https://github.com/apache/beam/issues/21014).")

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
        self._group_keys,
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
        self._group_keys,
        projection=name)

  @frame_base.with_docs_from(DataFrameGroupBy)
  def agg(self, fn, *args, **kwargs):
    if _is_associative(fn):
      return _liftable_agg(fn)(self, *args, **kwargs)
    elif _is_liftable_with_sum(fn):
      return _liftable_agg(fn, postagg_meth='sum')(self, *args, **kwargs)
    elif _is_unliftable(fn):
      return _unliftable_agg(fn)(self, *args, **kwargs)
    elif callable(fn):
      return DeferredDataFrame(
          expressions.ComputedExpression(
              'agg',
              lambda gb: gb.agg(fn, *args, **kwargs), [self._expr],
              requires_partition_by=partitionings.Index(),
              preserves_partition_by=partitionings.Singleton()))
    else:
      raise NotImplementedError(f"GroupBy.agg(func={fn!r})")

  @property
  def ndim(self):
    return self._expr.proxy().ndim

  @frame_base.with_docs_from(DataFrameGroupBy)
  def apply(self, func, *args, **kwargs):
    """Note that ``func`` will be called once during pipeline construction time
    with an empty pandas object, so take care if ``func`` has a side effect.

    When called with an empty pandas object, ``func`` is expected to return an
    object of the same type as what will be returned when the pipeline is
    processing actual data. If the result is a pandas object it should have the
    same type and name (for a Series) or column types and names (for
    a DataFrame) as the actual results.

    Note that in pandas, ``apply`` attempts to detect if the index is unmodified
    in ``func`` (indicating ``func`` is a transform) and drops the duplicate
    index in the output. To determine this, pandas tests the indexes for
    equality. However, Beam cannot do this since it is sensitive to the input
    data; instead this implementation tests if the indexes are equivalent
    with ``is``. See the `pandas 1.4.0 release notes
    <https://pandas.pydata.org/docs/dev/whatsnew/v1.4.0.html#groupby-apply-consistent-transform-detection>`_
    for a good explanation of the distinction between these approaches. In
    practice, this just means that in some cases the Beam result will have
    a duplicate index, whereas pandas would have dropped it."""

    project = _maybe_project_func(self._projection)
    grouping_indexes = self._grouping_indexes
    grouping_columns = self._grouping_columns
    group_keys = self._group_keys

    # Unfortunately pandas does not execute func to determine the right proxy.
    # We run user func on a proxy here to detect the return type and generate
    # the proxy.
    fn_input = project(self._ungrouped_with_index.proxy().reset_index(
        grouping_columns, drop=True))
    result = func(fn_input)
    def index_to_arrays(index):
      return [index.get_level_values(level)
              for level in range(index.nlevels)]


    # By default do_apply will just call pandas apply()
    # We override it below if necessary
    do_apply = lambda gb: gb.apply(func, *args, **kwargs)

    if (isinstance(result, pd.core.generic.NDFrame) and
        result.index is fn_input.index):
      # Special case where apply fn is a transform
      # Note we trust that if the user fn produces a proxy with the identical
      # index, it will produce results with identical indexes at execution
      # time too
      proxy = result
    elif isinstance(result, pd.DataFrame):
      # apply fn is not a transform, we need to make sure the original index
      # values are prepended to the result's index
      proxy = result[:0]

      # First adjust proxy
      proxy.index = pd.MultiIndex.from_arrays(
          index_to_arrays(self._ungrouped.proxy().index) +
          index_to_arrays(proxy.index),
          names=self._ungrouped.proxy().index.names + proxy.index.names)

      # Then override do_apply function
      new_index_names = self._ungrouped.proxy().index.names
      if len(new_index_names) > 1:
        def add_key_index(key, df):
          # df is a dataframe or Series representing the result of func for
          # a single key
          # key is a tuple with the MultiIndex values for this key
          df.index = pd.MultiIndex.from_arrays(
              [[key[i]] * len(df) for i in range(len(new_index_names))] +
              index_to_arrays(df.index),
              names=new_index_names + df.index.names)
          return df
      else:
        def add_key_index(key, df):
          # df is a dataframe or Series representing the result of func for
          # a single key
          df.index = pd.MultiIndex.from_arrays(
              [[key] * len(df)] + index_to_arrays(df.index),
              names=new_index_names + df.index.names)
          return df


      do_apply = lambda gb: pd.concat([
          add_key_index(k, func(gb.get_group(k), *args, **kwargs))
          for k in gb.groups.keys()])
    elif isinstance(result, pd.Series):
      if isinstance(fn_input, pd.DataFrame):
        # DataFrameGroupBy
        # In this case pandas transposes the Series result, s.t. the Series
        # index values are the columns, and the grouping keys are the new index
        # values.
        dtype = pd.Series([result]).dtype
        proxy = pd.DataFrame(columns=result.index,
                             dtype=result.dtype,
                             index=self._ungrouped.proxy().index)
      elif isinstance(fn_input, pd.Series):
        # SeriesGroupBy
        # In this case the output is still a Series, but with an additional
        # index with the grouping keys.
        proxy = pd.Series(dtype=result.dtype,
                          name=result.name,
                          index=index_to_arrays(self._ungrouped.proxy().index) +
                                index_to_arrays(result[:0].index))
    else:
      # The user fn returns some non-pandas type. The expected result is a
      # Series where each element is the result of one user fn call.
      dtype = pd.Series([result]).dtype
      proxy = pd.Series([], dtype=dtype, index=self._ungrouped.proxy().index)

    def do_partition_apply(df):
      # Remove columns from index, we only needed them there for partitioning
      df = df.reset_index(grouping_columns, drop=True)

      gb = df.groupby(level=grouping_indexes or None,
                      by=grouping_columns or None,
                      group_keys=group_keys)

      gb = project(gb)

      return do_apply(gb)

    return DeferredDataFrame(
        expressions.ComputedExpression(
            'apply',
            do_partition_apply,
            [self._ungrouped_with_index],
            proxy=proxy,
            requires_partition_by=partitionings.Index(grouping_indexes +
                                                      grouping_columns),
            preserves_partition_by=partitionings.Index(grouping_indexes)))


  @frame_base.with_docs_from(DataFrameGroupBy)
  def transform(self, fn, *args, **kwargs):
    """Note that ``func`` will be called once during pipeline construction time
    with an empty pandas object, so take care if ``func`` has a side effect.

    When called with an empty pandas object, ``func`` is expected to return an
    object of the same type as what will be returned when the pipeline is
    processing actual data. The result should have the same type and name (for
    a Series) or column types and names (for a DataFrame) as the actual
    results."""
    if not callable(fn):
      raise NotImplementedError(
          "String functions are not yet supported in transform.")

    if self._grouping_columns and not self._projection:
      grouping_columns = self._grouping_columns
      def fn_wrapper(x, *args, **kwargs):
        x = x.droplevel(grouping_columns)
        return fn(x, *args, **kwargs)
    else:
      fn_wrapper = fn

    project = _maybe_project_func(self._projection)
    group_keys = self._group_keys

    # pandas cannot execute fn to determine the right proxy.
    # We run user fn on a proxy here to detect the return type and generate the
    # proxy.
    result = fn_wrapper(project(self._ungrouped_with_index.proxy()))
    parent_frame = self._ungrouped.args()[0].proxy()
    if isinstance(result, pd.core.generic.NDFrame):
      proxy = result[:0]

    else:
      # The user fn returns some non-pandas type. The expected result is a
      # Series where each element is the result of one user fn call.
      dtype = pd.Series([result]).dtype
      proxy = pd.Series([], dtype=dtype, name=project(parent_frame).name)

      if not isinstance(self._projection, list):
        proxy.name = self._projection

    # The final result will have the original indexes
    proxy.index = parent_frame.index

    levels = self._grouping_indexes + self._grouping_columns

    return DeferredDataFrame(
        expressions.ComputedExpression(
            'transform',
            lambda df: project(
              df.groupby(level=levels, group_keys=group_keys)
            ).transform(
              fn_wrapper,
              *args,
              **kwargs).droplevel(self._grouping_columns),
            [self._ungrouped_with_index],
            proxy=proxy,
            requires_partition_by=partitionings.Index(levels),
            preserves_partition_by=partitionings.Index(self._grouping_indexes)))

  @frame_base.with_docs_from(DataFrameGroupBy)
  def pipe(self, func, *args, **kwargs):
    if isinstance(func, tuple):
      func, data = func
      kwargs[data] = self
      return func(*args, **kwargs)

    return func(self, *args, **kwargs)

  @frame_base.with_docs_from(DataFrameGroupBy)
  def filter(self, func=None, dropna=True):
    if func is None or not callable(func):
      raise TypeError("func must be specified and it must be callable")

    def apply_fn(df):
      if func(df):
        return df
      elif not dropna:
        result = df.copy()
        result.iloc[:, :] = np.nan
        return result
      else:
        return df.iloc[:0]

    return self.apply(apply_fn).droplevel(self._grouping_columns)

  @property  # type: ignore
  @frame_base.with_docs_from(DataFrameGroupBy)
  def dtypes(self):
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'dtypes',
            lambda gb: gb.dtypes,
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()
        )
    )

  if hasattr(DataFrameGroupBy, 'value_counts'):
    @frame_base.with_docs_from(DataFrameGroupBy)
    def value_counts(self, **kwargs):
      """
      DataFrameGroupBy.value_counts() is the same as DataFrame.value_counts()
      """
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'value_counts',
              lambda df: df.value_counts(**kwargs), [self._expr],
              preserves_partition_by=partitionings.Arbitrary(),
              requires_partition_by=partitionings.Arbitrary())
      )

  fillna = frame_base.wont_implement_method(
      DataFrameGroupBy, 'fillna', explanation=(
          "df.fillna() should be used instead. Only method=None is supported "
          "because other methods are order-sensitive. df.groupby(..).fillna() "
          "without a method is equivalent to df.fillna()."))

  ffill = frame_base.wont_implement_method(DataFrameGroupBy, 'ffill',
                                           reason="order-sensitive")
  bfill = frame_base.wont_implement_method(DataFrameGroupBy, 'bfill',
                                           reason="order-sensitive")
  pad = frame_base.wont_implement_method(DataFrameGroupBy, 'pad',
                                         reason="order-sensitive")
  backfill = frame_base.wont_implement_method(DataFrameGroupBy, 'backfill',
                                              reason="order-sensitive")

  aggregate = agg

  hist = frame_base.wont_implement_method(DataFrameGroupBy, 'hist',
                                          reason="plotting-tools")
  plot = frame_base.wont_implement_method(DataFrameGroupBy, 'plot',
                                          reason="plotting-tools")
  boxplot = frame_base.wont_implement_method(DataFrameGroupBy, 'boxplot',
                                             reason="plotting-tools")

  head = frame_base.wont_implement_method(
      DataFrameGroupBy, 'head', explanation=_PEEK_METHOD_EXPLANATION)
  tail = frame_base.wont_implement_method(
      DataFrameGroupBy, 'tail', explanation=_PEEK_METHOD_EXPLANATION)

  first = frame_base.not_implemented_method('first', base_type=DataFrameGroupBy)
  last = frame_base.not_implemented_method('last', base_type=DataFrameGroupBy)
  nth = property(frame_base.wont_implement_method(
      DataFrameGroupBy, 'nth', reason='order-sensitive'))
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
  diff = frame_base.wont_implement_method(DataFrameGroupBy, 'diff',
                                          reason='order-sensitive')
  shift = frame_base.wont_implement_method(DataFrameGroupBy, 'shift',
                                           reason='order-sensitive')

  pct_change = frame_base.wont_implement_method(DataFrameGroupBy, 'pct_change',
                                                reason='order-sensitive')
  ohlc = frame_base.wont_implement_method(DataFrameGroupBy, 'ohlc',
                                          reason='order-sensitive')

  # TODO(https://github.com/apache/beam/issues/20958): Consider allowing this
  # for categorical keys.
  __len__ = frame_base.wont_implement_method(
      DataFrameGroupBy, '__len__', reason="non-deferred-result")
  groups = property(frame_base.wont_implement_method(
      DataFrameGroupBy, 'groups', reason="non-deferred-result"))
  indices = property(frame_base.wont_implement_method(
      DataFrameGroupBy, 'indices', reason="non-deferred-result"))

  resample = frame_base.wont_implement_method(
      DataFrameGroupBy, 'resample', reason='event-time-semantics')
  rolling = frame_base.wont_implement_method(
      DataFrameGroupBy, 'rolling', reason='event-time-semantics')
  ewm = frame_base.wont_implement_method(
      DataFrameGroupBy, 'ewm', reason="event-time-semantics")
  expanding = frame_base.wont_implement_method(
      DataFrameGroupBy, 'expanding', reason="event-time-semantics")

  tshift = frame_base.wont_implement_method(
      DataFrameGroupBy, 'tshift', reason="deprecated")

def _maybe_project_func(projection: Optional[list[str]]):
  """ Returns identity func if projection is empty or None, else returns
  a function that projects the specified columns. """
  if projection:
    return lambda df: df[projection]
  else:
    return lambda x: x


def _liftable_agg(meth, postagg_meth=None):
  agg_name, _ = frame_base.name_and_func(meth)

  if postagg_meth is None:
    post_agg_name = agg_name
  else:
    post_agg_name, _ = frame_base.name_and_func(postagg_meth)

  @frame_base.with_docs_from(DataFrameGroupBy, name=agg_name)
  def wrapper(self, *args, **kwargs):
    assert isinstance(self, DeferredGroupBy)

    if 'min_count' in kwargs:
      return _unliftable_agg(meth)(self, *args, **kwargs)

    to_group = self._ungrouped.proxy().index
    is_categorical_grouping = any(
        isinstance(to_group.get_level_values(i).dtype, pd.CategoricalDtype)
        for i in self._grouping_indexes)
    groupby_kwargs = self._kwargs
    group_keys = self._group_keys

    # Don't include un-observed categorical values in the preagg
    preagg_groupby_kwargs = groupby_kwargs.copy()
    preagg_groupby_kwargs['observed'] = True

    project = _maybe_project_func(self._projection)
    pre_agg = expressions.ComputedExpression(
        'pre_combine_' + agg_name,
        lambda df: getattr(
            project(
                df.groupby(level=list(range(df.index.nlevels)),
                           group_keys=group_keys,
                           **preagg_groupby_kwargs)
            ),
            agg_name)(**kwargs),
        [self._ungrouped],
        requires_partition_by=partitionings.Arbitrary(),
        preserves_partition_by=partitionings.Arbitrary())


    post_agg = expressions.ComputedExpression(
        'post_combine_' + post_agg_name,
        lambda df: getattr(
            df.groupby(level=list(range(df.index.nlevels)),
                       group_keys=group_keys,
                       **groupby_kwargs),
            post_agg_name)(**kwargs),
        [pre_agg],
        requires_partition_by=(partitionings.Singleton(reason=(
            "Aggregations grouped by a categorical column are not currently "
            "parallelizable (https://github.com/apache/beam/issues/21827)."
        ))
                               if is_categorical_grouping
                               else partitionings.Index()),
        preserves_partition_by=partitionings.Arbitrary())
    return frame_base.DeferredFrame.wrap(post_agg)

  return wrapper


def _unliftable_agg(meth):
  agg_name, _ = frame_base.name_and_func(meth)

  @frame_base.with_docs_from(DataFrameGroupBy, name=agg_name)
  def wrapper(self, *args, **kwargs):
    assert isinstance(self, DeferredGroupBy)

    to_group = self._ungrouped.proxy().index
    group_keys = self._group_keys
    is_categorical_grouping = any(
        isinstance(to_group.get_level_values(i).dtype, pd.CategoricalDtype)
        for i in self._grouping_indexes)

    groupby_kwargs = self._kwargs
    project = _maybe_project_func(self._projection)
    post_agg = expressions.ComputedExpression(
        agg_name,
        lambda df: getattr(project(
            df.groupby(level=list(range(df.index.nlevels)),
                       group_keys=group_keys,
                       **groupby_kwargs),
        ), agg_name)(**kwargs),
        [self._ungrouped],
        requires_partition_by=(partitionings.Singleton(reason=(
            "Aggregations grouped by a categorical column are not currently "
            "parallelizable (https://github.com/apache/beam/issues/21827)."
        ))
                               if is_categorical_grouping
                               else partitionings.Index()),
        # Some aggregation methods (e.g. corr/cov) add additional index levels.
        # We only preserve the ones that existed _before_ the groupby.
        preserves_partition_by=partitionings.Index(
            list(range(self._ungrouped.proxy().index.nlevels))))
    return frame_base.DeferredFrame.wrap(post_agg)

  return wrapper

for meth in LIFTABLE_AGGREGATIONS:
  setattr(DeferredGroupBy, meth, _liftable_agg(meth))
for meth in LIFTABLE_WITH_SUM_AGGREGATIONS:
  setattr(DeferredGroupBy, meth, _liftable_agg(meth, postagg_meth='sum'))
for meth in UNLIFTABLE_AGGREGATIONS:
  if meth in ('kurt', 'kurtosis'):
    # pandas doesn't currently allow kurtosis on GroupBy:
    # https://github.com/pandas-dev/pandas/issues/40139
    continue
  setattr(DeferredGroupBy, meth, _unliftable_agg(meth))

def _check_str_or_np_builtin(agg_func, func_list):
  return agg_func in func_list or (
      getattr(agg_func, '__name__', None) in func_list
      and agg_func.__module__ in ('numpy', 'builtins'))


def _is_associative(agg_func):
  return _check_str_or_np_builtin(agg_func, LIFTABLE_AGGREGATIONS)

def _is_liftable_with_sum(agg_func):
  return _check_str_or_np_builtin(agg_func, LIFTABLE_WITH_SUM_AGGREGATIONS)

def _is_unliftable(agg_func):
  return _check_str_or_np_builtin(agg_func, UNLIFTABLE_AGGREGATIONS)

NUMERIC_AGGREGATIONS = ['max', 'min', 'prod', 'sum', 'mean', 'median', 'std',
                        'var', 'sem', 'skew', 'kurt', 'kurtosis']
# mad was removed in Pandas 2.0.
if PD_VERSION < (2, 0):
  NUMERIC_AGGREGATIONS.append('mad')

def _is_numeric(agg_func):
  return _check_str_or_np_builtin(agg_func, NUMERIC_AGGREGATIONS)


@populate_not_implemented(DataFrameGroupBy)
class _DeferredGroupByCols(frame_base.DeferredFrame):
  # It's not clear that all of these make sense in Pandas either...
  agg = aggregate = frame_base._elementwise_method('agg', base=DataFrameGroupBy)
  any = frame_base._elementwise_method('any', base=DataFrameGroupBy)
  all = frame_base._elementwise_method('all', base=DataFrameGroupBy)
  boxplot = frame_base.wont_implement_method(
      DataFrameGroupBy, 'boxplot', reason="plotting-tools")
  describe = frame_base.not_implemented_method('describe',
                                               base_type=DataFrameGroupBy)
  diff = frame_base._elementwise_method('diff', base=DataFrameGroupBy)
  fillna = frame_base._elementwise_method('fillna', base=DataFrameGroupBy)
  filter = frame_base._elementwise_method('filter', base=DataFrameGroupBy)
  first = frame_base._elementwise_method('first', base=DataFrameGroupBy)
  get_group = frame_base._elementwise_method('get_group', base=DataFrameGroupBy)
  head = frame_base.wont_implement_method(
      DataFrameGroupBy, 'head', explanation=_PEEK_METHOD_EXPLANATION)
  hist = frame_base.wont_implement_method(
      DataFrameGroupBy, 'hist', reason="plotting-tools")
  idxmax = frame_base._elementwise_method('idxmax', base=DataFrameGroupBy)
  idxmin = frame_base._elementwise_method('idxmin', base=DataFrameGroupBy)
  last = frame_base._elementwise_method('last', base=DataFrameGroupBy)
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
      DataFrameGroupBy, 'tail', explanation=_PEEK_METHOD_EXPLANATION)
  take = frame_base.wont_implement_method(
      DataFrameGroupBy, 'take', reason='deprecated')
  var = frame_base._elementwise_method('var', base=DataFrameGroupBy)
  # These already deprecated methods were removed in Pandas 2.0
  if PD_VERSION < (2, 0):
    mad = frame_base._elementwise_method('mad', base=DataFrameGroupBy)
    tshift = frame_base._elementwise_method('tshift', base=DataFrameGroupBy)

  @property # type: ignore
  @frame_base.with_docs_from(DataFrameGroupBy)
  def groups(self):
    return self._expr.proxy().groups

  @property # type: ignore
  @frame_base.with_docs_from(DataFrameGroupBy)
  def indices(self):
    return self._expr.proxy().indices

  @property # type: ignore
  @frame_base.with_docs_from(DataFrameGroupBy)
  def ndim(self):
    return self._expr.proxy().ndim

  @property # type: ignore
  @frame_base.with_docs_from(DataFrameGroupBy)
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
  def name(self):
    return self._frame._expr.proxy().index.name

  @name.setter
  def name(self, value):
    self.names = [value]

  @property
  def ndim(self):
    return self._frame._expr.proxy().index.ndim

  @property
  def dtype(self):
    return self._frame._expr.proxy().index.dtype

  @property
  def nlevels(self):
    return self._frame._expr.proxy().index.nlevels

  def __getattr__(self, name):
    raise NotImplementedError('index.%s' % name)


@populate_not_implemented(pd.core.indexing._LocIndexer)
class _DeferredLoc(object):
  def __init__(self, frame):
    self._frame = frame

  def __getitem__(self, key):
    if isinstance(key, tuple):
      rows, cols = key
      return self[rows][cols]
    elif isinstance(key, list) and key and isinstance(key[0], bool):
      # Aligned by numerical key.
      raise NotImplementedError(type(key))
    elif isinstance(key, list):
      # Select rows, but behaves poorly on missing values.
      raise NotImplementedError(type(key))
    elif isinstance(key, slice):
      args = [self._frame._expr]
      func = lambda df: df.loc[key]
    elif isinstance(key, frame_base.DeferredFrame):
      func = lambda df, key: df.loc[key]
      if pd.core.dtypes.common.is_bool_dtype(key._expr.proxy()):
        # Boolean indexer, just pass it in as-is
        args = [self._frame._expr, key._expr]
      else:
        # Likely a DeferredSeries of labels, overwrite the key's index with it's
        # values so we can colocate them with the labels they're selecting
        def data_to_index(s):
          s = s.copy()
          s.index = s
          return s

        reindexed_expr = expressions.ComputedExpression(
            'data_to_index',
            data_to_index,
            [key._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Singleton(),
        )
        args = [self._frame._expr, reindexed_expr]
    elif callable(key):

      def checked_callable_key(df):
        computed_index = key(df)
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
      func = lambda df: df.loc[checked_callable_key]
    else:
      raise NotImplementedError(type(key))

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'loc',
            func,
            args,
            requires_partition_by=(
                partitionings.JoinIndex()
                if len(args) > 1
                else partitionings.Arbitrary()),
            preserves_partition_by=partitionings.Arbitrary()))

  __setitem__ = frame_base.not_implemented_method(
      'loc.setitem', base_type=pd.core.indexing._LocIndexer)

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
  @frame_base.with_docs_from(pd.Series.str)
  @frame_base.args_to_kwargs(pd.Series.str)
  @frame_base.populate_defaults(pd.Series.str)
  def cat(self, others, join, **kwargs):
    """If defined, ``others`` must be a :class:`DeferredSeries` or a ``list`` of
    ``DeferredSeries``."""
    if others is None:
      # Concatenate series into a single String
      requires = partitionings.Singleton(reason=(
          "cat(others=None) concatenates all data in a Series into a single "
          "string, so it requires collecting all data on a single node."
      ))
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
          "others must be None, DeferredSeries, or list[DeferredSeries] "
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

  @frame_base.with_docs_from(pd.Series.str)
  @frame_base.args_to_kwargs(pd.Series.str)
  def repeat(self, repeats):
    """``repeats`` must be an ``int`` or a :class:`DeferredSeries`. Lists are
    not supported because they make this operation order-sensitive."""
    if isinstance(repeats, int):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'repeat',
              lambda series: series.str.repeat(repeats),
              [self._expr],
              # TODO(https://github.com/apache/beam/issues/20573): Defer to
              # pandas to compute this proxy. Currently it incorrectly infers
              # dtype bool, may require upstream fix.
              proxy=self._expr.proxy(),
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Arbitrary()))
    elif isinstance(repeats, frame_base.DeferredBase):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'repeat',
              lambda series, repeats_series: series.str.repeat(repeats_series),
              [self._expr, repeats._expr],
              # TODO(https://github.com/apache/beam/issues/20573): Defer to
              # pandas to compute this proxy. Currently it incorrectly infers
              # dtype bool, may require upstream fix.
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

  @frame_base.with_docs_from(pd.Series.str)
  @frame_base.args_to_kwargs(pd.Series.str)
  def get_dummies(self, **kwargs):
    """
    Series must be categorical dtype. Please cast to ``CategoricalDtype``
    to ensure correct categories.
    """
    dtype = self._expr.proxy().dtype
    if not isinstance(dtype, pd.CategoricalDtype):
      raise frame_base.WontImplementError(
          "get_dummies() of non-categorical type is not supported because "
          "the type of the output column depends on the data. Please use "
          "pd.CategoricalDtype with explicit categories.",
          reason="non-deferred-columns")

    split_cats = [
      cat.split(sep=kwargs.get('sep', '|')) for cat in dtype.categories
    ]

    # Adding the nan category because there could be the case that
    # the data includes NaNs, which is not valid to be casted as a Category,
    # but nevertheless would be broadcasted as a column in get_dummies()
    columns = sorted(set().union(*split_cats))
    columns = columns + ['nan'] if 'nan' not in columns else columns

    proxy = pd.DataFrame(columns=columns).astype(int)

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'get_dummies',
            lambda series: pd.concat(
              [proxy, series.str.get_dummies(**kwargs)]
              ).fillna(value=0, method=None).astype('int64'),
            [self._expr],
            proxy=proxy,
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()))

  def _split_helper(self, rsplit=False, **kwargs):
    expand = kwargs.get('expand', False)

    if not expand:
      # Not creating separate columns
      proxy = self._expr.proxy()
      if not rsplit:
        func = lambda s: pd.concat([proxy, s.str.split(**kwargs)])
      else:
        func = lambda s: pd.concat([proxy, s.str.rsplit(**kwargs)])
    else:
      # Creating separate columns, so must be more strict on dtype
      dtype = self._expr.proxy().dtype
      if not isinstance(dtype, pd.CategoricalDtype):
        method_name = 'rsplit' if rsplit else 'split'
        raise frame_base.WontImplementError(
            f"{method_name}() of non-categorical type is not supported because "
            "the type of the output column depends on the data. Please use "
            "pd.CategoricalDtype with explicit categories.",
            reason="non-deferred-columns")

      # Split the categories
      split_cats = dtype.categories.str.split(**kwargs)

      # Count the number of new columns to create for proxy
      max_splits = len(max(split_cats, key=len))
      proxy = pd.DataFrame(columns=range(max_splits))

      def func(s):
        if not rsplit:
          result = s.str.split(**kwargs)
        else:
          result = s.str.rsplit(**kwargs)
        result[~result.isna()].replace(np.nan, value=None)
        return result

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'split',
            func,
            [self._expr],
            proxy=proxy,
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()))

  @frame_base.with_docs_from(pd.Series.str)
  @frame_base.args_to_kwargs(pd.Series.str)
  @frame_base.populate_defaults(pd.Series.str)
  def split(self, **kwargs):
    """
    Like other non-deferred methods, dtype must be CategoricalDtype.
    One exception is when ``expand`` is ``False``. Because we are not
    creating new columns at construction time, dtype can be `str`.
    """
    return self._split_helper(rsplit=False, **kwargs)

  @frame_base.with_docs_from(pd.Series.str)
  @frame_base.args_to_kwargs(pd.Series.str)
  @frame_base.populate_defaults(pd.Series.str)
  def rsplit(self, **kwargs):
    """
    Like other non-deferred methods, dtype must be CategoricalDtype.
    One exception is when ``expand`` is ``False``. Because we are not
    creating new columns at construction time, dtype can be `str`.
    """
    return self._split_helper(rsplit=True, **kwargs)


ELEMENTWISE_STRING_METHODS = [
            'capitalize',
            'casefold',
            'center',
            'contains',
            'count',
            'decode',
            'encode',
            'endswith',
            'extract',
            'find',
            'findall',
            'fullmatch',
            'get',
            'index',
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
            'lfind',
            'ljust',
            'lower',
            'lstrip',
            'match',
            'normalize',
            'pad',
            'partition',
            'removeprefix',
            'removesuffix',
            'replace',
            'rpartition',
            'rfind',
            'rindex',
            'rjust',
            'rstrip',
            'slice',
            'slice_replace',
            'startswith',
            'strip',
            'swapcase',
            'title',
            'translate',
            'upper',
            'wrap',
            'zfill',
            '__getitem__',
]

NON_ELEMENTWISE_STRING_METHODS = [
            'extractall',
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
  if not hasattr(pd.Series.str, method):
    # older versions (1.0.x) don't support some of these methods
    continue
  setattr(_DeferredStringMethods,
          method,
          frame_base._elementwise_method(make_str_func(method),
                                         name=method,
                                         base=pd.Series.str))

for method in NON_ELEMENTWISE_STRING_METHODS:
  if not hasattr(pd.Series.str, method):
    # older versions (1.0.x) don't support some of these methods
    continue
  setattr(_DeferredStringMethods,
          method,
          frame_base._proxy_method(
              make_str_func(method),
              name=method,
              base=pd.Series.str,
              requires_partition_by=partitionings.Arbitrary(),
              preserves_partition_by=partitionings.Singleton()))


def make_cat_func(method):
  def func(df, *args, **kwargs):
    return getattr(df.cat, method)(*args, **kwargs)

  return func


class _DeferredCategoricalMethods(frame_base.DeferredBase):
  @property  # type: ignore
  @frame_base.with_docs_from(pd.core.arrays.categorical.CategoricalAccessor)
  def categories(self):
    return self._expr.proxy().cat.categories

  @property  # type: ignore
  @frame_base.with_docs_from(pd.core.arrays.categorical.CategoricalAccessor)
  def ordered(self):
    return self._expr.proxy().cat.ordered

  @property  # type: ignore
  @frame_base.with_docs_from(pd.core.arrays.categorical.CategoricalAccessor)
  def codes(self):
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'codes',
            lambda s: s.cat.codes,
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary(),
        )
    )

  remove_unused_categories = frame_base.wont_implement_method(
      pd.core.arrays.categorical.CategoricalAccessor,
      'remove_unused_categories', reason="non-deferred-columns")

ELEMENTWISE_CATEGORICAL_METHODS = [
    'add_categories',
    'as_ordered',
    'as_unordered',
    'remove_categories',
    'rename_categories',
    'reorder_categories',
    'set_categories',
]

for method in ELEMENTWISE_CATEGORICAL_METHODS:
  setattr(_DeferredCategoricalMethods,
          method,
          frame_base._elementwise_method(
              make_cat_func(method), name=method,
              base=pd.core.arrays.categorical.CategoricalAccessor))

class _DeferredDatetimeMethods(frame_base.DeferredBase):
  @property  # type: ignore
  @frame_base.with_docs_from(pd.core.indexes.accessors.DatetimeProperties)
  def tz(self):
    return self._expr.proxy().dt.tz

  @property  # type: ignore
  @frame_base.with_docs_from(pd.core.indexes.accessors.DatetimeProperties)
  def freq(self):
    return self._expr.proxy().dt.freq

  @frame_base.with_docs_from(pd.core.indexes.accessors.DatetimeProperties)
  def tz_localize(self, *args, ambiguous='infer', **kwargs):
    """``ambiguous`` cannot be set to ``"infer"`` as its semantics are
    order-sensitive. Similarly, specifying ``ambiguous`` as an
    :class:`~numpy.ndarray` is order-sensitive, but you can achieve similar
    functionality by specifying ``ambiguous`` as a Series."""
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
              lambda s,
              ambiguous: s.dt.tz_localize(*args, ambiguous=ambiguous, **kwargs),
              [self._expr, ambiguous._expr],
              requires_partition_by=partitionings.Index(),
              preserves_partition_by=partitionings.Arbitrary()))
    elif ambiguous == 'infer':
      # infer attempts to infer based on the order of the timestamps
      raise frame_base.WontImplementError(
          f"tz_localize(ambiguous={ambiguous!r}) is not allowed because it "
          "makes this operation sensitive to the order of the data.",
          reason="order-sensitive")

    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'tz_localize',
            lambda s: s.dt.tz_localize(*args, ambiguous=ambiguous, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Arbitrary()))


  to_period = frame_base.wont_implement_method(
      pd.core.indexes.accessors.DatetimeProperties, 'to_period',
      reason="event-time-semantics")
  to_pydatetime = frame_base.wont_implement_method(
      pd.core.indexes.accessors.DatetimeProperties, 'to_pydatetime',
      reason="non-deferred-result")
  to_pytimedelta = frame_base.wont_implement_method(
      pd.core.indexes.accessors.DatetimeProperties, 'to_pytimedelta',
      reason="non-deferred-result")

def make_dt_property(method):
  def func(df):
    return getattr(df.dt, method)

  return func

def make_dt_func(method):
  def func(df, *args, **kwargs):
    return getattr(df.dt, method)(*args, **kwargs)

  return func


ELEMENTWISE_DATETIME_METHODS = [
  'ceil',
  'day_name',
  'month_name',
  'floor',
  'isocalendar',
  'round',
  'normalize',
  'strftime',
  'tz_convert',
]

for method in ELEMENTWISE_DATETIME_METHODS:
  if not hasattr(pd.core.indexes.accessors.DatetimeProperties, method):
    # older versions (1.0.x) don't support some of these methods
    continue
  setattr(_DeferredDatetimeMethods,
          method,
          frame_base._elementwise_method(
              make_dt_func(method),
              name=method,
              base=pd.core.indexes.accessors.DatetimeProperties))

ELEMENTWISE_DATETIME_PROPERTIES = [
  'date',
  'day',
  'dayofweek',
  'dayofyear',
  'days_in_month',
  'daysinmonth',
  'hour',
  'is_leap_year',
  'is_month_end',
  'is_month_start',
  'is_quarter_end',
  'is_quarter_start',
  'is_year_end',
  'is_year_start',
  'microsecond',
  'minute',
  'month',
  'nanosecond',
  'quarter',
  'second',
  'time',
  'timetz',
  'weekday',
  'year',
]
# Pandas 2 removed these.
if PD_VERSION < (2, 0):
  ELEMENTWISE_DATETIME_PROPERTIES += ['week', 'weekofyear']

for method in ELEMENTWISE_DATETIME_PROPERTIES:
  setattr(_DeferredDatetimeMethods,
          method,
          property(frame_base._elementwise_method(
              make_dt_property(method),
              name=method,
              base=pd.core.indexes.accessors.DatetimeProperties)))


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

# Allow dataframe | SchemaTransform
def _create_maybe_elementwise_or(base):
  elementwise = frame_base._elementwise_method(
      '__or__', restrictions={'level': None}, base=base)

  def _maybe_elementwise_or(self, right):
    if isinstance(right, PTransform):
      return convert.to_dataframe(convert.to_pcollection(self) | right)
    else:
      return elementwise(self, right)

  return _maybe_elementwise_or


DeferredSeries.__or__ = _create_maybe_elementwise_or(pd.Series)  # type: ignore
DeferredDataFrame.__or__ = _create_maybe_elementwise_or(pd.DataFrame)  # type: ignore


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
DeferredSeries.subtract = DeferredSeries.sub  # type: ignore
DeferredDataFrame.subtract = DeferredDataFrame.sub  # type: ignore
DeferredSeries.divide = DeferredSeries.div  # type: ignore
DeferredDataFrame.divide = DeferredDataFrame.div  # type: ignore


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
