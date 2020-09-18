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

import pandas as pd

from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import partitionings


@frame_base.DeferredFrame._register_for(pd.Series)
class DeferredSeries(frame_base.DeferredFrame):
  def __array__(self, dtype=None):
    raise frame_base.WontImplementError(
        'Conversion to a non-deferred a numpy array.')

  astype = frame_base._elementwise_method('astype')

  between = frame_base._elementwise_method('between')

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  @frame_base.maybe_inplace
  def dropna(self, **kwargs):
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'dropna',
            lambda df: df.dropna(**kwargs), [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=partitionings.Nothing()))

  items = iteritems = frame_base.wont_implement_method('non-lazy')

  isin = frame_base._elementwise_method('isin')

  isna = frame_base._elementwise_method('isna')
  notnull = notna = frame_base._elementwise_method('notna')

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  @frame_base.maybe_inplace
  def fillna(self, value, method):
    if method is not None:
      raise frame_base.WontImplementError('order-sensitive')
    if isinstance(value, frame_base.DeferredBase):
      value_expr = value._expr
    else:
      value_expr = expressions.ConstantExpression(value)
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'fillna',
            lambda df,
            value: df.fillna(value, method=method), [self._expr, value_expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=partitionings.Nothing()))

  to_numpy = to_string = frame_base.wont_implement_method('non-deferred value')

  transform = frame_base._elementwise_method(
      'transform', restrictions={'axis': 0})

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
        intermediate = expressions.elementwise_expression(
            'pre_aggregate',
            lambda s: s.agg([base_func], *args, **kwargs), [self._expr])
        allow_nonparallel_final = True
      else:
        intermediate = self._expr
        allow_nonparallel_final = None  # i.e. don't change the value
      with expressions.allow_non_parallel_operations(allow_nonparallel_final):
        return frame_base.DeferredFrame.wrap(
            expressions.ComputedExpression(
                'aggregate',
                lambda s: s.agg(func, *args, **kwargs), [intermediate],
                preserves_partition_by=partitionings.Singleton(),
                requires_partition_by=partitionings.Singleton()))

  agg = aggregate

  all = frame_base._agg_method('all')
  any = frame_base._agg_method('any')
  min = frame_base._agg_method('min')
  max = frame_base._agg_method('max')
  prod = product = frame_base._agg_method('prod')
  sum = frame_base._agg_method('sum')

  cummax = cummin = cumsum = cumprod = frame_base.wont_implement_method(
      'order-sensitive')
  diff = frame_base.wont_implement_method('order-sensitive')

  head = tail = frame_base.wont_implement_method('order-sensitive')

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def nlargest(self, keep, **kwargs):
    # TODO(robertwb): Document 'any' option.
    # TODO(robertwb): Consider (conditionally) defaulting to 'any' if no
    # explicit keep parameter is requested.
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError('order-sensitive')
    kwargs['keep'] = keep
    per_partition = expressions.ComputedExpression(
        'nlargest-per-partition',
        lambda df: df.nlargest(**kwargs), [self._expr],
        preserves_partition_by=partitionings.Singleton(),
        requires_partition_by=partitionings.Nothing())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'nlargest',
              lambda df: df.nlargest(**kwargs), [per_partition],
              preserves_partition_by=partitionings.Singleton(),
              requires_partition_by=partitionings.Singleton()))

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  def nsmallest(self, keep, **kwargs):
    if keep == 'any':
      keep = 'first'
    elif keep != 'all':
      raise frame_base.WontImplementError('order-sensitive')
    kwargs['keep'] = keep
    per_partition = expressions.ComputedExpression(
        'nsmallest-per-partition',
        lambda df: df.nsmallest(**kwargs), [self._expr],
        preserves_partition_by=partitionings.Singleton(),
        requires_partition_by=partitionings.Nothing())
    with expressions.allow_non_parallel_operations(True):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'nsmallest',
              lambda df: df.nsmallest(**kwargs), [per_partition],
              preserves_partition_by=partitionings.Singleton(),
              requires_partition_by=partitionings.Singleton()))

  rename_axis = frame_base._elementwise_method('rename_axis')

  @frame_base.args_to_kwargs(pd.Series)
  @frame_base.populate_defaults(pd.Series)
  @frame_base.maybe_inplace
  def replace(self, limit, **kwargs):
    if limit is None:
      requires_partition_by = partitionings.Nothing()
    else:
      requires_partition_by = partitionings.Singleton()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'replace',
            lambda df: df.replace(limit=limit, **kwargs), [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  round = frame_base._elementwise_method('round')

  searchsorted = frame_base.wont_implement_method('order-sensitive')

  shift = frame_base.wont_implement_method('order-sensitive')

  take = frame_base.wont_implement_method('deprecated')

  to_dict = frame_base.wont_implement_method('non-deferred')

  to_frame = frame_base._elementwise_method('to_frame')

  def unique(self, as_series=False):
    if not as_series:
      raise frame_base.WontImplementError(
          'pass as_series=True to get the result as a (deferred) Series')
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
        preserves_partition_by=partitionings.Singleton(),
        requires_partition_by=partitionings.Index())

  unstack = frame_base.wont_implement_method('non-deferred column values')

  values = property(frame_base.wont_implement_method('non-deferred'))

  view = frame_base.wont_implement_method('memory sharing semantics')

  @property
  def str(self):
    expr = expressions.ComputedExpression(
        'str',
        lambda df: df.str, [self._expr],
        requires_partition_by=partitionings.Nothing(),
        preserves_partition_by=partitionings.Singleton())
    return _DeferredStringMethods(expr)


for base in ['add',
             'sub',
             'mul',
             'div',
             'truediv',
             'floordiv',
             'mod',
             'pow',
             'and',
             'or']:
  for p in ['%s', 'r%s', '__%s__', '__r%s__']:
    # TODO: non-trivial level?
    name = p % base
    setattr(
        DeferredSeries,
        name,
        frame_base._elementwise_method(name, restrictions={'level': None}))
  setattr(
      DeferredSeries,
      '__i%s__' % base,
      frame_base._elementwise_method('__i%s__' % base, inplace=True))
for name in ['__lt__', '__le__', '__gt__', '__ge__', '__eq__', '__ne__']:
  setattr(DeferredSeries, name, frame_base._elementwise_method(name))
for name in ['apply', 'map', 'transform']:
  setattr(DeferredSeries, name, frame_base._elementwise_method(name))


@frame_base.DeferredFrame._register_for(pd.DataFrame)
class DeferredDataFrame(frame_base.DeferredFrame):
  @property
  def T(self):
    return self.transpose()

  def groupby(self, cols):
    # TODO: what happens to the existing index?
    # We set the columns to index as we have a notion of being partitioned by
    # index, but not partitioned by an arbitrary subset of columns.
    return DeferredGroupBy(
        expressions.ComputedExpression(
            'groupbyindex',
            lambda df: df.groupby(level=list(range(df.index.nlevels))),
            [self.set_index(cols)._expr],
            requires_partition_by=partitionings.Index(),
            preserves_partition_by=partitionings.Singleton()))

  def __getattr__(self, name):
    # Column attribute access.
    if name in self._expr.proxy().columns:
      return self[name]
    else:
      return object.__getattribute__(self, name)

  def __getitem__(self, key):
    # TODO: Replicate pd.DataFrame.__getitem__ logic
    if (isinstance(key, list) and
        all(key_column in self._expr.proxy().columns
            for key_column in key)) or key in self._expr.proxy().columns:
      return self._elementwise(lambda df: df[key], 'get_column')
    else:
      raise NotImplementedError(key)

  def __setitem__(self, key, value):
    if isinstance(key, str):
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
  @frame_base.maybe_inplace
  def set_index(self, keys, **kwargs):
    if isinstance(keys, str):
      keys = [keys]
    if not set(keys).issubset(self._expr.proxy().columns):
      raise NotImplementedError(keys)
    return frame_base.DeferredFrame.wrap(
      expressions.ComputedExpression(
          'set_index',
          lambda df: df.set_index(keys, **kwargs),
          [self._expr],
          requires_partition_by=partitionings.Nothing(),
          preserves_partition_by=partitionings.Nothing()))

  def at(self, *args, **kwargs):
    raise NotImplementedError()

  @property
  def loc(self):
    return _DeferredLoc(self)

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
              requires_partition_by=partitionings.Nothing()))
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

  applymap = frame_base._elementwise_method('applymap')

  memory_usage = frame_base.wont_implement_method('non-deferred value')

  all = frame_base._agg_method('all')
  any = frame_base._agg_method('any')

  cummax = cummin = cumsum = cumprod = frame_base.wont_implement_method(
      'order-sensitive')
  diff = frame_base.wont_implement_method('order-sensitive')

  head = tail = frame_base.wont_implement_method('order-sensitive')

  max = frame_base._agg_method('max')
  min = frame_base._agg_method('min')

  def mode(self, axis=0, *args, **kwargs):
    if axis == 1 or axis == 'columns':
      raise frame_base.WontImplementError('non-deferred column values')
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
      requires_partition_by = partitionings.Nothing()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'dropna',
            lambda df: df.dropna(axis=axis, **kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def fillna(self, value, method, axis, **kwargs):
    if method is not None and axis in (0, 'index'):
      raise frame_base.WontImplementError('order-sensitive')
    if isinstance(value, frame_base.DeferredBase):
      value_expr = value._expr
    else:
      value_expr = expressions.ConstantExpression(value)
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'fillna',
            lambda df, value: df.fillna(
                value, method=method, axis=axis, **kwargs),
            [self._expr, value_expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=partitionings.Nothing()))

  isna = frame_base._elementwise_method('isna')
  notnull = notna = frame_base._elementwise_method('notna')

  items = itertuples = iterrows = iteritems = frame_base.wont_implement_method(
      'non-lazy')

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
              preserves_partition_by=partitionings.Nothing(),
              requires_partition_by=partitionings.Nothing()))
    def revert(df):
      return frame_base.DeferredFrame.wrap(
          expressions.ComputedExpression(
              'join_restoreindex',
              lambda df:
                  df.reset_index().set_index(new_index_names)
                  .rename_axis(index=original_index_names, copy=False),
              [df._expr],
              preserves_partition_by=partitionings.Nothing(),
              requires_partition_by=partitionings.Nothing()))
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
            preserves_partition_by=partitionings.Singleton(),
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
    if not any([on, left_on, right_on, left_index, right_index]):
      on = [col for col in self_proxy.columns() if col in right_proxy.columns()]
    if not left_on:
      left_on = on
    elif not isinstance(left_on, list):
      left_on = [left_on]
    if not right_on:
      right_on = on
    elif not isinstance(right_on, list):
      right_on = [right_on]

    if left_index:
      indexed_left = self
    else:
      indexed_left = self.set_index(left_on, drop=False)

    if right_index:
      indexed_right = right
    else:
      indexed_right = right.set_index(right_on, drop=False)

    merged = frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'merge',
            lambda left, right: left.merge(
                right, left_index=True, right_index=True, **kwargs),
            [indexed_left._expr, indexed_right._expr],
            preserves_partition_by=partitionings.Singleton(),
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
      raise frame_base.WontImplementError('order-sensitive')
    kwargs['keep'] = keep
    per_partition = expressions.ComputedExpression(
            'nlargest-per-partition',
            lambda df: df.nlargest(**kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=partitionings.Nothing())
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
      raise frame_base.WontImplementError('order-sensitive')
    kwargs['keep'] = keep
    per_partition = expressions.ComputedExpression(
            'nsmallest-per-partition',
            lambda df: df.nsmallest(**kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=partitionings.Nothing())
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
      requires_partition_by = partitionings.Nothing()
    else:
      requires_partition_by = partitionings.Singleton()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'nunique',
            lambda df: df.nunique(**kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  prod = product = frame_base._agg_method('prod')

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def quantile(self, axis, **kwargs):
    if axis == 1 or axis == 'columns':
      raise frame_base.WontImplementError('non-deferred column values')
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'quantile',
            lambda df: df.quantile(axis=axis, **kwargs),
            [self._expr],
            #TODO(robertwb): Approximate quantiles?
            requires_partition_by=partitionings.Singleton(),
            preserves_partition_by=partitionings.Singleton()))

  query = frame_base._elementwise_method('query')

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.maybe_inplace
  def rename(self, **kwargs):
    rename_index = (
        'index' in kwargs
        or kwargs.get('axis', None) in (0, 'index')
        or ('columns' not in kwargs and 'axis' not in kwargs))
    if rename_index:
      # Technically, it's still partitioned by index, but it's no longer
      # partitioned by the hash of the index.
      preserves_partition_by = partitionings.Nothing()
    else:
      preserves_partition_by = partitionings.Singleton()
    if kwargs.get('errors', None) == 'raise' and rename_index:
      # Renaming index with checking.
      requires_partition_by = partitionings.Singleton()
      proxy = self._expr.proxy()
    else:
      requires_partition_by = partitionings.Nothing()
      proxy = None
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'rename',
            lambda df: df.rename(**kwargs),
            [self._expr],
            proxy=proxy,
            preserves_partition_by=preserves_partition_by,
            requires_partition_by=requires_partition_by))

  rename_axis = frame_base._elementwise_method('rename_axis')

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def replace(self, limit, **kwargs):
    if limit is None:
      requires_partition_by = partitionings.Nothing()
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
    if level is not None and not isinstance(level, (tuple, list)):
      level = [level]
    if level is None or len(level) == len(self._expr.proxy().index.levels):
      # TODO: Could do distributed re-index with offsets.
      requires_partition_by = partitionings.Singleton()
    else:
      requires_partition_by = partitionings.Nothing()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'reset_index',
            lambda df: df.reset_index(level=level, **kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  round = frame_base._elementwise_method('round')
  select_dtypes = frame_base._elementwise_method('select_dtypes')

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  def shift(self, axis, **kwargs):
    if axis == 1 or axis == 'columns':
      requires_partition_by = partitionings.Nothing()
    else:
      requires_partition_by = partitionings.Singleton()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'shift',
            lambda df: df.shift(axis=axis, **kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  @property
  def shape(self):
    raise frame_base.WontImplementError('scalar value')

  @frame_base.args_to_kwargs(pd.DataFrame)
  @frame_base.populate_defaults(pd.DataFrame)
  @frame_base.maybe_inplace
  def sort_values(self, axis, **kwargs):
    if axis == 1 or axis == 'columns':
      requires_partition_by = partitionings.Nothing()
    else:
      requires_partition_by = partitionings.Singleton()
    return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'sort_values',
            lambda df: df.sort_values(axis=axis, **kwargs),
            [self._expr],
            preserves_partition_by=partitionings.Singleton(),
            requires_partition_by=requires_partition_by))

  stack = frame_base._elementwise_method('stack')

  sum = frame_base._agg_method('sum')

  take = frame_base.wont_implement_method('deprecated')

  to_records = to_dict = to_numpy = to_string = (
      frame_base.wont_implement_method('non-deferred value'))

  to_sparse = to_string # frame_base._elementwise_method('to_sparse')

  transform = frame_base._elementwise_method(
      'transform', restrictions={'axis': 0})

  def transpose(self, *args, **kwargs):
    raise frame_base.WontImplementError('non-deferred column values')

  def unstack(self, *args, **kwargs):
    if self._expr.proxy().index.nlevels == 1:
      return frame_base.DeferredFrame.wrap(
        expressions.ComputedExpression(
            'unstack',
            lambda df: df.unstack(*args, **kwargs),
            [self._expr],
            requires_partition_by=partitionings.Index()))
    else:
      raise frame_base.WontImplementError('non-deferred column values')

  update = frame_base._proxy_method(
      'update',
      inplace=True,
      requires_partition_by=partitionings.Index(),
      preserves_partition_by=partitionings.Index())

for meth in ('filter', ):
  setattr(DeferredDataFrame, meth, frame_base._elementwise_method(meth))


class DeferredGroupBy(frame_base.DeferredFrame):
  def agg(self, fn):
    if not callable(fn):
      raise NotImplementedError(fn)
    return DeferredDataFrame(
        expressions.ComputedExpression(
            'agg',
            lambda df: df.agg(fn), [self._expr],
            requires_partition_by=partitionings.Index(),
            preserves_partition_by=partitionings.Singleton()))


def _liftable_agg(meth):
  name, func = frame_base.name_and_func(meth)

  def wrapper(self, *args, **kargs):
    assert isinstance(self, DeferredGroupBy)
    ungrouped = self._expr.args()[0]
    pre_agg = expressions.ComputedExpression(
        'pre_combine_' + name,
        lambda df: func(df.groupby(level=list(range(df.index.nlevels)))),
        [ungrouped],
        requires_partition_by=partitionings.Nothing(),
        preserves_partition_by=partitionings.Singleton())
    post_agg = expressions.ComputedExpression(
        'post_combine_' + name,
        lambda df: func(df.groupby(level=list(range(df.index.nlevels)))),
        [pre_agg],
        requires_partition_by=partitionings.Index(),
        preserves_partition_by=partitionings.Singleton())
    return frame_base.DeferredFrame.wrap(post_agg)

  return wrapper


def _unliftable_agg(meth):
  name, func = frame_base.name_and_func(meth)

  def wrapper(self, *args, **kargs):
    assert isinstance(self, DeferredGroupBy)
    ungrouped = self._expr.args()[0]
    post_agg = expressions.ComputedExpression(
        name,
        lambda df: func(df.groupby(level=list(range(df.index.nlevels)))),
        [ungrouped],
        requires_partition_by=partitionings.Index(),
        preserves_partition_by=partitionings.Singleton())
    return frame_base.DeferredFrame.wrap(post_agg)

  return wrapper

LIFTABLE_AGGREGATIONS = ['all', 'any', 'max', 'min', 'prod', 'size', 'sum']
UNLIFTABLE_AGGREGATIONS = ['mean', 'median', 'std', 'var']

for meth in LIFTABLE_AGGREGATIONS:
  setattr(DeferredGroupBy, meth, _liftable_agg(meth))
for meth in UNLIFTABLE_AGGREGATIONS:
  setattr(DeferredGroupBy, meth, _unliftable_agg(meth))


def _is_associative(func):
  return func in LIFTABLE_AGGREGATIONS or (
      getattr(func, '__name__', None) in LIFTABLE_AGGREGATIONS
      and func.__module__ in ('numpy', 'builtins'))


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
                else partitionings.Nothing()),
            preserves_partition_by=partitionings.Singleton()))

class _DeferredStringMethods(frame_base.DeferredBase):
  pass

ELEMENTWISE_STRING_METHODS = [
            'capitalize',
            'casefold',
            'contains',
            'count',
            'endswith',
            'extract',
            'extractall',
            'findall',
            'get',
            'get_dummies',
            'isalpha',
            'isalnum',
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

for method in ELEMENTWISE_STRING_METHODS:
  setattr(_DeferredStringMethods,
          method,
          frame_base._elementwise_method(method))
