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

import math
import sys
import unittest

import numpy as np
import pandas as pd

import apache_beam as beam
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import frames  # pylint: disable=unused-import


class DeferredFrameTest(unittest.TestCase):
  def _run_test(self, func, *args, distributed=True):
    deferred_args = [
        frame_base.DeferredFrame.wrap(
            expressions.ConstantExpression(arg, arg[0:0])) for arg in args
    ]
    expected = func(*args)
    session_type = (
        expressions.PartitioningSession if distributed else expressions.Session)
    actual = session_type({}).evaluate(func(*deferred_args)._expr)
    if hasattr(expected, 'equals'):
      if distributed:
        cmp = lambda df: expected.sort_index().equals(df.sort_index())
      else:
        cmp = expected.equals
    elif isinstance(expected, float):
      cmp = lambda x: (math.isnan(x) and math.isnan(expected)
                       ) or x == expected == 0 or abs(expected - x) / (
                           abs(expected) + abs(x)) < 1e-8
    else:
      cmp = expected.__eq__
    self.assertTrue(
        cmp(actual), 'Expected:\n\n%r\n\nActual:\n\n%r' % (expected, actual))

  def test_series_arithmetic(self):
    a = pd.Series([1, 2, 3])
    b = pd.Series([100, 200, 300])
    self._run_test(lambda a, b: a - 2 * b, a, b)

  def test_get_column(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_test(lambda df: df['Animal'], df)
    self._run_test(lambda df: df.Speed, df)

  def test_set_column(self):
    def new_column(df):
      df['NewCol'] = df['Speed']
      return df

    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_test(new_column, df)

  def test_set_column_from_index(self):
    def new_column(df):
      df['NewCol'] = df.index
      return df

    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_test(new_column, df)

  def test_groupby(self):
    df = pd.DataFrame({
        'group': ['a' if i % 5 == 0 or i % 3 == 0 else 'b' for i in range(100)],
        'value': [None if i % 11 == 0 else i for i in range(100)]
    })
    self._run_test(lambda df: df.groupby('group').agg(sum), df)
    self._run_test(lambda df: df.groupby('group').sum(), df)
    self._run_test(lambda df: df.groupby('group').median(), df)
    self._run_test(lambda df: df.groupby('group').size(), df)
    self._run_test(lambda df: df.groupby('group').count(), df)
    self._run_test(lambda df: df.groupby('group').max(), df)
    self._run_test(lambda df: df.groupby('group').min(), df)
    self._run_test(lambda df: df.groupby('group').mean(), df)

    self._run_test(lambda df: df[df.value > 30].groupby('group').sum(), df)
    self._run_test(lambda df: df[df.value > 30].groupby('group').mean(), df)
    self._run_test(lambda df: df[df.value > 30].groupby('group').size(), df)

    # Grouping by a series is not currently supported
    #self._run_test(lambda df: df[df.value > 40].groupby(df.group).sum(), df)
    #self._run_test(lambda df: df[df.value > 40].groupby(df.group).mean(), df)
    #self._run_test(lambda df: df[df.value > 40].groupby(df.group).size(), df)

    # Example from https://pandas.pydata.org/docs/user_guide/groupby.html
    arrays = [['bar', 'bar', 'baz', 'baz', 'foo', 'foo', 'qux', 'qux'],
              ['one', 'two', 'one', 'two', 'one', 'two', 'one', 'two']]

    index = pd.MultiIndex.from_arrays(arrays, names=['first', 'second'])

    df = pd.DataFrame({
        'A': [1, 1, 1, 1, 2, 2, 3, 3], 'B': np.arange(8)
    },
                      index=index)

    self._run_test(lambda df: df.groupby(['second', 'A']).sum(), df)

  @unittest.skipIf(sys.version_info <= (3, ), 'differing signature')
  def test_merge(self):
    # This is from the pandas doctests, but fails due to re-indexing being
    # order-sensitive.
    df1 = pd.DataFrame({
        'lkey': ['foo', 'bar', 'baz', 'foo'], 'value': [1, 2, 3, 5]
    })
    df2 = pd.DataFrame({
        'rkey': ['foo', 'bar', 'baz', 'foo'], 'value': [5, 6, 7, 8]
    })
    with beam.dataframe.allow_non_parallel_operations():
      self._run_test(
          lambda df1,
          df2: df1.merge(df2, left_on='lkey', right_on='rkey').rename(
              index=lambda x: '*').sort_values(['value_x', 'value_y']),
          df1,
          df2)
      self._run_test(
          lambda df1,
          df2: df1.merge(
              df2,
              left_on='lkey',
              right_on='rkey',
              suffixes=('_left', '_right')).rename(index=lambda x: '*').
          sort_values(['value_left', 'value_right']),
          df1,
          df2)

  def test_series_getitem(self):
    s = pd.Series([x**2 for x in range(10)])
    self._run_test(lambda s: s[...], s)
    self._run_test(lambda s: s[:], s)
    self._run_test(lambda s: s[s < 10], s)
    self._run_test(lambda s: s[lambda s: s < 10], s)

    s.index = s.index.map(float)
    self._run_test(lambda s: s[1.5:6], s)

  def test_dataframe_getitem(self):
    df = pd.DataFrame({'A': [x**2 for x in range(6)], 'B': list('abcdef')})
    self._run_test(lambda df: df['A'], df)
    self._run_test(lambda df: df[['A', 'B']], df)

    self._run_test(lambda df: df[:], df)
    self._run_test(lambda df: df[df.A < 10], df)

    df.index = df.index.map(float)
    self._run_test(lambda df: df[1.5:4], df)

  def test_loc(self):
    dates = pd.date_range('1/1/2000', periods=8)
    df = pd.DataFrame(
        np.arange(32).reshape((8, 4)),
        index=dates,
        columns=['A', 'B', 'C', 'D'])
    self._run_test(lambda df: df.loc[:], df)
    self._run_test(lambda df: df.loc[:, 'A'], df)
    self._run_test(lambda df: df.loc[:dates[3]], df)
    self._run_test(lambda df: df.loc[df.A > 10], df)
    self._run_test(lambda df: df.loc[lambda df: df.A > 10], df)

  def test_series_agg(self):
    s = pd.Series(list(range(16)))
    self._run_test(lambda s: s.agg('sum'), s)
    self._run_test(lambda s: s.agg(['sum']), s)
    with beam.dataframe.allow_non_parallel_operations():
      self._run_test(lambda s: s.agg(['sum', 'mean']), s)
      self._run_test(lambda s: s.agg(['mean']), s)
      self._run_test(lambda s: s.agg('mean'), s)

  @unittest.skipIf(sys.version_info < (3, 6), 'Nondeterministic dict ordering.')
  def test_dataframe_agg(self):
    df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [2, 3, 5, 7]})
    self._run_test(lambda df: df.agg('sum'), df)
    with beam.dataframe.allow_non_parallel_operations():
      self._run_test(lambda df: df.agg(['sum', 'mean']), df)
      self._run_test(lambda df: df.agg({'A': 'sum', 'B': 'sum'}), df)
      self._run_test(lambda df: df.agg({'A': 'sum', 'B': 'mean'}), df)
      self._run_test(lambda df: df.agg({'A': ['sum', 'mean']}), df)
      self._run_test(lambda df: df.agg({'A': ['sum', 'mean'], 'B': 'min'}), df)

  @unittest.skipIf(sys.version_info < (3, 6), 'Nondeterministic dict ordering.')
  def test_smallest_largest(self):
    df = pd.DataFrame({'A': [1, 1, 2, 2], 'B': [2, 3, 5, 7]})
    self._run_test(lambda df: df.nlargest(1, 'A', keep='all'), df)
    self._run_test(lambda df: df.nsmallest(3, 'A', keep='all'), df)
    self._run_test(lambda df: df.nlargest(3, ['A', 'B'], keep='all'), df)

  def test_series_cov_corr(self):
    for s in [pd.Series([1, 2, 3]),
              pd.Series(range(100)),
              pd.Series([x**3 for x in range(-50, 50)])]:
      self._run_test(lambda s: s.std(), s)
      self._run_test(lambda s: s.corr(s), s)
      self._run_test(lambda s: s.corr(s + 1), s)
      self._run_test(lambda s: s.corr(s * s), s)
      self._run_test(lambda s: s.cov(s * s), s)

  def test_dataframe_cov_corr(self):
    df = pd.DataFrame(np.random.randn(20, 3), columns=['a', 'b', 'c'])
    df.loc[df.index[:5], 'a'] = np.nan
    df.loc[df.index[5:10], 'b'] = np.nan
    self._run_test(lambda df: df.corr().round(8), df)
    self._run_test(lambda df: df.cov().round(8), df)
    self._run_test(lambda df: df.corr(min_periods=12).round(8), df)
    self._run_test(lambda df: df.cov(min_periods=12).round(8), df)
    self._run_test(lambda df: df.corrwith(df.a).round(8), df)
    self._run_test(
        lambda df: df[['a', 'b']].corrwith(df[['b', 'c']]).round(8), df)

  def test_categorical_groupby(self):
    df = pd.DataFrame({'A': np.arange(6), 'B': list('aabbca')})
    df['B'] = df['B'].astype(pd.CategoricalDtype(list('cab')))
    df = df.set_index('B')
    # TODO(BEAM-11190): These aggregations can be done in index partitions, but
    # it will require a little more complex logic
    with beam.dataframe.allow_non_parallel_operations():
      self._run_test(lambda df: df.groupby(level=0).sum(), df)
      self._run_test(lambda df: df.groupby(level=0).mean(), df)

  def test_dataframe_eval_query(self):
    df = pd.DataFrame(np.random.randn(20, 3), columns=['a', 'b', 'c'])
    self._run_test(lambda df: df.eval('foo = a + b - c'), df)
    self._run_test(lambda df: df.query('a > b + c'), df)

    def eval_inplace(df):
      df.eval('foo = a + b - c', inplace=True)
      return df.foo

    self._run_test(eval_inplace, df)

    # Verify that attempting to access locals raises a useful error
    deferred_df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(df, df[0:0]))
    self.assertRaises(
        NotImplementedError, lambda: deferred_df.eval('foo = a + @b - c'))
    self.assertRaises(
        NotImplementedError, lambda: deferred_df.query('a > @b + c'))


class AllowNonParallelTest(unittest.TestCase):
  def _use_non_parallel_operation(self):
    _ = frame_base.DeferredFrame.wrap(
        expressions.PlaceholderExpression(pd.Series([1, 2, 3]))).replace(
            'a', 'b', limit=1)

  def test_disallow_non_parallel(self):
    with self.assertRaises(expressions.NonParallelOperation):
      self._use_non_parallel_operation()

  def test_allow_non_parallel_in_context(self):
    with beam.dataframe.allow_non_parallel_operations():
      self._use_non_parallel_operation()

  def test_allow_non_parallel_nesting(self):
    # disallowed
    with beam.dataframe.allow_non_parallel_operations():
      # allowed
      self._use_non_parallel_operation()
      with beam.dataframe.allow_non_parallel_operations(False):
        # disallowed again
        with self.assertRaises(expressions.NonParallelOperation):
          self._use_non_parallel_operation()
      # allowed
      self._use_non_parallel_operation()
    # disallowed
    with self.assertRaises(expressions.NonParallelOperation):
      self._use_non_parallel_operation()


if __name__ == '__main__':
  unittest.main()
