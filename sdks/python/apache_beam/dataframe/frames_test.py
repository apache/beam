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

import sys
import unittest

import numpy as np
import pandas as pd

import apache_beam as beam
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import frames  # pylint: disable=unused-import


class DeferredFrameTest(unittest.TestCase):
  def _run_test(self, func, *args):
    deferred_args = [
        frame_base.DeferredFrame.wrap(
            expressions.ConstantExpression(arg, arg[0:0])) for arg in args
    ]
    expected = func(*args)
    actual = expressions.Session({}).evaluate(func(*deferred_args)._expr)
    self.assertTrue(
        getattr(expected, 'equals', expected.__eq__)(actual),
        'Expected:\n\n%r\n\nActual:\n\n%r' % (expected, actual))

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

  def test_groupby(self):
    df = pd.DataFrame({'group': ['a', 'a', 'a', 'b'], 'value': [1, 2, 3, 5]})
    self._run_test(lambda df: df.groupby('group').agg(sum), df)
    self._run_test(lambda df: df.groupby('group').sum(), df)
    self._run_test(lambda df: df.groupby('group').median(), df)

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
