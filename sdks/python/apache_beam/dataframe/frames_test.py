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

import unittest

import numpy as np
import pandas as pd
from parameterized import parameterized

import apache_beam as beam
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import frames

PD_VERSION = tuple(map(int, pd.__version__.split('.')))

GROUPBY_DF = pd.DataFrame({
    'group': ['a' if i % 5 == 0 or i % 3 == 0 else 'b' for i in range(100)],
    'foo': [None if i % 11 == 0 else i for i in range(100)],
    'bar': [None if i % 7 == 0 else 99 - i for i in range(100)],
    'baz': [None if i % 13 == 0 else i * 2 for i in range(100)],
    'bool': [i % 17 == 0 for i in range(100)],
    'str': [str(i) for i in range(100)],
})


def _get_deferred_args(*args):
  return [
      frame_base.DeferredFrame.wrap(
          expressions.ConstantExpression(arg, arg[0:0])) for arg in args
  ]


class _AbstractFrameTest(unittest.TestCase):
  """Test sub-class with utilities for verifying DataFrame operations."""
  def _run_error_test(
      self, func, *args, construction_time=True, distributed=True):
    """Verify that func(*args) raises the same exception in pandas and in Beam.

    Note that by default this only checks for exceptions that the Beam DataFrame
    API raises during expression generation (i.e. construction time).
    Exceptions raised while the pipeline is executing are less helpful, but
    are sometimes unavoidable (e.g. data validation exceptions), to check for
    these exceptions use construction_time=False."""
    deferred_args = _get_deferred_args(*args)

    # Get expected error
    try:
      expected = func(*args)
    except Exception as e:
      expected_error = e
    else:
      raise AssertionError(
          "Expected an error, but executing with pandas successfully "
          f"returned:\n{expected}")

    # Get actual error
    if construction_time:
      try:
        _ = func(*deferred_args)._expr
      except Exception as e:
        actual = e
      else:
        raise AssertionError(
            f"Expected an error:\n{expected_error}\nbut Beam successfully "
            f"generated an expression.")
    else:  # not construction_time
      # Check for an error raised during pipeline execution
      expr = func(*deferred_args)._expr
      session_type = (
          expressions.PartitioningSession
          if distributed else expressions.Session)
      try:
        result = session_type({}).evaluate(expr)
      except Exception as e:
        actual = e
      else:
        raise AssertionError(
            f"Expected an error:\n{expected_error}\nbut Beam successfully "
            f"Computed the result:\n{result}.")

    # Verify
    if (not isinstance(actual, type(expected_error)) or
        str(expected_error) not in str(actual)):
      raise AssertionError(
          f'Expected {expected_error!r} to be raised, but got {actual!r}'
      ) from actual

  def _run_inplace_test(self, func, arg, **kwargs):
    """Verify an inplace operation performed by func.

    Checks that func performs the same inplace operation on arg, in pandas and
    in Beam."""
    def wrapper(df):
      df = df.copy()
      func(df)
      return df

    self._run_test(wrapper, arg, **kwargs)

  def _run_test(
      self, func, *args, distributed=True, nonparallel=False, check_proxy=True):
    """Verify that func(*args) produces the same result in pandas and in Beam.

    Args:
        distributed (bool): Whether or not to use PartitioningSession to
            simulate parallel execution.
        nonparallel (bool): Whether or not this function contains a
            non-parallelizable operation. If True, the expression will be
            generated twice, once outside of an allow_non_parallel_operations
            block (to verify NonParallelOperation is raised), and again inside
            of an allow_non_parallel_operations block to actually generate an
            expression to verify.
        check_proxy (bool): Whether or not to check that the proxy of the
            generated expression matches the actual result, defaults to True.
            This option should NOT be set to False in tests added for new
            operations if at all possible. Instead make sure the new operation
            produces the correct proxy. This flag only exists as an escape hatch
            until existing failures can be addressed (BEAM-12379)."""
    # Compute expected value
    expected = func(*args)

    # Compute actual value
    deferred_args = _get_deferred_args(*args)
    if nonparallel:
      # First run outside a nonparallel block to confirm this raises as expected
      with self.assertRaises(expressions.NonParallelOperation) as raised:
        func(*deferred_args)

      if raised.exception.msg.startswith(
          "Encountered non-parallelizable form of"):
        raise AssertionError(
            "Default NonParallelOperation raised, please specify a reason in "
            "the Singleton() partitioning requirement for this operation."
        ) from raised.exception

      # Re-run in an allow non parallel block to get an expression to verify
      with beam.dataframe.allow_non_parallel_operations():
        expr = func(*deferred_args)._expr
    else:
      expr = func(*deferred_args)._expr

    # Compute the result of the generated expression
    session_type = (
        expressions.PartitioningSession if distributed else expressions.Session)

    actual = session_type({}).evaluate(expr)

    # Verify
    if isinstance(expected, pd.core.generic.NDFrame):
      if distributed:
        if expected.index.is_unique:
          expected = expected.sort_index()
          actual = actual.sort_index()
        else:
          expected = expected.sort_values(list(expected.columns))
          actual = actual.sort_values(list(actual.columns))

      if isinstance(expected, pd.Series):
        pd.testing.assert_series_equal(expected, actual)
      elif isinstance(expected, pd.DataFrame):
        pd.testing.assert_frame_equal(expected, actual)
      else:
        raise ValueError(
            f"Expected value is a {type(expected)},"
            "not a Series or DataFrame.")

    else:
      # Expectation is not a pandas object
      if isinstance(expected, float):
        if np.isnan(expected):
          cmp = np.isnan
        else:
          cmp = lambda x: np.isclose(expected, x)
      else:
        cmp = expected.__eq__
      self.assertTrue(
          cmp(actual), 'Expected:\n\n%r\n\nActual:\n\n%r' % (expected, actual))

    if check_proxy:
      # Verify that the actual result agrees with the proxy
      proxy = expr.proxy()

      if type(actual) in (np.float32, np.float64):
        self.assertTrue(type(actual) == type(proxy) or np.isnan(proxy))
      else:
        self.assertEqual(type(actual), type(proxy))

      if isinstance(expected, pd.core.generic.NDFrame):
        if isinstance(expected, pd.Series):
          self.assertEqual(actual.dtype, proxy.dtype)
          self.assertEqual(actual.name, proxy.name)
        elif isinstance(expected, pd.DataFrame):
          pd.testing.assert_series_equal(actual.dtypes, proxy.dtypes)

        else:
          raise ValueError(
              f"Expected value is a {type(expected)},"
              "not a Series or DataFrame.")

        self.assertEqual(actual.index.names, proxy.index.names)
        for i in range(actual.index.nlevels):
          self.assertEqual(
              actual.index.get_level_values(i).dtype,
              proxy.index.get_level_values(i).dtype)


class DeferredFrameTest(_AbstractFrameTest):
  """Miscellaneous tessts for DataFrame operations."""
  def test_series_arithmetic(self):
    a = pd.Series([1, 2, 3])
    b = pd.Series([100, 200, 300])

    self._run_test(lambda a, b: a - 2 * b, a, b)
    self._run_test(lambda a, b: a.subtract(2).multiply(b).divide(a), a, b)

  def test_dataframe_arithmetic(self):
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [100, 200, 300]})
    df2 = pd.DataFrame({'a': [3000, 1000, 2000], 'b': [7, 11, 13]})

    self._run_test(lambda df, df2: df - 2 * df2, df, df2)
    self._run_test(
        lambda df, df2: df.subtract(2).multiply(df2).divide(df), df, df2)

  def test_get_column(self):
    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_test(lambda df: df['Animal'], df)
    self._run_test(lambda df: df.Speed, df)
    self._run_test(lambda df: df.get('Animal'), df)
    self._run_test(lambda df: df.get('FOO', df.Animal), df)

  def test_series_xs(self):
    # pandas doctests only verify DataFrame.xs, here we verify Series.xs as well
    d = {
        'num_legs': [4, 4, 2, 2],
        'num_wings': [0, 0, 2, 2],
        'class': ['mammal', 'mammal', 'mammal', 'bird'],
        'animal': ['cat', 'dog', 'bat', 'penguin'],
        'locomotion': ['walks', 'walks', 'flies', 'walks']
    }
    df = pd.DataFrame(data=d)
    df = df.set_index(['class', 'animal', 'locomotion'])

    self._run_test(lambda df: df.num_legs.xs('mammal'), df)
    self._run_test(lambda df: df.num_legs.xs(('mammal', 'dog')), df)
    self._run_test(lambda df: df.num_legs.xs('cat', level=1), df)
    self._run_test(
        lambda df: df.num_legs.xs(('bird', 'walks'), level=[0, 'locomotion']),
        df)

  def test_set_column(self):
    def new_column(df):
      df['NewCol'] = df['Speed']

    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_inplace_test(new_column, df)

  def test_set_column_from_index(self):
    def new_column(df):
      df['NewCol'] = df.index

    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_inplace_test(new_column, df)

  def test_tz_localize_ambiguous_series(self):
    # This replicates a tz_localize doctest:
    #   s.tz_localize('CET', ambiguous=np.array([True, True, False]))
    # But using a DeferredSeries instead of a np array

    s = pd.Series(
        range(3),
        index=pd.DatetimeIndex([
            '2018-10-28 01:20:00', '2018-10-28 02:36:00', '2018-10-28 03:46:00'
        ]))
    ambiguous = pd.Series([True, True, False], index=s.index)

    self._run_test(
        lambda s,
        ambiguous: s.tz_localize('CET', ambiguous=ambiguous),
        s,
        ambiguous)

  def test_tz_convert(self):
    # This replicates a tz_localize doctest:
    #   s.tz_localize('CET', ambiguous=np.array([True, True, False]))
    # But using a DeferredSeries instead of a np array

    s = pd.Series(
        range(3),
        index=pd.DatetimeIndex([
            '2018-10-27 01:20:00', '2018-10-27 02:36:00', '2018-10-27 03:46:00'
        ],
                               tz='Europe/Berlin'))

    self._run_test(lambda s: s.tz_convert('America/Los_Angeles'), s)

  def test_sort_index_columns(self):
    df = pd.DataFrame({
        'c': range(10),
        'a': range(10),
        'b': range(10),
        np.nan: range(10),
    })

    self._run_test(lambda df: df.sort_index(axis=1), df)
    self._run_test(lambda df: df.sort_index(axis=1, ascending=False), df)
    self._run_test(lambda df: df.sort_index(axis=1, na_position='first'), df)

  def test_where_callable_args(self):
    df = pd.DataFrame(
        np.arange(10, dtype=np.int64).reshape(-1, 2), columns=['A', 'B'])

    self._run_test(
        lambda df: df.where(lambda df: df % 2 == 0, lambda df: df * 10), df)

  def test_where_concrete_args(self):
    df = pd.DataFrame(
        np.arange(10, dtype=np.int64).reshape(-1, 2), columns=['A', 'B'])

    self._run_test(
        lambda df: df.where(
            df % 2 == 0, pd.Series({
                'A': 123, 'B': 456
            }), axis=1),
        df)

  def test_combine_dataframe(self):
    df = pd.DataFrame({'A': [0, 0], 'B': [4, 4]})
    df2 = pd.DataFrame({'A': [1, 1], 'B': [3, 3]})
    take_smaller = lambda s1, s2: s1 if s1.sum() < s2.sum() else s2
    self._run_test(
        lambda df,
        df2: df.combine(df2, take_smaller),
        df,
        df2,
        nonparallel=True)

  def test_combine_dataframe_fill(self):
    df1 = pd.DataFrame({'A': [0, 0], 'B': [None, 4]})
    df2 = pd.DataFrame({'A': [1, 1], 'B': [3, 3]})
    take_smaller = lambda s1, s2: s1 if s1.sum() < s2.sum() else s2
    self._run_test(
        lambda df1,
        df2: df1.combine(df2, take_smaller, fill_value=-5),
        df1,
        df2,
        nonparallel=True)

  def test_combine_Series(self):
    with expressions.allow_non_parallel_operations():
      s1 = pd.Series({'falcon': 330.0, 'eagle': 160.0})
      s2 = pd.Series({'falcon': 345.0, 'eagle': 200.0, 'duck': 30.0})
      self._run_test(lambda s1, s2: s1.combine(s2, max), s1, s2)

  def test_combine_first_dataframe(self):
    df1 = pd.DataFrame({'A': [None, 0], 'B': [None, 4]})
    df2 = pd.DataFrame({'A': [1, 1], 'B': [3, 3]})

    self._run_test(lambda df1, df2: df1.combine_first(df2), df1, df2)

  def test_combine_first_series(self):
    s1 = pd.Series([1, np.nan])
    s2 = pd.Series([3, 4])

    self._run_test(lambda s1, s2: s1.combine_first(s2), s1, s2)

  def test_add_prefix(self):
    df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [3, 4, 5, 6]})
    s = pd.Series([1, 2, 3, 4])

    self._run_test(lambda df: df.add_prefix('col_'), df)
    self._run_test(lambda s: s.add_prefix('col_'), s)

  def test_add_suffix(self):
    df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [3, 4, 5, 6]})
    s = pd.Series([1, 2, 3, 4])

    self._run_test(lambda df: df.add_suffix('_col'), df)
    self._run_test(lambda s: s.add_prefix('_col'), s)

  def test_set_index(self):
    df = pd.DataFrame({
        # [19, 18, ..]
        'index1': reversed(range(20)),  # [15, 16, .., 0, 1, .., 13, 14]
        'index2': np.roll(range(20), 5),  # ['', 'a', 'bb', ...]
        'values': [chr(ord('a') + i) * i for i in range(20)],
    })

    self._run_test(lambda df: df.set_index(['index1', 'index2']), df)
    self._run_test(lambda df: df.set_index(['index1', 'index2'], drop=True), df)
    self._run_test(lambda df: df.set_index('values'), df)

    self._run_error_test(lambda df: df.set_index('bad'), df)
    self._run_error_test(
        lambda df: df.set_index(['index2', 'bad', 'really_bad']), df)

  def test_series_drop_ignore_errors(self):
    midx = pd.MultiIndex(
        levels=[['lama', 'cow', 'falcon'], ['speed', 'weight', 'length']],
        codes=[[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]])
    s = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)

    # drop() requires singleton partitioning unless errors are ignored
    # Add some additional tests here to make sure the implementation works in
    # non-singleton partitioning.
    self._run_test(lambda s: s.drop('lama', level=0, errors='ignore'), s)
    self._run_test(lambda s: s.drop(('cow', 'speed'), errors='ignore'), s)
    self._run_test(lambda s: s.drop('falcon', level=0, errors='ignore'), s)

  def test_dataframe_drop_ignore_errors(self):
    midx = pd.MultiIndex(
        levels=[['lama', 'cow', 'falcon'], ['speed', 'weight', 'length']],
        codes=[[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]])
    df = pd.DataFrame(
        index=midx,
        columns=['big', 'small'],
        data=[[45, 30], [200, 100], [1.5, 1], [30, 20], [250, 150], [1.5, 0.8],
              [320, 250], [1, 0.8], [0.3, 0.2]])

    # drop() requires singleton partitioning unless errors are ignored
    # Add some additional tests here to make sure the implementation works in
    # non-singleton partitioning.
    self._run_test(
        lambda df: df.drop(index='lama', level=0, errors='ignore'), df)
    self._run_test(
        lambda df: df.drop(index=('cow', 'speed'), errors='ignore'), df)
    self._run_test(
        lambda df: df.drop(index='falcon', level=0, errors='ignore'), df)
    self._run_test(
        lambda df: df.drop(index='cow', columns='small', errors='ignore'), df)

  def test_merge(self):
    # This is from the pandas doctests, but fails due to re-indexing being
    # order-sensitive.
    df1 = pd.DataFrame({
        'lkey': ['foo', 'bar', 'baz', 'foo'], 'value': [1, 2, 3, 5]
    })
    df2 = pd.DataFrame({
        'rkey': ['foo', 'bar', 'baz', 'foo'], 'value': [5, 6, 7, 8]
    })
    self._run_test(
        lambda df1,
        df2: df1.merge(df2, left_on='lkey', right_on='rkey').rename(
            index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)
    self._run_test(
        lambda df1,
        df2: df1.merge(
            df2, left_on='lkey', right_on='rkey', suffixes=('_left', '_right')).
        rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)

  def test_merge_left_join(self):
    # This is from the pandas doctests, but fails due to re-indexing being
    # order-sensitive.
    df1 = pd.DataFrame({'a': ['foo', 'bar'], 'b': [1, 2]})
    df2 = pd.DataFrame({'a': ['foo', 'baz'], 'c': [3, 4]})

    self._run_test(
        lambda df1,
        df2: df1.merge(df2, how='left', on='a').rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)

  def test_merge_on_index(self):
    # This is from the pandas doctests, but fails due to re-indexing being
    # order-sensitive.
    df1 = pd.DataFrame({
        'lkey': ['foo', 'bar', 'baz', 'foo'], 'value': [1, 2, 3, 5]
    }).set_index('lkey')
    df2 = pd.DataFrame({
        'rkey': ['foo', 'bar', 'baz', 'foo'], 'value': [5, 6, 7, 8]
    }).set_index('rkey')

    self._run_test(
        lambda df1,
        df2: df1.merge(df2, left_index=True, right_index=True),
        df1,
        df2,
        check_proxy=False)

  def test_merge_same_key(self):
    df1 = pd.DataFrame({
        'key': ['foo', 'bar', 'baz', 'foo'], 'value': [1, 2, 3, 5]
    })
    df2 = pd.DataFrame({
        'key': ['foo', 'bar', 'baz', 'foo'], 'value': [5, 6, 7, 8]
    })
    self._run_test(
        lambda df1,
        df2: df1.merge(df2, on='key').rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)
    self._run_test(
        lambda df1,
        df2: df1.merge(df2, on='key', suffixes=('_left', '_right')).rename(
            index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)

  def test_merge_same_key_doctest(self):
    df1 = pd.DataFrame({'a': ['foo', 'bar'], 'b': [1, 2]})
    df2 = pd.DataFrame({'a': ['foo', 'baz'], 'c': [3, 4]})

    self._run_test(
        lambda df1,
        df2: df1.merge(df2, how='left', on='a').rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)
    # Test without specifying 'on'
    self._run_test(
        lambda df1,
        df2: df1.merge(df2, how='left').rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)

  def test_merge_same_key_suffix_collision(self):
    df1 = pd.DataFrame({'a': ['foo', 'bar'], 'b': [1, 2], 'a_lsuffix': [5, 6]})
    df2 = pd.DataFrame({'a': ['foo', 'baz'], 'c': [3, 4], 'a_rsuffix': [7, 8]})

    self._run_test(
        lambda df1,
        df2: df1.merge(
            df2, how='left', on='a', suffixes=('_lsuffix', '_rsuffix')).rename(
                index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)
    # Test without specifying 'on'
    self._run_test(
        lambda df1,
        df2: df1.merge(df2, how='left', suffixes=('_lsuffix', '_rsuffix')).
        rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True,
        check_proxy=False)

  def test_value_counts_with_nans(self):
    # similar to doctests that verify value_counts, but include nan values to
    # make sure we handle them correctly.
    df = pd.DataFrame({
        'num_legs': [2, 4, 4, 6, np.nan, np.nan],
        'num_wings': [2, 0, 0, 0, np.nan, 2]
    },
                      index=['falcon', 'dog', 'cat', 'ant', 'car', 'plane'])

    self._run_test(lambda df: df.value_counts(), df)
    self._run_test(lambda df: df.value_counts(normalize=True), df)

    self._run_test(lambda df: df.num_wings.value_counts(), df)
    self._run_test(lambda df: df.num_wings.value_counts(normalize=True), df)

  def test_value_counts_does_not_support_sort(self):
    df = pd.DataFrame({
        'num_legs': [2, 4, 4, 6, np.nan, np.nan],
        'num_wings': [2, 0, 0, 0, np.nan, 2]
    },
                      index=['falcon', 'dog', 'cat', 'ant', 'car', 'plane'])

    with self.assertRaisesRegex(frame_base.WontImplementError,
                                r"value_counts\(sort\=True\)"):
      self._run_test(lambda df: df.value_counts(sort=True), df)

    with self.assertRaisesRegex(frame_base.WontImplementError,
                                r"value_counts\(sort\=True\)"):
      self._run_test(lambda df: df.num_wings.value_counts(sort=True), df)

  def test_series_getitem(self):
    s = pd.Series([x**2 for x in range(10)])
    self._run_test(lambda s: s[...], s)
    self._run_test(lambda s: s[:], s)
    self._run_test(lambda s: s[s < 10], s)
    self._run_test(lambda s: s[lambda s: s < 10], s)

    s.index = s.index.map(float)
    self._run_test(lambda s: s[1.5:6], s)

  @parameterized.expand([
      (pd.Series(range(10)), ),  # unique
      (pd.Series(list(range(100)) + [0]), ),  # non-unique int
      (pd.Series(list(range(100)) + [0]) / 100, ),  # non-unique flt
      (pd.Series(['a', 'b', 'c', 'd']), ),  # unique str
      (pd.Series(['a', 'b', 'a', 'c', 'd']), ),  # non-unique str
  ])
  def test_series_is_unique(self, series):
    self._run_test(lambda s: s.is_unique, series)

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
    # TODO(BEAM-11757): We do not preserve the freq attribute on a DateTime
    # index
    dates.freq = None
    df = pd.DataFrame(
        np.arange(32).reshape((8, 4)),
        index=dates,
        columns=['A', 'B', 'C', 'D'])
    self._run_test(lambda df: df.loc[:], df)
    self._run_test(lambda df: df.loc[:, 'A'], df)
    self._run_test(lambda df: df.loc[:dates[3]], df)
    self._run_test(lambda df: df.loc[df.A > 10], df)
    self._run_test(lambda df: df.loc[lambda df: df.A > 10], df)
    self._run_test(lambda df: df.C.loc[df.A > 10], df)
    self._run_test(lambda df, s: df.loc[s.loc[1:3]], df, pd.Series(dates))

  def test_append_sort(self):
    # yapf: disable
    df1 = pd.DataFrame({'int': [1, 2, 3], 'str': ['a', 'b', 'c']},
                       columns=['int', 'str'],
                       index=[1, 3, 5])
    df2 = pd.DataFrame({'int': [4, 5, 6], 'str': ['d', 'e', 'f']},
                       columns=['str', 'int'],
                       index=[2, 4, 6])
    # yapf: enable

    self._run_test(lambda df1, df2: df1.append(df2, sort=True), df1, df2)
    self._run_test(lambda df1, df2: df1.append(df2, sort=False), df1, df2)
    self._run_test(lambda df1, df2: df2.append(df1, sort=True), df1, df2)
    self._run_test(lambda df1, df2: df2.append(df1, sort=False), df1, df2)

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
      self._run_test(lambda s: s.var(), s)
      self._run_test(lambda s: s.corr(s), s)
      self._run_test(lambda s: s.corr(s + 1), s)
      self._run_test(lambda s: s.corr(s * s), s)
      self._run_test(lambda s: s.cov(s * s), s)

  def test_dataframe_cov_corr(self):
    df = pd.DataFrame(np.random.randn(20, 3), columns=['a', 'b', 'c'])
    df.loc[df.index[:5], 'a'] = np.nan
    df.loc[df.index[5:10], 'b'] = np.nan
    self._run_test(lambda df: df.corr(), df)
    self._run_test(lambda df: df.cov(), df)
    self._run_test(lambda df: df.corr(min_periods=12), df)
    self._run_test(lambda df: df.cov(min_periods=12), df)
    self._run_test(lambda df: df.corrwith(df.a), df)
    self._run_test(lambda df: df[['a', 'b']].corrwith(df[['b', 'c']]), df)

    df2 = pd.DataFrame(np.random.randn(20, 3), columns=['a', 'b', 'c'])
    self._run_test(
        lambda df, df2: df.corrwith(df2, axis=1), df, df2, check_proxy=False)

  def test_corrwith_bad_axis(self):
    df = pd.DataFrame({'a': range(3), 'b': range(3, 6), 'c': range(6, 9)})
    self._run_error_test(lambda df: df.corrwith(df.a, axis=2), df)
    self._run_error_test(lambda df: df.corrwith(df, axis=5), df)

  @unittest.skipIf(PD_VERSION < (1, 2), "na_action added in pandas 1.2.0")
  def test_applymap_na_action(self):
    # Replicates a doctest for na_action which is incompatible with
    # doctest framework
    df = pd.DataFrame([[pd.NA, 2.12], [3.356, 4.567]])
    self._run_test(
        lambda df: df.applymap(lambda x: len(str(x)), na_action='ignore'),
        df,
        # TODO: generate proxy using naive type inference on fn
        check_proxy=False)

  def test_dataframe_eval_query(self):
    df = pd.DataFrame(np.random.randn(20, 3), columns=['a', 'b', 'c'])
    self._run_test(lambda df: df.eval('foo = a + b - c'), df)
    self._run_test(lambda df: df.query('a > b + c'), df)

    self._run_inplace_test(lambda df: df.eval('foo = a + b - c'), df)

    # Verify that attempting to access locals raises a useful error
    deferred_df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(df, df[0:0]))
    self.assertRaises(
        NotImplementedError, lambda: deferred_df.eval('foo = a + @b - c'))
    self.assertRaises(
        NotImplementedError, lambda: deferred_df.query('a > @b + c'))

  def test_index_name_assignment(self):
    df = pd.DataFrame({'a': ['foo', 'bar'], 'b': [1, 2]})
    df = df.set_index(['a', 'b'], drop=False)

    def change_index_names(df):
      df.index.names = ['A', None]

    self._run_inplace_test(change_index_names, df)

  def test_quantile(self):
    df = pd.DataFrame(
        np.array([[1, 1], [2, 10], [3, 100], [4, 100]]), columns=['a', 'b'])

    self._run_test(
        lambda df: df.quantile(0.1, axis='columns'), df, check_proxy=False)

    self._run_test(
        lambda df: df.quantile(0.1, axis='columns'), df, check_proxy=False)
    with self.assertRaisesRegex(frame_base.WontImplementError,
                                r"df\.quantile\(q=0\.1, axis='columns'\)"):
      self._run_test(lambda df: df.quantile([0.1, 0.5], axis='columns'), df)

  def test_dataframe_melt(self):

    df = pd.DataFrame({
        'A': {
            0: 'a', 1: 'b', 2: 'c'
        },
        'B': {
            0: 1, 1: 3, 2: 5
        },
        'C': {
            0: 2, 1: 4, 2: 6
        }
    })

    self._run_test(
        lambda df: df.melt(id_vars=['A'], value_vars=['B'], ignore_index=False),
        df)
    self._run_test(
        lambda df: df.melt(
            id_vars=['A'], value_vars=['B', 'C'], ignore_index=False),
        df)
    self._run_test(
        lambda df: df.melt(
            id_vars=['A'],
            value_vars=['B'],
            var_name='myVarname',
            value_name='myValname',
            ignore_index=False),
        df)
    self._run_test(
        lambda df: df.melt(
            id_vars=['A'], value_vars=['B', 'C'], ignore_index=False),
        df)

    df.columns = [list('ABC'), list('DEF')]
    self._run_test(
        lambda df: df.melt(
            col_level=0, id_vars=['A'], value_vars=['B'], ignore_index=False),
        df)
    self._run_test(
        lambda df: df.melt(
            id_vars=[('A', 'D')], value_vars=[('B', 'E')], ignore_index=False),
        df)

  def test_fillna_columns(self):
    df = pd.DataFrame(
        [[np.nan, 2, np.nan, 0], [3, 4, np.nan, 1], [np.nan, np.nan, np.nan, 5],
         [np.nan, 3, np.nan, 4], [3, np.nan, np.nan, 4]],
        columns=list('ABCD'))

    self._run_test(lambda df: df.fillna(method='ffill', axis='columns'), df)
    self._run_test(
        lambda df: df.fillna(method='ffill', axis='columns', limit=1), df)
    self._run_test(
        lambda df: df.fillna(method='bfill', axis='columns', limit=1), df)

    # Intended behavior is unclear here. See
    # https://github.com/pandas-dev/pandas/issues/40989
    # self._run_test(lambda df: df.fillna(axis='columns', value=100,
    #                                     limit=2), df)

  def test_dataframe_fillna_dataframe_as_value(self):
    df = pd.DataFrame([[np.nan, 2, np.nan, 0], [3, 4, np.nan, 1],
                       [np.nan, np.nan, np.nan, 5], [np.nan, 3, np.nan, 4]],
                      columns=list("ABCD"))
    df2 = pd.DataFrame(np.zeros((4, 4)), columns=list("ABCE"))

    self._run_test(lambda df, df2: df.fillna(df2), df, df2)

  def test_dataframe_fillna_series_as_value(self):
    df = pd.DataFrame([[np.nan, 2, np.nan, 0], [3, 4, np.nan, 1],
                       [np.nan, np.nan, np.nan, 5], [np.nan, 3, np.nan, 4]],
                      columns=list("ABCD"))
    s = pd.Series(range(4), index=list("ABCE"))

    self._run_test(lambda df, s: df.fillna(s), df, s)

  def test_series_fillna_series_as_value(self):
    df = pd.DataFrame([[np.nan, 2, np.nan, 0], [3, 4, np.nan, 1],
                       [np.nan, np.nan, np.nan, 5], [np.nan, 3, np.nan, 4]],
                      columns=list("ABCD"))
    df2 = pd.DataFrame(np.zeros((4, 4)), columns=list("ABCE"))

    self._run_test(lambda df, df2: df.A.fillna(df2.A), df, df2)

  def test_append_verify_integrity(self):
    df1 = pd.DataFrame({'A': range(10), 'B': range(10)}, index=range(10))
    df2 = pd.DataFrame({'A': range(10), 'B': range(10)}, index=range(9, 19))

    self._run_error_test(
        lambda s1,
        s2: s1.append(s2, verify_integrity=True),
        df1['A'],
        df2['A'],
        construction_time=False)
    self._run_error_test(
        lambda df1,
        df2: df1.append(df2, verify_integrity=True),
        df1,
        df2,
        construction_time=False)

  def test_categorical_groupby(self):
    df = pd.DataFrame({'A': np.arange(6), 'B': list('aabbca')})
    df['B'] = df['B'].astype(pd.CategoricalDtype(list('cab')))
    df = df.set_index('B')
    # TODO(BEAM-11190): These aggregations can be done in index partitions, but
    # it will require a little more complex logic
    self._run_test(lambda df: df.groupby(level=0).sum(), df, nonparallel=True)
    self._run_test(lambda df: df.groupby(level=0).mean(), df, nonparallel=True)

  def test_dataframe_sum_nonnumeric_raises(self):
    # Attempting a numeric aggregation with the str column present should
    # raise, and suggest the numeric_only argument
    with self.assertRaisesRegex(frame_base.WontImplementError, 'numeric_only'):
      self._run_test(lambda df: df.sum(), GROUPBY_DF)

    # numeric_only=True should work
    self._run_test(lambda df: df.sum(numeric_only=True), GROUPBY_DF)
    # projecting only numeric columns should too
    self._run_test(lambda df: df[['foo', 'bar']].sum(), GROUPBY_DF)

  def test_insert(self):
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    self._run_inplace_test(lambda df: df.insert(1, 'C', df.A * 2), df)
    self._run_inplace_test(
        lambda df: df.insert(0, 'foo', pd.Series([8], index=[1])),
        df,
        check_proxy=False)
    self._run_inplace_test(lambda df: df.insert(2, 'bar', value='q'), df)

  def test_insert_does_not_support_list_value(self):
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

    with self.assertRaisesRegex(frame_base.WontImplementError,
                                r"insert\(value=list\)"):
      self._run_inplace_test(lambda df: df.insert(1, 'C', [7, 8, 9]), df)

  def test_drop_duplicates(self):
    df = pd.DataFrame({
        'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        'rating': [4, 4, 3.5, 15, 5]
    })

    self._run_test(lambda df: df.drop_duplicates(keep=False), df)
    self._run_test(
        lambda df: df.drop_duplicates(subset=['brand'], keep=False), df)
    self._run_test(
        lambda df: df.drop_duplicates(subset=['brand', 'style'], keep=False),
        df)

  @parameterized.expand([
      (
          lambda base: base.from_dict({
              'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']
          }), ),
      (
          lambda base: base.from_dict({
              'row_1': [3, 2, 1, 0], 'row_2': ['a', 'b', 'c', 'd']
          },
                                      orient='index'), ),
      (
          lambda base: base.from_records(
              np.array([(3, 'a'), (2, 'b'), (1, 'c'), (0, 'd')],
                       dtype=[('col_1', 'i4'), ('col_2', 'U1')])), ),
  ])
  def test_create_methods(self, func):
    expected = func(pd.DataFrame)

    deferred_df = func(frames.DeferredDataFrame)
    actual = expressions.Session({}).evaluate(deferred_df._expr)

    pd.testing.assert_frame_equal(actual, expected)

  def test_replace(self):
    # verify a replace() doctest case that doesn't quite work in Beam as it uses
    # the default method='pad'
    df = pd.DataFrame({'A': ['bat', 'foo', 'bait'], 'B': ['abc', 'bar', 'xyz']})

    self._run_test(
        lambda df: df.replace(
            regex={
                r'^ba.$': 'new', 'foo': 'xyz'
            }, method=None),
        df)

  def test_sample_columns(self):
    df = pd.DataFrame({
        'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        'rating': [4, 4, 3.5, 15, 5]
    })

    self._run_test(lambda df: df.sample(axis=1, n=2, random_state=1), df)
    self._run_error_test(lambda df: df.sample(axis=1, n=10, random_state=2), df)
    self._run_test(
        lambda df: df.sample(axis=1, n=10, random_state=3, replace=True), df)

  def test_cat(self):
    # Replicate the doctests from CategorigcalAccessor
    # These tests don't translate into pandas_doctests_test.py because it
    # tries to use astype("category") in Beam, which makes a non-deferred
    # column type.
    s = pd.Series(list("abbccc")).astype("category")

    self._run_test(lambda s: s.cat.rename_categories(list("cba")), s)
    self._run_test(lambda s: s.cat.reorder_categories(list("cba")), s)
    self._run_test(lambda s: s.cat.add_categories(["d", "e"]), s)
    self._run_test(lambda s: s.cat.remove_categories(["a", "c"]), s)
    self._run_test(lambda s: s.cat.set_categories(list("abcde")), s)
    self._run_test(lambda s: s.cat.as_ordered(), s)
    self._run_test(lambda s: s.cat.as_unordered(), s)
    self._run_test(lambda s: s.cat.codes, s)

  @parameterized.expand(frames.ELEMENTWISE_DATETIME_PROPERTIES)
  def test_dt_property(self, prop_name):
    # Generate a series with a lot of unique timestamps
    s = pd.Series(
        pd.date_range('1/1/2000', periods=100, freq='m') +
        pd.timedelta_range(start='0 days', end='70 days', periods=100))
    self._run_test(lambda s: getattr(s.dt, prop_name), s)

  @parameterized.expand([
      ('month_name', {}),
      ('day_name', {}),
      ('normalize', {}),
      (
          'strftime',
          {
              'date_format': '%B %d, %Y, %r'
          },
      ),
      ('tz_convert', {
          'tz': 'Europe/Berlin'
      }),
  ])
  def test_dt_method(self, op, kwargs):
    # Generate a series with a lot of unique timestamps
    s = pd.Series(
        pd.date_range(
            '1/1/2000', periods=100, freq='m', tz='America/Los_Angeles') +
        pd.timedelta_range(start='0 days', end='70 days', periods=100))

    self._run_test(lambda s: getattr(s.dt, op)(**kwargs), s)

  def test_dt_tz_localize_ambiguous_series(self):
    # This replicates a dt.tz_localize doctest:
    #   s.tz_localize('CET', ambiguous=np.array([True, True, False]))
    # But using a DeferredSeries instead of a np array

    s = pd.to_datetime(
        pd.Series([
            '2018-10-28 01:20:00', '2018-10-28 02:36:00', '2018-10-28 03:46:00'
        ]))
    ambiguous = pd.Series([True, True, False], index=s.index)

    self._run_test(
        lambda s,
        ambiguous: s.dt.tz_localize('CET', ambiguous=ambiguous),
        s,
        ambiguous)

  def test_dt_tz_localize_nonexistent(self):
    # This replicates dt.tz_localize doctests that exercise `nonexistent`.
    # However they specify ambiguous='NaT' because the default,
    # ambiguous='infer', is not supported.
    s = pd.to_datetime(
        pd.Series(['2015-03-29 02:30:00', '2015-03-29 03:30:00']))

    self._run_test(
        lambda s: s.dt.tz_localize(
            'Europe/Warsaw', ambiguous='NaT', nonexistent='shift_forward'),
        s)
    self._run_test(
        lambda s: s.dt.tz_localize(
            'Europe/Warsaw', ambiguous='NaT', nonexistent='shift_backward'),
        s)
    self._run_test(
        lambda s: s.dt.tz_localize(
            'Europe/Warsaw', ambiguous='NaT', nonexistent=pd.Timedelta('1H')),
        s)


# pandas doesn't support kurtosis on GroupBys:
# https://github.com/pandas-dev/pandas/issues/40139
ALL_GROUPING_AGGREGATIONS = sorted(
    set(frames.ALL_AGGREGATIONS) - set(('kurt', 'kurtosis')))


class GroupByTest(_AbstractFrameTest):
  """Tests for DataFrame/Series GroupBy operations."""
  @parameterized.expand(ALL_GROUPING_AGGREGATIONS)
  def test_groupby_agg(self, agg_type):
    if agg_type == 'describe' and PD_VERSION < (1, 2):
      self.skipTest(
          "BEAM-12366: proxy generation of DataFrameGroupBy.describe "
          "fails in pandas < 1.2")
    self._run_test(
        lambda df: df.groupby('group').agg(agg_type),
        GROUPBY_DF,
        check_proxy=False)

  @parameterized.expand(ALL_GROUPING_AGGREGATIONS)
  def test_groupby_with_filter(self, agg_type):
    if agg_type == 'describe' and PD_VERSION < (1, 2):
      self.skipTest(
          "BEAM-12366: proxy generation of DataFrameGroupBy.describe "
          "fails in pandas < 1.2")
    self._run_test(
        lambda df: getattr(df[df.foo > 30].groupby('group'), agg_type)(),
        GROUPBY_DF,
        check_proxy=False)

  @parameterized.expand(ALL_GROUPING_AGGREGATIONS)
  def test_groupby(self, agg_type):
    if agg_type == 'describe' and PD_VERSION < (1, 2):
      self.skipTest(
          "BEAM-12366: proxy generation of DataFrameGroupBy.describe "
          "fails in pandas < 1.2")

    self._run_test(
        lambda df: getattr(df.groupby('group'), agg_type)(),
        GROUPBY_DF,
        check_proxy=False)

  @parameterized.expand(ALL_GROUPING_AGGREGATIONS)
  def test_groupby_series(self, agg_type):
    if agg_type == 'describe' and PD_VERSION < (1, 2):
      self.skipTest(
          "BEAM-12366: proxy generation of DataFrameGroupBy.describe "
          "fails in pandas < 1.2")

    self._run_test(
        lambda df: getattr(df[df.foo > 40].groupby(df.group), agg_type)(),
        GROUPBY_DF,
        check_proxy=False)

  def test_groupby_user_guide(self):
    # Example from https://pandas.pydata.org/docs/user_guide/groupby.html
    arrays = [['bar', 'bar', 'baz', 'baz', 'foo', 'foo', 'qux', 'qux'],
              ['one', 'two', 'one', 'two', 'one', 'two', 'one', 'two']]

    index = pd.MultiIndex.from_arrays(arrays, names=['first', 'second'])

    df = pd.DataFrame({
        'A': [1, 1, 1, 1, 2, 2, 3, 3], 'B': np.arange(8)
    },
                      index=index)

    self._run_test(lambda df: df.groupby(['second', 'A']).sum(), df)

  @parameterized.expand(ALL_GROUPING_AGGREGATIONS)
  def test_groupby_project_series(self, agg_type):
    df = GROUPBY_DF

    if agg_type == 'describe':
      self.skipTest(
          "BEAM-12366: proxy generation of SeriesGroupBy.describe "
          "fails")
    if agg_type in ('corr', 'cov'):
      self.skipTest(
          "BEAM-12367: SeriesGroupBy.{corr, cov} do not raise the "
          "expected error.")

    self._run_test(lambda df: getattr(df.groupby('group').foo, agg_type)(), df)
    self._run_test(lambda df: getattr(df.groupby('group').bar, agg_type)(), df)
    self._run_test(
        lambda df: getattr(df.groupby('group')['foo'], agg_type)(), df)
    self._run_test(
        lambda df: getattr(df.groupby('group')['bar'], agg_type)(), df)

  @parameterized.expand(ALL_GROUPING_AGGREGATIONS)
  def test_groupby_project_dataframe(self, agg_type):
    if agg_type == 'describe' and PD_VERSION < (1, 2):
      self.skipTest(
          "BEAM-12366: proxy generation of DataFrameGroupBy.describe "
          "fails in pandas < 1.2")
    self._run_test(
        lambda df: getattr(df.groupby('group')[['bar', 'baz']], agg_type)(),
        GROUPBY_DF,
        check_proxy=False)

  def test_groupby_errors_bad_projection(self):
    df = GROUPBY_DF

    # non-existent projection column
    self._run_error_test(
        lambda df: df.groupby('group')[['bar', 'baz']].bar.median(), df)
    self._run_error_test(lambda df: df.groupby('group')[['bad']].median(), df)

    self._run_error_test(lambda df: df.groupby('group').bad.median(), df)

    self._run_error_test(
        lambda df: df.groupby('group')[['bar', 'baz']].bar.sum(), df)
    self._run_error_test(lambda df: df.groupby('group')[['bat']].sum(), df)
    self._run_error_test(lambda df: df.groupby('group').bat.sum(), df)

  def test_groupby_errors_non_existent_label(self):
    df = GROUPBY_DF

    # non-existent grouping label
    self._run_error_test(
        lambda df: df.groupby(['really_bad', 'foo', 'bad']).foo.sum(), df)
    self._run_error_test(lambda df: df.groupby('bad').foo.sum(), df)

  def test_groupby_callable(self):
    df = GROUPBY_DF

    self._run_test(lambda df: df.groupby(lambda x: x % 2).foo.sum(), df)
    self._run_test(lambda df: df.groupby(lambda x: x % 5).median(), df)

  def test_groupby_apply(self):
    df = GROUPBY_DF

    def median_sum_fn(x):
      return (x.foo + x.bar).median()

    # Note this is the same as DataFrameGroupBy.describe. Using it here is
    # just a convenient way to test apply() with a user fn that returns a Series
    describe = lambda df: df.describe()

    self._run_test(lambda df: df.groupby('group').foo.apply(describe), df)
    self._run_test(
        lambda df: df.groupby('group')[['foo', 'bar']].apply(describe), df)
    self._run_test(lambda df: df.groupby('group').apply(median_sum_fn), df)
    self._run_test(
        lambda df: df.set_index('group').foo.groupby(level=0).apply(describe),
        df)
    self._run_test(lambda df: df.groupby(level=0).apply(median_sum_fn), df)
    self._run_test(lambda df: df.groupby(lambda x: x % 3).apply(describe), df)
    self._run_test(
        lambda df: df.bar.groupby(lambda x: x % 3).apply(describe), df)
    self._run_test(
        lambda df: df.set_index(['str', 'group', 'bool']).groupby(
            level='group').apply(median_sum_fn),
        df)

  def test_groupby_apply_preserves_column_order(self):
    df = GROUPBY_DF

    self._run_test(
        lambda df: df[['foo', 'group', 'bar']].groupby('group').apply(
            lambda x: x),
        df)

  def test_groupby_transform(self):
    df = pd.DataFrame({
        "Date": [
            "2015-05-08",
            "2015-05-07",
            "2015-05-06",
            "2015-05-05",
            "2015-05-08",
            "2015-05-07",
            "2015-05-06",
            "2015-05-05"
        ],
        "Data": [5, 8, 6, 1, 50, 100, 60, 120],
    })

    self._run_test(lambda df: df.groupby('Date')['Data'].transform(np.sum), df)
    self._run_test(
        lambda df: df.groupby('Date')['Data'].transform(
            lambda x: (x - x.mean()) / x.std()),
        df)

  def test_groupby_apply_modified_index(self):
    df = GROUPBY_DF

    # If apply fn modifies the index then the output will include the grouped
    # index
    self._run_test(
        lambda df: df.groupby('group').apply(
            lambda x: x[x.foo > x.foo.median()]),
        df)

  @unittest.skip('BEAM-11710')
  def test_groupby_aggregate_grouped_column(self):
    df = pd.DataFrame({
        'group': ['a' if i % 5 == 0 or i % 3 == 0 else 'b' for i in range(100)],
        'foo': [None if i % 11 == 0 else i for i in range(100)],
        'bar': [None if i % 7 == 0 else 99 - i for i in range(100)],
        'baz': [None if i % 13 == 0 else i * 2 for i in range(100)],
    })

    self._run_test(lambda df: df.groupby('group').group.count(), df)
    self._run_test(lambda df: df.groupby('group')[['group', 'bar']].count(), df)
    self._run_test(
        lambda df: df.groupby('group')[['group', 'bar']].apply(
            lambda x: x.describe()),
        df)

  @parameterized.expand((x, ) for x in [
      0,
      [1],
      3,
      [0, 3],
      [2, 1],
      ['foo', 0],
      [1, 'str'],
      [3, 0, 2, 1],
  ])
  def test_groupby_level_agg(self, level):
    df = GROUPBY_DF.set_index(['group', 'foo', 'bar', 'str'], drop=False)
    self._run_test(lambda df: df.groupby(level=level).bar.max(), df)
    self._run_test(
        lambda df: df.groupby(level=level).sum(numeric_only=True), df)
    self._run_test(
        lambda df: df.groupby(level=level).apply(
            lambda x: (x.foo + x.bar).median()),
        df)

  @unittest.skipIf(PD_VERSION < (1, 1), "drop_na added in pandas 1.1.0")
  def test_groupby_count_na(self):
    # Verify we can do a groupby.count() that doesn't drop NaN values
    self._run_test(
        lambda df: df.groupby('foo', dropna=True).bar.count(), GROUPBY_DF)
    self._run_test(
        lambda df: df.groupby('foo', dropna=False).bar.count(), GROUPBY_DF)

  def test_groupby_sum_min_count(self):
    df = pd.DataFrame({
        'good': [1, 2, 3, np.nan],
        'bad': [np.nan, np.nan, np.nan, 4],
        'group': ['a', 'b', 'a', 'b']
    })

    self._run_test(lambda df: df.groupby('group').sum(min_count=2), df)

  def test_groupby_dtypes(self):
    self._run_test(
        lambda df: df.groupby('group').dtypes, GROUPBY_DF, check_proxy=False)
    self._run_test(
        lambda df: df.groupby(level=0).dtypes, GROUPBY_DF, check_proxy=False)

  @parameterized.expand(ALL_GROUPING_AGGREGATIONS)
  def test_dataframe_groupby_series(self, agg_type):
    if agg_type == 'describe' and PD_VERSION < (1, 2):
      self.skipTest(
          "BEAM-12366: proxy generation of DataFrameGroupBy.describe "
          "fails in pandas < 1.2")
    self._run_test(
        lambda df: df[df.foo > 40].groupby(df.group).agg(agg_type),
        GROUPBY_DF,
        check_proxy=False)
    self._run_test(
        lambda df: df[df.foo > 40].groupby(df.foo % 3).agg(agg_type),
        GROUPBY_DF,
        check_proxy=False)

  @parameterized.expand(ALL_GROUPING_AGGREGATIONS)
  def test_series_groupby_series(self, agg_type):
    if agg_type == 'describe':
      self.skipTest(
          "BEAM-12366: proxy generation of SeriesGroupBy.describe "
          "fails")
    if agg_type in ('corr', 'cov'):
      self.skipTest(
          "BEAM-12367: SeriesGroupBy.{corr, cov} do not raise the "
          "expected error.")
    self._run_test(
        lambda df: df[df.foo < 40].bar.groupby(df.group).agg(agg_type),
        GROUPBY_DF)
    self._run_test(
        lambda df: df[df.foo < 40].bar.groupby(df.foo % 3).agg(agg_type),
        GROUPBY_DF)

  def test_groupby_series_apply(self):
    df = GROUPBY_DF

    def median_sum_fn(x):
      return (x.foo + x.bar).median()

    # Note this is the same as DataFrameGroupBy.describe. Using it here is
    # just a convenient way to test apply() with a user fn that returns a Series
    describe = lambda df: df.describe()

    self._run_test(lambda df: df.groupby(df.group).foo.apply(describe), df)
    self._run_test(
        lambda df: df.groupby(df.group)[['foo', 'bar']].apply(describe), df)
    self._run_test(lambda df: df.groupby(df.group).apply(median_sum_fn), df)

  def test_groupby_multiindex_keep_nans(self):
    # Due to https://github.com/pandas-dev/pandas/issues/36470
    # groupby(dropna=False) doesn't work with multiple columns
    with self.assertRaisesRegex(NotImplementedError, "BEAM-12495"):
      self._run_test(
          lambda df: df.groupby(['foo', 'bar'], dropna=False).sum(), GROUPBY_DF)


class AggregationTest(_AbstractFrameTest):
  """Tests for global aggregation methods on DataFrame/Series."""

  # corr, cov on Series require an other argument
  @parameterized.expand(
      sorted(set(frames.ALL_AGGREGATIONS) - set(['corr', 'cov'])))
  def test_series_agg(self, agg_method):
    s = pd.Series(list(range(16)))

    nonparallel = agg_method in (
        'quantile',
        'mean',
        'describe',
        'median',
        'sem',
        'mad',
        'skew',
        'kurtosis',
        'kurt')

    # TODO(BEAM-12379): max and min produce the wrong proxy
    check_proxy = agg_method not in ('max', 'min')

    self._run_test(
        lambda s: s.agg(agg_method),
        s,
        nonparallel=nonparallel,
        check_proxy=check_proxy)

  # corr, cov on Series require an other argument
  # Series.size is a property
  @parameterized.expand(
      sorted(set(frames.ALL_AGGREGATIONS) - set(['corr', 'cov', 'size'])))
  def test_series_agg_method(self, agg_method):
    s = pd.Series(list(range(16)))

    nonparallel = agg_method in (
        'quantile',
        'mean',
        'describe',
        'median',
        'sem',
        'mad',
        'skew',
        'kurtosis',
        'kurt')

    # TODO(BEAM-12379): max and min produce the wrong proxy
    check_proxy = agg_method not in ('max', 'min')

    self._run_test(
        lambda s: getattr(s, agg_method)(),
        s,
        nonparallel=nonparallel,
        check_proxy=check_proxy)

  @parameterized.expand(frames.ALL_AGGREGATIONS)
  def test_dataframe_agg(self, agg_method):
    df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [2, 3, 5, 7]})

    nonparallel = agg_method in (
        'quantile',
        'mean',
        'describe',
        'median',
        'sem',
        'mad',
        'skew',
        'kurtosis',
        'kurt')

    # TODO(BEAM-12379): max and min produce the wrong proxy
    check_proxy = agg_method not in ('max', 'min')

    self._run_test(
        lambda df: df.agg(agg_method),
        df,
        nonparallel=nonparallel,
        check_proxy=check_proxy)

  # DataFrame.size is a property
  @parameterized.expand(sorted(set(frames.ALL_AGGREGATIONS) - set(['size'])))
  def test_dataframe_agg_method(self, agg_method):
    df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [2, 3, 5, 7]})

    nonparallel = agg_method in (
        'quantile',
        'mean',
        'describe',
        'median',
        'sem',
        'mad',
        'skew',
        'kurtosis',
        'kurt')

    # TODO(BEAM-12379): max and min produce the wrong proxy
    check_proxy = agg_method not in ('max', 'min')

    self._run_test(
        lambda df: getattr(df, agg_method)(),
        df,
        nonparallel=nonparallel,
        check_proxy=check_proxy)

  def test_series_agg_modes(self):
    s = pd.Series(list(range(16)))
    self._run_test(lambda s: s.agg('sum'), s)
    self._run_test(lambda s: s.agg(['sum']), s)
    self._run_test(lambda s: s.agg(['sum', 'mean']), s, nonparallel=True)
    self._run_test(lambda s: s.agg(['mean']), s, nonparallel=True)
    self._run_test(lambda s: s.agg('mean'), s, nonparallel=True)

  def test_dataframe_agg_modes(self):
    df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [2, 3, 5, 7]})
    self._run_test(lambda df: df.agg('sum'), df)
    self._run_test(lambda df: df.agg(['sum', 'mean']), df, nonparallel=True)
    self._run_test(lambda df: df.agg({'A': 'sum', 'B': 'sum'}), df)
    self._run_test(
        lambda df: df.agg({
            'A': 'sum', 'B': 'mean'
        }), df, nonparallel=True)
    self._run_test(
        lambda df: df.agg({'A': ['sum', 'mean']}), df, nonparallel=True)
    self._run_test(
        lambda df: df.agg({
            'A': ['sum', 'mean'], 'B': 'min'
        }),
        df,
        nonparallel=True)

  def test_series_agg_level(self):
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).bar.count(level=0),
        GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).bar.max(level=0), GROUPBY_DF)

    self._run_test(
        lambda df: df.set_index(['group', 'foo']).bar.median(level=0),
        GROUPBY_DF)

    self._run_test(
        lambda df: df.set_index(['foo', 'group']).bar.count(level=1),
        GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).bar.max(level=1), GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).bar.max(level='foo'),
        GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).bar.median(level=1),
        GROUPBY_DF)

  def test_dataframe_agg_level(self):
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).count(level=0), GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).max(
            level=0, numeric_only=False),
        GROUPBY_DF,
        check_proxy=False)
    # pandas implementation doesn't respect numeric_only argument here
    # (https://github.com/pandas-dev/pandas/issues/40788), it
    # always acts as if numeric_only=True. Our implmentation respects it so we
    # need to make it explicit.
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).sum(
            level=0, numeric_only=True),
        GROUPBY_DF)

    self._run_test(
        lambda df: df.set_index(['group', 'foo'])[['bar']].count(level=1),
        GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).count(level=1), GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).max(
            level=1, numeric_only=False),
        GROUPBY_DF,
        check_proxy=False)
    # sum with str columns is order-sensitive
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).sum(
            level=1, numeric_only=True),
        GROUPBY_DF)

    self._run_test(
        lambda df: df.set_index(['group', 'foo']).median(
            level=0, numeric_only=True),
        GROUPBY_DF)
    self._run_test(
        lambda df: df.drop('str', axis=1).set_index(['foo', 'group']).median(
            level=1, numeric_only=True),
        GROUPBY_DF)

  def test_series_agg_multifunc_level(self):
    # level= is ignored for multiple agg fns
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).bar.agg(['min', 'max'],
                                                          level=0),
        GROUPBY_DF)

  def test_dataframe_agg_multifunc_level(self):
    # level= is ignored for multiple agg fns
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).agg(['min', 'max'], level=0),
        GROUPBY_DF,
        check_proxy=False)

  @parameterized.expand([(True, ), (False, )])
  @unittest.skipIf(
      PD_VERSION < (1, 2),
      "pandas 1.1.0 produces different dtypes for these examples")
  def test_dataframe_agg_numeric_only(self, numeric_only):
    # Note other aggregation functions can fail on this input with
    # numeric_only={False,None}. These are the only ones that actually work for
    # the string inputs.
    self._run_test(
        lambda df: df.max(numeric_only=numeric_only),
        GROUPBY_DF,
        check_proxy=False)
    self._run_test(
        lambda df: df.min(numeric_only=numeric_only),
        GROUPBY_DF,
        check_proxy=False)

  @unittest.skip(
      "pandas implementation doesn't respect numeric_only= with "
      "level= (https://github.com/pandas-dev/pandas/issues/40788)")
  def test_dataframe_agg_level_numeric_only(self):
    self._run_test(
        lambda df: df.set_index('foo').sum(level=0, numeric_only=True),
        GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index('foo').max(level=0, numeric_only=True),
        GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index('foo').mean(level=0, numeric_only=True),
        GROUPBY_DF)
    self._run_test(
        lambda df: df.set_index('foo').median(level=0, numeric_only=True),
        GROUPBY_DF)

  def test_dataframe_agg_bool_only(self):
    df = pd.DataFrame({
        'all': [True for i in range(10)],
        'any': [i % 3 == 0 for i in range(10)],
        'int': range(10)
    })

    self._run_test(lambda df: df.all(), df)
    self._run_test(lambda df: df.any(), df)
    self._run_test(lambda df: df.all(bool_only=True), df)
    self._run_test(lambda df: df.any(bool_only=True), df)

  @unittest.skip(
      "pandas doesn't implement bool_only= with level= "
      "(https://github.com/pandas-dev/pandas/blob/"
      "v1.2.3/pandas/core/generic.py#L10573)")
  def test_dataframe_agg_level_bool_only(self):
    df = pd.DataFrame({
        'all': [True for i in range(10)],
        'any': [i % 3 == 0 for i in range(10)],
        'int': range(10)
    })

    self._run_test(lambda df: df.set_index('int', drop=False).all(level=0), df)
    self._run_test(lambda df: df.set_index('int', drop=False).any(level=0), df)
    self._run_test(
        lambda df: df.set_index('int', drop=False).all(level=0, bool_only=True),
        df)
    self._run_test(
        lambda df: df.set_index('int', drop=False).any(level=0, bool_only=True),
        df)

  def test_series_agg_np_size(self):
    self._run_test(
        lambda df: df.set_index(['group', 'foo']).agg(np.size),
        GROUPBY_DF,
        check_proxy=False)

  def test_df_agg_invalid_kwarg_raises(self):
    self._run_error_test(lambda df: df.agg('mean', bool_only=True), GROUPBY_DF)
    self._run_error_test(
        lambda df: df.agg('any', numeric_only=True), GROUPBY_DF)
    self._run_error_test(
        lambda df: df.agg('median', min_count=3, numeric_only=True), GROUPBY_DF)

  def test_series_agg_method_invalid_kwarg_raises(self):
    self._run_error_test(lambda df: df.foo.median(min_count=3), GROUPBY_DF)
    self._run_error_test(
        lambda df: df.foo.agg('median', min_count=3), GROUPBY_DF)

  @unittest.skipIf(
      PD_VERSION < (1, 3),
      (
          "DataFrame.agg raises a different exception from the "
          "aggregation methods. Fixed in "
          "https://github.com/pandas-dev/pandas/pull/40543."))
  def test_df_agg_method_invalid_kwarg_raises(self):
    self._run_error_test(lambda df: df.mean(bool_only=True), GROUPBY_DF)
    self._run_error_test(lambda df: df.any(numeric_only=True), GROUPBY_DF)
    self._run_error_test(
        lambda df: df.median(min_count=3, numeric_only=True), GROUPBY_DF)

  def test_agg_min_count(self):
    df = pd.DataFrame({
        'good': [1, 2, 3, np.nan],
        'bad': [np.nan, np.nan, np.nan, 4],
    },
                      index=['a', 'b', 'a', 'b'])

    self._run_test(lambda df: df.sum(level=0, min_count=2), df)

    self._run_test(lambda df: df.sum(min_count=3), df, nonparallel=True)
    self._run_test(lambda df: df.sum(min_count=1), df, nonparallel=True)
    self._run_test(lambda df: df.good.sum(min_count=2), df, nonparallel=True)
    self._run_test(lambda df: df.bad.sum(min_count=2), df, nonparallel=True)

  def test_series_agg_std(self):
    s = pd.Series(range(10))

    self._run_test(lambda s: s.agg('std'), s)
    self._run_test(lambda s: s.agg('var'), s)
    self._run_test(lambda s: s.agg(['std', 'sum']), s)
    self._run_test(lambda s: s.agg(['var']), s)

  def test_std_all_na(self):
    s = pd.Series([np.nan] * 10)

    self._run_test(lambda s: s.agg('std'), s)
    self._run_test(lambda s: s.std(), s)

  def test_std_mostly_na_with_ddof(self):
    df = pd.DataFrame({
        'one': [i if i % 8 == 0 else np.nan for i in range(8)],
        'two': [i if i % 4 == 0 else np.nan for i in range(8)],
        'three': [i if i % 2 == 0 else np.nan for i in range(8)],
    },
                      index=pd.MultiIndex.from_arrays(
                          [list(range(8)), list(reversed(range(8)))],
                          names=['forward', None]))

    self._run_test(lambda df: df.std(), df)  # ddof=1
    self._run_test(lambda df: df.std(ddof=0), df)
    self._run_test(lambda df: df.std(ddof=2), df)
    self._run_test(lambda df: df.std(ddof=3), df)
    self._run_test(lambda df: df.std(ddof=4), df)

  def test_dataframe_std(self):
    self._run_test(lambda df: df.std(numeric_only=True), GROUPBY_DF)
    self._run_test(lambda df: df.var(numeric_only=True), GROUPBY_DF)

  def test_dataframe_mode(self):
    self._run_test(
        lambda df: df.mode(), GROUPBY_DF, nonparallel=True, check_proxy=False)
    self._run_test(
        lambda df: df.mode(numeric_only=True),
        GROUPBY_DF,
        nonparallel=True,
        check_proxy=False)
    self._run_test(
        lambda df: df.mode(dropna=True, numeric_only=True),
        GROUPBY_DF,
        nonparallel=True,
        check_proxy=False)

  def test_series_mode(self):
    self._run_test(lambda df: df.foo.mode(), GROUPBY_DF, nonparallel=True)
    self._run_test(
        lambda df: df.baz.mode(dropna=True), GROUPBY_DF, nonparallel=True)


class BeamSpecificTest(unittest.TestCase):
  """Tests for functionality that's specific to the Beam DataFrame API.

  These features don't exist in pandas so we must verify them independently."""
  def assert_frame_data_equivalent(self, actual, expected):
    """Verify that actual is the same as expected, ignoring the index and order
    of the data."""
    def sort_and_drop_index(df):
      if isinstance(df, pd.Series):
        df = df.sort_values()
      elif isinstance(df, pd.DataFrame):
        df = df.sort_values(by=list(df.columns))

      return df.reset_index(drop=True)

    actual = sort_and_drop_index(actual)
    expected = sort_and_drop_index(expected)

    if isinstance(expected, pd.Series):
      pd.testing.assert_series_equal(actual, expected)
    elif isinstance(expected, pd.DataFrame):
      pd.testing.assert_frame_equal(actual, expected)

  def _evaluate(self, func, *args, distributed=True):
    deferred_args = [
        frame_base.DeferredFrame.wrap(
            expressions.ConstantExpression(arg, arg[0:0])) for arg in args
    ]

    session_type = (
        expressions.PartitioningSession if distributed else expressions.Session)

    return session_type({}).evaluate(func(*deferred_args)._expr)

  def test_drop_duplicates_keep_any(self):
    df = pd.DataFrame({
        'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        'rating': [4, 4, 3.5, 15, 5]
    })

    result = self._evaluate(lambda df: df.drop_duplicates(keep='any'), df)

    # Verify that the result is the same as conventional drop_duplicates
    self.assert_frame_data_equivalent(result, df.drop_duplicates())

  def test_drop_duplicates_keep_any_subset(self):
    df = pd.DataFrame({
        'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        'rating': [4, 4, 3.5, 15, 5]
    })

    result = self._evaluate(
        lambda df: df.drop_duplicates(keep='any', subset=['brand']), df)

    self.assertTrue(result.brand.unique)
    self.assert_frame_data_equivalent(
        result.brand, df.drop_duplicates(subset=['brand']).brand)

  def test_series_drop_duplicates_keep_any(self):
    df = pd.DataFrame({
        'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        'rating': [4, 4, 3.5, 15, 5]
    })

    result = self._evaluate(lambda df: df.brand.drop_duplicates(keep='any'), df)

    self.assert_frame_data_equivalent(result, df.brand.drop_duplicates())

  def test_duplicated_keep_any(self):
    df = pd.DataFrame({
        'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        'rating': [4, 4, 3.5, 15, 5]
    })

    result = self._evaluate(lambda df: df.duplicated(keep='any'), df)

    # Verify that the result is the same as conventional duplicated
    self.assert_frame_data_equivalent(result, df.duplicated())

  def test_nsmallest_any(self):
    df = pd.DataFrame({
        'population': [
            59000000,
            65000000,
            434000,
            434000,
            434000,
            337000,
            337000,
            11300,
            11300
        ],
        'GDP': [1937894, 2583560, 12011, 4520, 12128, 17036, 182, 38, 311],
        'alpha-2': ["IT", "FR", "MT", "MV", "BN", "IS", "NR", "TV", "AI"]
    },
                      index=[
                          "Italy",
                          "France",
                          "Malta",
                          "Maldives",
                          "Brunei",
                          "Iceland",
                          "Nauru",
                          "Tuvalu",
                          "Anguilla"
                      ])

    result = self._evaluate(
        lambda df: df.population.nsmallest(3, keep='any'), df)

    # keep='any' should produce the same result as keep='first',
    # but not necessarily with the same index
    self.assert_frame_data_equivalent(result, df.population.nsmallest(3))

  def test_nlargest_any(self):
    df = pd.DataFrame({
        'population': [
            59000000,
            65000000,
            434000,
            434000,
            434000,
            337000,
            337000,
            11300,
            11300
        ],
        'GDP': [1937894, 2583560, 12011, 4520, 12128, 17036, 182, 38, 311],
        'alpha-2': ["IT", "FR", "MT", "MV", "BN", "IS", "NR", "TV", "AI"]
    },
                      index=[
                          "Italy",
                          "France",
                          "Malta",
                          "Maldives",
                          "Brunei",
                          "Iceland",
                          "Nauru",
                          "Tuvalu",
                          "Anguilla"
                      ])

    result = self._evaluate(
        lambda df: df.population.nlargest(3, keep='any'), df)

    # keep='any' should produce the same result as keep='first',
    # but not necessarily with the same index
    self.assert_frame_data_equivalent(result, df.population.nlargest(3))

  def test_sample(self):
    df = pd.DataFrame({
        'population': [
            59000000,
            65000000,
            434000,
            434000,
            434000,
            337000,
            337000,
            11300,
            11300
        ],
        'GDP': [1937894, 2583560, 12011, 4520, 12128, 17036, 182, 38, 311],
        'alpha-2': ["IT", "FR", "MT", "MV", "BN", "IS", "NR", "TV", "AI"]
    },
                      index=[
                          "Italy",
                          "France",
                          "Malta",
                          "Maldives",
                          "Brunei",
                          "Iceland",
                          "Nauru",
                          "Tuvalu",
                          "Anguilla"
                      ])

    result = self._evaluate(lambda df: df.sample(n=3), df)

    self.assertEqual(len(result), 3)

    series_result = self._evaluate(lambda df: df.GDP.sample(n=3), df)
    self.assertEqual(len(series_result), 3)
    self.assertEqual(series_result.name, "GDP")

  def test_sample_with_weights(self):
    df = pd.DataFrame({
        'population': [
            59000000,
            65000000,
            434000,
            434000,
            434000,
            337000,
            337000,
            11300,
            11300
        ],
        'GDP': [1937894, 2583560, 12011, 4520, 12128, 17036, 182, 38, 311],
        'alpha-2': ["IT", "FR", "MT", "MV", "BN", "IS", "NR", "TV", "AI"]
    },
                      index=[
                          "Italy",
                          "France",
                          "Malta",
                          "Maldives",
                          "Brunei",
                          "Iceland",
                          "Nauru",
                          "Tuvalu",
                          "Anguilla"
                      ])

    weights = pd.Series([0, 0, 0, 0, 0, 0, 0, 1, 1], index=df.index)

    result = self._evaluate(
        lambda df, weights: df.sample(n=2, weights=weights), df, weights)

    self.assertEqual(len(result), 2)
    self.assertEqual(set(result.index), set(["Tuvalu", "Anguilla"]))

    series_result = self._evaluate(
        lambda df, weights: df.GDP.sample(n=2, weights=weights), df, weights)
    self.assertEqual(len(series_result), 2)
    self.assertEqual(series_result.name, "GDP")
    self.assertEqual(set(series_result.index), set(["Tuvalu", "Anguilla"]))

  def test_sample_with_missing_weights(self):
    df = pd.DataFrame({
        'population': [
            59000000,
            65000000,
            434000,
            434000,
            434000,
            337000,
            337000,
            11300,
            11300
        ],
        'GDP': [1937894, 2583560, 12011, 4520, 12128, 17036, 182, 38, 311],
        'alpha-2': ["IT", "FR", "MT", "MV", "BN", "IS", "NR", "TV", "AI"]
    },
                      index=[
                          "Italy",
                          "France",
                          "Malta",
                          "Maldives",
                          "Brunei",
                          "Iceland",
                          "Nauru",
                          "Tuvalu",
                          "Anguilla"
                      ])

    # Missing weights are treated as 0
    weights = pd.Series([.1, .01, np.nan, 0],
                        index=["Nauru", "Iceland", "Anguilla", "Italy"])

    result = self._evaluate(
        lambda df, weights: df.sample(n=2, weights=weights), df, weights)

    self.assertEqual(len(result), 2)
    self.assertEqual(set(result.index), set(["Nauru", "Iceland"]))

    series_result = self._evaluate(
        lambda df, weights: df.GDP.sample(n=2, weights=weights), df, weights)

    self.assertEqual(len(series_result), 2)
    self.assertEqual(series_result.name, "GDP")
    self.assertEqual(set(series_result.index), set(["Nauru", "Iceland"]))

  def test_sample_with_weights_distribution(self):
    target_prob = 0.25
    num_samples = 100
    num_targets = 200
    num_other_elements = 10000

    target_weight = target_prob / num_targets
    other_weight = (1 - target_prob) / num_other_elements
    self.assertTrue(target_weight > other_weight * 10, "weights too close")

    result = self._evaluate(
        lambda s,
        weights: s.sample(n=num_samples, weights=weights).sum(),
        # The first elements are 1, the rest are all 0.  This means that when
        # we sum all the sampled elements (above), the result should be the
        # number of times the first elements (aka targets) were sampled.
        pd.Series([1] * num_targets + [0] * num_other_elements),
        pd.Series([target_weight] * num_targets +
                  [other_weight] * num_other_elements))

    # With the above constants, the probability of violating this invariant
    # (as computed using the Bernoulli distribution) is about 0.0012%.
    expected = num_samples * target_prob
    self.assertTrue(expected / 3 < result < expected * 2, (expected, result))


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


class ConstructionTimeTest(unittest.TestCase):
  """Tests for operations that can be executed eagerly."""
  DF = pd.DataFrame({
      'str_col': ['foo', 'bar'] * 3,
      'int_col': [1, 2] * 3,
      'flt_col': [1.1, 2.2] * 3,
      'cat_col': pd.Series(list('aabbca'), dtype="category"),
      'datetime_col': pd.Series(
          pd.date_range(
              '1/1/2000', periods=6, freq='m', tz='America/Los_Angeles'))
  })
  DEFERRED_DF = frame_base.DeferredFrame.wrap(
      expressions.PlaceholderExpression(DF.iloc[:0]))

  def _run_test(self, fn):
    expected = fn(self.DF)
    actual = fn(self.DEFERRED_DF)

    if isinstance(expected, pd.Index):
      pd.testing.assert_index_equal(expected, actual)
    elif isinstance(expected, pd.Series):
      pd.testing.assert_series_equal(expected, actual)
    elif isinstance(expected, pd.DataFrame):
      pd.testing.assert_frame_equal(expected, actual)
    else:
      self.assertEqual(expected, actual)

  @parameterized.expand(DF.columns)
  def test_series_name(self, col_name):
    self._run_test(lambda df: df[col_name].name)

  @parameterized.expand(DF.columns)
  def test_series_dtype(self, col_name):
    self._run_test(lambda df: df[col_name].dtype)
    self._run_test(lambda df: df[col_name].dtypes)

  def test_dataframe_columns(self):
    self._run_test(lambda df: list(df.columns))

  def test_dataframe_dtypes(self):
    self._run_test(lambda df: list(df.dtypes))

  def test_categories(self):
    self._run_test(lambda df: df.cat_col.cat.categories)

  def test_categorical_ordered(self):
    self._run_test(lambda df: df.cat_col.cat.ordered)

  def test_groupby_ndim(self):
    self._run_test(lambda df: df.groupby('int_col').ndim)

  def test_groupby_project_ndim(self):
    self._run_test(lambda df: df.groupby('int_col').flt_col.ndim)
    self._run_test(
        lambda df: df.groupby('int_col')[['flt_col', 'str_col']].ndim)

  def test_get_column_default_None(self):
    # .get just returns default_value=None at construction time if the column
    # doesn't exist
    self._run_test(lambda df: df.get('FOO'))

  def test_datetime_tz(self):
    self._run_test(lambda df: df.datetime_col.dt.tz)


class DocstringTest(unittest.TestCase):
  @parameterized.expand([
      (frames.DeferredDataFrame, pd.DataFrame),
      (frames.DeferredSeries, pd.Series),
      #(frames._DeferredIndex, pd.Index),
      (frames._DeferredStringMethods, pd.core.strings.StringMethods),
      (
          frames._DeferredCategoricalMethods,
          pd.core.arrays.categorical.CategoricalAccessor),
      (frames.DeferredGroupBy, pd.core.groupby.generic.DataFrameGroupBy),
      (frames._DeferredGroupByCols, pd.core.groupby.generic.DataFrameGroupBy),
      (
          frames._DeferredDatetimeMethods,
          pd.core.indexes.accessors.DatetimeProperties),
  ])
  def test_docs_defined(self, beam_type, pd_type):
    beam_attrs = set(dir(beam_type))
    pd_attrs = set(dir(pd_type))

    docstring_required = sorted([
        attr for attr in beam_attrs.intersection(pd_attrs)
        if getattr(pd_type, attr).__doc__ and not attr.startswith('_')
    ])

    docstring_missing = [
        attr for attr in docstring_required
        if not getattr(beam_type, attr).__doc__
    ]

    self.assertTrue(
        len(docstring_missing) == 0,
        f'{beam_type.__name__} is missing a docstring for '
        f'{len(docstring_missing)}/{len(docstring_required)} '
        f'({len(docstring_missing)/len(docstring_required):%}) '
        f'operations:\n{docstring_missing}')


class ReprTest(unittest.TestCase):
  def test_basic_dataframe(self):
    df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(GROUPBY_DF))
    self.assertEqual(
        repr(df),
        (
            "DeferredDataFrame(columns=['group', 'foo', 'bar', 'baz', 'bool', "
            "'str'], index=<unnamed>)"))

  def test_dataframe_with_named_index(self):
    df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(GROUPBY_DF.set_index('group')))
    self.assertEqual(
        repr(df),
        (
            "DeferredDataFrame(columns=['foo', 'bar', 'baz', 'bool', 'str'], "
            "index='group')"))

  def test_dataframe_with_partial_named_index(self):
    df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(
            GROUPBY_DF.set_index([GROUPBY_DF.index, 'group'])))
    self.assertEqual(
        repr(df),
        (
            "DeferredDataFrame(columns=['foo', 'bar', 'baz', 'bool', 'str'], "
            "indexes=[<unnamed>, 'group'])"))

  def test_dataframe_with_named_multi_index(self):
    df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(GROUPBY_DF.set_index(['str', 'group'])))
    self.assertEqual(
        repr(df),
        (
            "DeferredDataFrame(columns=['foo', 'bar', 'baz', 'bool'], "
            "indexes=['str', 'group'])"))

  def test_dataframe_with_multiple_column_levels(self):
    df = pd.DataFrame({
        'foofoofoo': ['one', 'one', 'one', 'two', 'two', 'two'],
        'barbar': ['A', 'B', 'C', 'A', 'B', 'C'],
        'bazzy': [1, 2, 3, 4, 5, 6],
        'zoop': ['x', 'y', 'z', 'q', 'w', 't']
    })

    df = df.pivot(index='foofoofoo', columns='barbar')
    df = frame_base.DeferredFrame.wrap(expressions.ConstantExpression(df))
    self.assertEqual(
        repr(df),
        (
            "DeferredDataFrame(columns=[('bazzy', 'A'), ('bazzy', 'B'), "
            "('bazzy', 'C'), ('zoop', 'A'), ('zoop', 'B'), ('zoop', 'C')], "
            "index='foofoofoo')"))

  def test_dataframe_with_multiple_column_and_multiple_index_levels(self):
    df = pd.DataFrame({
        'foofoofoo': ['one', 'one', 'one', 'two', 'two', 'two'],
        'barbar': ['A', 'B', 'C', 'A', 'B', 'C'],
        'bazzy': [1, 2, 3, 4, 5, 6],
        'zoop': ['x', 'y', 'z', 'q', 'w', 't']
    })

    df = df.pivot(index='foofoofoo', columns='barbar')
    df.index = [['a', 'b'], df.index]

    # pandas repr displays this:
    #             bazzy       zoop
    # barbar          A  B  C    A  B  C
    #   foofoofoo
    # a one           1  2  3    x  y  z
    # b two           4  5  6    q  w  t
    df = frame_base.DeferredFrame.wrap(expressions.ConstantExpression(df))
    self.assertEqual(
        repr(df),
        (
            "DeferredDataFrame(columns=[('bazzy', 'A'), ('bazzy', 'B'), "
            "('bazzy', 'C'), ('zoop', 'A'), ('zoop', 'B'), ('zoop', 'C')], "
            "indexes=[<unnamed>, 'foofoofoo'])"))

  def test_basic_series(self):
    df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(GROUPBY_DF['bool']))
    self.assertEqual(
        repr(df), "DeferredSeries(name='bool', dtype=bool, index=<unnamed>)")

  def test_series_with_named_index(self):
    df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(GROUPBY_DF.set_index('group')['str']))
    self.assertEqual(
        repr(df), "DeferredSeries(name='str', dtype=object, index='group')")

  def test_series_with_partial_named_index(self):
    df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(
            GROUPBY_DF.set_index([GROUPBY_DF.index, 'group'])['bar']))
    self.assertEqual(
        repr(df),
        (
            "DeferredSeries(name='bar', dtype=float64, "
            "indexes=[<unnamed>, 'group'])"))

  def test_series_with_named_multi_index(self):
    df = frame_base.DeferredFrame.wrap(
        expressions.ConstantExpression(
            GROUPBY_DF.set_index(['str', 'group'])['baz']))
    self.assertEqual(
        repr(df),
        "DeferredSeries(name='baz', dtype=float64, indexes=['str', 'group'])")


if __name__ == '__main__':
  unittest.main()
