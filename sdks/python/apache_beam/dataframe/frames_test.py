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
from apache_beam.dataframe import frames  # pylint: disable=unused-import

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


class DeferredFrameTest(unittest.TestCase):
  def _run_error_test(self, func, *args):
    """Verify that func(*args) raises the same exception in pandas and in Beam.

    Note that for Beam this only checks for exceptions that are raised during
    expression generation (i.e. construction time). Execution time exceptions
    are not helpful."""
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
    try:
      _ = func(*deferred_args)._expr
    except Exception as e:
      actual = e
    else:
      raise AssertionError(
          "Expected an error:\n{expected_error}\nbut Beam successfully "
          "generated an expression.")

    # Verify
    if (not isinstance(actual, type(expected_error)) or
        not str(actual) == str(expected_error)):
      raise AssertionError(
          f'Expected {expected_error!r} to be raised, but got {actual!r}'
      ) from actual

  def _run_test(self, func, *args, distributed=True, nonparallel=False):
    """Verify that func(*args) produces the same result in pandas and in Beam.

    Args:
        distributed (bool): Whether or not to use PartitioningSession to
            simulate parallel execution.
        nonparallel (bool): Whether or not this function contains a
            non-parallelizable operation. If True, the expression will be
            generated twice, once outside of an allow_non_parallel_operations
            block (to verify NonParallelOperation is raised), and again inside
            of an allow_non_parallel_operations block to actually generate an
            expression to verify."""
    # Compute expected value
    expected = func(*args)

    # Compute actual value
    deferred_args = _get_deferred_args(*args)
    if nonparallel:
      # First run outside a nonparallel block to confirm this raises as expected
      with self.assertRaises(expressions.NonParallelOperation):
        _ = func(*deferred_args)

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
        cmp = lambda x: np.isclose(expected, x)
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

  def test_str_split(self):
    s = pd.Series([
        "this is a regular sentence",
        "https://docs.python.org/3/tutorial/index.html",
        np.nan
    ])

    # TODO(BEAM-11931): pandas produces None for empty values with expand=True,
    # while we produce NaN (from pd.concat). This replicates some doctests that
    # verify that behavior, but with a replace call to ignore the difference.
    self._run_test(
        lambda s: s.str.split(expand=True).replace({None: np.nan}), s)
    self._run_test(
        lambda s: s.str.rsplit("/", n=1, expand=True).replace({None: np.nan}),
        s)

  def test_set_column_from_index(self):
    def new_column(df):
      df['NewCol'] = df.index
      return df

    df = pd.DataFrame({
        'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
        'Speed': [380., 370., 24., 26.]
    })
    self._run_test(new_column, df)

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
    df = pd.DataFrame(np.arange(10).reshape(-1, 2), columns=['A', 'B'])

    self._run_test(
        lambda df: df.where(lambda df: df % 2 == 0, lambda df: df * 10), df)

  def test_where_concrete_args(self):
    df = pd.DataFrame(np.arange(10).reshape(-1, 2), columns=['A', 'B'])

    self._run_test(
        lambda df: df.where(
            df % 2 == 0, pd.Series({
                'A': 123, 'B': 456
            }), axis=1),
        df)

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

  def test_groupby_project(self):
    df = GROUPBY_DF

    self._run_test(lambda df: df.groupby('group').foo.agg(sum), df)

    self._run_test(lambda df: df.groupby('group').sum(), df)
    self._run_test(lambda df: df.groupby('group').foo.sum(), df)
    self._run_test(lambda df: df.groupby('group').bar.sum(), df)
    self._run_test(lambda df: df.groupby('group')['foo'].sum(), df)
    self._run_test(lambda df: df.groupby('group')['baz'].sum(), df)
    self._run_error_test(
        lambda df: df.groupby('group')[['bar', 'baz']].bar.sum(), df)
    self._run_error_test(lambda df: df.groupby('group')[['bat']].sum(), df)
    self._run_error_test(lambda df: df.groupby('group').bat.sum(), df)

    self._run_test(lambda df: df.groupby('group').median(), df)
    self._run_test(lambda df: df.groupby('group').foo.median(), df)
    self._run_test(lambda df: df.groupby('group').bar.median(), df)
    self._run_test(lambda df: df.groupby('group')['foo'].median(), df)
    self._run_test(lambda df: df.groupby('group')['baz'].median(), df)
    self._run_test(lambda df: df.groupby('group')[['bar', 'baz']].median(), df)

  def test_groupby_errors_non_existent_projection(self):
    df = GROUPBY_DF

    # non-existent projection column
    self._run_error_test(
        lambda df: df.groupby('group')[['bar', 'baz']].bar.median(), df)
    self._run_error_test(lambda df: df.groupby('group')[['bad']].median(), df)

    self._run_error_test(lambda df: df.groupby('group').bad.median(), df)

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

  def test_set_index(self):
    df = pd.DataFrame({
        # [19, 18, ..]
        'index1': reversed(range(20)),
        # [15, 16, .., 0, 1, .., 13, 14]
        'index2': np.roll(range(20), 5),
        # ['', 'a', 'bb', ...]
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
        lambda df: df.set_index(['str', 'group', 'bool']).groupby(
            level='group').apply(median_sum_fn),
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
        nonparallel=True)
    self._run_test(
        lambda df1,
        df2: df1.merge(
            df2, left_on='lkey', right_on='rkey', suffixes=('_left', '_right')).
        rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True)

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
        nonparallel=True)

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
        df2)

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
        nonparallel=True)
    self._run_test(
        lambda df1,
        df2: df1.merge(df2, on='key', suffixes=('_left', '_right')).rename(
            index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True)

  def test_merge_same_key_doctest(self):
    df1 = pd.DataFrame({'a': ['foo', 'bar'], 'b': [1, 2]})
    df2 = pd.DataFrame({'a': ['foo', 'baz'], 'c': [3, 4]})

    self._run_test(
        lambda df1,
        df2: df1.merge(df2, how='left', on='a').rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True)
    # Test without specifying 'on'
    self._run_test(
        lambda df1,
        df2: df1.merge(df2, how='left').rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True)

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
        nonparallel=True)
    # Test without specifying 'on'
    self._run_test(
        lambda df1,
        df2: df1.merge(df2, how='left', suffixes=('_lsuffix', '_rsuffix')).
        rename(index=lambda x: '*'),
        df1,
        df2,
        nonparallel=True)

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

  def test_series_agg(self):
    s = pd.Series(list(range(16)))
    self._run_test(lambda s: s.agg('sum'), s)
    self._run_test(lambda s: s.agg(['sum']), s)
    self._run_test(lambda s: s.agg(['sum', 'mean']), s, nonparallel=True)
    self._run_test(lambda s: s.agg(['mean']), s, nonparallel=True)
    self._run_test(lambda s: s.agg('mean'), s, nonparallel=True)

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

  def test_dataframe_agg(self):
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

  @unittest.skipIf(PD_VERSION < (1, 2), "na_action added in pandas 1.2.0")
  def test_applymap_na_action(self):
    # Replicates a doctest for na_action which is incompatible with
    # doctest framework
    df = pd.DataFrame([[pd.NA, 2.12], [3.356, 4.567]])
    self._run_test(
        lambda df: df.applymap(lambda x: len(str(x)), na_action='ignore'), df)

  def test_categorical_groupby(self):
    df = pd.DataFrame({'A': np.arange(6), 'B': list('aabbca')})
    df['B'] = df['B'].astype(pd.CategoricalDtype(list('cab')))
    df = df.set_index('B')
    # TODO(BEAM-11190): These aggregations can be done in index partitions, but
    # it will require a little more complex logic
    self._run_test(lambda df: df.groupby(level=0).sum(), df, nonparallel=True)
    self._run_test(lambda df: df.groupby(level=0).mean(), df, nonparallel=True)

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

  def test_index_name_assignment(self):
    df = pd.DataFrame({'a': ['foo', 'bar'], 'b': [1, 2]})
    df = df.set_index(['a', 'b'], drop=False)

    def change_index_names(df):
      df.index.names = ['A', None]
      return df

    self._run_test(change_index_names, df)

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

  def test_quantile_axis_columns(self):
    df = pd.DataFrame(
        np.array([[1, 1], [2, 10], [3, 100], [4, 100]]), columns=['a', 'b'])

    with beam.dataframe.allow_non_parallel_operations():
      self._run_test(lambda df: df.quantile(0.1, axis='columns'), df)

    with self.assertRaisesRegex(frame_base.WontImplementError,
                                r"df\.quantile\(q=0\.1, axis='columns'\)"):
      self._run_test(lambda df: df.quantile([0.1, 0.5], axis='columns'), df)

  @unittest.skipIf(PD_VERSION < (1, 1), "drop_na added in pandas 1.1.0")
  def test_groupby_count_na(self):
    # Verify we can do a groupby.count() that doesn't drop NaN values
    self._run_test(
        lambda df: df.groupby('foo', dropna=True).bar.count(), GROUPBY_DF)
    self._run_test(
        lambda df: df.groupby('foo', dropna=False).bar.count(), GROUPBY_DF)


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
      'str_col': ['foo', 'bar'],
      'int_col': [1, 2],
      'flt_col': [1.1, 2.2],
  })
  DEFERRED_DF = frame_base.DeferredFrame.wrap(
      expressions.PlaceholderExpression(DF))

  def _run_test(self, fn):
    self.assertEqual(fn(self.DEFERRED_DF), fn(self.DF))

  @parameterized.expand(DF.columns)
  def test_series_name(self, col_name):
    self._run_test(lambda df: df[col_name])

  @parameterized.expand(DF.columns)
  def test_series_dtype(self, col_name):
    self._run_test(lambda df: df[col_name].dtype)
    self._run_test(lambda df: df[col_name].dtypes)

  def test_dataframe_columns(self):
    self._run_test(lambda df: list(df.columns))

  def test_dataframe_dtypes(self):
    self._run_test(lambda df: list(df.dtypes))


class DocstringTest(unittest.TestCase):
  @parameterized.expand([
      (frames.DeferredDataFrame, pd.DataFrame),
      (frames.DeferredSeries, pd.Series),
      (frames._DeferredIndex, pd.Index),
      (frames._DeferredStringMethods, pd.core.strings.StringMethods),
      (frames.DeferredGroupBy, pd.core.groupby.generic.DataFrameGroupBy),
      (frames._DeferredGroupByCols, pd.core.groupby.generic.DataFrameGroupBy),
  ])
  @unittest.skip('BEAM-12074')
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


if __name__ == '__main__':
  unittest.main()
