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

import pandas as pd

from apache_beam.dataframe import doctests
from apache_beam.dataframe.pandas_top_level_functions import _is_top_level_function


@unittest.skipIf(sys.platform == 'win32', '[BEAM-10626]')
class DoctestTest(unittest.TestCase):
  def test_dataframe_tests(self):
    result = doctests.testmod(
        pd.core.frame,
        use_beam=False,
        report=True,
        wont_implement_ok={
            'pandas.core.frame.DataFrame.T': ['*'],
            'pandas.core.frame.DataFrame.cummax': ['*'],
            'pandas.core.frame.DataFrame.cummin': ['*'],
            'pandas.core.frame.DataFrame.cumsum': ['*'],
            'pandas.core.frame.DataFrame.cumprod': ['*'],
            'pandas.core.frame.DataFrame.diff': ['*'],
            'pandas.core.frame.DataFrame.fillna': [
                "df.fillna(method='ffill')",
                'df.fillna(value=values, limit=1)',
            ],
            'pandas.core.frame.DataFrame.items': ['*'],
            'pandas.core.frame.DataFrame.itertuples': ['*'],
            'pandas.core.frame.DataFrame.iterrows': ['*'],
            'pandas.core.frame.DataFrame.iteritems': ['*'],
            # default keep is 'first'
            'pandas.core.frame.DataFrame.nlargest': [
                "df.nlargest(3, 'population')",
                "df.nlargest(3, ['population', 'GDP'])",
                "df.nlargest(3, 'population', keep='last')"
            ],
            'pandas.core.frame.DataFrame.nsmallest': [
                "df.nsmallest(3, 'population')",
                "df.nsmallest(3, ['population', 'GDP'])",
                "df.nsmallest(3, 'population', keep='last')",
            ],
            'pandas.core.frame.DataFrame.nunique': ['*'],
            'pandas.core.frame.DataFrame.replace': [
                "s.replace([1, 2], method='bfill')",
                # Relies on method='pad'
                "s.replace('a', None)",
            ],
            'pandas.core.frame.DataFrame.to_records': ['*'],
            'pandas.core.frame.DataFrame.to_dict': ['*'],
            'pandas.core.frame.DataFrame.to_numpy': ['*'],
            'pandas.core.frame.DataFrame.to_string': ['*'],
            'pandas.core.frame.DataFrame.transpose': ['*'],
            'pandas.core.frame.DataFrame.shape': ['*'],
            'pandas.core.frame.DataFrame.shift': [
                'df.shift(periods=3, freq="D")',
                'df.shift(periods=3, freq="infer")'
            ],
            'pandas.core.frame.DataFrame.unstack': ['*'],
            'pandas.core.frame.DataFrame.memory_usage': ['*'],
            'pandas.core.frame.DataFrame.info': ['*'],
            # Not equal to df.agg('mode', axis='columns', numeric_only=True)
            # because there can be multiple columns if a row has more than one
            # mode
            'pandas.core.frame.DataFrame.mode': [
                "df.mode(axis='columns', numeric_only=True)"
            ],
            'pandas.core.frame.DataFrame.append': [
                'df.append(df2, ignore_index=True)',
                "for i in range(5):\n" +
                "    df = df.append({'A': i}, ignore_index=True)",
            ],
        },
        not_implemented_ok={
            'pandas.core.frame.DataFrame.transform': ['*'],
            'pandas.core.frame.DataFrame.isin': ['*'],
            'pandas.core.frame.DataFrame.melt': ['*'],
            'pandas.core.frame.DataFrame.count': ['*'],
            'pandas.core.frame.DataFrame.reindex': ['*'],
            'pandas.core.frame.DataFrame.reindex_axis': ['*'],

            'pandas.core.frame.DataFrame.round': [
                'df.round(decimals)',
            ],

            # We should be able to support pivot and pivot_table for categorical
            # columns
            'pandas.core.frame.DataFrame.pivot': ['*'],

            # We can implement this as a zipping operator, but it won't have the
            # same capability. The doctest includes an example that branches on
            # a deferred result.
            'pandas.core.frame.DataFrame.combine': ['*'],

            # Can be implemented as a zipping operator
            'pandas.core.frame.DataFrame.combine_first': ['*'],

            # Difficult to parallelize but should be possible?
            'pandas.core.frame.DataFrame.dot': [
                # reindex not supported
                's2 = s.reindex([1, 0, 2, 3])',
                'df.dot(s2)',
            ],

            # Trivially elementwise for axis=columns. Relies on global indexing
            # for axis=rows.
            # Difficult to determine proxy, need to inspect function
            'pandas.core.frame.DataFrame.apply': ['*'],

            # Cross-join not implemented
            'pandas.core.frame.DataFrame.merge': [
                "df1.merge(df2, how='cross')"
            ],

            # TODO(BEAM-11711)
            'pandas.core.frame.DataFrame.set_index': [
                "df.set_index([s, s**2])",
            ],
        },
        skip={
            # Throws NotImplementedError when modifying df
            'pandas.core.frame.DataFrame.transform': ['df'],
            'pandas.core.frame.DataFrame.axes': [
                # Returns deferred index.
                'df.axes',
            ],
            'pandas.core.frame.DataFrame.compare': ['*'],
            'pandas.core.frame.DataFrame.cov': [
                # Relies on setting entries ahead of time.
                "df.loc[df.index[:5], 'a'] = np.nan",
                "df.loc[df.index[5:10], 'b'] = np.nan",
                'df.cov(min_periods=12)',
            ],
            'pandas.core.frame.DataFrame.drop_duplicates': ['*'],
            'pandas.core.frame.DataFrame.duplicated': ['*'],
            'pandas.core.frame.DataFrame.idxmax': ['*'],
            'pandas.core.frame.DataFrame.idxmin': ['*'],
            'pandas.core.frame.DataFrame.rename': [
                # Returns deferred index.
                'df.index',
                'df.rename(index=str).index',
            ],
            'pandas.core.frame.DataFrame.set_index': [
                # TODO(BEAM-11711): This could pass in the index as
                # a DeferredIndex, and we should fail it as order-sensitive.
                "df.set_index([pd.Index([1, 2, 3, 4]), 'year'])",
            ],
            'pandas.core.frame.DataFrame.set_axis': ['*'],
            'pandas.core.frame.DataFrame.sort_index': ['*'],
            'pandas.core.frame.DataFrame.to_markdown': ['*'],
            'pandas.core.frame.DataFrame.to_parquet': ['*'],
            'pandas.core.frame.DataFrame.value_counts': ['*'],

            'pandas.core.frame.DataFrame.to_records': [
                'df.index = df.index.rename("I")',
                'index_dtypes = f"<S{df.index.str.len().max()}"', # 1.x
                'index_dtypes = "<S{}".format(df.index.str.len().max())', #0.x
                'df.to_records(index_dtypes=index_dtypes)',
            ],
            # These tests use the static method pd.pivot_table, which doesn't
            # actually raise NotImplementedError
            'pandas.core.frame.DataFrame.pivot_table': ['*'],
            # Expected to raise a ValueError, but we raise NotImplementedError
            'pandas.core.frame.DataFrame.pivot': [
                "df.pivot(index='foo', columns='bar', values='baz')"
            ],
            'pandas.core.frame.DataFrame.append': [
                'df',
                # pylint: disable=line-too-long
                "pd.concat([pd.DataFrame([i], columns=['A']) for i in range(5)],\n"
                "          ignore_index=True)"
            ],
            'pandas.core.frame.DataFrame.eval': ['df'],
            'pandas.core.frame.DataFrame.melt': [
                "df.columns = [list('ABC'), list('DEF')]", "df"
            ],
            'pandas.core.frame.DataFrame.merge': [
                # Order-sensitive index, checked in frames_test.py.
                "df1.merge(df2, left_on='lkey', right_on='rkey')",
                "df1.merge(df2, left_on='lkey', right_on='rkey',\n"
                "          suffixes=('_left', '_right'))",
                "df1.merge(df2, how='left', on='a')",
            ],
            # Raises right exception, but testing framework has matching issues.
            'pandas.core.frame.DataFrame.replace': [
                "df.replace({'a string': 'new value', True: False})  # raises"
            ],
            'pandas.core.frame.DataFrame.to_sparse': ['type(df)'],

            # Skipped because "seen_wont_implement" is reset before getting to
            # these calls, so the NameError they raise is not ignored.
            'pandas.core.frame.DataFrame.T': [
                'df1_transposed.dtypes', 'df2_transposed.dtypes'
            ],
            'pandas.core.frame.DataFrame.transpose': [
                'df1_transposed.dtypes', 'df2_transposed.dtypes'
            ],
            # Skipped because the relies on iloc to set a cell to NA. Test is
            # replicated in frames_test::DeferredFrameTest::test_applymap.
            'pandas.core.frame.DataFrame.applymap': [
                'df_copy.iloc[0, 0] = pd.NA',
                "df_copy.applymap(lambda x: len(str(x)), na_action='ignore')",
            ],
            # Skipped so we don't need to install natsort
            'pandas.core.frame.DataFrame.sort_values': [
                'from natsort import index_natsorted',
                'df.sort_values(\n'
                '   by="time",\n'
                '   key=lambda x: np.argsort(index_natsorted(df["time"]))\n'
                ')'
            ],
            # Mode that we don't yet support, documentation added in pandas
            # 1.2.0 (https://github.com/pandas-dev/pandas/issues/35912)
            'pandas.core.frame.DataFrame.aggregate': [
                "df.agg(x=('A', max), y=('B', 'min'), z=('C', np.mean))"
            ],
        })
    self.assertEqual(result.failed, 0)

  def test_series_tests(self):
    result = doctests.testmod(
        pd.core.series,
        use_beam=False,
        report=True,
        wont_implement_ok={
            'pandas.core.series.Series.__array__': ['*'],
            'pandas.core.series.Series.array': ['*'],
            'pandas.core.series.Series.cummax': ['*'],
            'pandas.core.series.Series.cummin': ['*'],
            'pandas.core.series.Series.cumsum': ['*'],
            'pandas.core.series.Series.cumprod': ['*'],
            'pandas.core.series.Series.diff': ['*'],
            'pandas.core.series.Series.dot': [
                's.dot(arr)',  # non-deferred result
            ],
            'pandas.core.series.Series.fillna': [
                "df.fillna(method='ffill')",
                'df.fillna(value=values, limit=1)',
            ],
            'pandas.core.series.Series.items': ['*'],
            'pandas.core.series.Series.iteritems': ['*'],
            # default keep is 'first'
            'pandas.core.series.Series.nlargest': [
                "s.nlargest()",
                "s.nlargest(3)",
                "s.nlargest(3, keep='last')",
            ],
            'pandas.core.series.Series.memory_usage': ['*'],
            'pandas.core.series.Series.nsmallest': [
                "s.nsmallest()",
                "s.nsmallest(3)",
                "s.nsmallest(3, keep='last')",
            ],
            'pandas.core.series.Series.pop': ['*'],
            'pandas.core.series.Series.searchsorted': ['*'],
            'pandas.core.series.Series.shift': ['*'],
            'pandas.core.series.Series.take': ['*'],
            'pandas.core.series.Series.to_dict': ['*'],
            'pandas.core.series.Series.unique': ['*'],
            'pandas.core.series.Series.unstack': ['*'],
            'pandas.core.series.Series.values': ['*'],
            'pandas.core.series.Series.view': ['*'],
            'pandas.core.series.Series.append': [
                's1.append(s2, ignore_index=True)',
            ],
        },
        not_implemented_ok={
            'pandas.core.series.Series.transform': ['*'],
            'pandas.core.series.Series.groupby': [
                'ser.groupby(["a", "b", "a", "b"]).mean()',
                'ser.groupby(["a", "b", "a", np.nan]).mean()',
                'ser.groupby(["a", "b", "a", np.nan], dropna=False).mean()',
                # Grouping by a series is not supported
                'ser.groupby(ser > 100).mean()',
            ],
            'pandas.core.series.Series.reindex': ['*'],
        },
        skip={
            # error formatting
            'pandas.core.series.Series.append': [
                's1.append(s2, verify_integrity=True)',
            ],
            # Throws NotImplementedError when modifying df
            'pandas.core.series.Series.transform': ['df'],
            'pandas.core.series.Series.argmax': ['*'],
            'pandas.core.series.Series.argmin': ['*'],
            'pandas.core.series.Series.autocorr': ['*'],
            'pandas.core.series.Series.combine': ['*'],
            'pandas.core.series.Series.combine_first': ['*'],
            'pandas.core.series.Series.compare': ['*'],
            'pandas.core.series.Series.count': ['*'],
            'pandas.core.series.Series.cov': [
                # Differs in LSB on jenkins.
                "s1.cov(s2)",
            ],
            'pandas.core.series.Series.drop_duplicates': ['*'],
            'pandas.core.series.Series.duplicated': ['*'],
            'pandas.core.series.Series.explode': ['*'],
            'pandas.core.series.Series.idxmax': ['*'],
            'pandas.core.series.Series.idxmin': ['*'],
            'pandas.core.series.Series.name': ['*'],
            'pandas.core.series.Series.nonzero': ['*'],
            'pandas.core.series.Series.quantile': ['*'],
            'pandas.core.series.Series.pop': ['ser'],  # testing side effect
            'pandas.core.series.Series.repeat': ['*'],
            'pandas.core.series.Series.replace': ['*'],
            'pandas.core.series.Series.reset_index': ['*'],
            'pandas.core.series.Series.searchsorted': [
                # This doctest seems to be incorrectly parsed.
                "x = pd.Categorical(['apple', 'bread', 'bread',"
            ],
            'pandas.core.series.Series.set_axis': ['*'],
            'pandas.core.series.Series.sort_index': ['*'],
            'pandas.core.series.Series.sort_values': ['*'],
            'pandas.core.series.Series.to_csv': ['*'],
            'pandas.core.series.Series.to_markdown': ['*'],
            'pandas.core.series.Series.update': ['*'],
            'pandas.core.series.Series.view': [
                # Inspection after modification.
                's'
            ],
        })
    self.assertEqual(result.failed, 0)

  def test_string_tests(self):
    PD_VERSION = tuple(int(v) for v in pd.__version__.split('.'))
    if PD_VERSION < (1, 2, 0):
      module = pd.core.strings
    else:
      # Definitions were moved to accessor in pandas 1.2.0
      module = pd.core.strings.accessor

    module_name = module.__name__

    result = doctests.testmod(
        module,
        use_beam=False,
        wont_implement_ok={
            # These methods can accept deferred series objects, but not lists
            f'{module_name}.StringMethods.cat': [
                "s.str.cat(['A', 'B', 'C', 'D'], sep=',')",
                "s.str.cat(['A', 'B', 'C', 'D'], sep=',', na_rep='-')",
                "s.str.cat(['A', 'B', 'C', 'D'], na_rep='-')"
            ],
            f'{module_name}.StringMethods.repeat': [
                's.str.repeat(repeats=[1, 2, 3])'
            ],
            f'{module_name}.str_repeat': ['s.str.repeat(repeats=[1, 2, 3])'],
            f'{module_name}.StringMethods.get_dummies': ['*'],
            f'{module_name}.str_get_dummies': ['*'],
        },
        skip={
            # count() on Series with a NaN produces mismatched type if we
            # have a NaN-only partition.
            f'{module_name}.StringMethods.count': ["s.str.count('a')"],
            f'{module_name}.str_count': ["s.str.count('a')"],

            # Produce None instead of NaN, see
            # frames_test.py::DeferredFrameTest::test_str_split
            f'{module_name}.StringMethods.rsplit': [
                's.str.split(expand=True)',
                's.str.rsplit("/", n=1, expand=True)',
            ],
            f'{module_name}.StringMethods.split': [
                's.str.split(expand=True)',
                's.str.rsplit("/", n=1, expand=True)',
            ],

            # Bad test strings in pandas 1.1.x
            f'{module_name}.str_replace': [
                "pd.Series(['foo', 'fuz', np.nan]).str.replace('f', repr)"
            ],
            f'{module_name}.StringMethods.replace': [
                "pd.Series(['foo', 'fuz', np.nan]).str.replace('f', repr)"
            ],

            # output has incorrect formatting in 1.2.x
            f'{module_name}.StringMethods.extractall': ['*']
        })
    self.assertEqual(result.failed, 0)

  def test_datetime_tests(self):
    # TODO(BEAM-10721)
    datetimelike_result = doctests.testmod(
        pd.core.arrays.datetimelike,
        use_beam=False,
        skip={
            'pandas.core.arrays.datetimelike.AttributesMixin._unbox_scalar': [
                '*'
            ],
            'pandas.core.arrays.datetimelike.TimelikeOps.ceil': ['*'],
            'pandas.core.arrays.datetimelike.TimelikeOps.floor': ['*'],
            'pandas.core.arrays.datetimelike.TimelikeOps.round': ['*'],
        })

    datetime_result = doctests.testmod(
        pd.core.arrays.datetimes,
        use_beam=False,
        skip={
            'pandas.core.arrays.datetimes.DatetimeArray.day': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.hour': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.microsecond': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.minute': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.month': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.nanosecond': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.second': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.year': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.is_leap_year': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.is_month_end': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.is_month_start': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.is_quarter_end': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.is_quarter_start': [
                '*'
            ],
            'pandas.core.arrays.datetimes.DatetimeArray.is_year_end': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.is_year_start': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.to_period': ['*'],
            'pandas.core.arrays.datetimes.DatetimeArray.tz_localize': ['*'],
        })

    self.assertEqual(datetimelike_result.failed, 0)
    self.assertEqual(datetime_result.failed, 0)

  def test_indexing_tests(self):
    result = doctests.testmod(
        pd.core.indexing,
        use_beam=False,
        skip={
            'pandas.core.indexing._IndexSlice': ['*'],
            'pandas.core.indexing.IndexingMixin.at': ['*'],
            'pandas.core.indexing.IndexingMixin.iat': ['*'],
            'pandas.core.indexing.IndexingMixin.iloc': ['*'],
            'pandas.core.indexing.IndexingMixin.loc': ['*'],
            'pandas.core.indexing._AtIndexer': ['*'],
            'pandas.core.indexing._LocIndexer': ['*'],
            'pandas.core.indexing._iAtIndexer': ['*'],
            'pandas.core.indexing._iLocIndexer': ['*'],
        })
    self.assertEqual(result.failed, 0)

  def test_groupby_tests(self):
    result = doctests.testmod(
        pd.core.groupby.groupby,
        use_beam=False,
        wont_implement_ok={
            'pandas.core.groupby.groupby.GroupBy.head': ['*'],
            'pandas.core.groupby.groupby.GroupBy.tail': ['*'],
            'pandas.core.groupby.groupby.GroupBy.nth': ['*'],
            'pandas.core.groupby.groupby.GroupBy.cumcount': ['*'],
        },
        not_implemented_ok={
            'pandas.core.groupby.groupby.GroupBy.describe': ['*'],
            'pandas.core.groupby.groupby.GroupBy.ngroup': ['*'],
            'pandas.core.groupby.groupby.GroupBy.resample': ['*'],
            'pandas.core.groupby.groupby.GroupBy.sample': ['*'],
            'pandas.core.groupby.groupby.GroupBy.quantile': ['*'],
            'pandas.core.groupby.groupby.BaseGroupBy.pipe': ['*'],
            # pipe tests are in a different location in pandas 1.1.x
            'pandas.core.groupby.groupby._GroupBy.pipe': ['*'],
            'pandas.core.groupby.groupby.GroupBy.nth': [
                "df.groupby('A', as_index=False).nth(1)",
            ],
        },
        skip={
            # Uses iloc to mutate a DataFrame
            'pandas.core.groupby.groupby.GroupBy.resample': [
                'df.iloc[2, 0] = 5',
                'df',
            ],
            # TODO: Raise wont implement for list passed as a grouping column
            # Currently raises unhashable type: list
            'pandas.core.groupby.groupby.GroupBy.ngroup': [
                'df.groupby(["A", [1,1,2,3,2,1]]).ngroup()'
            ],
        })
    self.assertEqual(result.failed, 0)

    result = doctests.testmod(
        pd.core.groupby.generic,
        use_beam=False,
        wont_implement_ok={
            # Returns an array by default, not a Series. WontImplement
            # (non-deferred)
            'pandas.core.groupby.generic.SeriesGroupBy.unique': ['*'],
            # TODO: Is take actually deprecated?
            'pandas.core.groupby.generic.DataFrameGroupBy.take': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.take': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.nsmallest': [
                "s.nsmallest(3, keep='last')",
                "s.nsmallest(3)",
                "s.nsmallest()",
            ],
            'pandas.core.groupby.generic.SeriesGroupBy.nlargest': [
                "s.nlargest(3, keep='last')",
                "s.nlargest(3)",
                "s.nlargest()",
            ],
            'pandas.core.groupby.generic.DataFrameGroupBy.diff': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.diff': ['*'],
            'pandas.core.groupby.generic.DataFrameGroupBy.hist': ['*'],
            'pandas.core.groupby.generic.DataFrameGroupBy.fillna': [
                "df.fillna(method='ffill')",
                'df.fillna(value=values, limit=1)',
            ],
            'pandas.core.groupby.generic.SeriesGroupBy.fillna': [
                "df.fillna(method='ffill')",
                'df.fillna(value=values, limit=1)',
            ],
        },
        not_implemented_ok={
            'pandas.core.groupby.generic.DataFrameGroupBy.transform': ['*'],
            'pandas.core.groupby.generic.DataFrameGroupBy.idxmax': ['*'],
            'pandas.core.groupby.generic.DataFrameGroupBy.idxmin': ['*'],
            'pandas.core.groupby.generic.DataFrameGroupBy.filter': ['*'],
            'pandas.core.groupby.generic.DataFrameGroupBy.nunique': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.transform': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.idxmax': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.idxmin': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.filter': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.describe': ['*'],
        },
        skip={
            'pandas.core.groupby.generic.SeriesGroupBy.cov': [
                # Floating point comparison fails
                's1.cov(s2)',
            ],
            'pandas.core.groupby.generic.DataFrameGroupBy.cov': [
                # Mutates input DataFrame with loc
                # TODO: Replicate in frames_test.py
                "df.loc[df.index[:5], 'a'] = np.nan",
                "df.loc[df.index[5:10], 'b'] = np.nan",
                "df.cov(min_periods=12)",
            ],
            # These examples rely on grouping by a list
            'pandas.core.groupby.generic.SeriesGroupBy.aggregate': ['*'],
            'pandas.core.groupby.generic.DataFrameGroupBy.aggregate': ['*'],
        })
    self.assertEqual(result.failed, 0)

  def test_top_level(self):
    tests = {
        name: func.__doc__
        for (name, func) in pd.__dict__.items()
        if _is_top_level_function(func) and getattr(func, '__doc__', None)
    }

    skip_reads = {name: ['*'] for name in dir(pd) if name.startswith('read_')}

    result = doctests.teststrings(
        tests,
        use_beam=False,
        report=True,
        not_implemented_ok={
            'concat': ['pd.concat([s1, s2], ignore_index=True)'],
            'crosstab': ['*'],
            'cut': ['*'],
            'eval': ['*'],
            'factorize': ['*'],
            'get_dummies': ['*'],
            'infer_freq': ['*'],
            'lreshape': ['*'],
            'melt': ['*'],
            'merge': ["df1.merge(df2, how='cross')"],
            'merge_asof': ['*'],
            'pivot': ['*'],
            'pivot_table': ['*'],
            'qcut': ['*'],
            'reset_option': ['*'],
            'set_eng_float_format': ['*'],
            'set_option': ['*'],
            'to_numeric': ['*'],
            'to_timedelta': ['*'],
            'unique': ['*'],
            'value_counts': ['*'],
            'wide_to_long': ['*'],
        },
        wont_implement_ok={
            'to_datetime': ['s.head()'],
            'to_pickle': ['*'],
        },
        skip={
            # error formatting
            'concat': ['pd.concat([df5, df6], verify_integrity=True)'],
            # doctest DeprecationWarning
            'melt': ['df'],
            # Order-sensitive re-indexing.
            'merge': [
                "df1.merge(df2, left_on='lkey', right_on='rkey')",
                "df1.merge(df2, left_on='lkey', right_on='rkey',\n"
                "          suffixes=('_left', '_right'))",
                "df1.merge(df2, how='left', on='a')",
            ],
            # Not an actual test.
            'option_context': ['*'],
            'factorize': ['codes', 'uniques'],
            # Bad top-level use of un-imported function.
            'merge_ordered': [
                'merge_ordered(df1, df2, fill_method="ffill", left_by="group")'
            ],
            # Expected error.
            'pivot': ["df.pivot(index='foo', columns='bar', values='baz')"],
            # Never written.
            'to_pickle': ['os.remove("./dummy.pkl")'],
            **skip_reads
        })
    self.assertEqual(result.failed, 0)


if __name__ == '__main__':
  unittest.main()
