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


@unittest.skipIf(sys.version_info <= (3, ), 'Requires contextlib.ExitStack.')
@unittest.skipIf(sys.version_info < (3, 6), 'Nondeterministic dict ordering.')
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
        },
        not_implemented_ok={
            'pandas.core.frame.DataFrame.isin': ['*'],
            'pandas.core.frame.DataFrame.melt': ['*'],
            'pandas.core.frame.DataFrame.axes': ['*'],
            'pandas.core.frame.DataFrame.count': ['*'],
            'pandas.core.frame.DataFrame.reindex': ['*'],
            'pandas.core.frame.DataFrame.reindex_axis': ['*'],

            # We should be able to support pivot and pivot_table for categorical
            # columns
            'pandas.core.frame.DataFrame.pivot': ['*'],

            # DataFrame.__getitem__ cannot be used as loc
            'pandas.core.frame.DataFrame.query': [
                'df[df.A > df.B]', "df[df.B == df['C C']]"
            ],

            # We can implement this as a zipping operator, but it won't have the
            # same capability. The doctest includes an example that branches on
            # a deferred result.
            'pandas.core.frame.DataFrame.combine': ['*'],

            # Can be implemented as a zipping operator
            'pandas.core.frame.DataFrame.combine_first': ['*'],

            # Difficult to parallelize but should be possible?
            'pandas.core.frame.DataFrame.corr': ['*'],
            'pandas.core.frame.DataFrame.cov': ['*'],
            'pandas.core.frame.DataFrame.dot': [
                # reindex not supported
                's2 = s.reindex([1, 0, 2, 3])',
                'df.dot(s2)',
            ],

            # element-wise
            'pandas.core.frame.DataFrame.eval': ['*'],
            'pandas.core.frame.DataFrame.explode': ['*'],

            # Trivially elementwise for axis=columns. Relies on global indexing
            # for axis=rows.
            'pandas.core.frame.DataFrame.drop': ['*'],
            'pandas.core.frame.DataFrame.rename': ['*'],
            'pandas.core.frame.DataFrame.apply': ['*'],

            # Zipping operation if input is a DeferredSeries
            'pandas.core.frame.DataFrame.assign': ['*'],

            # In theory this is possible for bounded inputs?
            'pandas.core.frame.DataFrame.append': ['*'],
        },
        skip={
            'pandas.core.frame.DataFrame.compare': ['*'],
            'pandas.core.frame.DataFrame.drop_duplicates': ['*'],
            'pandas.core.frame.DataFrame.duplicated': ['*'],
            'pandas.core.frame.DataFrame.groupby': [
                'df.groupby(level=0).mean()',
                'df.groupby(level="Type").mean()',
                'df.groupby(by=["b"], dropna=False).sum()',
                'df.groupby(by="a", dropna=False).sum()'
            ],
            'pandas.core.frame.DataFrame.idxmax': ['*'],
            'pandas.core.frame.DataFrame.idxmin': ['*'],
            'pandas.core.frame.DataFrame.pop': ['*'],
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
            ],
            # Raises right exception, but testing framework has matching issues.
            'pandas.core.frame.DataFrame.replace': [
                "df.replace({'a string': 'new value', True: False})  # raises"
            ],
            # Should raise WontImplement order-sensitive
            'pandas.core.frame.DataFrame.set_index': [
                "df.set_index([pd.Index([1, 2, 3, 4]), 'year'])",
                "df.set_index([s, s**2])",
            ],
            'pandas.core.frame.DataFrame.to_sparse': ['type(df)'],

            # DeferredSeries has no attribute dtype. Should we allow this and
            # defer to proxy?
            'pandas.core.frame.DataFrame.iterrows': ["print(df['int'].dtype)"],

            # Skipped because "seen_wont_implement" is reset before getting to
            # these calls, so the NameError they raise is not ignored.
            'pandas.core.frame.DataFrame.T': [
                'df1_transposed.dtypes', 'df2_transposed.dtypes'
            ],
            'pandas.core.frame.DataFrame.transpose': [
                'df1_transposed.dtypes', 'df2_transposed.dtypes'
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
            'pandas.core.series.Series.cummax': ['*'],
            'pandas.core.series.Series.cummin': ['*'],
            'pandas.core.series.Series.cumsum': ['*'],
            'pandas.core.series.Series.cumprod': ['*'],
            'pandas.core.series.Series.diff': ['*'],
            'pandas.core.series.Series.dot': [
                's.dot(arr)',  # non-deferred result
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
            'pandas.core.series.Series.searchsorted': ['*'],
            'pandas.core.series.Series.shift': ['*'],
            'pandas.core.series.Series.take': ['*'],
            'pandas.core.series.Series.to_dict': ['*'],
            'pandas.core.series.Series.unique': ['*'],
            'pandas.core.series.Series.unstack': ['*'],
            'pandas.core.series.Series.values': ['*'],
            'pandas.core.series.Series.view': ['*'],
        },
        not_implemented_ok={
            'pandas.core.series.Series.reindex': ['*'],
        },
        skip={
            'pandas.core.series.Series.array': ['*'],
            'pandas.core.series.Series.append': ['*'],
            'pandas.core.series.Series.argmax': ['*'],
            'pandas.core.series.Series.argmin': ['*'],
            'pandas.core.series.Series.autocorr': ['*'],
            'pandas.core.series.Series.combine': ['*'],
            'pandas.core.series.Series.combine_first': ['*'],
            'pandas.core.series.Series.compare': ['*'],
            'pandas.core.series.Series.corr': ['*'],
            'pandas.core.series.Series.count': ['*'],
            'pandas.core.series.Series.cov': ['*'],
            'pandas.core.series.Series.drop': ['*'],
            'pandas.core.series.Series.drop_duplicates': ['*'],
            'pandas.core.series.Series.duplicated': ['*'],
            'pandas.core.series.Series.explode': ['*'],
            'pandas.core.series.Series.groupby': ['*'],
            'pandas.core.series.Series.idxmax': ['*'],
            'pandas.core.series.Series.idxmin': ['*'],
            'pandas.core.series.Series.name': ['*'],
            'pandas.core.series.Series.nonzero': ['*'],
            'pandas.core.series.Series.pop': ['*'],
            'pandas.core.series.Series.quantile': ['*'],
            'pandas.core.series.Series.rename': ['*'],
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
    result = doctests.testmod(
        pd.core.strings,
        use_beam=False,
        wont_implement_ok={
            'pandas.core.strings.StringMethods.cat': [
                "s.str.cat(['A', 'B', 'C', 'D'], sep=',')",
                "s.str.cat(['A', 'B', 'C', 'D'], sep=',', na_rep='-')",
                "s.str.cat(['A', 'B', 'C', 'D'], na_rep='-')"
            ],
        },
        skip={
            # TODO: These should probably raise WontImplement
            'pandas.core.strings.StringMethods.repeat': [
                's.str.repeat(repeats=[1, 2, 3])'
            ],
            'pandas.core.strings.str_repeat': [
                's.str.repeat(repeats=[1, 2, 3])'
            ],

            # The rest of the skipped tests represent bad test strings,
            # fixed upstream in
            # https://github.com/pandas-dev/pandas/commit/d095ac899da953d759992824592a72a1e6ff5e09
            'pandas.core.strings.StringMethods': [
                "s.str.split('_')", "s.str.replace('_', '')"
            ],
            'pandas.core.strings.str_split': ["s.str.split(expand=True)"],
            'pandas.core.strings.str_replace': [
                "pd.Series(['foo', 'fuz', np.nan]).str.replace('f', repr)"
            ],
            'pandas.core.strings.StringMethods.replace': [
                "pd.Series(['foo', 'fuz', np.nan]).str.replace('f', repr)"
            ],
            'pandas.core.strings.StringMethods.partition': [
                'idx.str.partition()'
            ],
            'pandas.core.strings.StringMethods.rpartition': [
                'idx.str.partition()'
            ],
            # rsplit/split are particularly troublesome because the first test,
            # defining a test series, is bad and must be skipped. But skipping
            # it breaks every other test. To run the rest we would need to
            # execute the first test but ignore the output.
            'pandas.core.strings.StringMethods.rsplit': ["*"],
            'pandas.core.strings.StringMethods.split': ["*"],
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


if __name__ == '__main__':
  unittest.main()
