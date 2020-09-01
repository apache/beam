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
            'pandas.core.frame.DataFrame.nlargest': [
                "df.nlargest(3, 'population', keep='last')"
            ],
            'pandas.core.frame.DataFrame.nsmallest': [
                "df.nsmallest(3, 'population', keep='last')"
            ],
            'pandas.core.frame.DataFrame.nunique': ['*'],
            'pandas.core.frame.DataFrame.to_records': ['*'],
            'pandas.core.frame.DataFrame.to_dict': ['*'],
            'pandas.core.frame.DataFrame.to_numpy': ['*'],
            'pandas.core.frame.DataFrame.to_string': ['*'],
            'pandas.core.frame.DataFrame.transpose': ['*'],
            'pandas.core.frame.DataFrame.shape': ['*'],
            'pandas.core.frame.DataFrame.unstack': ['*'],
            'pandas.core.frame.DataFrame.memory_usage': ['*'],
        },
        skip={
            'pandas.core.frame.DataFrame.T': [
                'df1_transposed.dtypes', 'df2_transposed.dtypes'
            ],
            'pandas.core.frame.DataFrame.agg': ['*'],
            'pandas.core.frame.DataFrame.aggregate': ['*'],
            'pandas.core.frame.DataFrame.append': ['*'],
            'pandas.core.frame.DataFrame.apply': ['*'],
            'pandas.core.frame.DataFrame.applymap': ['df ** 2'],
            'pandas.core.frame.DataFrame.assign': ['*'],
            'pandas.core.frame.DataFrame.axes': ['*'],
            'pandas.core.frame.DataFrame.combine': ['*'],
            'pandas.core.frame.DataFrame.combine_first': ['*'],
            'pandas.core.frame.DataFrame.corr': ['*'],
            'pandas.core.frame.DataFrame.count': ['*'],
            'pandas.core.frame.DataFrame.cov': ['*'],
            'pandas.core.frame.DataFrame.dot': ['*'],
            'pandas.core.frame.DataFrame.drop': ['*'],
            'pandas.core.frame.DataFrame.eval': ['*'],
            'pandas.core.frame.DataFrame.explode': ['*'],
            'pandas.core.frame.DataFrame.info': ['*'],
            'pandas.core.frame.DataFrame.isin': ['*'],
            'pandas.core.frame.DataFrame.iterrows': ["print(df['int'].dtype)"],
            'pandas.core.frame.DataFrame.melt': ['*'],
            'pandas.core.frame.DataFrame.memory_usage': ['*'],
            'pandas.core.frame.DataFrame.merge': [
                # Order-sensitive index, checked in frames_test.py.
                "df1.merge(df2, left_on='lkey', right_on='rkey')",
                "df1.merge(df2, left_on='lkey', right_on='rkey',\n"
                "          suffixes=('_left', '_right'))",
            ],
            # Not equal to df.agg('mode', axis='columns', numeric_only=True)
            'pandas.core.frame.DataFrame.mode': [
                "df.mode(axis='columns', numeric_only=True)"
            ],
            'pandas.core.frame.DataFrame.pivot': ['*'],
            'pandas.core.frame.DataFrame.pivot_table': ['*'],
            'pandas.core.frame.DataFrame.query': ['*'],
            'pandas.core.frame.DataFrame.reindex': ['*'],
            # Sets df.index
            'pandas.core.frame.DataFrame.reindex_axis': ['*'],
            'pandas.core.frame.DataFrame.rename': ['*'],
            # Raises right exception, but testing framework has matching issues.
            'pandas.core.frame.DataFrame.replace': [
                "df.replace({'a string': 'new value', True: False})  # raises"
            ],
            # Uses unseeded np.random.
            'pandas.core.frame.DataFrame.round': ['*'],
            'pandas.core.frame.DataFrame.set_index': ['*'],
            'pandas.core.frame.DataFrame.transpose': [
                'df1_transposed.dtypes', 'df2_transposed.dtypes'
            ],
            'pandas.core.frame.DataFrame.to_sparse': ['type(df)'],
            # Uses df.index
            'pandas.core.frame.DataFrame.to_records': ['*'],
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
            'pandas.core.series.Series.items': ['*'],
            'pandas.core.series.Series.iteritems': ['*'],
            'pandas.core.series.Series.nlargest': [
                "s.nlargest(3, keep='last')"
            ],
            'pandas.core.series.Series.nsmallest': [
                "s.nsmallest(3, keep='last')"
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
        skip={
            'pandas.core.series.Series.append': ['*'],
            'pandas.core.series.Series.argmax': ['*'],
            'pandas.core.series.Series.argmin': ['*'],
            'pandas.core.series.Series.autocorr': ['*'],
            'pandas.core.series.Series.combine': ['*'],
            'pandas.core.series.Series.combine_first': ['*'],
            'pandas.core.series.Series.corr': ['*'],
            'pandas.core.series.Series.count': ['*'],
            'pandas.core.series.Series.cov': ['*'],
            'pandas.core.series.Series.dot': ['*'],
            'pandas.core.series.Series.drop': ['*'],
            'pandas.core.series.Series.drop_duplicates': ['*'],
            'pandas.core.series.Series.duplicated': ['*'],
            'pandas.core.series.Series.explode': ['*'],
            'pandas.core.series.Series.idxmax': ['*'],
            'pandas.core.series.Series.idxmin': ['*'],
            'pandas.core.series.Series.memory_usage': ['*'],
            'pandas.core.series.Series.nonzero': ['*'],
            'pandas.core.series.Series.quantile': ['*'],
            'pandas.core.series.Series.reindex': ['*'],
            'pandas.core.series.Series.rename': ['*'],
            'pandas.core.series.Series.repeat': ['*'],
            'pandas.core.series.Series.replace': ['*'],
            'pandas.core.series.Series.reset_index': ['*'],
            'pandas.core.series.Series.searchsorted': [
                # This doctest seems to be incorrectly parsed.
                "x = pd.Categorical(['apple', 'bread', 'bread',"
            ],
            'pandas.core.series.Series.sort_index': ['*'],
            'pandas.core.series.Series.sort_values': ['*'],
            'pandas.core.series.Series.to_csv': ['*'],
            'pandas.core.series.Series.view': [
                # Inspection after modification.
                's'
            ],
        })
    self.assertEqual(result.failed, 0)

  def test_string_tests(self):
    # TODO(BEAM-10720)
    result = doctests.testmod(
        pd.core.strings,
        use_beam=False,
        skip={
            'pandas.core.strings.StringMethods': ['*'],
            'pandas.core.strings.StringMethods.capitalize': ['*'],
            'pandas.core.strings.StringMethods.casefold': ['*'],
            'pandas.core.strings.StringMethods.cat': ['*'],
            'pandas.core.strings.StringMethods.contains': ['*'],
            'pandas.core.strings.StringMethods.count': ['*'],
            'pandas.core.strings.StringMethods.endswith': ['*'],
            'pandas.core.strings.StringMethods.extract': ['*'],
            'pandas.core.strings.StringMethods.extractall': ['*'],
            'pandas.core.strings.StringMethods.findall': ['*'],
            'pandas.core.strings.StringMethods.get': ['*'],
            'pandas.core.strings.StringMethods.get_dummies': ['*'],
            'pandas.core.strings.StringMethods.isalnum': ['*'],
            'pandas.core.strings.StringMethods.isalpha': ['*'],
            'pandas.core.strings.StringMethods.isdecimal': ['*'],
            'pandas.core.strings.StringMethods.isdigit': ['*'],
            'pandas.core.strings.StringMethods.islower': ['*'],
            'pandas.core.strings.StringMethods.isnumeric': ['*'],
            'pandas.core.strings.StringMethods.isspace': ['*'],
            'pandas.core.strings.StringMethods.istitle': ['*'],
            'pandas.core.strings.StringMethods.isupper': ['*'],
            'pandas.core.strings.StringMethods.join': ['*'],
            'pandas.core.strings.StringMethods.len': ['*'],
            'pandas.core.strings.StringMethods.lower': ['*'],
            'pandas.core.strings.StringMethods.lstrip': ['*'],
            'pandas.core.strings.StringMethods.pad': ['*'],
            'pandas.core.strings.StringMethods.partition': ['*'],
            'pandas.core.strings.StringMethods.repeat': ['*'],
            'pandas.core.strings.StringMethods.replace': ['*'],
            'pandas.core.strings.StringMethods.rpartition': ['*'],
            'pandas.core.strings.StringMethods.rsplit': ['*'],
            'pandas.core.strings.StringMethods.rstrip': ['*'],
            'pandas.core.strings.StringMethods.slice': ['*'],
            'pandas.core.strings.StringMethods.slice_replace': ['*'],
            'pandas.core.strings.StringMethods.split': ['*'],
            'pandas.core.strings.StringMethods.startswith': ['*'],
            'pandas.core.strings.StringMethods.strip': ['*'],
            'pandas.core.strings.StringMethods.swapcase': ['*'],
            'pandas.core.strings.StringMethods.title': ['*'],
            'pandas.core.strings.StringMethods.upper': ['*'],
            'pandas.core.strings.StringMethods.wrap': ['*'],
            'pandas.core.strings.StringMethods.zfill': ['*'],
            'pandas.core.strings.str_contains': ['*'],
            'pandas.core.strings.str_count': ['*'],
            'pandas.core.strings.str_endswith': ['*'],
            'pandas.core.strings.str_extract': ['*'],
            'pandas.core.strings.str_extractall': ['*'],
            'pandas.core.strings.str_findall': ['*'],
            'pandas.core.strings.str_get': ['*'],
            'pandas.core.strings.str_get_dummies': ['*'],
            'pandas.core.strings.str_join': ['*'],
            'pandas.core.strings.str_pad': ['*'],
            'pandas.core.strings.str_repeat': ['*'],
            'pandas.core.strings.str_replace': ['*'],
            'pandas.core.strings.str_slice': ['*'],
            'pandas.core.strings.str_slice_replace': ['*'],
            'pandas.core.strings.str_startswith': ['*'],
            'pandas.core.strings.str_wrap': ['*'],
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


if __name__ == '__main__':
  unittest.main()
