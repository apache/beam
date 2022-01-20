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

import sys
import unittest

import pandas as pd

from apache_beam.dataframe import doctests
from apache_beam.dataframe.frames import PD_VERSION
from apache_beam.dataframe.pandas_top_level_functions import _is_top_level_function


@unittest.skipIf(sys.platform == 'win32', '[BEAM-10626]')
class DoctestTest(unittest.TestCase):
  def test_ndframe_tests(self):
    # IO methods are tested in io_test.py
    skip_writes = {
        f'pandas.core.generic.NDFrame.{name}': ['*']
        for name in dir(pd.core.generic.NDFrame) if name.startswith('to_')
    }

    result = doctests.testmod(
        pd.core.generic,
        use_beam=False,
        report=True,
        wont_implement_ok={
            'pandas.core.generic.NDFrame.head': ['*'],
            'pandas.core.generic.NDFrame.shift': [
                'df.shift(periods=3)',
                'df.shift(periods=3, fill_value=0)',
            ],
            'pandas.core.generic.NDFrame.tail': ['*'],
            'pandas.core.generic.NDFrame.take': ['*'],
            'pandas.core.generic.NDFrame.values': ['*'],
            'pandas.core.generic.NDFrame.tz_localize': [
                "s.tz_localize('CET', ambiguous='infer')",
                # np.array is not a deferred object. This use-case is possible
                # with a deferred Series though, which is tested in
                # frames_test.py
                "s.tz_localize('CET', ambiguous=np.array([True, True, False]))",
            ],
            'pandas.core.generic.NDFrame.truncate': [
                # These inputs rely on tail (wont implement, order
                # sensitive) for verification
                "df.tail()",
                "df.truncate(before=pd.Timestamp('2016-01-05'),\n"
                "            after=pd.Timestamp('2016-01-10')).tail()",
                "df.truncate('2016-01-05', '2016-01-10').tail()",
                "df.loc['2016-01-05':'2016-01-10', :].tail()"
            ],
            'pandas.core.generic.NDFrame.replace': [
                "s.replace([1, 2], method='bfill')",
                # Relies on method='pad'
                "s.replace('a', None)",
                # Implicitly uses method='pad', but output doesn't rely on that
                # behavior. Verified indepently in
                # frames_test.py::DeferredFrameTest::test_replace
                "df.replace(regex={r'^ba.$': 'new', 'foo': 'xyz'})"
            ],
            'pandas.core.generic.NDFrame.fillna': [
                'df.fillna(method=\'ffill\')',
                'df.fillna(method="ffill")',
                'df.fillna(value=values, limit=1)',
            ],
            'pandas.core.generic.NDFrame.sort_values': ['*'],
            'pandas.core.generic.NDFrame.mask': [
                'df.where(m, -df) == np.where(m, df, -df)'
            ],
            'pandas.core.generic.NDFrame.where': [
                'df.where(m, -df) == np.where(m, df, -df)'
            ],
            'pandas.core.generic.NDFrame.interpolate': ['*'],
            'pandas.core.generic.NDFrame.resample': ['*'],
            'pandas.core.generic.NDFrame.rolling': ['*'],
            # argsort wont implement
            'pandas.core.generic.NDFrame.abs': [
                'df.loc[(df.c - 43).abs().argsort()]',
            ],
            'pandas.core.generic.NDFrame.reindex': ['*'],
            'pandas.core.generic.NDFrame.pct_change': ['*'],
            'pandas.core.generic.NDFrame.asof': ['*'],
            'pandas.core.generic.NDFrame.infer_objects': ['*'],
            'pandas.core.generic.NDFrame.ewm': ['*'],
            'pandas.core.generic.NDFrame.expanding': ['*'],
        },
        not_implemented_ok={
            'pandas.core.generic.NDFrame.asof': ['*'],
            'pandas.core.generic.NDFrame.at_time': ['*'],
            'pandas.core.generic.NDFrame.between_time': ['*'],
            'pandas.core.generic.NDFrame.ewm': ['*'],
            'pandas.core.generic.NDFrame.expanding': ['*'],
            'pandas.core.generic.NDFrame.flags': ['*'],
            'pandas.core.generic.NDFrame.rank': ['*'],
            'pandas.core.generic.NDFrame.reindex_like': ['*'],
            'pandas.core.generic.NDFrame.replace': ['*'],
            'pandas.core.generic.NDFrame.sample': ['*'],
            'pandas.core.generic.NDFrame.set_flags': ['*'],
            'pandas.core.generic.NDFrame.squeeze': ['*'],
            'pandas.core.generic.NDFrame.truncate': ['*'],
        },
        skip={
            # Internal test
            'pandas.core.generic.NDFrame._set_axis_name': ['*'],
            # Fails to construct test series. asfreq is not implemented anyway.
            'pandas.core.generic.NDFrame.asfreq': ['*'],
            'pandas.core.generic.NDFrame.astype': ['*'],
            'pandas.core.generic.NDFrame.convert_dtypes': ['*'],
            'pandas.core.generic.NDFrame.copy': ['*'],
            'pandas.core.generic.NDFrame.droplevel': ['*'],
            'pandas.core.generic.NDFrame.rank': [
                # Modified dataframe
                'df'
            ],
            'pandas.core.generic.NDFrame.rename': [
                # Seems to be an upstream bug. The actual error has a different
                # message:
                #   TypeError: Index(...) must be called with a collection of
                #   some kind, 2 was passed
                # pandas doctests only verify the type of exception
                'df.rename(2)'
            ],
            # Tests rely on setting index
            'pandas.core.generic.NDFrame.rename_axis': ['*'],
            # Raises right exception, but testing framework has matching issues.
            'pandas.core.generic.NDFrame.replace': [
                "df.replace({'a string': 'new value', True: False})  # raises"
            ],
            'pandas.core.generic.NDFrame.squeeze': ['*'],

            # NameError
            'pandas.core.generic.NDFrame.resample': ['df'],

            # Skipped so we don't need to install natsort
            'pandas.core.generic.NDFrame.sort_values': [
                'from natsort import index_natsorted',
                'df.sort_values(\n'
                '   by="time",\n'
                '   key=lambda x: np.argsort(index_natsorted(df["time"]))\n'
                ')'
            ],
            **skip_writes
        })
    self.assertEqual(result.failed, 0)

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
                'df.fillna(method=\'ffill\')',
                'df.fillna(method="ffill")',
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
            'pandas.core.frame.DataFrame.replace': [
                "s.replace([1, 2], method='bfill')",
                # Relies on method='pad'
                "s.replace('a', None)",
                # Implicitly uses method='pad', but output doesn't rely on that
                # behavior. Verified indepently in
                # frames_test.py::DeferredFrameTest::test_replace
                "df.replace(regex={r'^ba.$': 'new', 'foo': 'xyz'})"
            ],
            'pandas.core.frame.DataFrame.to_records': ['*'],
            'pandas.core.frame.DataFrame.to_dict': ['*'],
            'pandas.core.frame.DataFrame.to_numpy': ['*'],
            'pandas.core.frame.DataFrame.to_string': ['*'],
            'pandas.core.frame.DataFrame.transpose': ['*'],
            'pandas.core.frame.DataFrame.shape': ['*'],
            'pandas.core.frame.DataFrame.shift': [
                'df.shift(periods=3)',
                'df.shift(periods=3, fill_value=0)',
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
            'pandas.core.frame.DataFrame.sort_index': ['*'],
            'pandas.core.frame.DataFrame.sort_values': ['*'],
            'pandas.core.frame.DataFrame.melt': [
                "df.melt(id_vars=['A'], value_vars=['B'])",
                "df.melt(id_vars=['A'], value_vars=['B', 'C'])",
                "df.melt(col_level=0, id_vars=['A'], value_vars=['B'])",
                "df.melt(id_vars=[('A', 'D')], value_vars=[('B', 'E')])",
                "df.melt(id_vars=['A'], value_vars=['B'],\n" +
                "        var_name='myVarname', value_name='myValname')"
            ],
            # Most keep= options are order-sensitive
            'pandas.core.frame.DataFrame.drop_duplicates': ['*'],
            'pandas.core.frame.DataFrame.duplicated': [
                'df.duplicated()',
                "df.duplicated(keep='last')",
                "df.duplicated(subset=['brand'])",
            ],
            'pandas.core.frame.DataFrame.reindex': ['*'],
            'pandas.core.frame.DataFrame.dot': [
                # reindex not supported
                's2 = s.reindex([1, 0, 2, 3])',
            ],
            'pandas.core.frame.DataFrame.resample': ['*'],
            'pandas.core.frame.DataFrame.values': ['*'],
        },
        not_implemented_ok={
            'pandas.core.frame.DataFrame.transform': [
                # str arg not supported. Tested with np.sum in
                # frames_test.py::DeferredFrameTest::test_groupby_transform_sum
                "df.groupby('Date')['Data'].transform('sum')",
            ],
            'pandas.core.frame.DataFrame.swaplevel': ['*'],
            'pandas.core.frame.DataFrame.melt': ['*'],
            'pandas.core.frame.DataFrame.reindex_axis': ['*'],
            'pandas.core.frame.DataFrame.round': [
                'df.round(decimals)',
            ],

            # We should be able to support pivot and pivot_table for categorical
            # columns
            'pandas.core.frame.DataFrame.pivot': ['*'],

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

            'pandas.core.frame.DataFrame.set_axis': [
                "df.set_axis(range(0,2), axis='index')",
            ],

            # TODO(BEAM-12495)
            'pandas.core.frame.DataFrame.value_counts': [
              'df.value_counts(dropna=False)'
            ],
        },
        skip={
            # s2 created with reindex
            'pandas.core.frame.DataFrame.dot': [
                'df.dot(s2)',
            ],

            'pandas.core.frame.DataFrame.resample': ['df'],
            'pandas.core.frame.DataFrame.asfreq': ['*'],
            # Throws NotImplementedError when modifying df
            'pandas.core.frame.DataFrame.axes': [
                # Returns deferred index.
                'df.axes',
            ],
            # Skipped because the relies on loc to set cells in df2
            'pandas.core.frame.DataFrame.compare': ['*'],
            'pandas.core.frame.DataFrame.cov': [
                # Relies on setting entries ahead of time.
                "df.loc[df.index[:5], 'a'] = np.nan",
                "df.loc[df.index[5:10], 'b'] = np.nan",
                'df.cov(min_periods=12)',
            ],
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
            'pandas.core.frame.DataFrame.set_axis': [
                # This should pass as set_axis(axis='columns')
                # and fail with set_axis(axis='index')
                "df.set_axis(['a', 'b', 'c'], axis='index')"
            ],
            'pandas.core.frame.DataFrame.to_markdown': ['*'],
            'pandas.core.frame.DataFrame.to_parquet': ['*'],

            # Raises right exception, but testing framework has matching issues.
            # Tested in `frames_test.py`.
            'pandas.core.frame.DataFrame.insert': [
                'df',
                'df.insert(1, "newcol", [99, 99])',
                'df.insert(0, "col1", [100, 100], allow_duplicates=True)'
            ],

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
                'df.fillna(method=\'ffill\')',
                'df.fillna(method="ffill")',
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
            'pandas.core.series.Series.shift': [
                'df.shift(periods=3)',
                'df.shift(periods=3, fill_value=0)',
            ],
            'pandas.core.series.Series.take': ['*'],
            'pandas.core.series.Series.to_dict': ['*'],
            'pandas.core.series.Series.unique': ['*'],
            'pandas.core.series.Series.unstack': ['*'],
            'pandas.core.series.Series.values': ['*'],
            'pandas.core.series.Series.view': ['*'],
            'pandas.core.series.Series.append': [
                's1.append(s2, ignore_index=True)',
            ],
            'pandas.core.series.Series.replace': [
                "s.replace([1, 2], method='bfill')",
                # Relies on method='pad'
                "s.replace('a', None)",
                # Implicitly uses method='pad', but output doesn't rely on that
                # behavior. Verified indepently in
                # frames_test.py::DeferredFrameTest::test_replace
                "df.replace(regex={r'^ba.$': 'new', 'foo': 'xyz'})"
            ],
            'pandas.core.series.Series.sort_index': ['*'],
            'pandas.core.series.Series.sort_values': ['*'],
            'pandas.core.series.Series.argmax': ['*'],
            'pandas.core.series.Series.argmin': ['*'],
            'pandas.core.series.Series.drop_duplicates': [
                's.drop_duplicates()',
                "s.drop_duplicates(keep='last')",
            ],
            'pandas.core.series.Series.reindex': ['*'],
            'pandas.core.series.Series.autocorr': ['*'],
            'pandas.core.series.Series.repeat': ['s.repeat([1, 2, 3])'],
            'pandas.core.series.Series.resample': ['*'],
            'pandas.core.series.Series': ['ser.iloc[0] = 999'],
        },
        not_implemented_ok={
            'pandas.core.series.Series.transform': [
                # str arg not supported. Tested with np.sum in
                # frames_test.py::DeferredFrameTest::test_groupby_transform_sum
                "df.groupby('Date')['Data'].transform('sum')",
            ],
            'pandas.core.series.Series.groupby': [
                'ser.groupby(["a", "b", "a", "b"]).mean()',
                'ser.groupby(["a", "b", "a", np.nan]).mean()',
                'ser.groupby(["a", "b", "a", np.nan], dropna=False).mean()',
            ],
            'pandas.core.series.Series.swaplevel' :['*']
        },
        skip={
            # Relies on setting values with iloc
            'pandas.core.series.Series': ['ser', 'r'],
            'pandas.core.series.Series.groupby': [
                # TODO(BEAM-11393): This example requires aligning two series
                # with non-unique indexes. It only works in pandas because
                # pandas can recognize the indexes are identical and elide the
                # alignment.
                'ser.groupby(ser > 100).mean()',
            ],
            'pandas.core.series.Series.asfreq': ['*'],
            # error formatting
            'pandas.core.series.Series.append': [
                's1.append(s2, verify_integrity=True)',
            ],
            'pandas.core.series.Series.cov': [
                # Differs in LSB on jenkins.
                "s1.cov(s2)",
            ],
            # Skipped idxmax/idxmin due an issue with the test framework
            'pandas.core.series.Series.idxmin': ['s.idxmin()'],
            'pandas.core.series.Series.idxmax': ['s.idxmax()'],
            'pandas.core.series.Series.duplicated': ['*'],
            'pandas.core.series.Series.set_axis': ['*'],
            'pandas.core.series.Series.nonzero': ['*'],
            'pandas.core.series.Series.pop': ['ser'],  # testing side effect
            # Raises right exception, but testing framework has matching issues.
            'pandas.core.series.Series.replace': [
                "df.replace({'a string': 'new value', True: False})  # raises"
            ],
            'pandas.core.series.Series.searchsorted': [
                # This doctest seems to be incorrectly parsed.
                "x = pd.Categorical(['apple', 'bread', 'bread',"
            ],
            'pandas.core.series.Series.to_csv': ['*'],
            'pandas.core.series.Series.to_markdown': ['*'],
            'pandas.core.series.Series.update': ['*'],
            'pandas.core.series.Series.view': [
                # Inspection after modification.
                's'
            ],
            'pandas.core.series.Series.resample': ['df'],
        })
    self.assertEqual(result.failed, 0)

  def test_string_tests(self):
    if PD_VERSION < (1, 2):
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
            f'{module_name}.StringMethods': ['s.str.split("_")'],
            f'{module_name}.StringMethods.rsplit': ['*'],
            f'{module_name}.StringMethods.split': ['*'],
        },
        skip={
            # count() on Series with a NaN produces mismatched type if we
            # have a NaN-only partition.
            f'{module_name}.StringMethods.count': ["s.str.count('a')"],
            f'{module_name}.str_count': ["s.str.count('a')"],

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
    indexes_accessors_result = doctests.testmod(
        pd.core.indexes.accessors,
        use_beam=False,
        skip={
            'pandas.core.indexes.accessors.TimedeltaProperties': [
                # Seems like an upstream bug. The property is 'second'
                'seconds_series.dt.seconds'
            ],

            # TODO(BEAM-12530): Test data creation fails for these
            #   s = pd.Series(pd.to_timedelta(np.arange(5), unit="d"))
            # pylint: disable=line-too-long
            'pandas.core.indexes.accessors.DatetimeProperties.to_pydatetime': [
                '*'
            ],
            'pandas.core.indexes.accessors.TimedeltaProperties.components': [
                '*'
            ],
            'pandas.core.indexes.accessors.TimedeltaProperties.to_pytimedelta': [
                '*'
            ],
            # pylint: enable=line-too-long
        })
    datetimelike_result = doctests.testmod(
        pd.core.arrays.datetimelike, use_beam=False)

    datetime_result = doctests.testmod(
        pd.core.arrays.datetimes,
        use_beam=False,
        wont_implement_ok={
            'pandas.core.arrays.datetimes.DatetimeArray.to_period': ['*'],
            # All tz_localize tests use unsupported values for ambiguous=
            # Verified seperately in
            # frames_test.py::DeferredFrameTest::test_dt_tz_localize_*
            'pandas.core.arrays.datetimes.DatetimeArray.tz_localize': ['*'],
        },
        not_implemented_ok={
            # Verifies index version of this method
            'pandas.core.arrays.datetimes.DatetimeArray.to_period': [
                'df.index.to_period("M")'
            ],
        })

    self.assertEqual(indexes_accessors_result.failed, 0)
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
            'pandas.core.groupby.groupby.GroupBy.resample': ['*'],
        },
        not_implemented_ok={
            'pandas.core.groupby.groupby.GroupBy.ngroup': ['*'],
            'pandas.core.groupby.groupby.GroupBy.sample': ['*'],
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
                'df.fillna(method=\'ffill\')',
                'df.fillna(method="ffill")',
                'df.fillna(value=values, limit=1)',
            ],
            'pandas.core.groupby.generic.SeriesGroupBy.fillna': [
                'df.fillna(method=\'ffill\')',
                'df.fillna(method="ffill")',
                'df.fillna(value=values, limit=1)',
            ],
        },
        not_implemented_ok={
            'pandas.core.groupby.generic.DataFrameGroupBy.idxmax': ['*'],
            'pandas.core.groupby.generic.DataFrameGroupBy.idxmin': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.transform': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.idxmax': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.idxmin': ['*'],
            'pandas.core.groupby.generic.SeriesGroupBy.apply': ['*'],
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
            'pandas.core.groupby.generic.SeriesGroupBy.transform': [
                # Dropping invalid columns during a transform is unsupported.
                'grouped.transform(lambda x: (x - x.mean()) / x.std())'
            ],
            'pandas.core.groupby.generic.DataFrameGroupBy.transform': [
                # Dropping invalid columns during a transform is unsupported.
                'grouped.transform(lambda x: (x - x.mean()) / x.std())'
            ],
            # Skipped idxmax/idxmin due an issue with the test framework
            'pandas.core.groupby.generic.SeriesGroupBy.idxmin': ['s.idxmin()'],
            'pandas.core.groupby.generic.SeriesGroupBy.idxmax': ['s.idxmax()'],
        })
    self.assertEqual(result.failed, 0)

  def test_top_level(self):
    tests = {
        name: func.__doc__
        for (name, func) in pd.__dict__.items()
        if _is_top_level_function(func) and getattr(func, '__doc__', None)
    }

    # IO methods are tested in io_test.py
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
            'wide_to_long': ['*'],
        },
        wont_implement_ok={
            'factorize': ['*'],
            'to_datetime': ['s.head()'],
            'to_pickle': ['*'],
            'melt': [
                "pd.melt(df, id_vars=['A'], value_vars=['B'])",
                "pd.melt(df, id_vars=['A'], value_vars=['B', 'C'])",
                "pd.melt(df, col_level=0, id_vars=['A'], value_vars=['B'])",
                "pd.melt(df, id_vars=[('A', 'D')], value_vars=[('B', 'E')])",
                "pd.melt(df, id_vars=['A'], value_vars=['B'],\n" +
                "        var_name='myVarname', value_name='myValname')"
            ],
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
