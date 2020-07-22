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
class DoctestTest(unittest.TestCase):
  def test_dataframe_tests(self):
    result = doctests.testmod(
        pd.core.frame,
        use_beam=False,
        skip={
            'pandas.core.frame.DataFrame.T': ['*'],
            'pandas.core.frame.DataFrame.agg': ['*'],
            'pandas.core.frame.DataFrame.aggregate': ['*'],
            'pandas.core.frame.DataFrame.all': ['*'],
            'pandas.core.frame.DataFrame.any': ['*'],
            'pandas.core.frame.DataFrame.append': ['*'],
            'pandas.core.frame.DataFrame.apply': ['*'],
            'pandas.core.frame.DataFrame.applymap': ['*'],
            'pandas.core.frame.DataFrame.assign': ['*'],
            'pandas.core.frame.DataFrame.axes': ['*'],
            'pandas.core.frame.DataFrame.combine': ['*'],
            'pandas.core.frame.DataFrame.combine_first': ['*'],
            'pandas.core.frame.DataFrame.corr': ['*'],
            'pandas.core.frame.DataFrame.count': ['*'],
            'pandas.core.frame.DataFrame.cov': ['*'],
            'pandas.core.frame.DataFrame.cummax': ['*'],
            'pandas.core.frame.DataFrame.cummin': ['*'],
            'pandas.core.frame.DataFrame.cumprod': ['*'],
            'pandas.core.frame.DataFrame.cumsum': ['*'],
            'pandas.core.frame.DataFrame.diff': ['*'],
            'pandas.core.frame.DataFrame.dot': ['*'],
            'pandas.core.frame.DataFrame.drop': ['*'],
            'pandas.core.frame.DataFrame.dropna': ['*'],
            'pandas.core.frame.DataFrame.eval': ['*'],
            'pandas.core.frame.DataFrame.explode': ['*'],
            'pandas.core.frame.DataFrame.fillna': ['*'],
            'pandas.core.frame.DataFrame.info': ['*'],
            'pandas.core.frame.DataFrame.isin': ['*'],
            'pandas.core.frame.DataFrame.isna': ['*'],
            'pandas.core.frame.DataFrame.isnull': ['*'],
            'pandas.core.frame.DataFrame.items': ['*'],
            'pandas.core.frame.DataFrame.iteritems': ['*'],
            'pandas.core.frame.DataFrame.iterrows': ['*'],
            'pandas.core.frame.DataFrame.itertuples': ['*'],
            'pandas.core.frame.DataFrame.join': ['*'],
            'pandas.core.frame.DataFrame.max': ['*'],
            'pandas.core.frame.DataFrame.melt': ['*'],
            'pandas.core.frame.DataFrame.memory_usage': ['*'],
            'pandas.core.frame.DataFrame.merge': ['*'],
            'pandas.core.frame.DataFrame.min': ['*'],
            'pandas.core.frame.DataFrame.mode': ['*'],
            'pandas.core.frame.DataFrame.nlargest': ['*'],
            'pandas.core.frame.DataFrame.notna': ['*'],
            'pandas.core.frame.DataFrame.notnull': ['*'],
            'pandas.core.frame.DataFrame.nsmallest': ['*'],
            'pandas.core.frame.DataFrame.nunique': ['*'],
            'pandas.core.frame.DataFrame.pivot': ['*'],
            'pandas.core.frame.DataFrame.pivot_table': ['*'],
            'pandas.core.frame.DataFrame.prod': ['*'],
            'pandas.core.frame.DataFrame.product': ['*'],
            'pandas.core.frame.DataFrame.quantile': ['*'],
            'pandas.core.frame.DataFrame.query': ['*'],
            'pandas.core.frame.DataFrame.reindex': ['*'],
            'pandas.core.frame.DataFrame.reindex_axis': ['*'],
            'pandas.core.frame.DataFrame.rename': ['*'],
            'pandas.core.frame.DataFrame.replace': ['*'],
            'pandas.core.frame.DataFrame.reset_index': ['*'],
            'pandas.core.frame.DataFrame.round': ['*'],
            'pandas.core.frame.DataFrame.select_dtypes': ['*'],
            'pandas.core.frame.DataFrame.set_index': ['*'],
            'pandas.core.frame.DataFrame.shape': ['*'],
            'pandas.core.frame.DataFrame.shift': ['*'],
            'pandas.core.frame.DataFrame.sort_values': ['*'],
            'pandas.core.frame.DataFrame.stack': ['*'],
            'pandas.core.frame.DataFrame.sum': ['*'],
            'pandas.core.frame.DataFrame.to_dict': ['*'],
            'pandas.core.frame.DataFrame.to_numpy': ['*'],
            'pandas.core.frame.DataFrame.to_records': ['*'],
            'pandas.core.frame.DataFrame.to_sparse': ['*'],
            'pandas.core.frame.DataFrame.to_string': ['*'],
            'pandas.core.frame.DataFrame.transform': ['*'],
            'pandas.core.frame.DataFrame.transpose': ['*'],
            'pandas.core.frame.DataFrame.unstack': ['*'],
            'pandas.core.frame.DataFrame.update': ['*'],
        })
    self.assertEqual(result.failed, 0)

  def test_series_tests(self):
    result = doctests.testmod(
        pd.core.series,
        use_beam=False,
        skip={
            'pandas.core.series.Series.agg': ['*'],
            'pandas.core.series.Series.aggregate': ['*'],
            'pandas.core.series.Series.all': ['*'],
            'pandas.core.series.Series.any': ['*'],
            'pandas.core.series.Series.append': ['*'],
            'pandas.core.series.Series.argmax': ['*'],
            'pandas.core.series.Series.argmin': ['*'],
            'pandas.core.series.Series.autocorr': ['*'],
            'pandas.core.series.Series.between': ['*'],
            'pandas.core.series.Series.combine': ['*'],
            'pandas.core.series.Series.combine_first': ['*'],
            'pandas.core.series.Series.corr': ['*'],
            'pandas.core.series.Series.count': ['*'],
            'pandas.core.series.Series.cov': ['*'],
            'pandas.core.series.Series.cummax': ['*'],
            'pandas.core.series.Series.cummin': ['*'],
            'pandas.core.series.Series.cumprod': ['*'],
            'pandas.core.series.Series.cumsum': ['*'],
            'pandas.core.series.Series.diff': ['*'],
            'pandas.core.series.Series.dot': ['*'],
            'pandas.core.series.Series.drop': ['*'],
            'pandas.core.series.Series.drop_duplicates': ['*'],
            'pandas.core.series.Series.dropna': ['*'],
            'pandas.core.series.Series.duplicated': ['*'],
            'pandas.core.series.Series.explode': ['*'],
            'pandas.core.series.Series.fillna': ['*'],
            'pandas.core.series.Series.idxmax': ['*'],
            'pandas.core.series.Series.idxmin': ['*'],
            'pandas.core.series.Series.isin': ['*'],
            'pandas.core.series.Series.isna': ['*'],
            'pandas.core.series.Series.isnull': ['*'],
            'pandas.core.series.Series.items': ['*'],
            'pandas.core.series.Series.iteritems': ['*'],
            'pandas.core.series.Series.max': ['*'],
            'pandas.core.series.Series.memory_usage': ['*'],
            'pandas.core.series.Series.min': ['*'],
            'pandas.core.series.Series.nlargest': ['*'],
            'pandas.core.series.Series.nonzero': ['*'],
            'pandas.core.series.Series.notna': ['*'],
            'pandas.core.series.Series.notnull': ['*'],
            'pandas.core.series.Series.nsmallest': ['*'],
            'pandas.core.series.Series.prod': ['*'],
            'pandas.core.series.Series.product': ['*'],
            'pandas.core.series.Series.quantile': ['*'],
            'pandas.core.series.Series.reindex': ['*'],
            'pandas.core.series.Series.rename': ['*'],
            'pandas.core.series.Series.repeat': ['*'],
            'pandas.core.series.Series.replace': ['*'],
            'pandas.core.series.Series.reset_index': ['*'],
            'pandas.core.series.Series.round': ['*'],
            'pandas.core.series.Series.searchsorted': ['*'],
            'pandas.core.series.Series.shift': ['*'],
            'pandas.core.series.Series.sort_index': ['*'],
            'pandas.core.series.Series.sort_values': ['*'],
            'pandas.core.series.Series.sum': ['*'],
            'pandas.core.series.Series.take': ['*'],
            'pandas.core.series.Series.to_csv': ['*'],
            'pandas.core.series.Series.to_dict': ['*'],
            'pandas.core.series.Series.to_frame': ['*'],
            'pandas.core.series.Series.transform': ['*'],
            'pandas.core.series.Series.unique': ['*'],
            'pandas.core.series.Series.unstack': ['*'],
            'pandas.core.series.Series.update': ['*'],
            'pandas.core.series.Series.values': ['*'],
            'pandas.core.series.Series.view': ['*'],
        })
    self.assertEqual(result.failed, 0)


if __name__ == '__main__':
  unittest.main()
