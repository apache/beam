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
from __future__ import print_function

import glob
import importlib
import math
import os
import platform
import shutil
import sys
import tempfile
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal
from parameterized import parameterized

import apache_beam as beam
from apache_beam.dataframe import convert
from apache_beam.dataframe import io
from apache_beam.testing.util import assert_that


@unittest.skipIf(platform.system() == 'Windows', 'BEAM-10929')
class IOTest(unittest.TestCase):
  def setUp(self):
    self._temp_roots = []

  def tearDown(self):
    for root in self._temp_roots:
      shutil.rmtree(root)

  def temp_dir(self, files=None):
    dir = tempfile.mkdtemp(prefix='beam-test')
    self._temp_roots.append(dir)
    if files:
      for name, contents in files.items():
        with open(os.path.join(dir, name), 'w') as fout:
          fout.write(contents)
    return dir + os.path.sep

  def read_all_lines(self, pattern):
    for path in glob.glob(pattern):
      with open(path) as fin:
        # TODO(Py3): yield from
        for line in fin:
          yield line.rstrip('\n')

  @unittest.skipIf(sys.version_info[0] < 3, 'unicode issues')
  def test_read_write_csv(self):
    input = self.temp_dir({'1.csv': 'a,b\n1,2\n', '2.csv': 'a,b\n3,4\n'})
    output = self.temp_dir()
    with beam.Pipeline() as p:
      df = p | io.read_csv(input + '*.csv')
      df['c'] = df.a + df.b
      df.to_csv(output + 'out.csv', index=False)
    self.assertCountEqual(['a,b,c', '1,2,3', '3,4,7'],
                          set(self.read_all_lines(output + 'out.csv*')))

  @parameterized.expand([
      ('csv', dict(index_col=0)),
      ('json', dict(orient='index'), dict(orient='index')),
      ('json', dict(orient='columns'), dict(orient='columns')),
      ('json', dict(orient='split'), dict(orient='split')),
      (
          'json',
          dict(orient='values'),
          dict(orient='values'),
          dict(check_index=False, check_names=False)),
      (
          'json',
          dict(orient='records'),
          dict(orient='records'),
          dict(check_index=False)),
      (
          'json',
          dict(orient='records', lines=True),
          dict(orient='records', lines=True),
          dict(check_index=False)),
      ('html', dict(index_col=0), {}, {}, ['lxml']),
      ('excel', dict(index_col=0), {}, {}, ['openpyxl', 'xlrd']),
      ('parquet', {}, {}, dict(check_index=False), ['pyarrow']),
  ])
  # pylint: disable=dangerous-default-value
  def test_read_write(
      self,
      format,
      read_kwargs={},
      write_kwargs={},
      check_options={},
      requires=()):
    for module in requires:
      try:
        importlib.import_module(module)
      except ImportError:
        raise unittest.SkipTest('Missing dependency: %s' % module)
    small = pd.DataFrame({'label': ['11a', '37a', '389a'], 'rank': [0, 1, 2]})
    big = pd.DataFrame({'number': list(range(1000))})
    big['float'] = big.number.map(math.sqrt)
    big['text'] = big.number.map(lambda n: 'f' + 'o' * n)

    def frame_equal_to(expected_, check_index=True, check_names=True):
      def check(actual):
        expected = expected_
        try:
          actual = pd.concat(actual)
          if not check_index:
            expected = expected.sort_values(list(
                expected.columns)).reset_index(drop=True)
            actual = actual.sort_values(list(
                actual.columns)).reset_index(drop=True)
          if not check_names:
            actual = actual.rename(
                columns=dict(zip(actual.columns, expected.columns)))
          return assert_frame_equal(expected, actual, check_like=True)
        except:
          print("EXPECTED")
          print(expected)
          print("ACTUAL")
          print(actual)
          raise

      return check

    for df in (small, big):
      with tempfile.TemporaryDirectory() as dir:
        dest = os.path.join(dir, 'out')
        try:
          with beam.Pipeline() as p:
            deferred_df = convert.to_dataframe(
                p | beam.Create([df[::3], df[1::3], df[2::3]]), proxy=df[:0])
            # This does the write.
            getattr(deferred_df, 'to_%s' % format)(dest, **write_kwargs)
          with beam.Pipeline() as p:
            # Now do the read.
            # TODO(robertwb): Allow reading from pcoll of paths to do it all in
            # one pipeline.

            result = convert.to_pcollection(
                p | getattr(io, 'read_%s' % format)(dest + '*', **read_kwargs),
                yield_elements='pandas')
            assert_that(result, frame_equal_to(df, **check_options))
        except:
          os.system('head -n 100 ' + dest + '*')
          raise


if __name__ == '__main__':
  unittest.main()
