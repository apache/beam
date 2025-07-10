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

import glob
import importlib
import math
import os
import platform
import shutil
import tempfile
import typing
import unittest
from datetime import datetime
from io import BytesIO
from io import StringIO

import mock
import pandas as pd
import pandas.testing
import pyarrow
import pytest
from pandas.testing import assert_frame_equal
from parameterized import parameterized

import apache_beam as beam
import apache_beam.io.gcp.bigquery
from apache_beam.dataframe import convert
from apache_beam.dataframe import io
from apache_beam.io import fileio
from apache_beam.io import restriction_trackers
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None

# Get major, minor version
PD_VERSION = tuple(map(int, pd.__version__.split('.')[0:2]))
PYARROW_VERSION = tuple(map(int, pyarrow.__version__.split('.')[0:2]))


class SimpleRow(typing.NamedTuple):
  value: int


class MyRow(typing.NamedTuple):
  timestamp: int
  value: int


@unittest.skipIf(
    platform.system() == 'Windows',
    'https://github.com/apache/beam/issues/20642')
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

  def read_all_lines(self, pattern, delete=False):
    for path in glob.glob(pattern):
      with open(path) as fin:
        # TODO(Py3): yield from
        for line in fin:
          yield line.rstrip('\n')
      if delete:
        os.remove(path)

  def test_read_fwf(self):
    input = self.temp_dir(
        {'all.fwf': '''
A     B
11a   0
37a   1
389a  2
    '''.strip()})
    with beam.Pipeline() as p:
      df = p | io.read_fwf(input + 'all.fwf')
      rows = convert.to_pcollection(df) | beam.Map(tuple)
      assert_that(rows, equal_to([('11a', 0), ('37a', 1), ('389a', 2)]))

  def test_read_write_csv(self):
    input = self.temp_dir({'1.csv': 'a,b\n1,2\n', '2.csv': 'a,b\n3,4\n'})
    output = self.temp_dir()
    with beam.Pipeline() as p:
      df = p | io.read_csv(input + '*.csv')
      df['c'] = df.a + df.b
      df.to_csv(output + 'out.csv', index=False)
    self.assertCountEqual(['a,b,c', '1,2,3', '3,4,7'],
                          set(self.read_all_lines(output + 'out.csv*')))

  def test_wide_csv_with_dtypes(self):
    # Verify https://github.com/apache/beam/issues/31152 is resolved.
    cols = ','.join(f'col{ix}' for ix in range(123))
    data = ','.join(str(ix) for ix in range(123))
    input = self.temp_dir({'tmp.csv': f'{cols}\n{data}'})
    with beam.Pipeline() as p:
      pcoll = p | beam.io.ReadFromCsv(f'{input}tmp.csv', dtype=str)
      assert_that(pcoll | beam.Map(max), equal_to(['99']))

  def test_sharding_parameters(self):
    data = pd.DataFrame({'label': ['11a', '37a', '389a'], 'rank': [0, 1, 2]})
    output = self.temp_dir()
    with beam.Pipeline() as p:
      df = convert.to_dataframe(p | beam.Create([data]), proxy=data[:0])
      df.to_csv(
          output,
          num_shards=1,
          file_naming=fileio.single_file_naming('out.csv'))
    self.assertEqual(glob.glob(output + '*'), [output + 'out.csv'])

  @pytest.mark.uses_pyarrow
  @unittest.skipIf(
      PD_VERSION >= (1, 4) and PYARROW_VERSION < (1, 0),
      "pandas 1.4 requires at least pyarrow 1.0.1")
  def test_read_write_parquet(self):
    self._run_read_write_test(
        'parquet', {}, {}, dict(check_index=False), ['pyarrow'])

  @parameterized.expand([
      ('csv', dict(index_col=0)),
      ('csv', dict(index_col=0, splittable=True)),
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
  ])
  # pylint: disable=dangerous-default-value
  def test_read_write(
      self,
      format,
      read_kwargs={},
      write_kwargs={},
      check_options={},
      requires=()):
    self._run_read_write_test(
        format, read_kwargs, write_kwargs, check_options, requires)

  # pylint: disable=dangerous-default-value
  def _run_read_write_test(
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

  def _run_truncating_file_handle_test(
      self, s, splits, delim=' ', chunk_size=10):
    split_results = []
    next_range = restriction_trackers.OffsetRange(0, len(s))
    for split in list(splits) + [None]:
      tracker = restriction_trackers.OffsetRestrictionTracker(next_range)
      handle = io._TruncatingFileHandle(
          StringIO(s), tracker, splitter=io._DelimSplitter(delim, chunk_size))
      data = ''
      chunk = handle.read(1)
      if split is not None:
        _, next_range = tracker.try_split(split)
      while chunk:
        data += chunk
        chunk = handle.read(7)
      split_results.append(data)
    return split_results

  def test_truncating_filehandle(self):
    self.assertEqual(
        self._run_truncating_file_handle_test('a b c d e', [0.5]),
        ['a b c ', 'd e'])
    self.assertEqual(
        self._run_truncating_file_handle_test('aaaaaaaaaaaaaaXaaa b', [0.5]),
        ['aaaaaaaaaaaaaaXaaa ', 'b'])
    self.assertEqual(
        self._run_truncating_file_handle_test(
            'aa bbbbbbbbbbbbbbbbbbbbbbbbbb ccc ', [0.01, 0.5]),
        ['aa ', 'bbbbbbbbbbbbbbbbbbbbbbbbbb ', 'ccc '])

    numbers = 'x'.join(str(k) for k in range(1000))
    splits = self._run_truncating_file_handle_test(
        numbers, [0.1] * 20, delim='x')
    self.assertEqual(numbers, ''.join(splits))
    self.assertTrue(s.endswith('x') for s in splits[:-1])
    self.assertLess(max(len(s) for s in splits), len(numbers) * 0.9 + 10)
    self.assertGreater(
        min(len(s) for s in splits), len(numbers) * 0.9**20 * 0.1)

  def _run_truncating_file_handle_iter_test(self, s, delim=' ', chunk_size=10):
    tracker = restriction_trackers.OffsetRestrictionTracker(
        restriction_trackers.OffsetRange(0, len(s)))
    handle = io._TruncatingFileHandle(
        StringIO(s), tracker, splitter=io._DelimSplitter(delim, chunk_size))
    self.assertEqual(s, ''.join(list(handle)))

  def test_truncating_filehandle_iter(self):
    self._run_truncating_file_handle_iter_test('a b c')
    self._run_truncating_file_handle_iter_test('aaaaaaaaaaaaaaaaaaaa b ccc')
    self._run_truncating_file_handle_iter_test('aaa b cccccccccccccccccccc')
    self._run_truncating_file_handle_iter_test('aaa b ccccccccccccccccc ')

  @parameterized.expand([
      ('defaults', {}),
      ('header', dict(header=1)),
      ('multi_header', dict(header=[0, 1])),
      ('multi_header', dict(header=[0, 1, 4])),
      ('names', dict(names=('m', 'n', 'o'))),
      ('names_and_header', dict(names=('m', 'n', 'o'), header=0)),
      ('skip_blank_lines', dict(header=4, skip_blank_lines=True)),
      ('skip_blank_lines', dict(header=4, skip_blank_lines=False)),
      ('comment', dict(comment='X', header=4)),
      ('comment', dict(comment='X', header=[0, 3])),
      ('skiprows', dict(skiprows=0, header=[0, 1])),
      ('skiprows', dict(skiprows=[1], header=[0, 3], skip_blank_lines=False)),
      ('skiprows', dict(skiprows=[0, 1], header=[0, 1], comment='X')),
  ])
  def test_csv_splitter(self, name, kwargs):
    def assert_frame_equal(expected, actual):
      try:
        pandas.testing.assert_frame_equal(expected, actual)
      except AssertionError:
        print("Expected:\n", expected)
        print("Actual:\n", actual)
        raise

    def read_truncated_csv(start, stop):
      return pd.read_csv(
          io._TruncatingFileHandle(
              BytesIO(contents.encode('ascii')),
              restriction_trackers.OffsetRestrictionTracker(
                  restriction_trackers.OffsetRange(start, stop)),
              splitter=io._TextFileSplitter((), kwargs, read_chunk_size=7)),
          index_col=0,
          **kwargs)

    contents = '''
    a0, a1, a2
    b0, b1, b2

X     , c1, c2
    e0, e1, e2
    f0, f1, f2
    w, daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaata, w
    x, daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaata, x
    y, daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaata, y
    z, daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaata, z
    '''.strip()
    expected = pd.read_csv(StringIO(contents), index_col=0, **kwargs)

    one_shard = read_truncated_csv(0, len(contents))
    assert_frame_equal(expected, one_shard)

    equal_shards = pd.concat([
        read_truncated_csv(0, len(contents) // 2),
        read_truncated_csv(len(contents) // 2, len(contents)),
    ])
    assert_frame_equal(expected, equal_shards)

    three_shards = pd.concat([
        read_truncated_csv(0, len(contents) // 3),
        read_truncated_csv(len(contents) // 3, len(contents) * 2 // 3),
        read_truncated_csv(len(contents) * 2 // 3, len(contents)),
    ])
    assert_frame_equal(expected, three_shards)

    # https://github.com/pandas-dev/pandas/issues/38292
    if not isinstance(kwargs.get('header'), list):
      split_in_header = pd.concat([
          read_truncated_csv(0, 1),
          read_truncated_csv(1, len(contents)),
      ])
      assert_frame_equal(expected, split_in_header)

    if not kwargs:
      # Make sure we're correct as we cross the header boundary.
      # We don't need to do this for every permutation.
      header_end = contents.index('a2') + 3
      for split in range(header_end - 2, header_end + 2):
        split_at_header = pd.concat([
            read_truncated_csv(0, split),
            read_truncated_csv(split, len(contents)),
        ])
        assert_frame_equal(expected, split_at_header)

  def test_file_not_found(self):
    with self.assertRaisesRegex(FileNotFoundError, r'/tmp/fake_dir/\*\*'):
      with beam.Pipeline() as p:
        _ = p | io.read_csv('/tmp/fake_dir/**')

  def test_windowed_write(self):
    output = self.temp_dir()
    with beam.Pipeline() as p:
      pc = (
          p | beam.Create([MyRow(timestamp=i, value=i % 3) for i in range(20)])
          | beam.Map(lambda v: beam.window.TimestampedValue(v, v.timestamp)).
          with_output_types(MyRow)
          | beam.WindowInto(
              beam.window.FixedWindows(10)).with_output_types(MyRow))

      deferred_df = convert.to_dataframe(pc)
      deferred_df.to_csv(output + 'out.csv', index=False)

    first_window_files = (
        f'{output}out.csv-'
        f'{datetime.utcfromtimestamp(0).isoformat()}*')
    self.assertCountEqual(
        ['timestamp,value'] + [f'{i},{i % 3}' for i in range(10)],
        set(self.read_all_lines(first_window_files, delete=True)))

    second_window_files = (
        f'{output}out.csv-'
        f'{datetime.utcfromtimestamp(10).isoformat()}*')
    self.assertCountEqual(
        ['timestamp,value'] + [f'{i},{i%3}' for i in range(10, 20)],
        set(self.read_all_lines(second_window_files, delete=True)))

    # Check that we've read (and removed) every output file
    self.assertEqual(len(glob.glob(f'{output}out.csv*')), 0)

  def test_double_write(self):
    output = self.temp_dir()
    with beam.Pipeline() as p:
      pc1 = p | 'create pc1' >> beam.Create(
          [SimpleRow(value=i) for i in [1, 2]])
      pc2 = p | 'create pc2' >> beam.Create(
          [SimpleRow(value=i) for i in [3, 4]])

      deferred_df1 = convert.to_dataframe(pc1)
      deferred_df2 = convert.to_dataframe(pc2)

      deferred_df1.to_csv(
          f'{output}out1.csv',
          transform_label="Writing to csv PC1",
          index=False)
      deferred_df2.to_csv(
          f'{output}out2.csv',
          transform_label="Writing to csv PC2",
          index=False)

    self.assertCountEqual(['value', '1', '2'],
                          set(self.read_all_lines(output + 'out1.csv*')))
    self.assertCountEqual(['value', '3', '4'],
                          set(self.read_all_lines(output + 'out2.csv*')))


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class ReadGbqTransformTests(unittest.TestCase):
  @mock.patch.object(BigQueryWrapper, 'get_table')
  def test_bad_schema_public_api_direct_read(self, get_table):
    try:
      bigquery.TableFieldSchema
    except AttributeError:
      raise ValueError('Please install GCP Dependencies.')
    fields = [
        bigquery.TableFieldSchema(name='stn', type='DOUBLE', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode=None)
    ]
    schema = bigquery.TableSchema(fields=fields)
    table = apache_beam.io.gcp.internal.clients.bigquery. \
        bigquery_v2_messages.Table(
        schema=schema)
    get_table.return_value = table

    with self.assertRaisesRegex(ValueError,
                                "Encountered an unsupported type: 'DOUBLE'"):
      p = apache_beam.Pipeline()
      _ = p | apache_beam.dataframe.io.read_gbq(
          table="dataset.sample_table", use_bqstorage_api=True)

  def test_unsupported_callable(self):
    def filterTable(table):
      if table is not None:
        return table

    res = filterTable
    with self.assertRaisesRegex(TypeError,
                                'ReadFromBigQuery: table must be of type string'
                                '; got a callable instead'):
      p = beam.Pipeline()
      _ = p | beam.dataframe.io.read_gbq(table=res)

  def test_ReadGbq_unsupported_param(self):
    with self.assertRaisesRegex(ValueError,
                                r"""Encountered unsupported parameter\(s\) """
                                r"""in read_gbq: dict_keys\(\['reauth']\)"""):
      p = beam.Pipeline()
      _ = p | beam.dataframe.io.read_gbq(
          table="table", use_bqstorage_api=False, reauth="true_config")


if __name__ == '__main__':
  unittest.main()
