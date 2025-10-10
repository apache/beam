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
#
# pytype: skip-file

import glob
import json
import logging
import os
import re
import shutil
import tempfile
import unittest
from datetime import datetime
from tempfile import TemporaryDirectory

import hamcrest as hc
import pandas
import pytest
import pytz
from parameterized import param
from parameterized import parameterized

import apache_beam as beam
from apache_beam import Create
from apache_beam import Map
from apache_beam.io import filebasedsource
from apache_beam.io import source_test_utils
from apache_beam.io.iobase import RangeTracker
from apache_beam.io.parquetio import ReadAllFromParquet
from apache_beam.io.parquetio import ReadAllFromParquetBatched
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.io.parquetio import ReadFromParquetBatched
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io.parquetio import WriteToParquetBatched
from apache_beam.io.parquetio import _create_parquet_sink
from apache_beam.io.parquetio import _create_parquet_source
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam.transforms.util import LogElements

try:
  import pyarrow as pa
  import pyarrow.parquet as pq
  ARROW_MAJOR_VERSION, _, _ = map(int, pa.__version__.split('.'))
except ImportError:
  pa = None
  pq = None
  ARROW_MAJOR_VERSION = 0


@unittest.skipIf(pa is None, "PyArrow is not installed.")
@pytest.mark.uses_pyarrow
class TestParquet(unittest.TestCase):
  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2
    self.temp_dir = tempfile.mkdtemp()

    self.RECORDS = [{
        'name': 'Thomas', 'favorite_number': 1, 'favorite_color': 'blue'
    },
                    {
                        'name': 'Henry',
                        'favorite_number': 3,
                        'favorite_color': 'green'
                    },
                    {
                        'name': 'Toby',
                        'favorite_number': 7,
                        'favorite_color': 'brown'
                    },
                    {
                        'name': 'Gordon',
                        'favorite_number': 4,
                        'favorite_color': 'blue'
                    },
                    {
                        'name': 'Emily',
                        'favorite_number': -1,
                        'favorite_color': 'Red'
                    },
                    {
                        'name': 'Percy',
                        'favorite_number': 6,
                        'favorite_color': 'Green'
                    },
                    {
                        'name': 'Peter',
                        'favorite_number': 3,
                        'favorite_color': None
                    }]

    self.SCHEMA = pa.schema([('name', pa.string(), False),
                             ('favorite_number', pa.int64(), False),
                             ('favorite_color', pa.string())])

    self.SCHEMA96 = pa.schema([('name', pa.string(), False),
                               ('favorite_number', pa.timestamp('ns'), False),
                               ('favorite_color', pa.string())])

    self.RECORDS_NESTED = [{
        'items': [
            {
                'name': 'Thomas',
                'favorite_number': 1,
                'favorite_color': 'blue'
            },
            {
                'name': 'Henry',
                'favorite_number': 3,
                'favorite_color': 'green'
            },
        ]
    },
                           {
                               'items': [
                                   {
                                       'name': 'Toby',
                                       'favorite_number': 7,
                                       'favorite_color': 'brown'
                                   },
                               ]
                           }]

    self.SCHEMA_NESTED = pa.schema([(
        'items',
        pa.list_(
            pa.struct([('name', pa.string(), False),
                       ('favorite_number', pa.int64(), False),
                       ('favorite_color', pa.string())])))])

  def tearDown(self):
    shutil.rmtree(self.temp_dir)

  def _record_to_columns(self, records, schema):
    col_list = []
    for n in schema.names:
      column = []
      for r in records:
        column.append(r[n])

      col_list.append(column)
    return col_list

  def _records_as_arrow(self, schema=None, count=None):
    if schema is None:
      schema = self.SCHEMA

    if count is None:
      count = len(self.RECORDS)

    len_records = len(self.RECORDS)
    data = []
    for i in range(count):
      data.append(self.RECORDS[i % len_records])
    col_data = self._record_to_columns(data, schema)
    col_array = [pa.array(c, schema.types[cn]) for cn, c in enumerate(col_data)]
    return pa.Table.from_arrays(col_array, schema=schema)

  def _write_data(
      self,
      directory=None,
      schema=None,
      prefix=tempfile.template,
      row_group_size=1000,
      codec='none',
      count=None):
    if directory is None:
      directory = self.temp_dir

    with tempfile.NamedTemporaryFile(delete=False, dir=directory,
                                     prefix=prefix) as f:
      table = self._records_as_arrow(schema, count)
      pq.write_table(
          table,
          f,
          row_group_size=row_group_size,
          compression=codec,
          use_deprecated_int96_timestamps=True)

      return f.name

  def _write_pattern(self, num_files, with_filename=False):
    assert num_files > 0
    temp_dir = tempfile.mkdtemp(dir=self.temp_dir)

    file_list = []
    for _ in range(num_files):
      file_list.append(self._write_data(directory=temp_dir, prefix='mytemp'))

    if with_filename:
      return (temp_dir + os.path.sep + 'mytemp*', file_list)
    return temp_dir + os.path.sep + 'mytemp*'

  def _run_parquet_test(
      self,
      pattern,
      columns,
      desired_bundle_size,
      perform_splitting,
      expected_result):
    source = _create_parquet_source(pattern, columns=columns)
    if perform_splitting:
      assert desired_bundle_size
      sources_info = [
          (split.source, split.start_position, split.stop_position)
          for split in source.split(desired_bundle_size=desired_bundle_size)
      ]
      if len(sources_info) < 2:
        raise ValueError(
            'Test is trivial. Please adjust it so that at least '
            'two splits get generated')

      source_test_utils.assert_sources_equal_reference_source(
          (source, None, None), sources_info)
    else:
      read_records = source_test_utils.read_from_source(source, None, None)
      self.assertCountEqual(expected_result, read_records)

  def test_read_without_splitting(self):
    file_name = self._write_data()
    expected_result = [self._records_as_arrow()]
    self._run_parquet_test(file_name, None, None, False, expected_result)

  def test_read_with_splitting(self):
    file_name = self._write_data()
    expected_result = [self._records_as_arrow()]
    self._run_parquet_test(file_name, None, 100, True, expected_result)

  def test_source_display_data(self):
    file_name = 'some_parquet_source'
    source = \
        _create_parquet_source(
            file_name,
            validate=False
        )
    dd = DisplayData.create_from(source)

    expected_items = [
        DisplayDataItemMatcher('compression', 'auto'),
        DisplayDataItemMatcher('file_pattern', file_name)
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_read_display_data(self):
    file_name = 'some_parquet_source'
    read = \
      ReadFromParquet(
          file_name,
          validate=False)
    read_batched = \
      ReadFromParquetBatched(
          file_name,
          validate=False)

    expected_items = [
        DisplayDataItemMatcher('compression', 'auto'),
        DisplayDataItemMatcher('file_pattern', file_name)
    ]

    hc.assert_that(
        DisplayData.create_from(read).items,
        hc.contains_inanyorder(*expected_items))
    hc.assert_that(
        DisplayData.create_from(read_batched).items,
        hc.contains_inanyorder(*expected_items))

  def test_sink_display_data(self):
    file_name = 'some_parquet_sink'
    sink = _create_parquet_sink(
        file_name,
        self.SCHEMA,
        'none',
        False,
        False,
        '.end',
        0,
        None,
        'application/x-parquet')
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher('schema', str(self.SCHEMA)),
        DisplayDataItemMatcher(
            'file_pattern',
            'some_parquet_sink-%(shard_num)05d-of-%(num_shards)05d.end'),
        DisplayDataItemMatcher('codec', 'none'),
        DisplayDataItemMatcher('compression', 'uncompressed')
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_write_display_data(self):
    file_name = 'some_parquet_sink'
    write = WriteToParquet(file_name, self.SCHEMA)
    dd = DisplayData.create_from(write)

    expected_items = [
        DisplayDataItemMatcher('codec', 'none'),
        DisplayDataItemMatcher('schema', str(self.SCHEMA)),
        DisplayDataItemMatcher('row_group_buffer_size', str(64 * 1024 * 1024)),
        DisplayDataItemMatcher(
            'file_pattern',
            'some_parquet_sink-%(shard_num)05d-of-%(num_shards)05d'),
        DisplayDataItemMatcher('compression', 'uncompressed')
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_write_batched_display_data(self):
    file_name = 'some_parquet_sink'
    write = WriteToParquetBatched(file_name, self.SCHEMA)
    dd = DisplayData.create_from(write)

    expected_items = [
        DisplayDataItemMatcher('codec', 'none'),
        DisplayDataItemMatcher('schema', str(self.SCHEMA)),
        DisplayDataItemMatcher(
            'file_pattern',
            'some_parquet_sink-%(shard_num)05d-of-%(num_shards)05d'),
        DisplayDataItemMatcher('compression', 'uncompressed')
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  @unittest.skipIf(
      ARROW_MAJOR_VERSION >= 13,
      'pyarrow 13.x and above does not throw ArrowInvalid error')
  def test_sink_transform_int96(self):
    with self.assertRaisesRegex(Exception, 'would lose data'):
      # Should throw an error "ArrowInvalid: Casting from timestamp[ns] to
      # timestamp[us] would lose data"
      dst = tempfile.NamedTemporaryFile()
      path = dst.name
      with TestPipeline() as p:
        _ = p \
        | Create(self.RECORDS) \
        | WriteToParquet(
            path, self.SCHEMA96, num_shards=1, shard_name_template='')

  def test_sink_transform(self):
    with TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname + "tmp_filename")
      with TestPipeline() as p:
        _ = p \
        | Create(self.RECORDS) \
        | WriteToParquet(
            path, self.SCHEMA, num_shards=1, shard_name_template='')
      with TestPipeline() as p:
        # json used for stable sortability
        readback = \
            p \
            | ReadFromParquet(path) \
            | Map(json.dumps)
        assert_that(readback, equal_to([json.dumps(r) for r in self.RECORDS]))

  def test_sink_transform_batched(self):
    with TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname + "tmp_filename")
      with TestPipeline() as p:
        _ = p \
        | Create([self._records_as_arrow()]) \
        | WriteToParquetBatched(
            path, self.SCHEMA, num_shards=1, shard_name_template='')
      with TestPipeline() as p:
        # json used for stable sortability
        readback = \
            p \
            | ReadFromParquet(path) \
            | Map(json.dumps)
        assert_that(readback, equal_to([json.dumps(r) for r in self.RECORDS]))

  def test_sink_transform_compliant_nested_type(self):
    if ARROW_MAJOR_VERSION < 4:
      return unittest.skip(
          'Writing with compliant nested type is only '
          'supported in pyarrow 4.x and above')
    with TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname + 'tmp_filename')
      with TestPipeline() as p:
        _ = p \
        | Create(self.RECORDS_NESTED) \
        | WriteToParquet(
            path, self.SCHEMA_NESTED, num_shards=1,
            shard_name_template='', use_compliant_nested_type=True)
      with TestPipeline() as p:
        # json used for stable sortability
        readback = \
            p \
            | ReadFromParquet(path) \
            | Map(json.dumps)
        assert_that(
            readback, equal_to([json.dumps(r) for r in self.RECORDS_NESTED]))

  def test_schema_read_write(self):
    with TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname, 'tmp_filename')
      rows = [beam.Row(a=1, b='x'), beam.Row(a=2, b='y')]
      stable_repr = lambda row: json.dumps(row._asdict())
      with TestPipeline() as p:
        _ = p | Create(rows) | WriteToParquet(path)
      with TestPipeline() as p:
        readback = (
            p
            | ReadFromParquet(path + '*', as_rows=True)
            | Map(stable_repr))
        assert_that(readback, equal_to([stable_repr(r) for r in rows]))

  def test_write_with_nullable_fields_missing_data(self):
    """Test WriteToParquet with nullable fields where some fields are missing.
    
    This test addresses the bug reported in:
    https://github.com/apache/beam/issues/35791
    where WriteToParquet fails with a KeyError if any nullable 
    field is missing in the data.
    """
    # Define PyArrow schema with all fields nullable
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("age", pa.int64(), nullable=True),
        pa.field("email", pa.string(), nullable=True),
    ])

    # Sample data with missing nullable fields
    data = [
        {
            'id': 1, 'name': 'Alice', 'age': 30
        },  # missing 'email'
        {
            'id': 2, 'name': 'Bob', 'age': 25, 'email': 'bob@example.com'
        },  # all fields present
        {
            'id': 3, 'name': 'Charlie', 'age': None, 'email': None
        },  # explicit None values
        {
            'id': 4, 'name': 'David'
        },  # missing 'age' and 'email'
    ]

    with TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname, 'nullable_test')

      # Write data with missing nullable fields - this should not raise KeyError
      with TestPipeline() as p:
        _ = (
            p
            | Create(data)
            | WriteToParquet(
                path, schema, num_shards=1, shard_name_template=''))

      # Read back and verify the data
      with TestPipeline() as p:
        readback = (
            p
            | ReadFromParquet(path + '*')
            | Map(json.dumps, sort_keys=True))

        # Expected data should have None for missing nullable fields
        expected_data = [
            {
                'id': 1, 'name': 'Alice', 'age': 30, 'email': None
            },
            {
                'id': 2, 'name': 'Bob', 'age': 25, 'email': 'bob@example.com'
            },
            {
                'id': 3, 'name': 'Charlie', 'age': None, 'email': None
            },
            {
                'id': 4, 'name': 'David', 'age': None, 'email': None
            },
        ]

        assert_that(
            readback,
            equal_to([json.dumps(r, sort_keys=True) for r in expected_data]))

  def test_batched_read(self):
    with TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname + "tmp_filename")
      with TestPipeline() as p:
        _ = p \
        | Create(self.RECORDS, reshuffle=False) \
        | WriteToParquet(
            path, self.SCHEMA, num_shards=1, shard_name_template='')
      with TestPipeline() as p:
        # json used for stable sortability
        readback = \
            p \
            | ReadFromParquetBatched(path)
        assert_that(readback, equal_to([self._records_as_arrow()]))

  @parameterized.expand([
      param(compression_type='snappy'),
      param(compression_type='gzip'),
      param(compression_type='brotli'),
      param(compression_type='lz4'),
      param(compression_type='zstd')
  ])
  def test_sink_transform_compressed(self, compression_type):
    if compression_type == 'lz4' and ARROW_MAJOR_VERSION == 1:
      return unittest.skip(
          "Writing with LZ4 compression is not supported in "
          "pyarrow 1.x")
    with TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname + "tmp_filename")
      with TestPipeline() as p:
        _ = p \
        | Create(self.RECORDS) \
        | WriteToParquet(
            path, self.SCHEMA, codec=compression_type,
            num_shards=1, shard_name_template='')
      with TestPipeline() as p:
        # json used for stable sortability
        readback = \
            p \
            | ReadFromParquet(path + '*') \
            | Map(json.dumps)
        assert_that(readback, equal_to([json.dumps(r) for r in self.RECORDS]))

  def test_read_reentrant(self):
    file_name = self._write_data(count=6, row_group_size=3)
    source = _create_parquet_source(file_name)
    source_test_utils.assert_reentrant_reads_succeed((source, None, None))

  def test_read_without_splitting_multiple_row_group(self):
    file_name = self._write_data(count=12000, row_group_size=1000)
    # We expect 12000 elements, split into batches of 1000 elements. Create
    # a list of pa.Table instances to model this expecation
    expected_result = [
        pa.Table.from_batches([batch]) for batch in self._records_as_arrow(
            count=12000).to_batches(max_chunksize=1000)
    ]
    self._run_parquet_test(file_name, None, None, False, expected_result)

  def test_read_with_splitting_multiple_row_group(self):
    file_name = self._write_data(count=12000, row_group_size=1000)
    # We expect 12000 elements, split into batches of 1000 elements. Create
    # a list of pa.Table instances to model this expecation
    expected_result = [
        pa.Table.from_batches([batch]) for batch in self._records_as_arrow(
            count=12000).to_batches(max_chunksize=1000)
    ]
    self._run_parquet_test(file_name, None, 10000, True, expected_result)

  def test_dynamic_work_rebalancing(self):
    # This test depends on count being sufficiently large + the ratio of
    # count to row_group_size also being sufficiently large (but the required
    # ratio to pass varies for values of row_group_size and, somehow, the
    # version of pyarrow being tested against.)
    file_name = self._write_data(count=280, row_group_size=20)
    source = _create_parquet_source(file_name)

    splits = [split for split in source.split(desired_bundle_size=float('inf'))]
    assert len(splits) == 1

    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position)

  def test_min_bundle_size(self):
    file_name = self._write_data(count=120, row_group_size=20)

    source = _create_parquet_source(
        file_name, min_bundle_size=100 * 1024 * 1024)
    splits = [split for split in source.split(desired_bundle_size=1)]
    self.assertEqual(len(splits), 1)

    source = _create_parquet_source(file_name, min_bundle_size=0)
    splits = [split for split in source.split(desired_bundle_size=1)]
    self.assertNotEqual(len(splits), 1)

  def _convert_to_timestamped_record(self, record):
    timestamped_record = record.copy()
    timestamped_record['favorite_number'] =\
      pandas.Timestamp(timestamped_record['favorite_number'])
    return timestamped_record

  def test_int96_type_conversion(self):
    file_name = self._write_data(
        count=120, row_group_size=20, schema=self.SCHEMA96)
    orig = self._records_as_arrow(count=120, schema=self.SCHEMA96)
    expected_result = [
        pa.Table.from_batches([batch], schema=self.SCHEMA96)
        for batch in orig.to_batches(max_chunksize=20)
    ]
    self._run_parquet_test(file_name, None, None, False, expected_result)

  def test_split_points(self):
    file_name = self._write_data(count=12000, row_group_size=3000)
    source = _create_parquet_source(file_name)

    splits = [split for split in source.split(desired_bundle_size=float('inf'))]
    assert len(splits) == 1

    range_tracker = splits[0].source.get_range_tracker(
        splits[0].start_position, splits[0].stop_position)

    split_points_report = []

    for _ in splits[0].source.read(range_tracker):
      split_points_report.append(range_tracker.split_points())

    # There are a total of four row groups. Each row group has 3000 records.

    # When reading records of the first group, range_tracker.split_points()
    # should return (0, iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)
    self.assertEqual(
        split_points_report,
        [
            (0, RangeTracker.SPLIT_POINTS_UNKNOWN),
            (1, RangeTracker.SPLIT_POINTS_UNKNOWN),
            (2, RangeTracker.SPLIT_POINTS_UNKNOWN),
            (3, 1),
        ])

  def test_selective_columns(self):
    file_name = self._write_data()
    orig = self._records_as_arrow()
    name_column = self.SCHEMA.field('name')
    expected_result = [
        pa.Table.from_arrays(
            [orig.column('name')],
            schema=pa.schema([('name', name_column.type, name_column.nullable)
                              ]))
    ]
    self._run_parquet_test(file_name, ['name'], None, False, expected_result)

  def test_sink_transform_multiple_row_group(self):
    with TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname + "tmp_filename")
      # Pin to FnApiRunner since test assumes fixed bundle size
      with TestPipeline('FnApiRunner') as p:
        # writing 623200 bytes of data
        _ = p \
        | Create(self.RECORDS * 4000) \
        | WriteToParquet(
            path, self.SCHEMA, num_shards=1, codec='none',
            shard_name_template='', row_group_buffer_size=250000)
      self.assertEqual(pq.read_metadata(path).num_row_groups, 3)

  def test_read_all_from_parquet_single_file(self):
    path = self._write_data()
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([path]) \
          | ReadAllFromParquet(),
          equal_to(self.RECORDS))

    with TestPipeline() as p:
      assert_that(
          p \
          | Create([path]) \
          | ReadAllFromParquetBatched(),
          equal_to([self._records_as_arrow()]))

  def test_read_all_from_parquet_many_single_files(self):
    path1 = self._write_data()
    path2 = self._write_data()
    path3 = self._write_data()
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([path1, path2, path3]) \
          | ReadAllFromParquet(),
          equal_to(self.RECORDS * 3))
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([path1, path2, path3]) \
          | ReadAllFromParquetBatched(),
          equal_to([self._records_as_arrow()] * 3))

  def test_read_all_from_parquet_file_pattern(self):
    file_pattern = self._write_pattern(5)
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([file_pattern]) \
          | ReadAllFromParquet(),
          equal_to(self.RECORDS * 5))
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([file_pattern]) \
          | ReadAllFromParquetBatched(),
          equal_to([self._records_as_arrow()] * 5))

  def test_read_all_from_parquet_many_file_patterns(self):
    file_pattern1 = self._write_pattern(5)
    file_pattern2 = self._write_pattern(2)
    file_pattern3 = self._write_pattern(3)
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([file_pattern1, file_pattern2, file_pattern3]) \
          | ReadAllFromParquet(),
          equal_to(self.RECORDS * 10))
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([file_pattern1, file_pattern2, file_pattern3]) \
          | ReadAllFromParquetBatched(),
          equal_to([self._records_as_arrow()] * 10))

  def test_read_all_from_parquet_with_filename(self):
    file_pattern, file_paths = self._write_pattern(3, with_filename=True)
    result = [(path, record) for path in file_paths for record in self.RECORDS]
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([file_pattern]) \
          | ReadAllFromParquet(with_filename=True),
          equal_to(result))


class GenerateEvent(beam.PTransform):
  @staticmethod
  def sample_data():
    return GenerateEvent()

  def expand(self, input):
    elemlist = [{'age': 10}, {'age': 20}, {'age': 30}]
    elem = elemlist
    return (
        input
        | TestStream().add_elements(
            elements=elem,
            event_timestamp=datetime(
                2021, 3, 1, 0, 0, 1, 0,
                tzinfo=pytz.UTC).timestamp()).add_elements(
                    elements=elem,
                    event_timestamp=datetime(
                        2021, 3, 1, 0, 0, 2, 0,
                        tzinfo=pytz.UTC).timestamp()).add_elements(
                            elements=elem,
                            event_timestamp=datetime(
                                2021, 3, 1, 0, 0, 3, 0,
                                tzinfo=pytz.UTC).timestamp()).add_elements(
                                    elements=elem,
                                    event_timestamp=datetime(
                                        2021, 3, 1, 0, 0, 4, 0,
                                        tzinfo=pytz.UTC).timestamp()).
        advance_watermark_to(
            datetime(2021, 3, 1, 0, 0, 5, 0,
                     tzinfo=pytz.UTC).timestamp()).add_elements(
                         elements=elem,
                         event_timestamp=datetime(
                             2021, 3, 1, 0, 0, 5, 0,
                             tzinfo=pytz.UTC).timestamp()).
        add_elements(
            elements=elem,
            event_timestamp=datetime(
                2021, 3, 1, 0, 0, 6,
                0, tzinfo=pytz.UTC).timestamp()).add_elements(
                    elements=elem,
                    event_timestamp=datetime(
                        2021, 3, 1, 0, 0, 7, 0,
                        tzinfo=pytz.UTC).timestamp()).add_elements(
                            elements=elem,
                            event_timestamp=datetime(
                                2021, 3, 1, 0, 0, 8, 0,
                                tzinfo=pytz.UTC).timestamp()).add_elements(
                                    elements=elem,
                                    event_timestamp=datetime(
                                        2021, 3, 1, 0, 0, 9, 0,
                                        tzinfo=pytz.UTC).timestamp()).
        advance_watermark_to(
            datetime(2021, 3, 1, 0, 0, 10, 0,
                     tzinfo=pytz.UTC).timestamp()).add_elements(
                         elements=elem,
                         event_timestamp=datetime(
                             2021, 3, 1, 0, 0, 10, 0,
                             tzinfo=pytz.UTC).timestamp()).add_elements(
                                 elements=elem,
                                 event_timestamp=datetime(
                                     2021, 3, 1, 0, 0, 11, 0,
                                     tzinfo=pytz.UTC).timestamp()).
        add_elements(
            elements=elem,
            event_timestamp=datetime(
                2021, 3, 1, 0, 0, 12, 0,
                tzinfo=pytz.UTC).timestamp()).add_elements(
                    elements=elem,
                    event_timestamp=datetime(
                        2021, 3, 1, 0, 0, 13, 0,
                        tzinfo=pytz.UTC).timestamp()).add_elements(
                            elements=elem,
                            event_timestamp=datetime(
                                2021, 3, 1, 0, 0, 14, 0,
                                tzinfo=pytz.UTC).timestamp()).
        advance_watermark_to(
            datetime(2021, 3, 1, 0, 0, 15, 0,
                     tzinfo=pytz.UTC).timestamp()).add_elements(
                         elements=elem,
                         event_timestamp=datetime(
                             2021, 3, 1, 0, 0, 15, 0,
                             tzinfo=pytz.UTC).timestamp()).add_elements(
                                 elements=elem,
                                 event_timestamp=datetime(
                                     2021, 3, 1, 0, 0, 16, 0,
                                     tzinfo=pytz.UTC).timestamp()).
        add_elements(
            elements=elem,
            event_timestamp=datetime(
                2021, 3, 1, 0, 0, 17, 0,
                tzinfo=pytz.UTC).timestamp()).add_elements(
                    elements=elem,
                    event_timestamp=datetime(
                        2021, 3, 1, 0, 0, 18, 0,
                        tzinfo=pytz.UTC).timestamp()).add_elements(
                            elements=elem,
                            event_timestamp=datetime(
                                2021, 3, 1, 0, 0, 19, 0,
                                tzinfo=pytz.UTC).timestamp()).
        advance_watermark_to(
            datetime(2021, 3, 1, 0, 0, 20, 0,
                     tzinfo=pytz.UTC).timestamp()).add_elements(
                         elements=elem,
                         event_timestamp=datetime(
                             2021, 3, 1, 0, 0, 20, 0,
                             tzinfo=pytz.UTC).timestamp()).advance_watermark_to(
                                 datetime(
                                     2021, 3, 1, 0, 0, 25, 0, tzinfo=pytz.UTC).
                                 timestamp()).advance_watermark_to_infinity())


class WriteStreamingTest(unittest.TestCase):
  def setUp(self):
    super().setUp()
    self.tempdir = tempfile.mkdtemp()

  def tearDown(self):
    if os.path.exists(self.tempdir):
      shutil.rmtree(self.tempdir)

  def test_write_streaming_2_shards_default_shard_name_template(
      self, num_shards=2):
    with TestPipeline() as p:
      output = (p | GenerateEvent.sample_data())
      #ParquetIO
      pyschema = pa.schema([('age', pa.int64())])
      output2 = output | 'WriteToParquet' >> beam.io.WriteToParquet(
          file_path_prefix=self.tempdir + "/ouput_WriteToParquet",
          file_name_suffix=".parquet",
          num_shards=num_shards,
          triggering_frequency=60,
          schema=pyschema)
      _ = output2 | 'LogElements after WriteToParquet' >> LogElements(
          prefix='after WriteToParquet ', with_window=True, level=logging.INFO)

    # Regex to match the expected windowed file pattern
    # Example:
    # ouput_WriteToParquet-[1614556800.0, 1614556805.0)-00000-of-00002.parquet
    # It captures: window_interval, shard_num, total_shards
    pattern_string = (
        r'.*-\[(?P<window_start>[\d\.]+), '
        r'(?P<window_end>[\d\.]+|Infinity)\)-'
        r'(?P<shard_num>\d{5})-of-(?P<total_shards>\d{5})\.parquet$')
    pattern = re.compile(pattern_string)
    file_names = []
    for file_name in glob.glob(self.tempdir + '/ouput_WriteToParquet*'):
      match = pattern.match(file_name)
      self.assertIsNotNone(
          match, f"File name {file_name} did not match expected pattern.")
      if match:
        file_names.append(file_name)
    print("Found files matching expected pattern:", file_names)
    self.assertEqual(
        len(file_names),
        num_shards,
        "expected %d files, but got: %d" % (num_shards, len(file_names)))

  def test_write_streaming_2_shards_custom_shard_name_template(
      self, num_shards=2, shard_name_template='-V-SSSSS-of-NNNNN'):
    with TestPipeline() as p:
      output = (p | GenerateEvent.sample_data())
      #ParquetIO
      pyschema = pa.schema([('age', pa.int64())])
      output2 = output | 'WriteToParquet' >> beam.io.WriteToParquet(
          file_path_prefix=self.tempdir + "/ouput_WriteToParquet",
          file_name_suffix=".parquet",
          shard_name_template=shard_name_template,
          num_shards=num_shards,
          triggering_frequency=60,
          schema=pyschema)
      _ = output2 | 'LogElements after WriteToParquet' >> LogElements(
          prefix='after WriteToParquet ', with_window=True, level=logging.INFO)

    # Regex to match the expected windowed file pattern
    # Example:
    # ouput_WriteToParquet-[2021-03-01T00-00-00, 2021-03-01T00-01-00)-
    #   00000-of-00002.parquet
    # It captures: window_interval, shard_num, total_shards
    pattern_string = (
        r'.*-\[(?P<window_start>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}), '
        r'(?P<window_end>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}|Infinity)\)-'
        r'(?P<shard_num>\d{5})-of-(?P<total_shards>\d{5})\.parquet$')
    pattern = re.compile(pattern_string)
    file_names = []
    for file_name in glob.glob(self.tempdir + '/ouput_WriteToParquet*'):
      match = pattern.match(file_name)
      self.assertIsNotNone(
          match, f"File name {file_name} did not match expected pattern.")
      if match:
        file_names.append(file_name)
    print("Found files matching expected pattern:", file_names)
    self.assertEqual(
        len(file_names),
        num_shards,
        "expected %d files, but got: %d" % (num_shards, len(file_names)))

  def test_write_streaming_2_shards_custom_shard_name_template_5s_window(
      self,
      num_shards=2,
      shard_name_template='-V-SSSSS-of-NNNNN',
      triggering_frequency=5):
    with TestPipeline() as p:
      output = (p | GenerateEvent.sample_data())
      #ParquetIO
      pyschema = pa.schema([('age', pa.int64())])
      output2 = output | 'WriteToParquet' >> beam.io.WriteToParquet(
          file_path_prefix=self.tempdir + "/ouput_WriteToParquet",
          file_name_suffix=".parquet",
          shard_name_template=shard_name_template,
          num_shards=num_shards,
          triggering_frequency=triggering_frequency,
          schema=pyschema)
      _ = output2 | 'LogElements after WriteToParquet' >> LogElements(
          prefix='after WriteToParquet ', with_window=True, level=logging.INFO)

    # Regex to match the expected windowed file pattern
    # Example:
    # ouput_WriteToParquet-[2021-03-01T00-00-00, 2021-03-01T00-01-00)-
    #   00000-of-00002.parquet
    # It captures: window_interval, shard_num, total_shards
    pattern_string = (
        r'.*-\[(?P<window_start>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}), '
        r'(?P<window_end>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}|Infinity)\)-'
        r'(?P<shard_num>\d{5})-of-(?P<total_shards>\d{5})\.parquet$')
    pattern = re.compile(pattern_string)
    file_names = []
    for file_name in glob.glob(self.tempdir + '/ouput_WriteToParquet*'):
      match = pattern.match(file_name)
      self.assertIsNotNone(
          match, f"File name {file_name} did not match expected pattern.")
      if match:
        file_names.append(file_name)
    print("Found files matching expected pattern:", file_names)
    # for 5s window size, the input should be processed by 5 windows with
    # 2 shards per window
    self.assertEqual(
        len(file_names),
        10,
        "expected %d files, but got: %d" % (num_shards, len(file_names)))

  def test_write_streaming_undef_shards_default_shard_name_template_windowed_pcoll(  # pylint: disable=line-too-long
      self):
    with TestPipeline() as p:
      output = (
          p | GenerateEvent.sample_data()
          | 'User windowing' >> beam.transforms.core.WindowInto(
              beam.transforms.window.FixedWindows(10),
              trigger=beam.transforms.trigger.AfterWatermark(),
              accumulation_mode=beam.transforms.trigger.AccumulationMode.
              DISCARDING,
              allowed_lateness=beam.utils.timestamp.Duration(seconds=0)))
      #ParquetIO
      pyschema = pa.schema([('age', pa.int64())])
      output2 = output | 'WriteToParquet' >> beam.io.WriteToParquet(
          file_path_prefix=self.tempdir + "/ouput_WriteToParquet",
          file_name_suffix=".parquet",
          num_shards=0,
          schema=pyschema)
      _ = output2 | 'LogElements after WriteToParquet' >> LogElements(
          prefix='after WriteToParquet ', with_window=True, level=logging.INFO)

    # Regex to match the expected windowed file pattern
    # Example:
    # ouput_WriteToParquet-[1614556800.0, 1614556805.0)-00000-of-00002.parquet
    # It captures: window_interval, shard_num, total_shards
    pattern_string = (
        r'.*-\[(?P<window_start>[\d\.]+), '
        r'(?P<window_end>[\d\.]+|Infinity)\)-'
        r'(?P<shard_num>\d{5})-of-(?P<total_shards>\d{5})\.parquet$')
    pattern = re.compile(pattern_string)
    file_names = []
    for file_name in glob.glob(self.tempdir + '/ouput_WriteToParquet*'):
      match = pattern.match(file_name)
      self.assertIsNotNone(
          match, f"File name {file_name} did not match expected pattern.")
      if match:
        file_names.append(file_name)
    print("Found files matching expected pattern:", file_names)
    self.assertGreaterEqual(
        len(file_names),
        1 * 3,  #25s of data covered by 3 10s windows
        "expected %d files, but got: %d" % (1 * 3, len(file_names)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
