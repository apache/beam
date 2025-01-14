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

import json
import logging
import math
import os
import pytest
import tempfile
import unittest
from typing import List, Any

import fastavro
import hamcrest as hc

from fastavro.schema import parse_schema
from fastavro import writer

import apache_beam as beam
from apache_beam import Create, schema_pb2
from apache_beam.io import avroio
from apache_beam.io import filebasedsource
from apache_beam.io import iobase
from apache_beam.io import source_test_utils
from apache_beam.io.avroio import _FastAvroSource  # For testing
from apache_beam.io.avroio import avro_schema_to_beam_schema  # For testing
from apache_beam.io.avroio import beam_schema_to_avro_schema  # For testing
from apache_beam.io.avroio import avro_atomic_value_to_beam_atomic_value  # For testing
from apache_beam.io.avroio import avro_union_type_to_beam_type  # For testing
from apache_beam.io.avroio import beam_atomic_value_to_avro_atomic_value  # For testing
from apache_beam.io.avroio import avro_dict_to_beam_row  # For testing
from apache_beam.io.avroio import beam_row_to_avro_dict  # For testing
from apache_beam.io.avroio import _create_avro_sink  # For testing
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam.transforms.sql import SqlTransform
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.utils.timestamp import Timestamp
from apache_beam.typehints import schemas

# Import snappy optionally; some tests will be skipped when import fails.
try:
  import snappy  # pylint: disable=import-error
except ImportError:
  snappy = None  # pylint: disable=invalid-name
  logging.warning('python-snappy is not installed; some tests will be skipped.')

RECORDS = [{
    'name': 'Thomas', 'favorite_number': 1, 'favorite_color': 'blue'
}, {
    'name': 'Henry', 'favorite_number': 3, 'favorite_color': 'green'
}, {
    'name': 'Toby', 'favorite_number': 7, 'favorite_color': 'brown'
}, {
    'name': 'Gordon', 'favorite_number': 4, 'favorite_color': 'blue'
}, {
    'name': 'Emily', 'favorite_number': -1, 'favorite_color': 'Red'
}, {
    'name': 'Percy', 'favorite_number': 6, 'favorite_color': 'Green'
}]


class AvroBase(object):

  _temp_files: List[str] = []

  def __init__(self, methodName='runTest'):
    super().__init__(methodName)
    self.RECORDS = RECORDS
    self.SCHEMA_STRING = '''
          {"namespace": "example.avro",
           "type": "record",
           "name": "User",
           "fields": [
               {"name": "name", "type": "string"},
               {"name": "favorite_number",  "type": ["int", "null"]},
               {"name": "favorite_color", "type": ["string", "null"]}
           ]
          }
          '''

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  def tearDown(self):
    for path in self._temp_files:
      if os.path.exists(path):
        os.remove(path)
    self._temp_files = []

  def _write_data(
      self,
      directory=None,
      prefix=None,
      codec=None,
      count=None,
      sync_interval=None):
    raise NotImplementedError

  def _write_pattern(self, num_files, return_filenames=False):
    assert num_files > 0
    temp_dir = tempfile.mkdtemp()

    file_name = None
    file_list = []
    for _ in range(num_files):
      file_name = self._write_data(directory=temp_dir, prefix='mytemp')
      file_list.append(file_name)

    assert file_name
    file_name_prefix = file_name[:file_name.rfind(os.path.sep)]
    if return_filenames:
      return (file_name_prefix + os.path.sep + 'mytemp*', file_list)
    return file_name_prefix + os.path.sep + 'mytemp*'

  def _run_avro_test(
      self, pattern, desired_bundle_size, perform_splitting, expected_result):
    source = _FastAvroSource(pattern)

    if perform_splitting:
      assert desired_bundle_size
      splits = [
          split
          for split in source.split(desired_bundle_size=desired_bundle_size)
      ]
      if len(splits) < 2:
        raise ValueError(
            'Test is trivial. Please adjust it so that at least '
            'two splits get generated')

      sources_info = [(split.source, split.start_position, split.stop_position)
                      for split in splits]
      source_test_utils.assert_sources_equal_reference_source(
          (source, None, None), sources_info)
    else:
      read_records = source_test_utils.read_from_source(source, None, None)
      self.assertCountEqual(expected_result, read_records)

  def test_schema_read_write(self):
    with tempfile.TemporaryDirectory() as tmp_dirname:
      path = os.path.join(tmp_dirname, 'tmp_filename')
      rows = [beam.Row(a=1, b=['x', 'y']), beam.Row(a=2, b=['t', 'u'])]
      stable_repr = lambda row: json.dumps(row._asdict())
      with TestPipeline() as p:
        _ = p | Create(rows) | avroio.WriteToAvro(path) | beam.Map(print)
      with TestPipeline() as p:
        readback = (
            p
            | avroio.ReadFromAvro(path + '*', as_rows=True)
            | beam.Map(stable_repr))
        assert_that(readback, equal_to([stable_repr(r) for r in rows]))

  @pytest.mark.xlang_sql_expansion_service
  @unittest.skipIf(
      TestPipeline().get_pipeline_options().view_as(StandardOptions).runner is
      None,
      "Must be run with a runner that supports staging java artifacts.")
  def test_avro_schema_to_beam_schema_with_nullable_atomic_fields(self):
    records = []
    records.extend(self.RECORDS)
    records.append({
        'name': 'Bruce', 'favorite_number': None, 'favorite_color': None
    })
    avro_schema = fastavro.parse_schema(json.loads(self.SCHEMA_STRING))
    beam_schema = avro_schema_to_beam_schema(avro_schema)

    with TestPipeline() as p:
      readback = (
          p
          | Create(records)
          | beam.Map(avro_dict_to_beam_row(avro_schema, beam_schema))
          | SqlTransform("SELECT * FROM PCOLLECTION")
          | beam.Map(beam_row_to_avro_dict(avro_schema, beam_schema)))
      assert_that(readback, equal_to(records))

  def test_avro_atomic_value_to_beam_atomic_value(self):
    input_outputs = [('int', 1, 1), ('int', -1, 0xffffffff),
                     ('int', None, None), ('long', 1, 1),
                     ('long', -1, 0xffffffffffffffff), ('long', None, None),
                     ('string', 'foo', 'foo')]
    for test_avro_type, test_value, expected_value in input_outputs:
      actual_value = avro_atomic_value_to_beam_atomic_value(
          test_avro_type, test_value)
      hc.assert_that(actual_value, hc.equal_to(expected_value))

  def test_beam_atomic_value_to_avro_atomic_value(self):
    input_outputs = [('int', 1, 1), ('int', 0xffffffff, -1),
                     ('int', None, None), ('long', 1, 1),
                     ('long', 0xffffffffffffffff, -1), ('long', None, None),
                     ('string', 'foo', 'foo')]
    for test_avro_type, test_value, expected_value in input_outputs:
      actual_value = beam_atomic_value_to_avro_atomic_value(
          test_avro_type, test_value)
      hc.assert_that(actual_value, hc.equal_to(expected_value))

  def test_avro_union_type_to_beam_type_with_nullable_long(self):
    union_type = ['null', 'long']
    beam_type = avro_union_type_to_beam_type(union_type)
    expected_beam_type = schema_pb2.FieldType(
        atomic_type=schema_pb2.INT64, nullable=True)
    hc.assert_that(beam_type, hc.equal_to(expected_beam_type))

  def test_avro_union_type_to_beam_type_with_string_long(self):
    union_type = ['string', 'long']
    beam_type = avro_union_type_to_beam_type(union_type)
    expected_beam_type = schemas.typing_to_runner_api(Any)
    hc.assert_that(beam_type, hc.equal_to(expected_beam_type))

  def test_avro_schema_to_beam_and_back(self):
    avro_schema = fastavro.parse_schema(json.loads(self.SCHEMA_STRING))
    beam_schema = avro_schema_to_beam_schema(avro_schema)
    converted_avro_schema = beam_schema_to_avro_schema(beam_schema)
    expected_fields = json.loads(self.SCHEMA_STRING)["fields"]
    hc.assert_that(
        converted_avro_schema["fields"], hc.equal_to(expected_fields))

  def test_read_without_splitting(self):
    file_name = self._write_data()
    expected_result = self.RECORDS
    self._run_avro_test(file_name, None, False, expected_result)

  def test_read_with_splitting(self):
    file_name = self._write_data()
    expected_result = self.RECORDS
    self._run_avro_test(file_name, 100, True, expected_result)

  def test_source_display_data(self):
    file_name = 'some_avro_source'
    source = \
        _FastAvroSource(
            file_name,
            validate=False,
        )
    dd = DisplayData.create_from(source)

    # No extra avro parameters for AvroSource.
    expected_items = [
        DisplayDataItemMatcher('compression', 'auto'),
        DisplayDataItemMatcher('file_pattern', file_name)
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_read_display_data(self):
    file_name = 'some_avro_source'
    read = \
        avroio.ReadFromAvro(
            file_name,
            validate=False)
    dd = DisplayData.create_from(read)

    # No extra avro parameters for AvroSource.
    expected_items = [
        DisplayDataItemMatcher('compression', 'auto'),
        DisplayDataItemMatcher('file_pattern', file_name)
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_sink_display_data(self):
    file_name = 'some_avro_sink'
    sink = _create_avro_sink(
        file_name, self.SCHEMA, 'null', '.end', 0, None, 'application/x-avro')
    dd = DisplayData.create_from(sink)

    expected_items = [
        DisplayDataItemMatcher('schema', str(self.SCHEMA)),
        DisplayDataItemMatcher(
            'file_pattern',
            'some_avro_sink-%(shard_num)05d-of-%(num_shards)05d.end'),
        DisplayDataItemMatcher('codec', 'null'),
        DisplayDataItemMatcher('compression', 'uncompressed')
    ]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_write_display_data(self):
    file_name = 'some_avro_sink'
    write = avroio.WriteToAvro(file_name, self.SCHEMA)
    write.expand(beam.PCollection(beam.Pipeline()))
    dd = DisplayData.create_from(write)
    expected_items = [
        DisplayDataItemMatcher('schema', str(self.SCHEMA)),
        DisplayDataItemMatcher(
            'file_pattern',
            'some_avro_sink-%(shard_num)05d-of-%(num_shards)05d'),
        DisplayDataItemMatcher('codec', 'deflate'),
        DisplayDataItemMatcher('compression', 'uncompressed')
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_read_reentrant_without_splitting(self):
    file_name = self._write_data()
    source = _FastAvroSource(file_name)
    source_test_utils.assert_reentrant_reads_succeed((source, None, None))

  def test_read_reantrant_with_splitting(self):
    file_name = self._write_data()
    source = _FastAvroSource(file_name)
    splits = [split for split in source.split(desired_bundle_size=100000)]
    assert len(splits) == 1
    source_test_utils.assert_reentrant_reads_succeed(
        (splits[0].source, splits[0].start_position, splits[0].stop_position))

  def test_read_without_splitting_multiple_blocks(self):
    file_name = self._write_data(count=12000)
    expected_result = self.RECORDS * 2000
    self._run_avro_test(file_name, None, False, expected_result)

  def test_read_with_splitting_multiple_blocks(self):
    file_name = self._write_data(count=12000)
    expected_result = self.RECORDS * 2000
    self._run_avro_test(file_name, 10000, True, expected_result)

  def test_split_points(self):
    num_records = 12000
    sync_interval = 16000
    file_name = self._write_data(count=num_records, sync_interval=sync_interval)

    source = _FastAvroSource(file_name)

    splits = [split for split in source.split(desired_bundle_size=float('inf'))]
    assert len(splits) == 1
    range_tracker = splits[0].source.get_range_tracker(
        splits[0].start_position, splits[0].stop_position)

    split_points_report = []

    for _ in splits[0].source.read(range_tracker):
      split_points_report.append(range_tracker.split_points())
    # There will be a total of num_blocks in the generated test file,
    # proportional to number of records in the file divided by syncronization
    # interval used by avro during write. Each block has more than 10 records.
    num_blocks = int(math.ceil(14.5 * num_records / sync_interval))
    assert num_blocks > 1
    # When reading records of the first block, range_tracker.split_points()
    # should return (0, iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)
    self.assertEqual(
        split_points_report[:10],
        [(0, iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)] * 10)

    # When reading records of last block, range_tracker.split_points() should
    # return (num_blocks - 1, 1)
    self.assertEqual(split_points_report[-10:], [(num_blocks - 1, 1)] * 10)

  def test_read_without_splitting_compressed_deflate(self):
    file_name = self._write_data(codec='deflate')
    expected_result = self.RECORDS
    self._run_avro_test(file_name, None, False, expected_result)

  def test_read_with_splitting_compressed_deflate(self):
    file_name = self._write_data(codec='deflate')
    expected_result = self.RECORDS
    self._run_avro_test(file_name, 100, True, expected_result)

  @unittest.skipIf(snappy is None, 'python-snappy not installed.')
  def test_read_without_splitting_compressed_snappy(self):
    file_name = self._write_data(codec='snappy')
    expected_result = self.RECORDS
    self._run_avro_test(file_name, None, False, expected_result)

  @unittest.skipIf(snappy is None, 'python-snappy not installed.')
  def test_read_with_splitting_compressed_snappy(self):
    file_name = self._write_data(codec='snappy')
    expected_result = self.RECORDS
    self._run_avro_test(file_name, 100, True, expected_result)

  def test_read_without_splitting_pattern(self):
    pattern = self._write_pattern(3)
    expected_result = self.RECORDS * 3
    self._run_avro_test(pattern, None, False, expected_result)

  def test_read_with_splitting_pattern(self):
    pattern = self._write_pattern(3)
    expected_result = self.RECORDS * 3
    self._run_avro_test(pattern, 100, True, expected_result)

  def test_dynamic_work_rebalancing_exhaustive(self):
    def compare_split_points(file_name):
      source = _FastAvroSource(file_name)
      splits = [
          split for split in source.split(desired_bundle_size=float('inf'))
      ]
      assert len(splits) == 1
      source_test_utils.assert_split_at_fraction_exhaustive(splits[0].source)

    # Adjusting block size so that we can perform a exhaustive dynamic
    # work rebalancing test that completes within an acceptable amount of time.
    file_name = self._write_data(count=5, sync_interval=2)

    compare_split_points(file_name)

  def test_corrupted_file(self):
    file_name = self._write_data()
    with open(file_name, 'rb') as f:
      data = f.read()

    # Corrupt the last character of the file which is also the last character of
    # the last sync_marker.
    # https://avro.apache.org/docs/current/spec.html#Object+Container+Files
    corrupted_data = bytearray(data)
    corrupted_data[-1] = (corrupted_data[-1] + 1) % 256
    with tempfile.NamedTemporaryFile(delete=False,
                                     prefix=tempfile.template) as f:
      f.write(corrupted_data)
      corrupted_file_name = f.name

    source = _FastAvroSource(corrupted_file_name)
    with self.assertRaisesRegex(ValueError, r'expected sync marker'):
      source_test_utils.read_from_source(source, None, None)

  def test_read_from_avro(self):
    path = self._write_data()
    with TestPipeline() as p:
      assert_that(p | avroio.ReadFromAvro(path), equal_to(self.RECORDS))

  def test_read_all_from_avro_single_file(self):
    path = self._write_data()
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([path]) \
          | avroio.ReadAllFromAvro(),
          equal_to(self.RECORDS))

  def test_read_all_from_avro_many_single_files(self):
    path1 = self._write_data()
    path2 = self._write_data()
    path3 = self._write_data()
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([path1, path2, path3]) \
          | avroio.ReadAllFromAvro(),
          equal_to(self.RECORDS * 3))

  def test_read_all_from_avro_file_pattern(self):
    file_pattern = self._write_pattern(5)
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([file_pattern]) \
          | avroio.ReadAllFromAvro(),
          equal_to(self.RECORDS * 5))

  def test_read_all_from_avro_many_file_patterns(self):
    file_pattern1 = self._write_pattern(5)
    file_pattern2 = self._write_pattern(2)
    file_pattern3 = self._write_pattern(3)
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([file_pattern1, file_pattern2, file_pattern3]) \
          | avroio.ReadAllFromAvro(),
          equal_to(self.RECORDS * 10))

  def test_read_all_from_avro_with_filename(self):
    file_pattern, file_paths = self._write_pattern(3, return_filenames=True)
    result = [(path, record) for path in file_paths for record in self.RECORDS]
    with TestPipeline() as p:
      assert_that(
          p \
          | Create([file_pattern]) \
          | avroio.ReadAllFromAvro(with_filename=True),
          equal_to(result))

  class _WriteFilesFn(beam.DoFn):
    """writes a couple of files with deferral."""

    COUNT_STATE = CombiningValueStateSpec('count', combine_fn=sum)

    def __init__(self, SCHEMA, RECORDS, tempdir):
      self._thread = None
      self.SCHEMA = SCHEMA
      self.RECORDS = RECORDS
      self.tempdir = tempdir

    def get_expect(self, match_updated_files):
      results_file1 = [('file1', x) for x in self.gen_records(1)]
      results_file2 = [('file2', x) for x in self.gen_records(3)]
      if match_updated_files:
        results_file1 += [('file1', x) for x in self.gen_records(2)]
      return results_file1 + results_file2

    def gen_records(self, count):
      return self.RECORDS * (count // len(self.RECORDS)) + self.RECORDS[:(
          count % len(self.RECORDS))]

    def process(self, element, count_state=beam.DoFn.StateParam(COUNT_STATE)):
      counter = count_state.read()
      if counter == 0:
        count_state.add(1)
        with open(FileSystems.join(self.tempdir, 'file1'), 'wb') as f:
          writer(f, self.SCHEMA, self.gen_records(2))
        with open(FileSystems.join(self.tempdir, 'file2'), 'wb') as f:
          writer(f, self.SCHEMA, self.gen_records(3))
      # convert dumb key to basename in output
      basename = FileSystems.split(element[1][0])[1]
      content = element[1][1]
      yield basename, content

  def test_read_all_continuously_new(self):
    with TestPipeline() as pipeline:
      tempdir = tempfile.mkdtemp()
      writer_fn = self._WriteFilesFn(self.SCHEMA, self.RECORDS, tempdir)
      with open(FileSystems.join(tempdir, 'file1'), 'wb') as f:
        writer(f, writer_fn.SCHEMA, writer_fn.gen_records(1))
      match_pattern = FileSystems.join(tempdir, '*')
      interval = 0.5
      last = 2

      p_read_once = (
          pipeline
          | 'Continuously read new files' >> avroio.ReadAllFromAvroContinuously(
              match_pattern,
              with_filename=True,
              start_timestamp=Timestamp.now(),
              interval=interval,
              stop_timestamp=Timestamp.now() + last,
              match_updated_files=False)
          | 'add dumb key' >> beam.Map(lambda x: (0, x))
          | 'Write files on-the-fly' >> beam.ParDo(writer_fn))
      assert_that(
          p_read_once,
          equal_to(writer_fn.get_expect(match_updated_files=False)),
          label='assert read new files results')

  def test_read_all_continuously_update(self):
    with TestPipeline() as pipeline:
      tempdir = tempfile.mkdtemp()
      writer_fn = self._WriteFilesFn(self.SCHEMA, self.RECORDS, tempdir)
      with open(FileSystems.join(tempdir, 'file1'), 'wb') as f:
        writer(f, writer_fn.SCHEMA, writer_fn.gen_records(1))
      match_pattern = FileSystems.join(tempdir, '*')
      interval = 0.5
      last = 2

      p_read_upd = (
          pipeline
          | 'Continuously read updated files' >>
          avroio.ReadAllFromAvroContinuously(
              match_pattern,
              with_filename=True,
              start_timestamp=Timestamp.now(),
              interval=interval,
              stop_timestamp=Timestamp.now() + last,
              match_updated_files=True)
          | 'add dumb key' >> beam.Map(lambda x: (0, x))
          | 'Write files on-the-fly' >> beam.ParDo(writer_fn))
      assert_that(
          p_read_upd,
          equal_to(writer_fn.get_expect(match_updated_files=True)),
          label='assert read updated files results')

  def test_sink_transform(self):
    with tempfile.NamedTemporaryFile() as dst:
      path = dst.name
      with TestPipeline() as p:
        # pylint: disable=expression-not-assigned
        p \
        | beam.Create(self.RECORDS) \
        | avroio.WriteToAvro(path, self.SCHEMA,)
      with TestPipeline() as p:
        # json used for stable sortability
        readback = \
            p \
            | avroio.ReadFromAvro(path + '*', ) \
            | beam.Map(json.dumps)
        assert_that(readback, equal_to([json.dumps(r) for r in self.RECORDS]))

  @unittest.skipIf(snappy is None, 'python-snappy not installed.')
  def test_sink_transform_snappy(self):
    with tempfile.NamedTemporaryFile() as dst:
      path = dst.name
      with TestPipeline() as p:
        # pylint: disable=expression-not-assigned
        p \
        | beam.Create(self.RECORDS) \
        | avroio.WriteToAvro(
            path,
            self.SCHEMA,
            codec='snappy')
      with TestPipeline() as p:
        # json used for stable sortability
        readback = \
            p \
            | avroio.ReadFromAvro(path + '*') \
            | beam.Map(json.dumps)
        assert_that(readback, equal_to([json.dumps(r) for r in self.RECORDS]))

  def test_writer_open_and_close(self):
    # Create and then close a temp file so we can manually open it later
    dst = tempfile.NamedTemporaryFile(delete=False)
    dst.close()

    schema = parse_schema(json.loads(self.SCHEMA_STRING))
    sink = _create_avro_sink(
        'some_avro_sink', schema, 'null', '.end', 0, None, 'application/x-avro')

    w = sink.open(dst.name)

    sink.close(w)

    os.unlink(dst.name)


class TestFastAvro(AvroBase, unittest.TestCase):
  def __init__(self, methodName='runTest'):
    super().__init__(methodName)
    self.SCHEMA = parse_schema(json.loads(self.SCHEMA_STRING))

  def _write_data(
      self,
      directory=None,
      prefix=tempfile.template,
      codec='null',
      count=len(RECORDS),
      **kwargs):
    all_records = self.RECORDS * \
      (count // len(self.RECORDS)) + self.RECORDS[:(count % len(self.RECORDS))]
    with tempfile.NamedTemporaryFile(delete=False,
                                     dir=directory,
                                     prefix=prefix,
                                     mode='w+b') as f:
      writer(f, self.SCHEMA, all_records, codec=codec, **kwargs)
      self._temp_files.append(f.name)
    return f.name


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
