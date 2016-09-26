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

import json
import logging
import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.io import avroio
from apache_beam.io import filebasedsource
from apache_beam.io import source_test_utils
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to

# Importing following private class for testing purposes.
from apache_beam.io.avroio import _AvroSource as AvroSource

import avro.datafile
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema


class TestAvro(unittest.TestCase):

  _temp_files = []

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  def tearDown(self):
    for path in self._temp_files:
      if os.path.exists(path):
        os.remove(path)
    self._temp_files = []

  RECORDS = [{'name': 'Thomas',
              'favorite_number': 1,
              'favorite_color': 'blue'}, {'name': 'Henry',
                                          'favorite_number': 3,
                                          'favorite_color': 'green'},
             {'name': 'Toby',
              'favorite_number': 7,
              'favorite_color': 'brown'}, {'name': 'Gordon',
                                           'favorite_number': 4,
                                           'favorite_color': 'blue'},
             {'name': 'Emily',
              'favorite_number': -1,
              'favorite_color': 'Red'}, {'name': 'Percy',
                                         'favorite_number': 6,
                                         'favorite_color': 'Green'}]

  SCHEMA = avro.schema.parse('''
  {"namespace": "example.avro",
   "type": "record",
   "name": "User",
   "fields": [
       {"name": "name", "type": "string"},
       {"name": "favorite_number",  "type": ["int", "null"]},
       {"name": "favorite_color", "type": ["string", "null"]}
   ]
  }
  ''')

  def _write_data(self,
                  directory=None,
                  prefix=tempfile.template,
                  codec='null',
                  count=len(RECORDS)):

    with tempfile.NamedTemporaryFile(
        delete=False, dir=directory, prefix=prefix) as f:
      writer = DataFileWriter(f, DatumWriter(), self.SCHEMA, codec=codec)
      len_records = len(self.RECORDS)
      for i in range(count):
        writer.append(self.RECORDS[i % len_records])
      writer.close()

      self._temp_files.append(f.name)
      return f.name

  def _write_pattern(self, num_files):
    assert num_files > 0
    temp_dir = tempfile.mkdtemp()

    file_name = None
    for _ in range(num_files):
      file_name = self._write_data(directory=temp_dir, prefix='mytemp')

    assert file_name
    file_name_prefix = file_name[:file_name.rfind(os.path.sep)]
    return file_name_prefix + os.path.sep + 'mytemp*'

  def _run_avro_test(self, pattern, desired_bundle_size, perform_splitting,
                     expected_result):
    source = AvroSource(pattern)

    read_records = []
    if perform_splitting:
      assert desired_bundle_size
      splits = [
          split
          for split in source.split(desired_bundle_size=desired_bundle_size)
      ]
      if len(splits) < 2:
        raise ValueError('Test is trivial. Please adjust it so that at least '
                         'two splits get generated')

      sources_info = [
          (split.source, split.start_position, split.stop_position)
          for split in splits
      ]
      source_test_utils.assertSourcesEqualReferenceSource((source, None, None),
                                                          sources_info)
    else:
      read_records = source_test_utils.readFromSource(source, None, None)
      self.assertItemsEqual(expected_result, read_records)

  def test_read_without_splitting(self):
    file_name = self._write_data()
    expected_result = self.RECORDS
    self._run_avro_test(file_name, None, False, expected_result)

  def test_read_with_splitting(self):
    file_name = self._write_data()
    expected_result = self.RECORDS
    self._run_avro_test(file_name, 100, True, expected_result)

  def test_read_without_splitting_multiple_blocks(self):
    file_name = self._write_data(count=12000)
    expected_result = self.RECORDS * 2000
    self._run_avro_test(file_name, None, False, expected_result)

  def test_read_with_splitting_multiple_blocks(self):
    file_name = self._write_data(count=12000)
    expected_result = self.RECORDS * 2000
    self._run_avro_test(file_name, 10000, True, expected_result)

  def test_read_without_splitting_compressed_deflate(self):
    file_name = self._write_data(codec='deflate')
    expected_result = self.RECORDS
    self._run_avro_test(file_name, None, False, expected_result)

  def test_read_with_splitting_compressed_deflate(self):
    file_name = self._write_data(codec='deflate')
    expected_result = self.RECORDS
    self._run_avro_test(file_name, 100, True, expected_result)

  def test_read_without_splitting_compressed_snappy(self):
    try:
      import snappy  # pylint: disable=unused-variable
      file_name = self._write_data(codec='snappy')
      expected_result = self.RECORDS
      self._run_avro_test(file_name, None, False, expected_result)
    except ImportError:
      logging.warning(
          'Skipped test_read_without_splitting_compressed_snappy since snappy '
          'appears to not be installed.')

  def test_read_with_splitting_compressed_snappy(self):
    try:
      import snappy  # pylint: disable=unused-variable
      file_name = self._write_data(codec='snappy')
      expected_result = self.RECORDS
      self._run_avro_test(file_name, 100, True, expected_result)
    except ImportError:
      logging.warning(
          'Skipped test_read_with_splitting_compressed_snappy since snappy '
          'appears to not be installed.')

  def test_read_without_splitting_pattern(self):
    pattern = self._write_pattern(3)
    expected_result = self.RECORDS * 3
    self._run_avro_test(pattern, None, False, expected_result)

  def test_read_with_splitting_pattern(self):
    pattern = self._write_pattern(3)
    expected_result = self.RECORDS * 3
    self._run_avro_test(pattern, 100, True, expected_result)

  def test_dynamic_work_rebalancing_exhaustive(self):
    # Adjusting block size so that we can perform a exhaustive dynamic
    # work rebalancing test that completes within an acceptable amount of time.
    old_sync_interval = avro.datafile.SYNC_INTERVAL
    try:
      avro.datafile.SYNC_INTERVAL = 5
      file_name = self._write_data(count=20)
      source = AvroSource(file_name)
      splits = [split
                for split in source.split(desired_bundle_size=float('inf'))]
      assert len(splits) == 1
      source_test_utils.assertSplitAtFractionExhaustive(splits[0].source)
    finally:
      avro.datafile.SYNC_INTERVAL = old_sync_interval

  def test_corrupted_file(self):
    file_name = self._write_data()
    with open(file_name, 'r') as f:
      data = bytearray(f.read())

    # Corrupt the last character of the file which is also the last character of
    # the last sync_marker.
    with tempfile.NamedTemporaryFile(
        delete=False, prefix=tempfile.template) as f:
      last_char_index = len(data) - 1
      data[last_char_index] = 'A' if data[last_char_index] == 'B' else 'A'
      f.write(data)
      corrupted_file_name = f.name

    source = AvroSource(corrupted_file_name)
    with self.assertRaises(ValueError) as exn:
      source_test_utils.readFromSource(source, None, None)
      self.assertEqual(0, exn.exception.message.find('Unexpected sync marker'))

  def test_source_transform(self):
    path = self._write_data()
    with beam.Pipeline('DirectPipelineRunner') as p:
      assert_that(p | avroio.ReadFromAvro(path), equal_to(self.RECORDS))

  def test_sink_transform(self):
    with tempfile.NamedTemporaryFile() as dst:
      path = dst.name
      with beam.Pipeline('DirectPipelineRunner') as p:
        # pylint: disable=expression-not-assigned
        p | beam.Create(self.RECORDS) | avroio.WriteToAvro(path, self.SCHEMA)
      with beam.Pipeline('DirectPipelineRunner') as p:
        # json used for stable sortability
        readback = p | avroio.ReadFromAvro(path + '*') | beam.Map(json.dumps)
        assert_that(readback, equal_to([json.dumps(r) for r in self.RECORDS]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
