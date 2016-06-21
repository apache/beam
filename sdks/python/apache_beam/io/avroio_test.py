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

import logging
import os
import tempfile
import unittest

from apache_beam.io import avroio
from apache_beam.io import filebasedsource
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema as avro_schema


class TestAvro(unittest.TestCase):

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  RECORDS = [{'name': 'Thomas', 'favorite_number': 1, 'favorite_color': 'blue'},
             {'name': 'Henry', 'favorite_number': 3, 'favorite_color': 'green'},
             {'name': 'Toby', 'favorite_number': 7, 'favorite_color': 'brown'},
             {'name': 'Gordon', 'favorite_number': 4, 'favorite_color': 'blue'},
             {'name': 'Emily', 'favorite_number': -1, 'favorite_color': 'Red'},
             {'name': 'Percy', 'favorite_number': 6, 'favorite_color': 'Green'}]

  def _write_data(self, directory=None,
                  prefix=tempfile.template,
                  codec='null',
                  count=len(RECORDS)):
    schema = ('{\"namespace\": \"example.avro\",'
              '\"type\": \"record\",'
              '\"name\": \"User\",'
              '\"fields\": ['
              '{\"name\": \"name\", \"type\": \"string\"},'
              '{\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},'
              '{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}'
              ']}')

    schema = avro_schema.parse(schema)

    with tempfile.NamedTemporaryFile(
        delete=False, dir=directory, prefix=prefix) as f:
      writer = DataFileWriter(f, DatumWriter(), schema, codec=codec)
      len_records = len(self.RECORDS)
      for i in range(count):
        writer.append(self.RECORDS[i % len_records])
      writer.close()

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

  def _run_avro_test(
      self, pattern, desired_bundle_size, perform_splitting, expected_result):
    source = avroio.AvroSource(pattern)

    read_records = []
    if perform_splitting:
      assert desired_bundle_size
      splits = [split for split in source.split(
          desired_bundle_size=desired_bundle_size)]
      if len(splits) < 2:
        raise ValueError('Test is trivial. Please adjust it so that at least '
                         'two splits get generated')
      for split in splits:
        records = [record for record in split.source.read(
            split.source.get_range_tracker(split.start_position,
                                           split.stop_position))]
        read_records.extend(records)
    else:
      range_tracker = source.get_range_tracker(None, None)
      read_records = [record for record in source.read(range_tracker)]

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

  def test_read_without_splitting_pattern(self):
    pattern = self._write_pattern(3)
    expected_result = self.RECORDS * 3
    self._run_avro_test(pattern, None, False, expected_result)

  def test_read_with_splitting_pattern(self):
    pattern = self._write_pattern(3)
    expected_result = self.RECORDS * 3
    self._run_avro_test(pattern, 100, True, expected_result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
