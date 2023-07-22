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

import unittest

import apache_beam as beam
from apache_beam.io.filesystem import FileMetadata
from apache_beam.ml.inference import utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class WatchFilePatternTest(unittest.TestCase):
  def test_latest_file_by_timestamp_default_value(self):
    # match continuously returns the files in sorted timestamp order.
    main_input_pcoll = [
        FileMetadata(
            'path1.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP - 20),
        FileMetadata(
            'path2.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP - 10)
    ]
    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create(main_input_pcoll)
          | beam.Map(lambda x: (x.path, x))
          | beam.ParDo(utils._GetLatestFileByTimeStamp())
          | beam.Map(lambda x: x[0]))
      assert_that(files_pc, equal_to(['', '']))

  def test_latest_file_with_timestamp_after_pipeline_construction_time(self):
    main_input_pcoll = [
        FileMetadata(
            'path1.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP + 10)
    ]
    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create(main_input_pcoll)
          | beam.Map(lambda x: (x.path, x))
          | beam.ParDo(utils._GetLatestFileByTimeStamp())
          | beam.Map(lambda x: x[0]))
      assert_that(files_pc, equal_to(['path1.py']))

  def test_emitting_singleton_output(self):
    # match continuously returns the files in sorted timestamp order.
    main_input_pcoll = [
        FileMetadata(
            'path1.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP - 20),
        # returns default
        FileMetadata(
            'path2.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP - 10),
        # returns default
        FileMetadata(
            'path3.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP + 10),
        FileMetadata(
            'path4.py',
            10,
            last_updated_in_seconds=utils._START_TIME_STAMP + 20)
    ]
    # returns path3.py

    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create(main_input_pcoll)
          | beam.Map(lambda x: (x.path, x))
          | beam.ParDo(utils._GetLatestFileByTimeStamp())
          | beam.ParDo(utils._ConvertIterToSingleton())
          | beam.Map(lambda x: x[0]))
      assert_that(files_pc, equal_to(['', 'path3.py', 'path4.py']))


if __name__ == '__main__':
  unittest.main()
