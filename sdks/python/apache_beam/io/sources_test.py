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

"""Unit tests for the sources framework."""

import logging
import os
import tempfile
import unittest

import apache_beam as beam

from apache_beam import coders
from apache_beam.io import iobase
from apache_beam.io import range_trackers
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to


class LineSource(iobase.BoundedSource):
  """A simple source that reads lines from a given file."""

  TEST_BUNDLE_SIZE = 10

  def __init__(self, file_name):
    self._file_name = file_name

  def read(self, range_tracker):
    with open(self._file_name, 'rb') as f:
      start = range_tracker.start_position()
      f.seek(start)
      if start > 0:
        f.seek(-1, os.SEEK_CUR)
        start -= 1
        start += len(f.readline())
      current = start
      for line in f:
        if not range_tracker.try_claim(current):
          return
        yield line.rstrip('\n')
        current += len(line)

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    assert start_position is None
    assert stop_position is None
    with open(self._file_name, 'rb') as f:
      f.seek(0, os.SEEK_END)
      size = f.tell()

    bundle_start = 0
    while bundle_start < size:
      bundle_stop = min(bundle_start + LineSource.TEST_BUNDLE_SIZE, size)
      yield iobase.SourceBundle(1, self, bundle_start, bundle_stop)
      bundle_start = bundle_stop

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      with open(self._file_name, 'rb') as f:
        f.seek(0, os.SEEK_END)
        stop_position = f.tell()
    return range_trackers.OffsetRangeTracker(start_position, stop_position)

  def default_output_coder(self):
    return coders.BytesCoder()


class SourcesTest(unittest.TestCase):

  def _create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents)
      return f.name

  def test_read_from_source(self):
    file_name = self._create_temp_file('aaaa\nbbbb\ncccc\ndddd')

    source = LineSource(file_name)
    range_tracker = source.get_range_tracker(None, None)
    result = [line for line in source.read(range_tracker)]

    self.assertItemsEqual(['aaaa', 'bbbb', 'cccc', 'dddd'], result)

  def test_run_direct(self):
    file_name = self._create_temp_file('aaaa\nbbbb\ncccc\ndddd')
    pipeline = beam.Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Read(LineSource(file_name))
    assert_that(pcoll, equal_to(['aaaa', 'bbbb', 'cccc', 'dddd']))

    pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
