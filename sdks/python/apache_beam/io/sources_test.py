# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the sources framework."""

import logging
import tempfile
import unittest

import google.cloud.dataflow as df

from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.transforms.util import assert_that
from google.cloud.dataflow.transforms.util import equal_to


class LineSource(iobase.BoundedSource):
  """A simple source that reads lines from a given file."""

  def __init__(self, file_name):
    self._file_name = file_name

  def read(self, _):
    with open(self._file_name) as f:
      for line in f:
        yield line.rstrip('\n')


class SourcesTest(unittest.TestCase):

  def _create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents)
      return f.name

  def test_read_from_source(self):
    file_name = self._create_temp_file('aaaa\nbbbb\ncccc\ndddd')

    source = LineSource(file_name)
    result = [line for line in source.read(None)]

    self.assertItemsEqual(['aaaa', 'bbbb', 'cccc', 'dddd'], result)

  def test_run_direct(self):
    file_name = self._create_temp_file('aaaa\nbbbb\ncccc\ndddd')
    pipeline = df.Pipeline('DirectPipelineRunner')
    pcoll = pipeline | df.Read(LineSource(file_name))
    assert_that(pcoll, equal_to(['aaaa', 'bbbb', 'cccc', 'dddd']))

    pipeline.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
