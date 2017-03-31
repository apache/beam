# -*- coding: utf-8 -*-
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

"""Unit tests for filesystem module."""
import shutil
import os
import unittest
import tempfile
import bz2
import gzip
from random import randint
from StringIO import StringIO

from parameterized import parameterized

from apache_beam.io.filesystem import CompressedFile, CompressionTypes


class _TestCaseWithTempDirCleanUp(unittest.TestCase):
  """Base class for TestCases that deals with TempDir clean-up.

  Inherited test cases will call self._new_tempdir() to start a temporary dir
  which will be deleted at the end of the tests (when tearDown() is called).
  """

  def setUp(self):
    self._tempdirs = []

  def tearDown(self):
    for path in self._tempdirs:
      if os.path.exists(path):
        shutil.rmtree(path)
    self._tempdirs = []

  def _new_tempdir(self):
    result = tempfile.mkdtemp()
    self._tempdirs.append(result)
    return result

  def _create_temp_file(self, name='', suffix=''):
    if not name:
      name = tempfile.template
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=name,
        dir=self._new_tempdir(), suffix=suffix).name
    return file_name


class TestCompressedFile(_TestCaseWithTempDirCleanUp):
  content = """- the BEAM -
How things really are we would like to know.
Does
     Time
          flow, is it elastic, or is it
atomized in instants hammered around the
    clock's face? ...
- May Swenson"""

  # Keep the read block size small so that we exercise the seek functionality
  # in compressed file and not just in the internal buffer
  read_block_size = 4

  def _create_compressed_file(self, compression_type, content,
                              name='', suffix=''):
    file_name = self._create_temp_file(name, suffix)

    if compression_type == CompressionTypes.BZIP2:
      compress_factory = bz2.BZ2File
    elif compression_type == CompressionTypes.GZIP:
      compress_factory = gzip.open
    else:
      assert False, "Invalid compression type: %s" % compression_type

    with compress_factory(file_name, 'w') as f:
      f.write(content)

    return file_name

  def test_seekable_enabled_on_read(self):
    readable = CompressedFile(open(self._create_temp_file(), 'r'))
    self.assertTrue(readable.seekable)

  def test_seekable_disabled_on_write(self):
    writeable = CompressedFile(open(self._create_temp_file(), 'w'))
    self.assertFalse(writeable.seekable)

  def test_seekable_disabled_on_append(self):
    writeable = CompressedFile(open(self._create_temp_file(), 'a'))
    self.assertFalse(writeable.seekable)

  @parameterized.expand([CompressionTypes.BZIP2, CompressionTypes.GZIP])
  def test_seek_set(self, compression_type):
    file_name = self._create_compressed_file(compression_type, self.content)

    compressed_fd = CompressedFile(open(file_name, 'r'), compression_type,
                                   read_size=self.read_block_size)
    reference_fd = StringIO(self.content)

    random_position = randint(0, len(self.content) - 1)
    compressed_fd.seek(random_position, os.SEEK_SET)
    reference_fd.seek(random_position, os.SEEK_SET)

    expected_position = random_position
    uncompressed_position = compressed_fd.tell()
    reference_position = reference_fd.tell()

    self.assertEqual(uncompressed_position, expected_position)
    self.assertEqual(reference_position, expected_position)

    uncompressed_line = compressed_fd.readline()
    reference_line = reference_fd.readline()

    self.assertEqual(uncompressed_line, reference_line)

  @parameterized.expand([CompressionTypes.BZIP2, CompressionTypes.GZIP])
  def test_seek_cur(self, compression_type):
    file_name = self._create_compressed_file(compression_type, self.content)

    compressed_fd = CompressedFile(open(file_name, 'r'), compression_type,
                                   read_size=self.read_block_size)
    reference_fd = StringIO(self.content)

    # skip to the middle without seeking
    mid_point = len(self.content) / 2
    _ = compressed_fd.read(mid_point)
    _ = reference_fd.read(mid_point)

    expected_position = mid_point
    uncompressed_position = compressed_fd.tell()
    reference_position = reference_fd.tell()

    self.assertEqual(uncompressed_position, expected_position)
    self.assertEqual(reference_position, expected_position)

    seek_position = randint(-1 * mid_point, mid_point)
    compressed_fd.seek(seek_position, os.SEEK_CUR)
    reference_fd.seek(seek_position, os.SEEK_CUR)

    expected_position = mid_point + seek_position
    reference_position = reference_fd.tell()
    uncompressed_position = compressed_fd.tell()

    self.assertEqual(uncompressed_position, expected_position)
    self.assertEqual(reference_position, expected_position)

    uncompressed_line = compressed_fd.readline()
    expected_line = reference_fd.readline()

    self.assertEqual(uncompressed_line, expected_line)

  @parameterized.expand([CompressionTypes.BZIP2, CompressionTypes.GZIP])
  def test_read_from_end_returns_no_data(self, compression_type):
    file_name = self._create_compressed_file(compression_type, self.content)

    compressed_fd = CompressedFile(open(file_name, 'r'), compression_type,
                                   read_size=self.read_block_size)

    seek_position = 0
    compressed_fd.seek(seek_position, os.SEEK_END)

    expected_data = ''
    uncompressed_data = compressed_fd.read(10)

    self.assertEqual(uncompressed_data, expected_data)

  @parameterized.expand([CompressionTypes.BZIP2, CompressionTypes.GZIP])
  def test_seek_outside(self, compression_type):
    file_name = self._create_compressed_file(compression_type, self.content)

    compressed_fd = CompressedFile(open(file_name, 'r'), compression_type,
                                   read_size=self.read_block_size)

    for whence in (os.SEEK_CUR, os.SEEK_SET, os.SEEK_END):
      seek_position = -1 * len(self.content) - 10
      compressed_fd.seek(seek_position, whence)

      expected_position = 0
      uncompressed_position = compressed_fd.tell()
      self.assertEqual(uncompressed_position, expected_position)

      seek_position = len(self.content) + 20
      compressed_fd.seek(seek_position, whence)

      expected_position = len(self.content)
      uncompressed_position = compressed_fd.tell()
      self.assertEqual(uncompressed_position, expected_position)

  @parameterized.expand([CompressionTypes.BZIP2, CompressionTypes.GZIP])
  def test_read_and_seek_back_to_beginning(self, compression_type):
    file_name = self._create_compressed_file(compression_type, self.content)
    compressed_fd = CompressedFile(open(file_name, 'r'), compression_type,
                                   read_size=self.read_block_size)

    first_pass = compressed_fd.readline()
    compressed_fd.seek(0, os.SEEK_SET)
    second_pass = compressed_fd.readline()

    self.assertEqual(first_pass, second_pass)

  def test_tell(self):
    lines = ['line%d\n' % i for i in range(10)]
    tmpfile = self._create_temp_file()
    writeable = CompressedFile(open(tmpfile, 'w'))
    current_offset = 0
    for line in lines:
      writeable.write(line)
      current_offset += len(line)
      self.assertEqual(current_offset, writeable.tell())

    writeable.close()
    readable = CompressedFile(open(tmpfile))
    current_offset = 0
    while True:
      line = readable.readline()
      current_offset += len(line)
      self.assertEqual(current_offset, readable.tell())
      if not line:
        break
