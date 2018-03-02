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
import bz2
import gzip
import os
import tempfile
import unittest
from StringIO import StringIO

from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes


class TestCompressedFile(unittest.TestCase):
  """Base class for TestCases that deals with TempDir clean-up.

  Inherited test cases will call self._new_tempdir() to start a temporary dir
  which will be deleted at the end of the tests (when tearDown() is called).
  """

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

  def setUp(self):
    self._tempfiles = []

  def tearDown(self):
    for path in self._tempfiles:
      if os.path.exists(path):
        os.remove(path)

  def _create_temp_file(self):
    path = tempfile.NamedTemporaryFile(delete=False).name
    self._tempfiles.append(path)
    return path

  def _create_compressed_file(self, compression_type, content):
    file_name = self._create_temp_file()

    if compression_type == CompressionTypes.BZIP2:
      compress_factory = bz2.BZ2File
    elif compression_type == CompressionTypes.GZIP:
      compress_factory = gzip.open
    else:
      assert False, "Invalid compression type: %s" % compression_type

    with compress_factory(file_name, 'wb') as f:
      f.write(content)

    return file_name

  def test_seekable_enabled_on_read(self):
    with open(self._create_temp_file(), 'rb') as f:
      readable = CompressedFile(f)
      self.assertTrue(readable.seekable)

  def test_seekable_disabled_on_write(self):
    with open(self._create_temp_file(), 'wb') as f:
      writeable = CompressedFile(f)
      self.assertFalse(writeable.seekable)

  def test_seekable_disabled_on_append(self):
    with open(self._create_temp_file(), 'ab') as f:
      writeable = CompressedFile(f)
      self.assertFalse(writeable.seekable)

  def test_seek_set(self):
    for compression_type in [CompressionTypes.BZIP2, CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(f, compression_type,
                                       read_size=self.read_block_size)
        reference_fd = StringIO(self.content)

        # Note: content (readline) check must come before position (tell) check
        # because cStringIO's tell() reports out of bound positions (if we seek
        # beyond the file) up until a real read occurs.
        # _CompressedFile.tell() always stays within the bounds of the
        # uncompressed content.
        for seek_position in (-1, 0, 1,
                              len(self.content)-1, len(self.content),
                              len(self.content) + 1):
          compressed_fd.seek(seek_position, os.SEEK_SET)
          reference_fd.seek(seek_position, os.SEEK_SET)

          uncompressed_line = compressed_fd.readline()
          reference_line = reference_fd.readline()
          self.assertEqual(uncompressed_line, reference_line)

          uncompressed_position = compressed_fd.tell()
          reference_position = reference_fd.tell()
          self.assertEqual(uncompressed_position, reference_position)

  def test_seek_cur(self):
    for compression_type in [CompressionTypes.BZIP2, CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(f, compression_type,
                                       read_size=self.read_block_size)
        reference_fd = StringIO(self.content)

        # Test out of bound, inbound seeking in both directions
        for seek_position in (-1, 0, 1,
                              len(self.content) / 2,
                              len(self.content) / 2,
                              -1 * len(self.content) / 2):
          compressed_fd.seek(seek_position, os.SEEK_CUR)
          reference_fd.seek(seek_position, os.SEEK_CUR)

          uncompressed_line = compressed_fd.readline()
          expected_line = reference_fd.readline()
          self.assertEqual(uncompressed_line, expected_line)

          reference_position = reference_fd.tell()
          uncompressed_position = compressed_fd.tell()
          self.assertEqual(uncompressed_position, reference_position)

  def test_read_from_end_returns_no_data(self):
    for compression_type in [CompressionTypes.BZIP2, CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(f, compression_type,
                                       read_size=self.read_block_size)

        seek_position = 0
        compressed_fd.seek(seek_position, os.SEEK_END)

        expected_data = ''
        uncompressed_data = compressed_fd.read(10)

        self.assertEqual(uncompressed_data, expected_data)

  def test_seek_outside(self):
    for compression_type in [CompressionTypes.BZIP2, CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(f, compression_type,
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

  def test_read_and_seek_back_to_beginning(self):
    for compression_type in [CompressionTypes.BZIP2, CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(f, compression_type,
                                       read_size=self.read_block_size)

        first_pass = compressed_fd.readline()
        compressed_fd.seek(0, os.SEEK_SET)
        second_pass = compressed_fd.readline()

        self.assertEqual(first_pass, second_pass)

  def test_tell(self):
    lines = ['line%d\n' % i for i in range(10)]
    tmpfile = self._create_temp_file()
    with open(tmpfile, 'w') as f:
      writeable = CompressedFile(f)
      current_offset = 0
      for line in lines:
        writeable.write(line)
        current_offset += len(line)
        self.assertEqual(current_offset, writeable.tell())

    with open(tmpfile) as f:
      readable = CompressedFile(f)
      current_offset = 0
      while True:
        line = readable.readline()
        current_offset += len(line)
        self.assertEqual(current_offset, readable.tell())
        if not line:
          break


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
