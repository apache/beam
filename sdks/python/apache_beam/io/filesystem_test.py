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
# pytype: skip-file

import bz2
import gzip
import logging
import ntpath
import os
import posixpath
import sys
import tempfile
import unittest
import zlib
from io import BytesIO

from parameterized import param
from parameterized import parameterized

from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem


class TestingFileSystem(FileSystem):
  def __init__(self, pipeline_options, has_dirs=False):
    super().__init__(pipeline_options)
    self._has_dirs = has_dirs
    self._files = {}

  @classmethod
  def scheme(cls):
    # Required for FileSystems.get_filesystem().
    return 'test'

  def join(self, basepath, *paths):
    raise NotImplementedError

  def split(self, path):
    raise NotImplementedError

  def mkdirs(self, path):
    raise NotImplementedError

  def has_dirs(self):
    return self._has_dirs

  def _insert_random_file(self, path, size):
    self._files[path] = size

  def _list(self, dir_or_prefix):
    for path, size in self._files.items():
      if path.startswith(dir_or_prefix):
        yield FileMetadata(path, size)

  def create(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    raise NotImplementedError

  def open(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    raise NotImplementedError

  def copy(self, source_file_names, destination_file_names):
    raise NotImplementedError

  def rename(self, source_file_names, destination_file_names):
    raise NotImplementedError

  def exists(self, path):
    raise NotImplementedError

  def size(self, path):
    raise NotImplementedError

  def last_updated(self, path):
    raise NotImplementedError

  def checksum(self, path):
    raise NotImplementedError

  def delete(self, paths):
    raise NotImplementedError


class TestFileSystem(unittest.TestCase):
  def setUp(self):
    self.fs = TestingFileSystem(pipeline_options=None)

  def _flatten_match(self, match_results):
    return [
        file_metadata for match_result in match_results
        for file_metadata in match_result.metadata_list
    ]

  @parameterized.expand([
      ('gs://gcsio-test/**', all),
      # Does not match root-level files
      ('gs://gcsio-test/**/*', lambda n, i: n not in ['cat.png']),
      # Only matches root-level files
      ('gs://gcsio-test/*', [('cat.png', 19)]),
      (
          'gs://gcsio-test/cow/**', [
              ('cow/cat/fish', 2),
              ('cow/cat/blubber', 3),
              ('cow/dog/blubber', 4),
          ]),
      (
          'gs://gcsio-test/cow/ca**', [
              ('cow/cat/fish', 2),
              ('cow/cat/blubber', 3),
          ]),
      (
          'gs://gcsio-test/apple/[df]ish/ca*',
          [
              ('apple/fish/cat', 10),
              ('apple/fish/cart', 11),
              ('apple/fish/carl', 12),
              ('apple/dish/cat', 14),
              ('apple/dish/carl', 15),
          ]),
      (
          'gs://gcsio-test/apple/?ish/?a?',
          [
              ('apple/fish/cat', 10),
              ('apple/dish/bat', 13),
              ('apple/dish/cat', 14),
          ]),
      (
          'gs://gcsio-test/apple/fish/car?', [
              ('apple/fish/cart', 11),
              ('apple/fish/carl', 12),
          ]),
      (
          'gs://gcsio-test/apple/fish/b*',
          [
              ('apple/fish/blubber', 6),
              ('apple/fish/blowfish', 7),
              ('apple/fish/bambi', 8),
              ('apple/fish/balloon', 9),
          ]),
      (
          'gs://gcsio-test/apple/f*/b*',
          [
              ('apple/fish/blubber', 6),
              ('apple/fish/blowfish', 7),
              ('apple/fish/bambi', 8),
              ('apple/fish/balloon', 9),
          ]),
      (
          'gs://gcsio-test/apple/dish/[cb]at', [
              ('apple/dish/bat', 13),
              ('apple/dish/cat', 14),
          ]),
      (
          'gs://gcsio-test/banana/cyrano.m?', [
              ('banana/cyrano.md', 17),
              ('banana/cyrano.mb', 18),
          ]),
  ])
  def test_match_glob(self, file_pattern, expected_object_names):
    objects = [
        ('cow/cat/fish', 2), ('cow/cat/blubber', 3), ('cow/dog/blubber', 4),
        ('apple/dog/blubber', 5), ('apple/fish/blubber', 6),
        ('apple/fish/blowfish', 7), ('apple/fish/bambi', 8),
        ('apple/fish/balloon', 9), ('apple/fish/cat', 10),
        ('apple/fish/cart', 11), ('apple/fish/carl', 12),
        ('apple/dish/bat', 13), ('apple/dish/cat', 14), ('apple/dish/carl', 15),
        ('banana/cat', 16), ('banana/cyrano.md', 17), ('banana/cyrano.mb',
                                                       18), ('cat.png', 19)
    ]
    bucket_name = 'gcsio-test'

    if callable(expected_object_names):
      # A hack around the fact that the parameters do not have access to
      # the "objects" list.

      if expected_object_names is all:
        # It's a placeholder for "all" objects
        expected_object_names = objects
      else:
        # It's a filter function of type (str, int) -> bool
        # that returns true for expected objects
        filter_func = expected_object_names
        expected_object_names = [(short_path, size) for short_path,
                                 size in objects
                                 if filter_func(short_path, size)]

    for object_name, size in objects:
      file_name = 'gs://%s/%s' % (bucket_name, object_name)
      self.fs._insert_random_file(file_name, size)

    expected_file_names = [('gs://%s/%s' % (bucket_name, object_name), size)
                           for object_name,
                           size in expected_object_names]
    actual_file_names = [
        (file_metadata.path, file_metadata.size_in_bytes)
        for file_metadata in self._flatten_match(self.fs.match([file_pattern]))
    ]

    self.assertEqual(set(actual_file_names), set(expected_file_names))

    # Check if limits are followed correctly
    limit = 3
    expected_num_items = min(len(expected_object_names), limit)
    self.assertEqual(
        len(self._flatten_match(self.fs.match([file_pattern], [limit]))),
        expected_num_items)

  @parameterized.expand([
      param(
          os_path=posixpath,
          # re.escape does not escape forward slashes since Python 3.7
          # https://docs.python.org/3/whatsnew/3.7.html ("bpo-29995")
          sep_re='\\/' if sys.version_info < (3, 7, 0) else '/'),
      param(os_path=ntpath, sep_re='\\\\'),
  ])
  def test_translate_pattern(self, os_path, sep_re):
    star = r'[^/\\]*'
    double_star = r'.*'
    join = os_path.join

    sep = os_path.sep
    pattern__expected = [
        (join('a', '*'), sep_re.join(['a', star])),
        (join('b', '*') + sep, sep_re.join(['b', star]) + sep_re),
        (r'*[abc\]', star + r'[abc\\]'),
        (join('d', '**', '*'), sep_re.join(['d', double_star, star])),
    ]
    for pattern, expected in pattern__expected:
      expected = r'(?ms)' + expected + r'\Z'
      result = self.fs.translate_pattern(pattern)
      self.assertEqual(expected, result)


class TestFileSystemWithDirs(TestFileSystem):
  def setUp(self):
    self.fs = TestingFileSystem(pipeline_options=None, has_dirs=True)


class TestCompressedFile(unittest.TestCase):
  """Base class for TestCases that deals with TempDir clean-up.

  Inherited test cases will call self._new_tempdir() to start a temporary dir
  which will be deleted at the end of the tests (when tearDown() is called).
  """

  content = b"""- the BEAM -
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

    if compression_type == CompressionTypes.DEFLATE:
      with open(file_name, 'wb') as f:
        f.write(zlib.compress(content))
    elif compression_type == CompressionTypes.BZIP2 or \
            compression_type == CompressionTypes.GZIP:
      compress_open = bz2.BZ2File \
          if compression_type == CompressionTypes.BZIP2 \
          else gzip.open
      with compress_open(file_name, 'wb') as f:
        f.write(content)
    else:
      assert False, "Invalid compression type: %s" % compression_type

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
    for compression_type in [CompressionTypes.BZIP2,
                             CompressionTypes.DEFLATE,
                             CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(
            f, compression_type, read_size=self.read_block_size)
        reference_fd = BytesIO(self.content)

        # Note: BytesIO's tell() reports out of bound positions (if we seek
        # beyond the file), therefore we need to cap it to max_position
        # _CompressedFile.tell() always stays within the bounds of the
        # uncompressed content.
        # Negative seek position argument is not supported for BytesIO with
        # whence set to SEEK_SET.
        for seek_position in (0,
                              1,
                              len(self.content) - 1,
                              len(self.content),
                              len(self.content) + 1):
          compressed_fd.seek(seek_position, os.SEEK_SET)
          reference_fd.seek(seek_position, os.SEEK_SET)

          uncompressed_line = compressed_fd.readline()
          reference_line = reference_fd.readline()
          self.assertEqual(uncompressed_line, reference_line)

          uncompressed_position = compressed_fd.tell()
          reference_position = reference_fd.tell()
          max_position = len(self.content)
          reference_position = min(reference_position, max_position)
          self.assertEqual(uncompressed_position, reference_position)

  def test_seek_cur(self):
    for compression_type in [CompressionTypes.BZIP2,
                             CompressionTypes.DEFLATE,
                             CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(
            f, compression_type, read_size=self.read_block_size)
        reference_fd = BytesIO(self.content)

        # Test out of bound, inbound seeking in both directions
        # Note: BytesIO's seek() reports out of bound positions (if we seek
        # beyond the file), therefore we need to cap it to max_position (to
        # make it consistent with the old StringIO behavior
        for seek_position in (-1,
                              0,
                              1,
                              len(self.content) // 2,
                              len(self.content) // 2,
                              -1 * len(self.content) // 2):
          compressed_fd.seek(seek_position, os.SEEK_CUR)
          reference_fd.seek(seek_position, os.SEEK_CUR)

          uncompressed_line = compressed_fd.readline()
          expected_line = reference_fd.readline()
          self.assertEqual(uncompressed_line, expected_line)

          reference_position = reference_fd.tell()
          uncompressed_position = compressed_fd.tell()
          max_position = len(self.content)
          reference_position = min(reference_position, max_position)
          reference_fd.seek(reference_position, os.SEEK_SET)
          self.assertEqual(uncompressed_position, reference_position)

  def test_read_from_end_returns_no_data(self):
    for compression_type in [CompressionTypes.BZIP2,
                             CompressionTypes.DEFLATE,
                             CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(
            f, compression_type, read_size=self.read_block_size)

        seek_position = 0
        compressed_fd.seek(seek_position, os.SEEK_END)

        expected_data = b''
        uncompressed_data = compressed_fd.read(10)

        self.assertEqual(uncompressed_data, expected_data)

  def test_seek_outside(self):
    for compression_type in [CompressionTypes.BZIP2,
                             CompressionTypes.DEFLATE,
                             CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(
            f, compression_type, read_size=self.read_block_size)

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
    for compression_type in [CompressionTypes.BZIP2,
                             CompressionTypes.DEFLATE,
                             CompressionTypes.GZIP]:
      file_name = self._create_compressed_file(compression_type, self.content)
      with open(file_name, 'rb') as f:
        compressed_fd = CompressedFile(
            f, compression_type, read_size=self.read_block_size)

        first_pass = compressed_fd.readline()
        compressed_fd.seek(0, os.SEEK_SET)
        second_pass = compressed_fd.readline()

        self.assertEqual(first_pass, second_pass)

  def test_tell(self):
    lines = [b'line%d\n' % i for i in range(10)]
    tmpfile = self._create_temp_file()
    with open(tmpfile, 'wb') as f:
      writeable = CompressedFile(f)
      current_offset = 0
      for line in lines:
        writeable.write(line)
        current_offset += len(line)
        self.assertEqual(current_offset, writeable.tell())

    with open(tmpfile, 'rb') as f:
      readable = CompressedFile(f)
      current_offset = 0
      while True:
        line = readable.readline()
        current_offset += len(line)
        self.assertEqual(current_offset, readable.tell())
        if not line:
          break

  def test_concatenated_compressed_file(self):
    # The test apache_beam.io.textio_test.test_read_gzip_concat
    # does not encounter the problem in the Beam 2.13 and earlier
    # code base because the test data is too small: the data is
    # smaller than read_size, so it goes through logic in the code
    # that avoids the problem in the code.  So, this test sets
    # read_size smaller and test data bigger, in order to
    # encounter the problem. It would be difficult to test in the
    # textio_test module, because you'd need very large test data
    # because default read_size is 16MiB, and the ReadFromText
    # interface does not allow you to modify the read_size.
    import random
    import threading
    from six import int2byte
    num_test_lines = 10
    timeout = 30
    read_size = (64 << 10)  # set much smaller than the line size
    byte_table = tuple(int2byte(i) for i in range(32, 96))

    def generate_random_line():
      byte_list = list(
          b for i in range(4096) for b in random.sample(byte_table, 64))
      byte_list.append(b'\n')
      return b''.join(byte_list)

    def create_test_file(compression_type, lines):
      filenames = []
      file_name = self._create_temp_file()
      if compression_type == CompressionTypes.BZIP2:
        compress_factory = bz2.BZ2File
      elif compression_type == CompressionTypes.GZIP:
        compress_factory = gzip.open
      else:
        assert False, "Invalid compression type: %s" % compression_type
      for line in lines:
        filenames.append(self._create_temp_file())
        with compress_factory(filenames[-1], 'wb') as f:
          f.write(line)
      with open(file_name, 'wb') as o:
        for name in filenames:
          with open(name, 'rb') as i:
            o.write(i.read())
      return file_name

    # I remember some time ago when a job ran with a real concatenated
    # gzip file, I got into an endless loop in the beam filesystem module.
    # That's why I put this handler in to trap an endless loop. However,
    # this unit test doesn't encounter an endless loop, it encounters a
    # different error, in the Beam 2.13 and earlier implementation.
    # So it's not strictly necessary to have this handler in this unit test.

    def timeout_handler():
      raise IOError('Exiting due to likley infinite loop logic in code.')

    timer = threading.Timer(timeout, timeout_handler)
    try:
      test_lines = tuple(generate_random_line() for i in range(num_test_lines))
      for compression_type in [CompressionTypes.BZIP2, CompressionTypes.GZIP]:
        file_name = create_test_file(compression_type, test_lines)
        timer.start()
        with open(file_name, 'rb') as f:
          data = CompressedFile(f, compression_type, read_size=read_size)
          for written_line in test_lines:
            read_line = data.readline()
            self.assertEqual(written_line, read_line)
        timer.cancel()
        # Starting a new timer for the next iteration/test.
        timer = threading.Timer(timeout, timeout_handler)
    finally:
      timer.cancel()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
