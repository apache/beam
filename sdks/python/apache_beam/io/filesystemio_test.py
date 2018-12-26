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
"""Tests for filesystemio."""

from __future__ import absolute_import

import io
import logging
import multiprocessing
import os
import threading
import unittest
from builtins import range

from apache_beam.io import filesystemio


class FakeDownloader(filesystemio.Downloader):

  def __init__(self, data):
    self._data = data
    self.last_read_size = -1

  @property
  def size(self):
    return len(self._data)

  def get_range(self, start, end):
    self.last_read_size = end - start
    return self._data[start:end]


class FakeUploader(filesystemio.Uploader):

  def __init__(self):
    self.data = b''
    self.last_write_size = -1
    self.finished = False

  def last_error(self):
    return None

  def put(self, data):
    assert not self.finished
    self.data += data.tobytes()
    self.last_write_size = len(data)

  def finish(self):
    self.finished = True


class TestDownloaderStream(unittest.TestCase):

  def test_file_attributes(self):
    downloader = FakeDownloader(data=None)
    stream = filesystemio.DownloaderStream(downloader)
    self.assertEqual(stream.mode, 'rb')
    self.assertTrue(stream.readable())
    self.assertFalse(stream.writable())
    self.assertTrue(stream.seekable())

  def test_read_empty(self):
    downloader = FakeDownloader(data=b'')
    stream = filesystemio.DownloaderStream(downloader)
    self.assertEqual(stream.read(), b'')

  def test_read(self):
    data = b'abcde'
    downloader = FakeDownloader(data)
    stream = filesystemio.DownloaderStream(downloader)

    # Read size is exactly what was passed to read() (unbuffered).
    self.assertEqual(stream.read(1), data[0:1])
    self.assertEqual(downloader.last_read_size, 1)
    self.assertEqual(stream.read(), data[1:])
    self.assertEqual(downloader.last_read_size, len(data) - 1)

  def test_read_buffered(self):
    data = b'abcde'
    downloader = FakeDownloader(data)
    buffer_size = 2
    stream = io.BufferedReader(filesystemio.DownloaderStream(downloader),
                               buffer_size)

    # Verify that buffering works and is reading ahead.
    self.assertEqual(stream.read(1), data[0:1])
    self.assertEqual(downloader.last_read_size, buffer_size)
    self.assertEqual(stream.read(), data[1:])


class TestUploaderStream(unittest.TestCase):

  def test_file_attributes(self):
    uploader = FakeUploader()
    stream = filesystemio.UploaderStream(uploader)
    self.assertEqual(stream.mode, 'wb')
    self.assertFalse(stream.readable())
    self.assertTrue(stream.writable())
    self.assertFalse(stream.seekable())

  def test_write_empty(self):
    uploader = FakeUploader()
    stream = filesystemio.UploaderStream(uploader)
    data = b''
    stream.write(memoryview(data))
    self.assertEqual(uploader.data, data)

  def test_write(self):
    data = b'abcde'
    uploader = FakeUploader()
    stream = filesystemio.UploaderStream(uploader)

    # Unbuffered writes.
    stream.write(memoryview(data[0:1]))
    self.assertEqual(uploader.data[0], data[0])
    self.assertEqual(uploader.last_write_size, 1)
    stream.write(memoryview(data[1:]))
    self.assertEqual(uploader.data, data)
    self.assertEqual(uploader.last_write_size, len(data) - 1)

  def test_write_buffered(self):
    data = b'abcde'
    uploader = FakeUploader()
    buffer_size = 2
    stream = io.BufferedWriter(filesystemio.UploaderStream(uploader),
                               buffer_size)

    # Verify that buffering works: doesn't write to uploader until buffer is
    # filled.
    stream.write(data[0:1])
    self.assertEqual(-1, uploader.last_write_size)
    stream.write(data[1:])
    stream.close()
    self.assertEqual(data, uploader.data)


class TestPipeStream(unittest.TestCase):

  def _read_and_verify(self, stream, expected, buffer_size):
    data_list = []
    bytes_read = 0
    seen_last_block = False
    while True:
      data = stream.read(buffer_size)
      self.assertLessEqual(len(data), buffer_size)
      if len(data) < buffer_size:
        # Test the constraint that the pipe stream returns less than the buffer
        # size only when at the end of the stream.
        if data:
          self.assertFalse(seen_last_block)
        seen_last_block = True
      if not data:
        break
      data_list.append(data)
      bytes_read += len(data)
      self.assertEqual(stream.tell(), bytes_read)
    self.assertEqual(b''.join(data_list), expected)

  def test_pipe_stream(self):
    block_sizes = list(4**i for i in range(0, 12))
    data_blocks = list(os.urandom(size) for size in block_sizes)
    expected = b''.join(data_blocks)

    buffer_sizes = [100001, 512 * 1024, 1024 * 1024]

    for buffer_size in buffer_sizes:
      parent_conn, child_conn = multiprocessing.Pipe()
      stream = filesystemio.PipeStream(child_conn)
      child_thread = threading.Thread(
          target=self._read_and_verify, args=(stream, expected, buffer_size))
      child_thread.start()
      for data in data_blocks:
        parent_conn.send_bytes(data)
      parent_conn.close()
      child_thread.join()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
