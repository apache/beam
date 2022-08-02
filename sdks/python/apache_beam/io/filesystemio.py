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

"""Utilities for ``FileSystem`` implementations."""

# pytype: skip-file

import abc
import io
import os

__all__ = [
    'Downloader',
    'Uploader',
    'DownloaderStream',
    'UploaderStream',
    'PipeStream'
]


class Downloader(metaclass=abc.ABCMeta):
  """Download interface for a single file.

  Implementations should support random access reads.
  """
  @property
  @abc.abstractmethod
  def size(self):
    """Size of file to download."""

  @abc.abstractmethod
  def get_range(self, start, end):
    """Retrieve a given byte range [start, end) from this download.

    Range must be in this form:
      0 <= start < end: Fetch the bytes from start to end.

    Args:
      start: (int) Initial byte offset.
      end: (int) Final byte offset, exclusive.

    Returns:
      (string) A buffer containing the requested data.
    """


class Uploader(metaclass=abc.ABCMeta):
  """Upload interface for a single file."""
  @abc.abstractmethod
  def put(self, data):
    """Write data to file sequentially.

    Args:
      data: (memoryview) Data to write.
    """

  @abc.abstractmethod
  def finish(self):
    """Signal to upload any remaining data and close the file.

    File should be fully written upon return from this method.

    Raises:
      Any error encountered during the upload.
    """


class DownloaderStream(io.RawIOBase):
  """Provides a stream interface for Downloader objects."""
  def __init__(
      self, downloader, read_buffer_size=io.DEFAULT_BUFFER_SIZE, mode='rb'):
    """Initializes the stream.

    Args:
      downloader: (Downloader) Filesystem dependent implementation.
      read_buffer_size: (int) Buffer size to use during read operations.
      mode: (string) Python mode attribute for this stream.
    """
    self._downloader = downloader
    self.mode = mode
    self._position = 0
    self._reader_buffer_size = read_buffer_size

  def readinto(self, b):
    """Read up to len(b) bytes into b.

    Returns number of bytes read (0 for EOF).

    Args:
      b: (bytearray/memoryview) Buffer to read into.
    """
    self._checkClosed()
    if self._position >= self._downloader.size:
      return 0

    start = self._position
    end = min(self._position + len(b), self._downloader.size)
    data = self._downloader.get_range(start, end)
    self._position += len(data)
    b[:len(data)] = data
    return len(data)

  def seek(self, offset, whence=os.SEEK_SET):
    """Set the stream's current offset.

    Note if the new offset is out of bound, it is adjusted to either 0 or EOF.

    Args:
      offset: seek offset as number.
      whence: seek mode. Supported modes are os.SEEK_SET (absolute seek),
        os.SEEK_CUR (seek relative to the current position), and os.SEEK_END
        (seek relative to the end, offset should be negative).

    Raises:
      ``ValueError``: When this stream is closed or if whence is invalid.
    """
    self._checkClosed()

    if whence == os.SEEK_SET:
      self._position = offset
    elif whence == os.SEEK_CUR:
      self._position += offset
    elif whence == os.SEEK_END:
      self._position = self._downloader.size + offset
    else:
      raise ValueError('Whence mode %r is invalid.' % whence)

    self._position = min(self._position, self._downloader.size)
    self._position = max(self._position, 0)
    return self._position

  def tell(self):
    """Tell the stream's current offset.

    Returns:
      current offset in reading this stream.

    Raises:
      ``ValueError``: When this stream is closed.
    """
    self._checkClosed()
    return self._position

  def seekable(self):
    return True

  def readable(self):
    return True

  def readall(self):
    """Read until EOF, using multiple read() call."""
    res = []
    while True:
      data = self.read(self._reader_buffer_size)
      if not data:
        break
      res.append(data)
    return b''.join(res)


class UploaderStream(io.RawIOBase):
  """Provides a stream interface for Uploader objects."""
  def __init__(self, uploader, mode='wb'):
    """Initializes the stream.

    Args:
      uploader: (Uploader) Filesystem dependent implementation.
      mode: (string) Python mode attribute for this stream.
    """
    self._uploader = uploader
    self.mode = mode
    self._position = 0

  def tell(self):
    return self._position

  def write(self, b):
    """Write bytes from b.

    Returns number of bytes written (<= len(b)).

    Args:
      b: (memoryview) Buffer with data to write.
    """
    self._checkClosed()
    self._uploader.put(b)

    bytes_written = len(b)
    self._position += bytes_written
    return bytes_written

  def close(self):
    """Complete the upload and close this stream.

    This method has no effect if the stream is already closed.

    Raises:
      Any error encountered by the uploader.
    """
    if not self.closed:
      self._uploader.finish()

    super().close()

  def writable(self):
    return True


class PipeStream(object):
  """A class that presents a pipe connection as a readable stream.

  Not thread-safe.

  Remembers the last ``size`` bytes read and allows rewinding the stream by that
  amount exactly. See BEAM-6380 for more.
  """
  def __init__(self, recv_pipe):
    self.conn = recv_pipe
    self.closed = False
    self.position = 0
    self.remaining = b''

    # Data and position of last block streamed. Allows limited seeking backwards
    # of stream.
    self.last_block_position = None
    self.last_block = b''

  def read(self, size):
    """Read data from the wrapped pipe connection.

    Args:
      size: Number of bytes to read. Actual number of bytes read is always
            equal to size unless EOF is reached.

    Returns:
      data read as str.
    """
    data_list = []
    bytes_read = 0
    last_block_position = self.position

    while bytes_read < size:
      bytes_from_remaining = min(size - bytes_read, len(self.remaining))
      data_list.append(self.remaining[0:bytes_from_remaining])
      self.remaining = self.remaining[bytes_from_remaining:]
      self.position += bytes_from_remaining
      bytes_read += bytes_from_remaining
      if not self.remaining:
        try:
          self.remaining = self.conn.recv_bytes()
        except EOFError:
          break

    last_block = b''.join(data_list)
    if last_block:
      self.last_block_position = last_block_position
      self.last_block = last_block
    return last_block

  def tell(self):
    """Tell the file's current offset.

    Returns:
      current offset in reading this file.

    Raises:
      ``ValueError``: When this stream is closed.
    """
    self._check_open()
    return self.position

  def seek(self, offset, whence=os.SEEK_SET):
    # The apitools library used by the gcsio.Uploader class insists on seeking
    # to the end of a stream to do a check before completing an upload, so we
    # must have this no-op method here in that case.
    if whence == os.SEEK_END and offset == 0:
      return
    elif whence == os.SEEK_SET:
      if offset == self.position:
        return
      elif offset == self.last_block_position and self.last_block:
        self.position = offset
        self.remaining = b''.join([self.last_block, self.remaining])
        self.last_block = b''
        return
    raise NotImplementedError(
        'offset: %s, whence: %s, position: %s, last: %s' %
        (offset, whence, self.position, self.last_block_position))

  def _check_open(self):
    if self.closed:
      raise IOError('Stream is closed.')
