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

import abc
import io
import os

__all__ = ['Downloader', 'Uploader', 'DownloaderStream', 'UploaderStream',
           'PipeStream']


class Downloader(object):
  """Download interface for a single file.

  Implementations should support random access reads.
  """

  __metaclass__ = abc.ABCMeta

  @abc.abstractproperty
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


class Uploader(object):
  """Upload interface for a single file."""

  __metaclass__ = abc.ABCMeta

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

  def __init__(self, downloader, mode='r'):
    """Initializes the stream.

    Args:
      downloader: (Downloader) Filesystem dependent implementation.
      mode: (string) Python mode attribute for this stream.
    """
    self._downloader = downloader
    self.mode = mode
    self._position = 0

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


class UploaderStream(io.RawIOBase):
  """Provides a stream interface for Uploader objects."""

  def __init__(self, uploader, mode='w'):
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

    super(UploaderStream, self).close()

  def writable(self):
    return True


class PipeStream(object):
  """A class that presents a pipe connection as a readable stream."""

  def __init__(self, recv_pipe):
    self.conn = recv_pipe
    self.closed = False
    self.position = 0
    self.remaining = ''

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
    return ''.join(data_list)

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
    elif whence == os.SEEK_SET and offset == self.position:
      return
    raise NotImplementedError

  def _check_open(self):
    if self.closed:
      raise IOError('Stream is closed.')
