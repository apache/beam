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
"""File system abstraction for file-based sources and sinks."""

from __future__ import absolute_import

import abc
import bz2
import cStringIO
import os
import zlib
import logging
import time

logger = logging.getLogger(__name__)

DEFAULT_READ_BUFFER_SIZE = 16 * 1024 * 1024


class CompressionTypes(object):
  """Enum-like class representing known compression types."""

  # Detect compression based on filename extension.
  #
  # The following extensions are currently recognized by auto-detection:
  #   .bz2 (implies BZIP2 as described below).
  #   .gz  (implies GZIP as described below)
  # Any non-recognized extension implies UNCOMPRESSED as described below.
  AUTO = 'auto'

  # BZIP2 compression.
  BZIP2 = 'bzip2'

  # GZIP compression (deflate with GZIP headers).
  GZIP = 'gzip'

  # Uncompressed (i.e., may be split).
  UNCOMPRESSED = 'uncompressed'

  @classmethod
  def is_valid_compression_type(cls, compression_type):
    """Returns True for valid compression types, False otherwise."""
    types = set([
        CompressionTypes.AUTO,
        CompressionTypes.BZIP2,
        CompressionTypes.GZIP,
        CompressionTypes.UNCOMPRESSED
    ])
    return compression_type in types

  @classmethod
  def mime_type(cls, compression_type, default='application/octet-stream'):
    mime_types_by_compression_type = {
        cls.BZIP2: 'application/x-bz2',
        cls.GZIP: 'application/x-gzip',
    }
    return mime_types_by_compression_type.get(compression_type, default)

  @classmethod
  def detect_compression_type(cls, file_path):
    """Returns the compression type of a file (based on its suffix)."""
    compression_types_by_suffix = {'.bz2': cls.BZIP2, '.gz': cls.GZIP}
    lowercased_path = file_path.lower()
    for suffix, compression_type in compression_types_by_suffix.iteritems():
      if lowercased_path.endswith(suffix):
        return compression_type
    return cls.UNCOMPRESSED


class CompressedFile(object):
  """File wrapper for easier handling of compressed files."""
  # XXX: This class is not thread safe in the read path.

  # The bit mask to use for the wbits parameters of the zlib compressor and
  # decompressor objects.
  _gzip_mask = zlib.MAX_WBITS | 16  # Mask when using GZIP headers.

  def __init__(self,
               fileobj,
               compression_type=CompressionTypes.GZIP,
               read_size=DEFAULT_READ_BUFFER_SIZE):
    if not fileobj:
      raise ValueError('File object must not be None')

    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    if compression_type in (CompressionTypes.AUTO, CompressionTypes.UNCOMPRESSED
                           ):
      raise ValueError(
          'Cannot create object with unspecified or no compression')

    self._file = fileobj
    self._compression_type = compression_type

    if self._file.tell() != 0:
      raise ValueError('File object must be at position 0 but was %d' %
                       self._file.tell())
    self._uncompressed_position = 0
    self._uncompressed_size = None

    if self.readable():
      self._read_size = read_size
      self._read_buffer = cStringIO.StringIO()
      self._read_position = 0
      self._read_eof = False

      self._initialize_decompressor()
    else:
      self._decompressor = None

    if self.writeable():
      self._initialize_compressor()
    else:
      self._compressor = None

  def _initialize_decompressor(self):
    if self._compression_type == CompressionTypes.BZIP2:
      self._decompressor = bz2.BZ2Decompressor()
    else:
      assert self._compression_type == CompressionTypes.GZIP
      self._decompressor = zlib.decompressobj(self._gzip_mask)

  def _initialize_compressor(self):
    if self._compression_type == CompressionTypes.BZIP2:
      self._compressor = bz2.BZ2Compressor()
    else:
      assert self._compression_type == CompressionTypes.GZIP
      self._compressor = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION,
                                          zlib.DEFLATED, self._gzip_mask)

  def readable(self):
    mode = self._file.mode
    return 'r' in mode or 'a' in mode

  def writeable(self):
    mode = self._file.mode
    return 'w' in mode or 'a' in mode

  def write(self, data):
    """Write data to file."""
    if not self._compressor:
      raise ValueError('compressor not initialized')
    self._uncompressed_position += len(data)
    compressed = self._compressor.compress(data)
    if compressed:
      self._file.write(compressed)

  def _fetch_to_internal_buffer(self, num_bytes):
    """Fetch up to num_bytes into the internal buffer."""
    if (not self._read_eof and self._read_position > 0 and
        (self._read_buffer.tell() - self._read_position) < num_bytes):
      # There aren't enough number of bytes to accommodate a read, so we
      # prepare for a possibly large read by clearing up all internal buffers
      # but without dropping any previous held data.
      self._read_buffer.seek(self._read_position)
      data = self._read_buffer.read()
      self._clear_read_buffer()
      self._read_buffer.write(data)

    while not self._read_eof and (self._read_buffer.tell() - self._read_position
                                 ) < num_bytes:
      # Continue reading from the underlying file object until enough bytes are
      # available, or EOF is reached.
      buf = self._file.read(self._read_size)
      if buf:
        decompressed = self._decompressor.decompress(buf)
        del buf  # Free up some possibly large and no-longer-needed memory.
        self._read_buffer.write(decompressed)
      else:
        # EOF reached.
        # Verify completeness and no corruption and flush (if needed by
        # the underlying algorithm).
        if self._compression_type == CompressionTypes.BZIP2:
          # Having unused_data past end of stream would imply file corruption.
          assert not self._decompressor.unused_data, 'Possible file corruption.'
          try:
            # EOF implies that the underlying BZIP2 stream must also have
            # reached EOF. We expect this to raise an EOFError and we catch it
            # below. Any other kind of error though would be problematic.
            self._decompressor.decompress('dummy')
            assert False, 'Possible file corruption.'
          except EOFError:
            pass  # All is as expected!
        else:
          self._read_buffer.write(self._decompressor.flush())

        # Record that we have hit the end of file, so we won't unnecessarily
        # repeat the completeness verification step above.
        self._read_eof = True

  def _read_from_internal_buffer(self, read_fn):
    """Read from the internal buffer by using the supplied read_fn."""
    self._read_buffer.seek(self._read_position)
    result = read_fn()
    self._read_position += len(result)
    self._uncompressed_position += len(result)
    self._read_buffer.seek(0, os.SEEK_END)  # Allow future writes.
    return result

  def read(self, num_bytes):
    if not self._decompressor:
      raise ValueError('decompressor not initialized')

    self._fetch_to_internal_buffer(num_bytes)
    return self._read_from_internal_buffer(
        lambda: self._read_buffer.read(num_bytes))

  def readline(self):
    """Equivalent to standard file.readline(). Same return conventions apply."""
    if not self._decompressor:
      raise ValueError('decompressor not initialized')

    io = cStringIO.StringIO()
    while True:
      # Ensure that the internal buffer has at least half the read_size. Going
      # with half the _read_size (as opposed to a full _read_size) to ensure
      # that actual fetches are more evenly spread out, as opposed to having 2
      # consecutive reads at the beginning of a read.
      self._fetch_to_internal_buffer(self._read_size / 2)
      line = self._read_from_internal_buffer(
          lambda: self._read_buffer.readline())
      io.write(line)
      if line.endswith('\n') or not line:
        break  # Newline or EOF reached.

    return io.getvalue()

  def closed(self):
    return not self._file or self._file.closed()

  def close(self):
    if self.readable():
      self._read_buffer.close()

    if self.writeable():
      self._file.write(self._compressor.flush())

    self._file.close()

  def flush(self):
    if self.writeable():
      self._file.write(self._compressor.flush())
    self._file.flush()

  @property
  def seekable(self):
    return 'r' in self._file.mode

  def _clear_read_buffer(self):
    """Clears the read buffer by removing all the contents and
    resetting _read_position to 0"""
    self._read_position = 0
    self._read_buffer.seek(0)
    self._read_buffer.truncate(0)

  def _rewind_file(self):
    """Seeks to the beginning of the input file. Input file's EOF marker
    is cleared and _uncompressed_position is reset to zero"""
    self._file.seek(0, os.SEEK_SET)
    self._read_eof = False
    self._uncompressed_position = 0

  def _rewind(self):
    """Seeks to the beginning of the input file and resets the internal read
    buffer. The decompressor object is re-initialized to ensure that no data
    left in it's buffer."""
    self._clear_read_buffer()
    self._rewind_file()

    # Re-initialize decompressor to clear any data buffered prior to rewind
    self._initialize_decompressor()

  def seek(self, offset, whence=os.SEEK_SET):
    """Set the file's current offset.

    Seeking behavior:
      * seeking from the end (SEEK_END) the whole file is decompressed once to
        determine it's size. Therefore it is preferred to use
        SEEK_SET or SEEK_CUR to avoid the processing overhead
      * seeking backwards from the current position rewinds the file to 0
        and decompresses the chunks to the requested offset
      * seeking is only supported in files opened for reading
      * if the new offset is out of bound, it is adjusted to either 0 or EOF.

    Args:
      offset: seek offset in the uncompressed content represented as number
      whence: seek mode. Supported modes are os.SEEK_SET (absolute seek),
        os.SEEK_CUR (seek relative to the current position), and os.SEEK_END
        (seek relative to the end, offset should be negative).

    Raises:
      IOError: When this buffer is closed.
      ValueError: When whence is invalid or the file is not seekable
    """
    if whence == os.SEEK_SET:
      absolute_offset = offset
    elif whence == os.SEEK_CUR:
      absolute_offset = self._uncompressed_position + offset
    elif whence == os.SEEK_END:
      # Determine and cache the uncompressed size of the file
      if not self._uncompressed_size:
        logger.warn("Seeking relative from end of file is requested. "
                    "Need to decompress the whole file once to determine "
                    "its size. This might take a while...")
        uncompress_start_time = time.time()
        while self.read(self._read_size):
          pass
        uncompress_end_time = time.time()
        logger.warn("Full file decompression for seek from end took %.2f secs",
                    (uncompress_end_time - uncompress_start_time))
        self._uncompressed_size = self._uncompressed_position
      absolute_offset = self._uncompressed_size + offset
    else:
      raise ValueError("Whence mode %r is invalid." % whence)

    # Determine how many bytes needs to be read before we reach
    # the requested offset. Rewind if we already passed the position.
    if absolute_offset < self._uncompressed_position:
      self._rewind()
    bytes_to_skip = absolute_offset - self._uncompressed_position

    # Read until the desired position is reached or EOF occurs.
    while bytes_to_skip:
      data = self.read(min(self._read_size, bytes_to_skip))
      if not data:
        break
      bytes_to_skip -= len(data)

  def tell(self):
    """Returns current position in uncompressed file."""
    return self._uncompressed_position

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.close()


class FileMetadata(object):
  """Metadata about a file path that is the output of FileSystem.match
  """
  def __init__(self, path, size_in_bytes):
    assert isinstance(path, basestring) and path, "Path should be a string"
    assert isinstance(size_in_bytes, (int, long)) and size_in_bytes >= 0, \
        "Invalid value for size_in_bytes should %s (of type %s)" % (
            size_in_bytes, type(size_in_bytes))
    self.path = path
    self.size_in_bytes = size_in_bytes

  def __eq__(self, other):
    """Note: This is only used in tests where we verify that mock objects match.
    """
    return (isinstance(other, FileMetadata) and
            self.path == other.path and
            self.size_in_bytes == other.size_in_bytes)

  def __hash__(self):
    return hash((self.path, self.size_in_bytes))

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    return 'FileMetadata(%s, %s)' % (self.path, self.size_in_bytes)


class MatchResult(object):
  """Result from the ``FileSystem`` match operation which contains the list
   of matched FileMetadata.
  """
  def __init__(self, pattern, metadata_list):
    self.metadata_list = metadata_list
    self.pattern = pattern


class BeamIOError(IOError):
  def __init__(self, msg, exception_details=None):
    """Class representing the errors thrown in the batch file operations.
    Args:
      msg: Message string for the exception thrown
      exception_details: Optional map of individual input to exception for
        failed operations in batch. This parameter is optional so if specified
        the user can assume that the all errors in the filesystem operation
        have been reported. When the details are missing then the operation
        may have failed anywhere so the user should use match to determine
        the current state of the system.
    """
    message = "%s with exceptions %s" % (msg, exception_details)
    super(BeamIOError, self).__init__(message)
    self.exception_details = exception_details


class FileSystem(object):
  """A class that defines the functions that can be performed on a filesystem.

  All methods are abstract and they are for file system providers to
  implement. Clients should use the FileSystemUtil class to interact with
  the correct file system based on the provided file pattern scheme.
  """
  __metaclass__ = abc.ABCMeta
  CHUNK_SIZE = 1  # Chuck size in the batch operations

  @staticmethod
  def _get_compression_type(path, compression_type):
    if compression_type == CompressionTypes.AUTO:
      compression_type = CompressionTypes.detect_compression_type(path)
    elif not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    return compression_type

  @abc.abstractmethod
  def mkdirs(self, path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError if leaf directory already exists.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def match(self, patterns, limits=None):
    """Find all matching paths to the patterns provided.

    Args:
      patterns: list of string for the file path pattern to match against
      limits: list of maximum number of responses that need to be fetched

    Returns: list of ``MatchResult`` objects.

    Raises:
      ``BeamIOError`` if any of the pattern match operations fail
    """
    raise NotImplementedError

  @abc.abstractmethod
  def create(self, path, mime_type, compression_type):
    """Returns a write channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    raise NotImplementedError

  @abc.abstractmethod
  def open(self, path, mime_type, compression_type):
    """Returns a read channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    raise NotImplementedError

  @abc.abstractmethod
  def copy(self, source_file_names, destination_file_names):
    """Recursively copy the file tree from the source to the destination

    Args:
      source_file_names: list of source file objects that needs to be copied
      destination_file_names: list of destination of the new object

    Raises:
      ``BeamIOError`` if any of the copy operations fail
    """
    raise NotImplementedError

  @abc.abstractmethod
  def rename(self, source_file_names, destination_file_names):
    """Rename the files at the source list to the destination list.
    Source and destination lists should be of the same size.

    Args:
      source_file_names: List of file paths that need to be moved
      destination_file_names: List of destination_file_names for the files

    Raises:
      ``BeamIOError`` if any of the rename operations fail
    """
    raise NotImplementedError

  @abc.abstractmethod
  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    raise NotImplementedError

  @abc.abstractmethod
  def delete(self, paths):
    """Deletes files or directories at the provided paths.
    Directories will be deleted recursively.

    Args:
      paths: list of paths that give the file objects to be deleted

    Raises:
      ``BeamIOError`` if any of the delete operations fail
    """
    raise NotImplementedError
