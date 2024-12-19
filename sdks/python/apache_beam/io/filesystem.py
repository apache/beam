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

"""File system abstraction for file-based sources and sinks.

Note to implementors:
  "path" arguments will be URLs in the form scheme://foo/bar. The exception is
  LocalFileSystem, which gets unix-style paths in the form /foo/bar.
"""

# pytype: skip-file

import abc
import bz2
import io
import logging
import lzma
import os
import posixpath
import re
import time
import zlib
from typing import BinaryIO  # pylint: disable=unused-import
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import zstandard

from apache_beam.utils.plugin import BeamPlugin

logger = logging.getLogger(__name__)

DEFAULT_READ_BUFFER_SIZE = 16 * 1024 * 1024

__all__ = [
    'CompressionTypes',
    'CompressedFile',
    'FileMetadata',
    'FileSystem',
    'MatchResult'
]


class CompressionTypes(object):
  """Enum-like class representing known compression types."""

  # Detect compression based on filename extension.
  #
  # The following extensions are currently recognized by auto-detection:
  #   .bz2 (implies BZIP2 as described below).
  #   .gz  (implies GZIP as described below)
  #   .deflate (implies DEFLATE as described below)
  #   .zst (implies ZSTD as described below)
  #   .zst (implies ZSTD as described below)
  #   .xz (implies LZMA as described below)
  #   .lzma (implies LZMA as described below)
  # Any non-recognized extension implies UNCOMPRESSED as described below.
  AUTO = 'auto'

  # BZIP2 compression.
  BZIP2 = 'bzip2'

  # DEFLATE compression
  DEFLATE = 'deflate'

  # ZSTD compression
  ZSTD = 'zstd'

  # GZIP compression (deflate with GZIP headers).
  GZIP = 'gzip'

  # LZMA compression
  LZMA = 'lzma'

  # Uncompressed (i.e., may be split).
  UNCOMPRESSED = 'uncompressed'

  @classmethod
  def is_valid_compression_type(cls, compression_type):
    """Returns True for valid compression types, False otherwise."""
    types = set([
        CompressionTypes.AUTO,
        CompressionTypes.BZIP2,
        CompressionTypes.DEFLATE,
        CompressionTypes.GZIP,
        CompressionTypes.ZSTD,
        CompressionTypes.LZMA,
        CompressionTypes.UNCOMPRESSED
    ])
    return compression_type in types

  @classmethod
  def mime_type(cls, compression_type, default='application/octet-stream'):
    mime_types_by_compression_type = {
        cls.BZIP2: 'application/x-bz2',
        cls.DEFLATE: 'application/x-deflate',
        cls.GZIP: 'application/x-gzip',
        cls.ZSTD: 'application/zstd',
        cls.LZMA: 'application/lzma'
    }
    return mime_types_by_compression_type.get(compression_type, default)

  @classmethod
  def detect_compression_type(cls, file_path):
    """Returns the compression type of a file (based on its suffix)."""
    compression_types_by_suffix = {
        '.bz2': cls.BZIP2,
        '.deflate': cls.DEFLATE,
        '.gz': cls.GZIP,
        '.zst': cls.ZSTD,
        '.zstd': cls.ZSTD,
        '.xz': cls.LZMA,
        '.lzma': cls.LZMA
    }
    lowercased_path = file_path.lower()
    for suffix, compression_type in compression_types_by_suffix.items():
      if lowercased_path.endswith(suffix):
        return compression_type
    return cls.UNCOMPRESSED


class CompressedFile(object):
  """File wrapper for easier handling of compressed files."""
  # XXX: This class is not thread safe in the read path.

  # The bit mask to use for the wbits parameters of the zlib compressor and
  # decompressor objects.
  _gzip_mask = zlib.MAX_WBITS | 16  # Mask when using GZIP headers.

  def __init__(
      self,
      fileobj: BinaryIO,
      compression_type=CompressionTypes.GZIP,
      read_size=DEFAULT_READ_BUFFER_SIZE):
    if not fileobj:
      raise ValueError('File object must not be None')

    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError(
          'compression_type must be CompressionType object but '
          'was %s' % type(compression_type))
    if compression_type in (CompressionTypes.AUTO,
                            CompressionTypes.UNCOMPRESSED):
      raise ValueError(
          'Cannot create object with unspecified or no compression')

    self._file = fileobj
    self._compression_type = compression_type

    if self._file.tell() != 0:
      raise ValueError(
          'File object must be at position 0 but was %d' % self._file.tell())
    self._uncompressed_position = 0
    self._uncompressed_size: Optional[int] = None

    if self.readable():
      self._read_size = read_size
      self._read_buffer = io.BytesIO()
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
    elif self._compression_type == CompressionTypes.DEFLATE:
      self._decompressor = zlib.decompressobj()
    elif self._compression_type == CompressionTypes.ZSTD:
      # hardcoded max_window_size to avoid too much memory
      # errors when reading big files, please refer
      # to the following issue for further explanation:
      # https://github.com/indygreg/python-zstandard/issues/157
      self._decompressor = zstandard.ZstdDecompressor(
          max_window_size=2147483648).decompressobj()
    elif self._compression_type == CompressionTypes.LZMA:
      self._decompressor = lzma.LZMADecompressor()
    else:
      assert self._compression_type == CompressionTypes.GZIP
      self._decompressor = zlib.decompressobj(self._gzip_mask)

  def _initialize_compressor(self):
    if self._compression_type == CompressionTypes.BZIP2:
      self._compressor = bz2.BZ2Compressor()
    elif self._compression_type == CompressionTypes.DEFLATE:
      self._compressor = zlib.compressobj(
          zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED)
    elif self._compression_type == CompressionTypes.ZSTD:
      self._compressor = zstandard.ZstdCompressor().compressobj()
    elif self._compression_type == CompressionTypes.LZMA:
      self._compressor = lzma.LZMACompressor()
    else:
      assert self._compression_type == CompressionTypes.GZIP
      self._compressor = zlib.compressobj(
          zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, self._gzip_mask)

  def readable(self) -> bool:
    mode = self._file.mode
    return 'r' in mode or 'a' in mode

  def writeable(self) -> bool:
    mode = self._file.mode
    return 'w' in mode or 'a' in mode

  def write(self, data: bytes) -> None:
    """Write data to file."""
    if not self._compressor:
      raise ValueError('compressor not initialized')
    self._uncompressed_position += len(data)
    compressed = self._compressor.compress(data)
    if compressed:
      self._file.write(compressed)

  def _fetch_to_internal_buffer(self, num_bytes: int) -> None:
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

    assert self._decompressor
    while not self._read_eof and (self._read_buffer.tell() -
                                  self._read_position) < num_bytes:
      # Continue reading from the underlying file object until enough bytes are
      # available, or EOF is reached.
      if not self._decompressor.unused_data:
        buf = self._file.read(self._read_size)
      else:
        # Any uncompressed data at the end of the stream of a gzip or bzip2
        # file that is not corrupted points to a concatenated compressed
        # file. We read concatenated files by recursively creating decompressor
        # objects for the unused compressed data.
        buf = self._decompressor.unused_data
        self._initialize_decompressor()
      if buf:
        decompressed = self._decompressor.decompress(buf)
        del buf  # Free up some possibly large and no-longer-needed memory.
        self._read_buffer.write(decompressed)
      else:
        # EOF of current stream reached.
        if (self._compression_type == CompressionTypes.BZIP2 or
            self._compression_type == CompressionTypes.DEFLATE or
            self._compression_type == CompressionTypes.ZSTD or
            self._compression_type == CompressionTypes.GZIP or
            self._compression_type == CompressionTypes.LZMA):
          pass
        else:
          # Deflate, Gzip and bzip2 formats do not require flushing
          # remaining data in the decompressor into the read buffer when
          # fully decompressing files.
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

  def read(self, num_bytes: Optional[int] = None) -> bytes:
    if not self._decompressor:
      raise ValueError('decompressor not initialized')

    self._fetch_to_internal_buffer(num_bytes)
    return self._read_from_internal_buffer(
        lambda: self._read_buffer.read(num_bytes))

  def readline(self) -> bytes:
    """Equivalent to standard file.readline(). Same return conventions apply."""
    if not self._decompressor:
      raise ValueError('decompressor not initialized')

    bytes_io = io.BytesIO()
    while True:
      # Ensure that the internal buffer has at least half the read_size. Going
      # with half the _read_size (as opposed to a full _read_size) to ensure
      # that actual fetches are more evenly spread out, as opposed to having 2
      # consecutive reads at the beginning of a read.
      self._fetch_to_internal_buffer(self._read_size // 2)
      line = self._read_from_internal_buffer(
          lambda: self._read_buffer.readline())
      bytes_io.write(line)
      if line.endswith(b'\n') or not line:
        break  # Newline or EOF reached.

    return bytes_io.getvalue()

  def closed(self) -> bool:
    return not self._file or self._file.closed

  def close(self) -> None:
    if self.readable():
      self._read_buffer.close()

    if self.writeable():
      assert self._compressor
      self._file.write(self._compressor.flush())

    self._file.close()

  def flush(self) -> None:
    if self.writeable():
      assert self._compressor
      self._file.write(self._compressor.flush())
    self._file.flush()

  @property
  def seekable(self) -> bool:
    return 'r' in self._file.mode

  def _clear_read_buffer(self) -> None:
    """Clears the read buffer by removing all the contents and
    resetting _read_position to 0"""
    self._read_position = 0
    self._read_buffer.seek(0)
    self._read_buffer.truncate(0)

  def _rewind_file(self) -> None:
    """Seeks to the beginning of the input file. Input file's EOF marker
    is cleared and _uncompressed_position is reset to zero"""
    self._file.seek(0, os.SEEK_SET)
    self._read_eof = False
    self._uncompressed_position = 0

  def _rewind(self) -> None:
    """Seeks to the beginning of the input file and resets the internal read
    buffer. The decompressor object is re-initialized to ensure that no data
    left in it's buffer."""
    self._clear_read_buffer()
    self._rewind_file()

    # Re-initialize decompressor to clear any data buffered prior to rewind
    self._initialize_decompressor()

  def seek(self, offset: int, whence: int = os.SEEK_SET) -> None:
    """Set the file's current offset.

    Seeking behavior:

      * seeking from the end :data:`os.SEEK_END` the whole file is decompressed
        once to determine its size. Therefore it is preferred to use
        :data:`os.SEEK_SET` or :data:`os.SEEK_CUR` to avoid the processing
        overhead
      * seeking backwards from the current position rewinds the file to ``0``
        and decompresses the chunks to the requested offset
      * seeking is only supported in files opened for reading
      * if the new offset is out of bound, it is adjusted to either ``0`` or
        ``EOF``.

    Args:
      offset (int): seek offset in the uncompressed content represented as
        number
      whence (int): seek mode. Supported modes are :data:`os.SEEK_SET`
        (absolute seek), :data:`os.SEEK_CUR` (seek relative to the current
        position), and :data:`os.SEEK_END` (seek relative to the end, offset
        should be negative).

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
        logger.warning(
            "Seeking relative from end of file is requested. "
            "Need to decompress the whole file once to determine "
            "its size. This might take a while...")
        uncompress_start_time = time.time()
        while self.read(self._read_size):
          pass
        uncompress_end_time = time.time()
        logger.warning(
            "Full file decompression for seek "
            "from end took %.2f secs",
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

  def tell(self) -> int:
    """Returns current position in uncompressed file."""
    return self._uncompressed_position

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.close()


class FileMetadata(object):
  """Metadata about a file path that is the output of FileSystem.match.

  Fields:
    path: [Required] file path.
    size_in_bytes: [Required] file size in bytes.
    last_updated_in_seconds: [Optional] last modified timestamp of the file, or
    valued 0.0 if not specified.
  """
  def __init__(
      self,
      path: str,
      size_in_bytes: int,
      last_updated_in_seconds: float = 0.0):
    assert isinstance(path, str) and path, "Path should be a string"
    assert isinstance(size_in_bytes, int) and size_in_bytes >= 0, \
        "Invalid value for size_in_bytes should %s (of type %s)" % (
            size_in_bytes, type(size_in_bytes))
    self.path = path
    self.size_in_bytes = size_in_bytes
    self.last_updated_in_seconds = last_updated_in_seconds

  def __eq__(self, other):
    """Note: This is only used in tests where we verify that mock objects match.
    """
    return (
        isinstance(other, FileMetadata) and self.path == other.path and
        self.size_in_bytes == other.size_in_bytes and
        self.last_updated_in_seconds == other.last_updated_in_seconds)

  def __hash__(self):
    return hash((self.path, self.size_in_bytes, self.last_updated_in_seconds))

  def __repr__(self):
    if self.last_updated_in_seconds == 0.0:
      return 'FileMetadata(%s, %s)' % (self.path, self.size_in_bytes)
    else:
      return 'FileMetadata(%s, %s, %s)' % (
          self.path, self.size_in_bytes, self.last_updated_in_seconds)


class MatchResult(object):
  """Result from the ``FileSystem`` match operation which contains the list
   of matched ``FileMetadata``.
  """
  def __init__(self, pattern: str, metadata_list: List[FileMetadata]) -> None:
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
    super().__init__(message)
    self.exception_details = exception_details


class FileSystem(BeamPlugin, metaclass=abc.ABCMeta):
  """A class that defines the functions that can be performed on a filesystem.

  All methods are abstract and they are for file system providers to
  implement. Clients should use the FileSystems class to interact with
  the correct file system based on the provided file pattern scheme.
  """
  CHUNK_SIZE = 1  # Chuck size in the batch operations

  def __init__(self, pipeline_options):
    """
    Args:
      pipeline_options: Instance of ``PipelineOptions`` or dict of options and
        values (like ``RuntimeValueProvider.runtime_options``).
    """

  @staticmethod
  def _get_compression_type(path, compression_type):
    if compression_type == CompressionTypes.AUTO:
      compression_type = CompressionTypes.detect_compression_type(path)
    elif not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError(
          'compression_type must be CompressionType object but '
          'was %s' % type(compression_type))
    return compression_type

  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    raise NotImplementedError

  @abc.abstractmethod
  def join(self, basepath: str, *paths: str) -> str:
    """Join two or more pathname components for the filesystem

    Args:
      basepath: string path of the first component of the path
      paths: path components to be added

    Returns: full path after combining all the passed components
    """
    raise NotImplementedError

  @abc.abstractmethod
  def split(self, path: str) -> Tuple[str, str]:
    """Splits the given path into two parts.

    Splits the path into a pair (head, tail) such that tail contains the last
    component of the path and head contains everything up to that.

    For file-systems other than the local file-system, head should include the
    prefix.

    Args:
      path: path as a string
    Returns:
      a pair of path components as strings.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def mkdirs(self, path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError: if leaf directory already exists.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def has_dirs(self):
    """Whether this FileSystem supports directories."""
    raise NotImplementedError

  @abc.abstractmethod
  def _list(self, dir_or_prefix):
    """List files in a location.

    Listing is non-recursive (for filesystems that support directories).

    Args:
      dir_or_prefix: (string) A directory or location prefix (for filesystems
        that don't have directories).

    Returns:
      Generator of ``FileMetadata`` objects.

    Raises:
      ``BeamIOError``: if listing fails, but not if no files were found.
    """
    raise NotImplementedError

  @staticmethod
  def _split_scheme(url_or_path):
    match = re.match(r'(^[a-z]+)://(.*)', url_or_path)
    if match is not None:
      return match.groups()
    return None, url_or_path

  @staticmethod
  def _combine_scheme(scheme, path):
    if scheme is None:
      return path
    return '{}://{}'.format(scheme, path)

  def _url_dirname(self, url_or_path):
    """Like posixpath.dirname, but preserves scheme:// prefix.

    Args:
      url_or_path: A string in the form of scheme://some/path OR /some/path.
    """
    scheme, path = self._split_scheme(url_or_path)
    return self._combine_scheme(scheme, posixpath.dirname(path))

  def match_files(self, file_metas: List[FileMetadata],
                  pattern: str) -> Iterator[FileMetadata]:
    """Filter :class:`FileMetadata` objects by *pattern*

    Args:
      file_metas (list of :class:`FileMetadata`):
        Files to consider when matching
      pattern (str): File pattern

    See Also:
      :meth:`translate_pattern`

    Returns:
      Generator of matching :class:`FileMetadata`
    """
    re_pattern = re.compile(self.translate_pattern(pattern))
    match = re_pattern.match
    for file_metadata in file_metas:
      if match(file_metadata.path):
        yield file_metadata

  @staticmethod
  def translate_pattern(pattern: str) -> str:
    """
    Translate a *pattern* to a regular expression.
    There is no way to quote meta-characters.

    Pattern syntax:
      The pattern syntax is based on the fnmatch_ syntax, with the following
      differences:

      -   ``*`` Is equivalent to ``[^/\\]*`` rather than ``.*``.
      -   ``**`` Is equivalent to ``.*``.

    See also:
      :meth:`match` uses this method

    This method is based on `Python 2.7's fnmatch.translate`_.
    The code in this method is licensed under
    PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2.

    .. _`fnmatch`: https://docs.python.org/2/library/fnmatch.html

    .. _`Python 2.7's fnmatch.translate`: https://github.com/python/cpython\
/blob/170ea8ccd4235d28538ab713041502d07ad1cacd/Lib/fnmatch.py#L85-L120
    """
    i, n = 0, len(pattern)
    res = ''
    while i < n:
      c = pattern[i]
      i = i + 1
      if c == '*':
        # One char lookahead for "**"
        if i < n and pattern[i] == "*":
          res = res + '.*'
          i = i + 1
        else:
          res = res + r'[^/\\]*'
      elif c == '?':
        res = res + '.'
      elif c == '[':
        j = i
        if j < n and pattern[j] == '!':
          j = j + 1
        if j < n and pattern[j] == ']':
          j = j + 1
        while j < n and pattern[j] != ']':
          j = j + 1
        if j >= n:
          res = res + r'\['
        else:
          stuff = pattern[i:j].replace('\\', '\\\\')
          i = j + 1
          if stuff[0] == '!':
            stuff = '^' + stuff[1:]
          elif stuff[0] == '^':
            stuff = '\\' + stuff
          res = '%s[%s]' % (res, stuff)
      else:
        res = res + re.escape(c)

    logger.debug('translate_pattern: %r -> %r', pattern, res)
    return r'(?ms)' + res + r'\Z'

  def match(self, patterns, limits=None):
    """Find all matching paths to the patterns provided.

    See Also:
      :meth:`translate_pattern`

    Patterns ending with '/' or '\\' will be appended with '*'.

    Args:
      patterns: list of string for the file path pattern to match against
      limits: list of maximum number of responses that need to be fetched

    Returns: list of ``MatchResult`` objects.

    Raises:
      ``BeamIOError``: if any of the pattern match operations fail
    """
    if limits is None:
      limits = [None] * len(patterns)
    else:
      err_msg = "Patterns and limits should be equal in length"
      assert len(patterns) == len(limits), err_msg

    def _match(pattern, limit):
      """Find all matching paths to the pattern provided."""
      if pattern.endswith('/') or pattern.endswith('\\'):
        pattern += '*'
      # Get the part of the pattern before the first globbing character.
      # For example scheme://path/foo* will become scheme://path/foo for
      # filesystems like GCS, or converted to scheme://path for filesystems with
      # directories.
      prefix_or_dir = re.match('^[^[*?]*', pattern).group(0)

      file_metadatas = []
      if prefix_or_dir == pattern:
        # Short-circuit calling self.list() if there's no glob pattern to match.
        if self.exists(pattern):
          file_metadatas = [self.metadata(pattern)]
      else:
        if self.has_dirs():
          prefix_dirname = self._url_dirname(prefix_or_dir)
          if not prefix_dirname == prefix_or_dir:
            logger.debug(
                "Changed prefix_or_dir %r -> %r", prefix_or_dir, prefix_dirname)
            prefix_or_dir = prefix_dirname

        logger.debug("Listing files in %r", prefix_or_dir)
        file_metadatas = self._list(prefix_or_dir)

      metadata_list = []
      for file_metadata in self.match_files(file_metadatas, pattern):
        if limit is not None and len(metadata_list) >= limit:
          break
        metadata_list.append(file_metadata)

      return MatchResult(pattern, metadata_list)

    exceptions = {}
    result = []
    for pattern, limit in zip(patterns, limits):
      try:
        result.append(_match(pattern, limit))
      except Exception as e:  # pylint: disable=broad-except
        exceptions[pattern] = e

    if exceptions:
      raise BeamIOError("Match operation failed", exceptions)
    return result

  @abc.abstractmethod
  def create(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO) -> BinaryIO:
    """Returns a write channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    raise NotImplementedError

  @abc.abstractmethod
  def open(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO) -> BinaryIO:
    """Returns a read channel for the given file path.

    Args:
      path: string path of the file object to be read
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
      ``BeamIOError``: if any of the copy operations fail
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
      ``BeamIOError``: if any of the rename operations fail
    """
    raise NotImplementedError

  @abc.abstractmethod
  def exists(self, path: str) -> bool:
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    raise NotImplementedError

  @abc.abstractmethod
  def size(self, path: str) -> int:
    """Get size in bytes of a file on the FileSystem.

    Args:
      path: string filepath of file.

    Returns: int size of file according to the FileSystem.

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def last_updated(self, path):
    """Get UNIX Epoch time in seconds on the FileSystem.

    Args:
      path: string path of file.

    Returns: float UNIX Epoch time

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    raise NotImplementedError

  def checksum(self, path):
    """Fetch checksum metadata of a file on the
    :class:`~apache_beam.io.filesystem.FileSystem`.

    This operation returns checksum metadata as stored in the underlying
    FileSystem. It should not need to read file data to obtain this value.
    Checksum type and format are FileSystem dependent and are not compatible
    between FileSystems.
    FileSystem implementations may return file size if a checksum isn't
    available.

    Args:
      path: string path of a file.

    Returns: string containing checksum

    Raises:
      ``BeamIOError``: if path isn't a file or doesn't exist.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def metadata(self, path):
    """Fetch metadata of a file on the
    :class:`~apache_beam.io.filesystem.FileSystem`.

    This operation returns metadata as stored in the underlying
    FileSystem. It should not need to read file data to obtain this value.
    For web based file systems, this method should also incur as few as
    possible requests.

    Args:
      path: string path of a file.

    Returns:
      :class:`~apache_beam.io.filesystem.FileMetadata`.

    Raises:
      ``BeamIOError``: if path isn't a file or doesn't exist.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def delete(self, paths):
    """Deletes files or directories at the provided paths.
    Directories will be deleted recursively.

    Args:
      paths: list of paths that give the file objects to be deleted

    Raises:
      ``BeamIOError``: if any of the delete operations fail
    """
    raise NotImplementedError

  class LineageLevel:
    FILE = 'FILE'
    TOP_LEVEL = 'TOP_LEVEL'

  def report_lineage(self, path, unused_lineage, level=None):
    """
    Report Lineage metrics for path.

    Unless override by FileSystem implementations, default to no-op.
    """
    pass

  def check_splittability(self, path):
    return True
