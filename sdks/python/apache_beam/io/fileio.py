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
"""File-based sources and sinks."""

from __future__ import absolute_import

import glob
import logging
from multiprocessing.pool import ThreadPool
import os
import re
import shutil
import threading
import time
import zlib
import weakref

from apache_beam import coders
from apache_beam.io import iobase
from apache_beam.io import range_trackers

__all__ = ['TextFileSource', 'TextFileSink']

DEFAULT_SHARD_NAME_TEMPLATE = '-SSSSS-of-NNNNN'


class _CompressionType(object):
  """Object representing single compression type."""

  def __init__(self, identifier):
    self.identifier = identifier

  def __eq__(self, other):
    return (isinstance(other, _CompressionType) and
            self.identifier == other.identifier)

  def __hash__(self):
    return hash(self.identifier)

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    return '_CompressionType(%s)' % self.identifier


class CompressionTypes(object):
  """Enum-like class representing known compression types."""

  # Detect compression based on filename extension.
  #
  # The following extensions are currently recognized by auto-detection:
  #   .gz (implies GZIP as described below)
  #   .z  (implies ZLIB as described below).
  # Any non-recognized extension implies UNCOMPRESSED as described below.
  AUTO = _CompressionType('auto')

  # GZIP compression (deflate with GZIP headers).
  GZIP = _CompressionType('gzip')

  # ZLIB compression (deflate with ZLIB headers).
  ZLIB = _CompressionType('zlib')

  # Uncompressed (i.e., may be split).
  UNCOMPRESSED = _CompressionType('uncompressed')

  # TODO: Remove this backwards-compatibility soon.
  # Deprecated. Use UNCOMPRESSED instead.
  NO_COMPRESSION = UNCOMPRESSED

  @classmethod
  def is_valid_compression_type(cls, compression_type):
    """Returns true for valid compression types, false otherwise."""
    return isinstance(compression_type, _CompressionType)

  @classmethod
  def mime_type(cls, compression_type, default='application/octet-stream'):
    mime_types_by_compression_type = {
        cls.GZIP: 'application/x-gzip',
        cls.ZLIB: 'application/octet-stream'
    }
    return mime_types_by_compression_type.get(compression_type, default)

  @classmethod
  def detect_compression_type(cls, file_path):
    """Returns the compression type of a file (based on its suffix)"""
    compression_types_by_suffix = {'.gz': cls.GZIP, '.z': cls.ZLIB}
    lowercased_path = file_path.lower()
    for suffix, compression_type in compression_types_by_suffix.iteritems():
      if lowercased_path.endswith(suffix):
        return compression_type
    return cls.UNCOMPRESSED


class NativeFileSource(iobase.NativeSource):
  """A source implemented by Dataflow service from a GCS or local file or files.

  This class is to be only inherited by sources natively implemented by Cloud
  Dataflow service, hence should not be sub-classed by users.
  """

  def __init__(self,
               file_path,
               start_offset=None,
               end_offset=None,
               coder=coders.BytesCoder(),
               compression_type=CompressionTypes.AUTO,
               mime_type='application/octet-stream'):
    """Initialize a NativeFileSource.

    Args:
      file_path: The file path to read from as a local file path or a GCS
        gs:// path. The path can contain glob characters (*, ?, and [...]
        sets).
      start_offset: The byte offset in the source file that the reader
        should start reading. By default is 0 (beginning of file).
      end_offset: The byte offset in the file that the reader should stop
        reading. By default it is the end of the file.
      compression_type: Used to handle compressed input files. Typical value
          is CompressionTypes.AUTO, in which case the file_path's extension will
          be used to detect the compression.
      coder: Coder used to decode each record.

    Raises:
      TypeError: if file_path is not a string.

    If the file_path contains glob characters then the start_offset and
    end_offset must not be specified.

    The 'start_offset' and 'end_offset' pair provide a mechanism to divide the
    file into multiple pieces for individual sources. Because the offset
    is measured by bytes, some complication arises when the offset splits in
    the middle of a record. To avoid the scenario where two adjacent sources
    each get a fraction of a line we adopt the following rules:

    If start_offset falls inside a record (any character except the first one)
    then the source will skip the record and start with the next one.

    If end_offset falls inside a record (any character except the first one)
    then the source will contain that entire record.
    """
    if not isinstance(file_path, basestring):
      raise TypeError('%s: file_path must be a string;  got %r instead' %
                      (self.__class__.__name__, file_path))

    self.file_path = file_path
    self.start_offset = start_offset
    self.end_offset = end_offset
    self.compression_type = compression_type
    self.coder = coder
    self.mime_type = mime_type

  def __eq__(self, other):
    # TODO: Remove this backwards-compatibility soon.
    def equiv_autos(lhs, rhs):
      return ((lhs == 'AUTO' and rhs == CompressionTypes.AUTO) or
              (lhs == CompressionTypes.AUTO and rhs == 'AUTO'))

    return (self.file_path == other.file_path and
            self.start_offset == other.start_offset and
            self.end_offset == other.end_offset and
            (self.compression_type == other.compression_type or
             equiv_autos(self.compression_type, other.compression_type)) and
            self.coder == other.coder and self.mime_type == other.mime_type)

  @property
  def path(self):
    return self.file_path

  def reader(self):
    return NativeFileSourceReader(self)


class NativeFileSourceReader(iobase.NativeSourceReader,
                             coders.observable.ObservableMixin):
  """The source reader for a NativeFileSource.

  This class is to be only inherited by source readers natively implemented by
  Cloud Dataflow service, hence should not be sub-classed by users.
  """

  def __init__(self, source):
    super(NativeFileSourceReader, self).__init__()
    self.source = source
    self.start_offset = self.source.start_offset or 0
    self.end_offset = self.source.end_offset
    self.current_offset = self.start_offset

  def __enter__(self):
    self.file = ChannelFactory.open(
        self.source.file_path,
        'rb',
        mime_type=self.source.mime_type,
        compression_type=self.source.compression_type)

    # Determine the real end_offset.
    #
    # If not specified or if the source is not splittable it will be the length
    # of the file (or infinity for compressed files) as appropriate.
    if ChannelFactory.is_compressed(self.file):
      if not isinstance(self.source, TextFileSource):
        raise ValueError('Unexpected compressed file for a non-TextFileSource.')
      self.end_offset = range_trackers.OffsetRangeTracker.OFFSET_INFINITY

    elif self.end_offset is None:
      self.file.seek(0, os.SEEK_END)
      self.end_offset = self.file.tell()
      self.file.seek(self.start_offset)

    # Initializing range tracker after self.end_offset is finalized.
    self.range_tracker = range_trackers.OffsetRangeTracker(self.start_offset,
                                                           self.end_offset)

    # Position to the appropriate start_offset.
    if self.start_offset > 0:
      if ChannelFactory.is_compressed(self.file):
        # TODO: Turns this warning into an exception soon.
        logging.warning(
            'Encountered initial split starting at (%s) for compressed source.',
            self.start_offset)
    self.seek_to_true_start_offset()

    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.file.close()

  def __iter__(self):
    if self.current_offset > 0 and ChannelFactory.is_compressed(self.file):
      # When compression is enabled both initial and dynamic splitting should be
      # prevented. Here we prevent initial splitting by ignoring all splits
      # other than the split that starts at byte 0.
      #
      # TODO: Turns this warning into an exception soon.
      logging.warning('Ignoring split starting at (%s) for compressed source.',
                      self.current_offset)
      return

    while True:
      if not self.range_tracker.try_claim(record_start=self.current_offset):
        # Reader has completed reading the set of records in its range. Note
        # that the end offset of the range may be smaller than the original
        # end offset defined when creating the reader due to reader accepting
        # a dynamic split request from the service.
        return

      # Note that for compressed sources, delta_offsets are virtual and don't
      # actually correspond to byte offsets in the underlying file. They
      # nonetheless correspond to unique virtual position locations.
      for eof, record, delta_offset in self.read_records():
        if eof:
          # Can't read from this source anymore and the record and delta_offset
          # are non-sensical; hence we are done.
          return
        else:
          self.notify_observers(record, is_encoded=False)
          self.current_offset += delta_offset
          yield record

  def seek_to_true_start_offset(self):
    """Seeks the underlying file to the appropriate start_offset that is
       compatible with range-tracking and position models and updates
       self.current_offset accordingly.
    """
    raise NotImplementedError

  def read_records(self):
    """
      Yields information about (possibly multiple) records corresponding to
      self.current_offset

      If a read_records() invocation returns multiple results, the first record
      must be at a split point and other records should not be at split points.
      The first record is assumed to be at self.current_offset and the caller
      should use the yielded delta_offsets to update self.current_offset
      accordingly.

      The yielded value is a tripplet of the form:
        eof, record, delta_offset
      eof: A boolean indicating whether eof has been reached, in which case
        the contents of record and delta_offset cannot be trusted or used.
      record: The (possibly decoded) record (ie payload) read from the
        underlying source.
      delta_offset: The delta_offfset (from self.current_offset) in bytes, that
        has been consumed from the underlying source, to the starting position
        of the next record (or EOF if no record exists).
    """
    raise NotImplementedError

  def get_progress(self):
    return iobase.ReaderProgress(position=iobase.ReaderPosition(
        byte_offset=self.range_tracker.last_record_start))

  def request_dynamic_split(self, dynamic_split_request):
    if ChannelFactory.is_compressed(self.file):
      # When compression is enabled both initial and dynamic splitting should be
      # prevented. Here we prevent dynamic splitting by ignoring all dynamic
      # split requests at the reader.
      #
      # TODO: Turns this warning into an exception soon.
      logging.warning('FileBasedReader cannot be split since it is compressed. '
                      'Requested: %r', dynamic_split_request)
      return

    assert dynamic_split_request is not None
    progress = dynamic_split_request.progress
    split_position = progress.position
    if split_position is None:
      percent_complete = progress.percent_complete
      if percent_complete is not None:
        if percent_complete <= 0 or percent_complete >= 1:
          logging.warning(
              'FileBasedReader cannot be split since the provided percentage '
              'of work to be completed is out of the valid range (0, '
              '1). Requested: %r', dynamic_split_request)
          return
        split_position = iobase.ReaderPosition()
        split_position.byte_offset = (
            self.range_tracker.position_at_fraction(percent_complete))
      else:
        logging.warning(
            'FileBasedReader requires either a position or a percentage of '
            'work to be complete to perform a dynamic split request. '
            'Requested: %r', dynamic_split_request)
        return

    if self.range_tracker.try_split(split_position.byte_offset):
      return iobase.DynamicSplitResultWithPosition(split_position)
    else:
      return

# -----------------------------------------------------------------------------
# TextFileSource, TextFileSink.


class TextFileSource(NativeFileSource):
  """A source for a GCS or local text file.

  Parses a text file as newline-delimited elements, by default assuming
  UTF-8 encoding.

  This implementation has only been tested to read text encoded using UTF-8 or
  ASCII. This has not been tested for other encodings such as UTF-16 or UTF-32.
  """

  def __init__(self,
               file_path,
               start_offset=None,
               end_offset=None,
               compression_type=CompressionTypes.AUTO,
               strip_trailing_newlines=True,
               coder=coders.StrUtf8Coder(),
               mime_type='text/plain'):
    """Initialize a TextSource.

    Args:
      file_path: The file path to read from as a local file path or a GCS
        gs:// path. The path can contain glob characters (*, ?, and [...]
        sets).
      start_offset: The byte offset in the source text file that the reader
        should start reading. By default is 0 (beginning of file).
      end_offset: The byte offset in the file that the reader should stop
        reading. By default it is the end of the file.
      compression_type: Used to handle compressed input files. Typical value
          is CompressionTypes.AUTO, in which case the file_path's extension will
          be used to detect the compression.
      strip_trailing_newlines: Indicates whether this source should remove
          the newline char in each line it reads before decoding that line.
          This feature only works for ASCII and UTF-8 encoded input.
      coder: Coder used to decode each line.

    Raises:
      TypeError: if file_path is not a string.

    If the file_path contains glob characters then the start_offset and
    end_offset must not be specified.

    The 'start_offset' and 'end_offset' pair provide a mechanism to divide the
    text file into multiple pieces for individual sources. Because the offset
    is measured by bytes, some complication arises when the offset splits in
    the middle of a text line. To avoid the scenario where two adjacent sources
    each get a fraction of a line we adopt the following rules:

    If start_offset falls inside a line (any character except the first one)
    then the source will skip the line and start with the next one.

    If end_offset falls inside a line (any character except the first one) then
    the source will contain that entire line.
    """
    super(TextFileSource, self).__init__(
        file_path,
        start_offset=start_offset,
        end_offset=end_offset,
        coder=coder,
        compression_type=compression_type)
    self.strip_trailing_newlines = strip_trailing_newlines

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'text'

  def __eq__(self, other):
    return (super(TextFileSource, self).__eq__(other) and
            self.strip_trailing_newlines == other.strip_trailing_newlines)

  def reader(self):
    # If a multi-file pattern was specified as a source then make sure the
    # start/end offsets use the default values for reading the entire file.
    if re.search(r'[*?\[\]]', self.file_path) is not None:
      if self.start_offset is not None:
        raise ValueError(
            'start offset cannot be specified for a multi-file source: '
            '%s' % self.file_path)
      if self.end_offset is not None:
        raise ValueError(
            'End offset cannot be specified for a multi-file source: '
            '%s' % self.file_path)
      return TextMultiFileReader(self)
    else:
      return TextFileReader(self)


class ChannelFactory(object):
  # TODO: Generalize into extensible framework.

  @staticmethod
  def mkdir(path):
    if path.startswith('gs://'):
      return
    else:
      try:
        os.makedirs(path)
      except OSError as err:
        raise IOError(err)

  @staticmethod
  def open(path,
           mode,
           mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    if compression_type == CompressionTypes.AUTO:
      compression_type = CompressionTypes.detect_compression_type(path)
    elif compression_type == 'AUTO':
      # TODO: Remove this backwards-compatibility soon.
      compression_type = CompressionTypes.detect_compression_type(path)
    else:
      if not CompressionTypes.is_valid_compression_type(compression_type):
        raise TypeError('compression_type must be CompressionType object but '
                        'was %s' % type(compression_type))

    if path.startswith('gs://'):
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      raw_file = gcsio.GcsIO().open(
          path,
          mode,
          mime_type=CompressionTypes.mime_type(compression_type, mime_type))
    else:
      raw_file = open(path, mode)

    if compression_type == CompressionTypes.UNCOMPRESSED:
      return raw_file
    else:
      return _CompressedFile(raw_file, compression_type=compression_type)

  @staticmethod
  def is_compressed(fileobj):
    return isinstance(fileobj, _CompressedFile)

  @staticmethod
  def rename(src, dst):
    if src.startswith('gs://'):
      assert dst.startswith('gs://'), dst
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      gcsio.GcsIO().rename(src, dst)
    else:
      try:
        os.rename(src, dst)
      except OSError as err:
        raise IOError(err)

  @staticmethod
  def copytree(src, dst):
    if src.startswith('gs://'):
      assert dst.startswith('gs://'), dst
      assert src.endswith('/'), src
      assert dst.endswith('/'), dst
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      gcsio.GcsIO().copytree(src, dst)
    else:
      try:
        if os.path.exists(dst):
          shutil.rmtree(dst)
        shutil.copytree(src, dst)
      except OSError as err:
        raise IOError(err)

  @staticmethod
  def exists(path):
    if path.startswith('gs://'):
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      return gcsio.GcsIO().exists(path)
    else:
      return os.path.exists(path)

  @staticmethod
  def rmdir(path):
    if path.startswith('gs://'):
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      gcs = gcsio.GcsIO()
      if not path.endswith('/'):
        path += '/'
      # TODO: Threadpool?
      for entry in gcs.glob(path + '*'):
        gcs.delete(entry)
    else:
      try:
        shutil.rmtree(path)
      except OSError as err:
        raise IOError(err)

  @staticmethod
  def rm(path):
    if path.startswith('gs://'):
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      gcsio.GcsIO().delete(path)
    else:
      try:
        os.remove(path)
      except OSError as err:
        raise IOError(err)

  @staticmethod
  def glob(path):
    if path.startswith('gs://'):
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      return gcsio.GcsIO().glob(path)
    else:
      return glob.glob(path)

  @staticmethod
  def size_in_bytes(path):
    """Returns the size of a file in bytes.

    Args:
      path: a string that gives the path of a single file.
    """
    if path.startswith('gs://'):
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      return gcsio.GcsIO().size(path)
    else:
      return os.path.getsize(path)


class _CompressedFile(object):
  """Somewhat limited file wrapper for easier handling of compressed files."""
  _type_mask = {
      CompressionTypes.GZIP: zlib.MAX_WBITS | 16,
      CompressionTypes.ZLIB: zlib.MAX_WBITS,
  }

  def __init__(self,
               fileobj,
               compression_type=CompressionTypes.GZIP,
               read_size=16384):
    if not fileobj:
      raise ValueError('fileobj must be opened file but was %s' % fileobj)
    self._validate_compression_type(compression_type)

    self._file = fileobj
    self._data = ''
    self._read_size = read_size
    self._compression_type = compression_type

    if self._readable():
      self._decompressor = zlib.decompressobj(self._type_mask[compression_type])
    else:
      self._decompressor = None

    if self._writeable():
      self._compressor = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION,
                                          zlib.DEFLATED,
                                          self._type_mask[compression_type])
    else:
      self._compressor = None

  def _validate_compression_type(self, compression_type):
    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    if (compression_type == CompressionTypes.AUTO or
        compression_type == CompressionTypes.UNCOMPRESSED):
      raise ValueError(
          'cannot create object with unspecified or no compression')

  def _readable(self):
    mode = self._file.mode
    return 'r' in mode or 'a' in mode

  def _writeable(self):
    mode = self._file.mode
    return 'w' in mode or 'a' in mode

  def write(self, data):
    """Write data to file."""
    if not self._compressor:
      raise ValueError('compressor not initialized')
    compressed = self._compressor.compress(data)
    if compressed:
      self._file.write(compressed)

  def _fetch_to_internal_buffer(self, num_bytes):
    """Fetch up to num_bytes into the internal buffer."""
    while len(self._data) < num_bytes:
      buf = self._file.read(self._read_size)
      if buf:
        self._data += self._decompressor.decompress(buf)
      else:
        # EOF reached, flush.
        self._data += self._decompressor.flush()
        return

  def _read_from_internal_buffer(self, num_bytes):
    """Read up to num_bytes from the internal buffer."""
    result = self._data[:num_bytes]
    self._data = self._data[num_bytes:]
    return result

  def read(self, num_bytes):
    if not self._decompressor:
      raise ValueError('decompressor not initialized')
    self._fetch_to_internal_buffer(num_bytes)
    return self._read_from_internal_buffer(num_bytes)

  def readline(self):
    """Equivalent to standard file.readline(). Same return conventions apply."""
    if not self._decompressor:
      raise ValueError('decompressor not initialized')
    result = ''
    while True:
      self._fetch_to_internal_buffer(self._read_size)
      if not self._data:
        break  # EOF reached.
      index = self._data.find('\n')
      if index == -1:
        result += self._read_from_internal_buffer(len(self._data))
      else:
        result += self._read_from_internal_buffer(index + 1)
        break  # Newline reached.
    return result

  @property
  def closed(self):
    return not self._file or self._file.closed()

  def close(self):
    if self._file is None:
      return

    if self._writeable():
      self._file.write(self._compressor.flush())
    self._file.close()

  def flush(self):
    if self._writeable():
      self._file.write(self._compressor.flush())
    self._file.flush()

  # TODO: Add support for seeking to a file position.
  @property
  def seekable(self):
    return False


class FileSink(iobase.Sink):
  """A sink to a GCS or local files.

  To implement a file-based sink, extend this class and override
  either ``write_record()`` or ``write_encoded_record()``.

  If needed, also overwrite ``open()`` and/or ``close()`` to customize the
  file handling or write headers and footers.

  The output of this write is a PCollection of all written shards.
  """

  # Max number of threads to be used for renaming.
  _MAX_RENAME_THREADS = 64

  def __init__(self,
               file_path_prefix,
               coder,
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               mime_type='application/octet-stream',
               compression_type=CompressionTypes.AUTO):
    """
     Raises:
      TypeError: if file path parameters are not a string or if compression_type
        is not member of CompressionTypes.
      ValueError: if shard_name_template is not of expected format.
    """
    if not isinstance(file_path_prefix, basestring):
      raise TypeError('file_path_prefix must be a string; got %r instead' %
                      file_path_prefix)
    if not isinstance(file_name_suffix, basestring):
      raise TypeError('file_name_suffix must be a string; got %r instead' %
                      file_name_suffix)

    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))

    if shard_name_template is None:
      shard_name_template = DEFAULT_SHARD_NAME_TEMPLATE
    elif shard_name_template is '':
      num_shards = 1
    self.file_path_prefix = file_path_prefix
    self.file_name_suffix = file_name_suffix
    self.num_shards = num_shards
    self.coder = coder
    self.shard_name_format = self._template_to_format(shard_name_template)
    self.compression_type = compression_type
    self.mime_type = mime_type

  def open(self, temp_path):
    """Opens ``temp_path``, returning an opaque file handle object.

    The returned file handle is passed to ``write_[encoded_]record`` and
    ``close``.
    """
    return ChannelFactory.open(
        temp_path,
        'wb',
        mime_type=self.mime_type,
        compression_type=self.compression_type)

  def write_record(self, file_handle, value):
    """Writes a single record go the file handle returned by ``open()``.

    By default, calls ``write_encoded_record`` after encoding the record with
    this sink's Coder.
    """
    self.write_encoded_record(file_handle, self.coder.encode(value))

  def write_encoded_record(self, file_handle, encoded_value):
    """Writes a single encoded record to the file handle returned by ``open()``.
    """
    raise NotImplementedError

  def close(self, file_handle):
    """Finalize and close the file handle returned from ``open()``.

    Called after all records are written.

    By default, calls ``file_handle.close()`` iff it is not None.
    """
    if file_handle is not None:
      file_handle.close()

  def initialize_write(self):
    tmp_dir = self.file_path_prefix + self.file_name_suffix + time.strftime(
        '-temp-%Y-%m-%d_%H-%M-%S')
    ChannelFactory().mkdir(tmp_dir)
    return tmp_dir

  def open_writer(self, init_result, uid):
    return FileSinkWriter(self, os.path.join(init_result, uid))

  def finalize_write(self, init_result, writer_results):
    writer_results = sorted(writer_results)
    num_shards = len(writer_results)
    channel_factory = ChannelFactory()
    min_threads = min(num_shards, FileSink._MAX_RENAME_THREADS)
    num_threads = max(1, min_threads)

    rename_ops = []
    for shard_num, shard in enumerate(writer_results):
      final_name = ''.join([
          self.file_path_prefix, self.shard_name_format % dict(
              shard_num=shard_num, num_shards=num_shards), self.file_name_suffix
      ])
      rename_ops.append((shard, final_name))

    logging.info(
        'Starting finalize_write threads with num_shards: %d, num_threads: %d',
        num_shards, num_threads)
    start_time = time.time()

    # Use a thread pool for renaming operations.
    def _rename_file(rename_op):
      """_rename_file executes single (old_name, new_name) rename operation."""
      old_name, final_name = rename_op
      try:
        channel_factory.rename(old_name, final_name)
      except IOError as e:
        # May have already been copied.
        try:
          exists = channel_factory.exists(final_name)
        except Exception as exists_e:  # pylint: disable=broad-except
          logging.warning('Exception when checking if file %s exists: '
                          '%s', final_name, exists_e)
          # Returning original exception after logging the exception from
          # exists() call.
          return (None, e)
        if not exists:
          logging.warning(('IOError in _rename_file. old_name: %s, '
                           'final_name: %s, err: %s'), old_name, final_name, e)
          return (None, e)
      except Exception as e:  # pylint: disable=broad-except
        logging.warning(('Exception in _rename_file. old_name: %s, '
                         'final_name: %s, err: %s'), old_name, final_name, e)
        return (None, e)
      return (final_name, None)

    # ThreadPool crashes in old versions of Python (< 2.7.5) if created from a
    # child thread. (http://bugs.python.org/issue10015)
    if not hasattr(threading.current_thread(), '_children'):
      threading.current_thread()._children = weakref.WeakKeyDictionary()
    rename_results = ThreadPool(num_threads).map(_rename_file, rename_ops)

    for final_name, err in rename_results:
      if err:
        logging.warning('Error when processing rename_results: %s', err)
        raise err
      else:
        yield final_name

    logging.info('Renamed %d shards in %.2f seconds.', num_shards,
                 time.time() - start_time)

    try:
      channel_factory.rmdir(init_result)
    except IOError:
      # May have already been removed.
      pass

  @staticmethod
  def _template_to_format(shard_name_template):
    if not shard_name_template:
      return ''
    m = re.search('S+', shard_name_template)
    if m is None:
      raise ValueError("Shard number pattern S+ not found in template '%s'" %
                       shard_name_template)
    shard_name_format = shard_name_template.replace(
        m.group(0), '%%(shard_num)0%dd' % len(m.group(0)))
    m = re.search('N+', shard_name_format)
    if m:
      shard_name_format = shard_name_format.replace(
          m.group(0), '%%(num_shards)0%dd' % len(m.group(0)))
    return shard_name_format

  def __eq__(self, other):
    # TODO: Clean up workitem_test which uses this.
    # pylint: disable=unidiomatic-typecheck
    return type(self) == type(other) and self.__dict__ == other.__dict__


class FileSinkWriter(iobase.Writer):
  """The writer for FileSink.
  """

  def __init__(self, sink, temp_shard_path):
    self.sink = sink
    self.temp_shard_path = temp_shard_path
    self.temp_handle = self.sink.open(temp_shard_path)

  def write(self, value):
    self.sink.write_record(self.temp_handle, value)

  def close(self):
    self.sink.close(self.temp_handle)
    return self.temp_shard_path


class TextFileSink(FileSink):
  """A sink to a GCS or local text file or files."""

  def __init__(self,
               file_path_prefix,
               file_name_suffix='',
               append_trailing_newlines=True,
               num_shards=0,
               shard_name_template=None,
               coder=coders.ToStringCoder(),
               compression_type=CompressionTypes.AUTO):
    """Initialize a TextFileSink.

    Args:
      file_path_prefix: The file path to write to. The files written will begin
        with this prefix, followed by a shard identifier (see num_shards), and
        end in a common extension, if given by file_name_suffix. In most cases,
        only this argument is specified and num_shards, shard_name_template, and
        file_name_suffix use default values.
      file_name_suffix: Suffix for the files written.
      append_trailing_newlines: indicate whether this sink should write an
        additional newline char after writing each element.
      num_shards: The number of files (shards) used for output. If not set, the
        service will decide on the optimal number of shards.
        Constraining the number of shards is likely to reduce
        the performance of a pipeline.  Setting this value is not recommended
        unless you require a specific number of output files.
      shard_name_template: A template string containing placeholders for
        the shard number and shard count. Currently only '' and
        '-SSSSS-of-NNNNN' are patterns accepted by the service.
        When constructing a filename for a particular shard number, the
        upper-case letters 'S' and 'N' are replaced with the 0-padded shard
        number and shard count respectively.  This argument can be '' in which
        case it behaves as if num_shards was set to 1 and only one file will be
        generated. The default pattern used is '-SSSSS-of-NNNNN'.
      coder: Coder used to encode each line.
      compression_type: Used to handle compressed output files. Typical value
          is CompressionTypes.AUTO, in which case the final file path's
          extension (as determined by file_path_prefix, file_name_suffix,
          num_shards and shard_name_template) will be used to detect the
          compression.

    Returns:
      A TextFileSink object usable for writing.
    """
    super(TextFileSink, self).__init__(
        file_path_prefix,
        file_name_suffix=file_name_suffix,
        num_shards=num_shards,
        shard_name_template=shard_name_template,
        coder=coder,
        mime_type='text/plain',
        compression_type=compression_type)
    self.append_trailing_newlines = append_trailing_newlines

    if type(self) is TextFileSink:
      logging.warning('Direct usage of TextFileSink is deprecated. Please use '
                      '\'textio.WriteToText()\' instead of directly '
                      'instantiating a TextFileSink object.')

  def write_encoded_record(self, file_handle, encoded_value):
    """Writes a single encoded record."""
    file_handle.write(encoded_value)
    if self.append_trailing_newlines:
      file_handle.write('\n')


class NativeFileSink(iobase.NativeSink):
  """A sink implemented by Dataflow service to a GCS or local file or files.

  This class is to be only inherited by sinks natively implemented by Cloud
  Dataflow service, hence should not be sub-classed by users.
  """

  def __init__(self,
               file_path_prefix,
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               validate=True,
               coder=coders.BytesCoder(),
               mime_type='application/octet-stream',
               compression_type=CompressionTypes.AUTO):
    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))

    # We initialize a file_path attribute containing just the prefix part for
    # local runner environment. For now, sharding is not supported in the local
    # runner and sharding options (template, num, suffix) are ignored.
    # The attribute is also used in the worker environment when we just write
    # to a specific file.
    # TODO: Add support for file sharding in the local runner.
    self.file_path = file_path_prefix
    self.coder = coder
    self.file_name_prefix = file_path_prefix
    self.file_name_suffix = file_name_suffix
    self.num_shards = num_shards
    # TODO: Update this when the service supports more patterns.
    self.shard_name_template = ('-SSSSS-of-NNNNN' if shard_name_template is None
                                else shard_name_template)
    # TODO: Implement sink validation.
    self.validate = validate
    self.mime_type = mime_type
    self.compression_type = compression_type

  @property
  def path(self):
    return self.file_path

  def writer(self):
    return NativeFileSinkWriter(self)

  def __eq__(self, other):
    return (self.file_path == other.file_path and self.coder == other.coder and
            self.file_name_prefix == other.file_name_prefix and
            self.file_name_suffix == other.file_name_suffix and
            self.num_shards == other.num_shards and
            self.shard_name_template == other.shard_name_template and
            self.validate == other.validate and
            self.mime_type == other.mime_type and
            self.compression_type == other.compression_type)


class NativeFileSinkWriter(iobase.NativeSinkWriter):
  """The sink writer for a NativeFileSink.

  This class is to be only inherited by sink writers natively implemented by
  Cloud Dataflow service, hence should not be sub-classed by users.
  """

  def __init__(self, sink):
    self.sink = sink

  def __enter__(self):
    self.file = ChannelFactory.open(
        self.sink.file_path,
        'wb',
        mime_type=self.sink.mime_type,
        compression_type=self.sink.compression_type)

    if (ChannelFactory.is_compressed(self.file) and
        not isinstance(self.sink, NativeTextFileSink)):
      raise ValueError(
          'Unexpected compressed file for a non-NativeTextFileSink.')

    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.file.close()

  def Write(self, value):
    self.file.write(self.sink.coder.encode(value))


class NativeTextFileSink(NativeFileSink):
  """A sink to a GCS or local text file or files."""

  def __init__(self,
               file_path_prefix,
               append_trailing_newlines=True,
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               validate=True,
               coder=coders.ToStringCoder(),
               mime_type='text/plain',
               compression_type=CompressionTypes.AUTO):
    super(NativeTextFileSink, self).__init__(
        file_path_prefix,
        file_name_suffix=file_name_suffix,
        num_shards=num_shards,
        shard_name_template=shard_name_template,
        validate=validate,
        coder=coder,
        mime_type=mime_type,
        compression_type=compression_type)
    self.append_trailing_newlines = append_trailing_newlines

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'text'

  def writer(self):
    return TextFileWriter(self)

  def __eq__(self, other):
    return (super(NativeTextFileSink, self).__eq__(other) and
            self.append_trailing_newlines == other.append_trailing_newlines)

# -----------------------------------------------------------------------------
# TextFileReader, TextMultiFileReader.


class TextFileReader(NativeFileSourceReader):
  """A reader for a text file source."""

  def seek_to_true_start_offset(self):
    if ChannelFactory.is_compressed(self.file):
      # When compression is enabled both initial and dynamic splitting should be
      # prevented. Here we don't perform any seeking to a different offset, nor
      # do we update the current_offset so that the rest of the framework can
      # properly deal with compressed files.
      return

    if self.start_offset > 0:
      # Read one byte before. This operation will either consume a previous
      # newline if start_offset was at the beginning of a line or consume the
      # line if we were in the middle of it. Either way we get the read
      # position exactly where we wanted: at the beginning of the first full
      # line.
      self.file.seek(self.start_offset - 1)
      self.current_offset -= 1
      line = self.file.readline()
      self.notify_observers(line, is_encoded=True)
      self.current_offset += len(line)

  def read_records(self):
    line = self.file.readline()
    delta_offset = len(line)

    if delta_offset == 0:
      yield True, None, None  # Reached EOF.
    else:
      if self.source.strip_trailing_newlines:
        line = line.rstrip('\n')
      yield False, self.source.coder.decode(line), delta_offset


class TextMultiFileReader(iobase.NativeSourceReader):
  """A reader for a multi-file text source."""

  def __init__(self, source):
    self.source = source
    self.file_paths = ChannelFactory.glob(self.source.file_path)
    if not self.file_paths:
      raise RuntimeError('No files found for path: %s' % self.source.file_path)

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    pass

  def __iter__(self):
    index = 0
    for path in self.file_paths:
      index += 1
      logging.info('Reading from %s (%d/%d)', path, index, len(self.file_paths))
      with TextFileSource(
          path,
          strip_trailing_newlines=self.source.strip_trailing_newlines,
          coder=self.source.coder).reader() as reader:
        for line in reader:
          yield line

# -----------------------------------------------------------------------------
# TextFileWriter.


class TextFileWriter(NativeFileSinkWriter):
  """The sink writer for a TextFileSink."""

  def Write(self, value):
    super(TextFileWriter, self).Write(value)
    if self.sink.append_trailing_newlines:
      self.file.write('\n')
