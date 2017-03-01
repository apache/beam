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

import bz2
import cStringIO
import glob
import logging
import os
import re
import shutil
import time
import zlib

from apache_beam.internal import util
from apache_beam.io import iobase
from apache_beam.transforms.display import DisplayDataItem

# TODO(sourabhbajaj): Fix the constant values after the new IO factory
# Current constants are copy pasted from gcsio.py till we fix this.
# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.gcp import gcsio
  DEFAULT_READ_BUFFER_SIZE = gcsio.DEFAULT_READ_BUFFER_SIZE
  MAX_BATCH_OPERATION_SIZE = gcsio.MAX_BATCH_OPERATION_SIZE
except ImportError:
  gcsio = None
  DEFAULT_READ_BUFFER_SIZE = 16 * 1024 * 1024
  MAX_BATCH_OPERATION_SIZE = 100
# pylint: enable=wrong-import-order, wrong-import-position


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

  def __str__(self):
    return self.identifier

  def __repr__(self):
    return '_CompressionType(%s)' % self.identifier


class CompressionTypes(object):
  """Enum-like class representing known compression types."""

  # Detect compression based on filename extension.
  #
  # The following extensions are currently recognized by auto-detection:
  #   .bz2 (implies BZIP2 as described below).
  #   .gz  (implies GZIP as described below)
  # Any non-recognized extension implies UNCOMPRESSED as described below.
  AUTO = _CompressionType('auto')

  # BZIP2 compression.
  BZIP2 = _CompressionType('bzip2')

  # GZIP compression (deflate with GZIP headers).
  GZIP = _CompressionType('gzip')

  # Uncompressed (i.e., may be split).
  UNCOMPRESSED = _CompressionType('uncompressed')

  @classmethod
  def is_valid_compression_type(cls, compression_type):
    """Returns true for valid compression types, false otherwise."""
    return isinstance(compression_type, _CompressionType)

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
    elif not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))

    if path.startswith('gs://'):
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
  def rename(src, dest):
    if src.startswith('gs://'):
      if not dest.startswith('gs://'):
        raise ValueError('Destination %r must be GCS path.', dest)
      gcsio.GcsIO().rename(src, dest)
    else:
      try:
        os.rename(src, dest)
      except OSError as err:
        raise IOError(err)

  @staticmethod
  def rename_batch(src_dest_pairs):
    # Filter out local and GCS operations.
    local_src_dest_pairs = []
    gcs_src_dest_pairs = []
    for src, dest in src_dest_pairs:
      if src.startswith('gs://'):
        if not dest.startswith('gs://'):
          raise ValueError('Destination %r must be GCS path.', dest)
        gcs_src_dest_pairs.append((src, dest))
      else:
        local_src_dest_pairs.append((src, dest))

    # Execute local operations.
    exceptions = []
    for src, dest in local_src_dest_pairs:
      try:
        ChannelFactory.rename(src, dest)
      except Exception as e:  # pylint: disable=broad-except
        exceptions.append((src, dest, e))

    # Execute GCS operations.
    exceptions += ChannelFactory._rename_gcs_batch(gcs_src_dest_pairs)

    return exceptions

  @staticmethod
  def _rename_gcs_batch(src_dest_pairs):
    # Prepare batches.
    gcs_batches = []
    gcs_current_batch = []
    for src, dest in src_dest_pairs:
      gcs_current_batch.append((src, dest))
      if len(gcs_current_batch) == MAX_BATCH_OPERATION_SIZE:
        gcs_batches.append(gcs_current_batch)
        gcs_current_batch = []
    if gcs_current_batch:
      gcs_batches.append(gcs_current_batch)

    # Execute GCS renames if any and return exceptions.
    exceptions = []
    for batch in gcs_batches:
      copy_statuses = gcsio.GcsIO().copy_batch(batch)
      copy_succeeded = []
      for src, dest, exception in copy_statuses:
        if exception:
          exceptions.append((src, dest, exception))
        else:
          copy_succeeded.append((src, dest))
      delete_batch = [src for src, dest in copy_succeeded]
      delete_statuses = gcsio.GcsIO().delete_batch(delete_batch)
      for i, (src, exception) in enumerate(delete_statuses):
        dest = copy_succeeded[i]
        if exception:
          exceptions.append((src, dest, exception))
    return exceptions

  @staticmethod
  def copytree(src, dest):
    if src.startswith('gs://'):
      if not dest.startswith('gs://'):
        raise ValueError('Destination %r must be GCS path.', dest)
      assert src.endswith('/'), src
      assert dest.endswith('/'), dest
      gcsio.GcsIO().copytree(src, dest)
    else:
      try:
        if os.path.exists(dest):
          shutil.rmtree(dest)
        shutil.copytree(src, dest)
      except OSError as err:
        raise IOError(err)

  @staticmethod
  def exists(path):
    if path.startswith('gs://'):
      return gcsio.GcsIO().exists(path)
    else:
      return os.path.exists(path)

  @staticmethod
  def rmdir(path):
    if path.startswith('gs://'):
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
      gcsio.GcsIO().delete(path)
    else:
      try:
        os.remove(path)
      except OSError as err:
        raise IOError(err)

  @staticmethod
  def glob(path, limit=None):
    if path.startswith('gs://'):
      return gcsio.GcsIO().glob(path, limit)
    else:
      files = glob.glob(path)
      return files[:limit]

  @staticmethod
  def size_in_bytes(path):
    """Returns the size of a file in bytes.

    Args:
      path: a string that gives the path of a single file.
    """
    if path.startswith('gs://'):
      return gcsio.GcsIO().size(path)
    else:
      return os.path.getsize(path)

  @staticmethod
  def size_of_files_in_glob(path, file_names=None):
    """Returns a map of file names to sizes.

    Args:
      path: a file path pattern that reads the size of all the files
      file_names: List of file names that we need size for, this is added to
        support eventually consistent sources where two expantions of glob
        might yield to different files.
    """
    if path.startswith('gs://'):
      file_sizes = gcsio.GcsIO().size_of_files_in_glob(path)
      if file_names is None:
        return file_sizes
      else:
        result = {}
        # We need to make sure we fetched the size for all the files as the
        # list API in GCS is eventually consistent so directly call size for
        # any files that may be missing.
        for file_name in file_names:
          if file_name in file_sizes:
            result[file_name] = file_sizes[file_name]
          else:
            result[file_name] = ChannelFactory.size_in_bytes(file_name)
        return result
    else:
      if file_names is None:
        file_names = ChannelFactory.glob(path)
      return {file_name: ChannelFactory.size_in_bytes(file_name)
              for file_name in file_names}


class _CompressedFile(object):
  """Somewhat limited file wrapper for easier handling of compressed files."""

  # The bit mask to use for the wbits parameters of the zlib compressor and
  # decompressor objects.
  _gzip_mask = zlib.MAX_WBITS | 16  # Mask when using GZIP headers.

  def __init__(self,
               fileobj,
               compression_type=CompressionTypes.GZIP,
               read_size=DEFAULT_READ_BUFFER_SIZE):
    if not fileobj:
      raise ValueError('File object must be opened file but was at %s' %
                       fileobj)

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

    if self.readable():
      self._read_size = read_size
      self._read_buffer = cStringIO.StringIO()
      self._read_position = 0
      self._read_eof = False

      if self._compression_type == CompressionTypes.BZIP2:
        self._decompressor = bz2.BZ2Decompressor()
      else:
        assert self._compression_type == CompressionTypes.GZIP
        self._decompressor = zlib.decompressobj(self._gzip_mask)
    else:
      self._decompressor = None

    if self.writeable():
      if self._compression_type == CompressionTypes.BZIP2:
        self._compressor = bz2.BZ2Compressor()
      else:
        assert self._compression_type == CompressionTypes.GZIP
        self._compressor = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION,
                                            zlib.DEFLATED, self._gzip_mask)
    else:
      self._compressor = None

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
      self._read_position = 0
      self._read_buffer.seek(0)
      self._read_buffer.truncate(0)
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
    # TODO: Add support for seeking to a file position.
    return False

  def tell(self):
    """Returns current position in uncompressed file."""
    return self._uncompressed_position

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.close()


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

  def display_data(self):
    return {'shards':
            DisplayDataItem(self.num_shards,
                            label='Number of Shards').drop_if_default(0),
            'compression':
            DisplayDataItem(str(self.compression_type)),
            'file_pattern':
            DisplayDataItem('{}{}{}'.format(self.file_path_prefix,
                                            self.shard_name_format,
                                            self.file_name_suffix),
                            label='File Pattern')}

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
    # A proper suffix is needed for AUTO compression detection.
    # We also ensure there will be no collisions with uid and a
    # (possibly unsharded) file_path_prefix and a (possibly empty)
    # file_name_suffix.
    suffix = (
        '.' + os.path.basename(self.file_path_prefix) + self.file_name_suffix)
    return FileSinkWriter(self, os.path.join(init_result, uid) + suffix)

  def finalize_write(self, init_result, writer_results):
    writer_results = sorted(writer_results)
    num_shards = len(writer_results)
    min_threads = min(num_shards, FileSink._MAX_RENAME_THREADS)
    num_threads = max(1, min_threads)

    rename_ops = []
    for shard_num, shard in enumerate(writer_results):
      final_name = ''.join([
          self.file_path_prefix, self.shard_name_format % dict(
              shard_num=shard_num, num_shards=num_shards), self.file_name_suffix
      ])
      rename_ops.append((shard, final_name))

    batches = []
    current_batch = []
    for rename_op in rename_ops:
      current_batch.append(rename_op)
      if len(current_batch) == MAX_BATCH_OPERATION_SIZE:
        batches.append(current_batch)
        current_batch = []
    if current_batch:
      batches.append(current_batch)

    logging.info(
        'Starting finalize_write threads with num_shards: %d, '
        'batches: %d, num_threads: %d',
        num_shards, len(batches), num_threads)
    start_time = time.time()

    # Use a thread pool for renaming operations.
    def _rename_batch(batch):
      """_rename_batch executes batch rename operations."""
      exceptions = []
      exception_infos = ChannelFactory.rename_batch(batch)
      for src, dest, exception in exception_infos:
        if exception:
          logging.warning('Rename not successful: %s -> %s, %s', src, dest,
                          exception)
          should_report = True
          if isinstance(exception, IOError):
            # May have already been copied.
            try:
              if ChannelFactory.exists(dest):
                should_report = False
            except Exception as exists_e:  # pylint: disable=broad-except
              logging.warning('Exception when checking if file %s exists: '
                              '%s', dest, exists_e)
          if should_report:
            logging.warning(('Exception in _rename_batch. src: %s, '
                             'dest: %s, err: %s'), src, dest, exception)
            exceptions.append(exception)
        else:
          logging.debug('Rename successful: %s -> %s', src, dest)
      return exceptions

    exception_batches = util.run_using_threadpool(
        _rename_batch, batches, num_threads)

    all_exceptions = []
    for exceptions in exception_batches:
      if exceptions:
        all_exceptions += exceptions
    if all_exceptions:
      raise Exception('Encountered exceptions in finalize_write: %s',
                      all_exceptions)

    for shard, final_name in rename_ops:
      yield final_name

    logging.info('Renamed %d shards in %.2f seconds.', num_shards,
                 time.time() - start_time)

    try:
      ChannelFactory.rmdir(init_result)
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
