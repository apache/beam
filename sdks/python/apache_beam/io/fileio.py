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


# -----------------------------------------------------------------------------
# TextFileSource, TextFileSink.


class TextFileSource(iobase.NativeSource):
  """A source for a GCS or local text file.

  Parses a text file as newline-delimited elements, by default assuming
  UTF-8 encoding.

  This implementation has only been tested to read text encoded using UTF-8 or
  ASCII. This has not been tested for other encodings such as UTF-16 or UTF-32.
  """

  def __init__(self, file_path, start_offset=None, end_offset=None,
               compression_type='AUTO', strip_trailing_newlines=True,
               coder=coders.StrUtf8Coder()):
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
          is 'AUTO'.
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

    If start_offset falls inside a line (any character except the firt one)
    then the source will skip the line and start with the next one.

    If end_offset falls inside a line (any character except the first one) then
    the source will contain that entire line.
    """
    if not isinstance(file_path, basestring):
      raise TypeError(
          '%s: file_path must be a string;  got %r instead' %
          (self.__class__.__name__, file_path))

    self.file_path = file_path
    self.start_offset = start_offset
    self.end_offset = end_offset
    self.compression_type = compression_type
    self.strip_trailing_newlines = strip_trailing_newlines
    self.coder = coder

    self.is_gcs_source = file_path.startswith('gs://')

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'text'

  def __eq__(self, other):
    return (self.file_path == other.file_path and
            self.start_offset == other.start_offset and
            self.end_offset == other.end_offset and
            self.strip_trailing_newlines == other.strip_trailing_newlines and
            self.coder == other.coder)

  @property
  def path(self):
    return self.file_path

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
  # TODO(robertwb): Generalize into extensible framework.

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
  def open(path, mode, mime_type):
    if path.startswith('gs://'):
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      return gcsio.GcsIO().open(path, mode, mime_type=mime_type)
    else:
      return open(path, mode)

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
      # TODO(robertwb): Threadpool?
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
  NO_COMPRESSION = _CompressionType(1)  # No compression.
  DEFLATE = _CompressionType(2)  # 'Deflate' compression (without headers).
  GZIP = _CompressionType(3)  # gzip compression (deflate with gzip headers).
  ZLIB = _CompressionType(4)  # zlib compression (deflate with zlib headers).

  @staticmethod
  def is_valid_compression_type(compression_type):
    """Returns true for valid compression types, false otherwise."""
    return isinstance(compression_type, _CompressionType)

  @staticmethod
  def mime_type(compression_type, default='application/octet-stream'):
    if compression_type == CompressionTypes.GZIP:
      return 'application/x-gzip'
    elif compression_type == CompressionTypes.ZLIB:
      return 'application/octet-stream'
    elif compression_type == CompressionTypes.DEFLATE:
      return 'application/octet-stream'
    else:
      return default


class _CompressedFile(object):
  """Somewhat limited file wrapper for easier handling of compressed files."""
  _type_mask = {
      CompressionTypes.ZLIB:  zlib.MAX_WBITS,
      CompressionTypes.GZIP: zlib.MAX_WBITS | 16,
      CompressionTypes.DEFLATE: -zlib.MAX_WBITS,
  }

  def __init__(self,
               fileobj=None,
               compression_type=CompressionTypes.ZLIB,
               read_size=16384):
    self._validate_compression_type(compression_type)
    if not fileobj:
      raise ValueError('fileobj must be opened file but was %s' % fileobj)

    self.fileobj = fileobj
    self.data = ''
    self.read_size = read_size
    self.compression_type = compression_type
    if self._readable():
      self.decompressor = self._create_decompressor(self.compression_type)
    else:
      self.decompressor = None
    if self._writeable():
      self.compressor = self._create_compressor(self.compression_type)
    else:
      self.compressor = None

  def _validate_compression_type(self, compression_type):
    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    if compression_type == CompressionTypes.NO_COMPRESSION:
      raise ValueError('cannot create object with no compression')

  def _create_compressor(self, compression_type):
    self._validate_compression_type(compression_type)
    return zlib.compressobj(9, zlib.DEFLATED,
                            self._type_mask[compression_type])

  def _create_decompressor(self, compression_type):
    self._validate_compression_type(compression_type)
    return zlib.decompressobj(self._type_mask[compression_type])

  def _readable(self):
    mode = self.fileobj.mode
    return 'r' in mode or 'a' in mode

  def _writeable(self):
    mode = self.fileobj.mode
    return 'w' in mode or 'a' in mode

  def write(self, data):
    """Write data to file."""
    if not self.compressor:
      raise ValueError('compressor not initialized')
    compressed = self.compressor.compress(data)
    if compressed:
      self.fileobj.write(compressed)

  def _read(self, num_bytes):
    """Read num_bytes into internal buffer."""
    while not num_bytes or len(self.data) < num_bytes:
      buf = self.fileobj.read(self.read_size)
      if not buf:
        # EOF reached, flush.
        self.data += self.decompressor.flush()
        break

      self.data += self.decompressor.decompress(buf)
    result = self.data[:num_bytes]
    self.data = self.data[num_bytes:]
    return result

  def read(self, num_bytes):
    if not self.decompressor:
      raise ValueError('decompressor not initialized')
    return self._read(num_bytes)

  @property
  def closed(self):
    return not self.fileobj or self.fileobj.closed()

  def close(self):
    if self.fileobj is None:
      return

    if self._writeable():
      self.fileobj.write(self.compressor.flush())
    self.fileobj.close()

  def flush(self):
    if self._writeable():
      self.fileobj.write(self.compressor.flush())
    self.fileobj.flush()

  # TODO(slaven): Add support for seeking to a file position.
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

  # Approximate number of write results be assigned for each rename thread.
  _WRITE_RESULTS_PER_RENAME_THREAD = 100

  # Max number of threads to be used for renaming even if it means each thread
  # will process more write results.
  _MAX_RENAME_THREADS = 64

  def __init__(self,
               file_path_prefix,
               coder,
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               mime_type='application/octet-stream',
               compression_type=CompressionTypes.NO_COMPRESSION):
    if shard_name_template is None:
      shard_name_template = DEFAULT_SHARD_NAME_TEMPLATE
    elif shard_name_template is '':
      num_shards = 1
    self.file_path_prefix = file_path_prefix
    self.file_name_suffix = file_name_suffix
    self.num_shards = num_shards
    self.coder = coder
    self.shard_name_format = self._template_to_format(shard_name_template)
    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    self.compression_type = compression_type
    self.mime_type = CompressionTypes.mime_type(compression_type, mime_type)

  def open(self, temp_path):
    """Opens ``temp_path``, returning an opaque file handle object.

    The returned file handle is passed to ``write_[encoded_]record`` and
    ``close``.
    """
    raw_file = ChannelFactory.open(temp_path, 'wb', self.mime_type)
    if self.compression_type == CompressionTypes.NO_COMPRESSION:
      return raw_file
    else:
      return _CompressedFile(fileobj=raw_file,
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
    num_threads = max(1, min(
        num_shards / FileSink._WRITE_RESULTS_PER_RENAME_THREAD,
        FileSink._MAX_RENAME_THREADS))

    rename_ops = []
    for shard_num, shard in enumerate(writer_results):
      final_name = ''.join([
          self.file_path_prefix,
          self.shard_name_format % dict(shard_num=shard_num,
                                        num_shards=num_shards),
          self.file_name_suffix])
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
    if not hasattr(threading.current_thread(), "_children"):
      threading.current_thread()._children = weakref.WeakKeyDictionary()
    rename_results = ThreadPool(num_threads).map(_rename_file, rename_ops)

    for final_name, err in rename_results:
      if err:
        logging.warning('Error when processing rename_results: %s', err)
        raise err
      else:
        yield final_name

    logging.info('Renamed %d shards in %.2f seconds.',
                 num_shards, time.time() - start_time)

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
      raise ValueError("Shard number pattern S+ not found in template '%s'"
                       % shard_name_template)
    shard_name_format = shard_name_template.replace(
        m.group(0), '%%(shard_num)0%dd' % len(m.group(0)))
    m = re.search('N+', shard_name_format)
    if m:
      shard_name_format = shard_name_format.replace(
          m.group(0), '%%(num_shards)0%dd' % len(m.group(0)))
    return shard_name_format

  def __eq__(self, other):
    # TODO(robertwb): Clean up workitem_test which uses this.
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
               compression_type=CompressionTypes.NO_COMPRESSION,
              ):
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
      compression_type: Type of compression to use for this sink.

    Raises:
      TypeError: if file path parameters are not a string or if compression_type
        is not member of CompressionTypes.
      ValueError: if shard_name_template is not of expected format.

    Returns:
      A TextFileSink object usable for writing.
    """
    if not isinstance(file_path_prefix, basestring):
      raise TypeError(
          'TextFileSink: file_path_prefix must be a string; got %r instead' %
          file_path_prefix)
    if not isinstance(file_name_suffix, basestring):
      raise TypeError(
          'TextFileSink: file_name_suffix must be a string; got %r instead' %
          file_name_suffix)

    super(TextFileSink, self).__init__(file_path_prefix,
                                       file_name_suffix=file_name_suffix,
                                       num_shards=num_shards,
                                       shard_name_template=shard_name_template,
                                       coder=coder,
                                       mime_type='text/plain',
                                       compression_type=compression_type)

    self.compression_type = compression_type
    self.append_trailing_newlines = append_trailing_newlines

  def write_encoded_record(self, file_handle, encoded_value):
    """Writes a single encoded record."""
    file_handle.write(encoded_value)
    if self.append_trailing_newlines:
      file_handle.write('\n')

  def close(self, file_handle):
    """Finalize and close the file handle returned from ``open()``.

    Args:
      file_handle: file handle to be closed.
    Raises:
      ValueError: if file_handle is already closed.
    """
    if file_handle is not None:
      file_handle.close()


class NativeTextFileSink(iobase.NativeSink):
  """A sink to a GCS or local text file or files."""

  def __init__(self, file_path_prefix,
               append_trailing_newlines=True,
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               validate=True,
               coder=coders.ToStringCoder()):
    # We initialize a file_path attribute containing just the prefix part for
    # local runner environment. For now, sharding is not supported in the local
    # runner and sharding options (template, num, suffix) are ignored.
    # The attribute is also used in the worker environment when we just write
    # to a specific file.
    # TODO(silviuc): Add support for file sharding in the local runner.
    self.file_path = file_path_prefix
    self.append_trailing_newlines = append_trailing_newlines
    self.coder = coder

    self.is_gcs_sink = self.file_path.startswith('gs://')

    self.file_name_prefix = file_path_prefix
    self.file_name_suffix = file_name_suffix
    self.num_shards = num_shards
    # TODO(silviuc): Update this when the service supports more patterns.
    self.shard_name_template = ('-SSSSS-of-NNNNN' if shard_name_template is None
                                else shard_name_template)
    # TODO(silviuc): Implement sink validation.
    self.validate = validate

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'text'

  @property
  def path(self):
    return self.file_path

  def writer(self):
    return TextFileWriter(self)

  def __eq__(self, other):
    return (self.file_path == other.file_path and
            self.append_trailing_newlines == other.append_trailing_newlines and
            self.coder == other.coder and
            self.file_name_prefix == other.file_name_prefix and
            self.file_name_suffix == other.file_name_suffix and
            self.num_shards == other.num_shards and
            self.shard_name_template == other.shard_name_template and
            self.validate == other.validate)


# -----------------------------------------------------------------------------
# TextFileReader, TextMultiFileReader.


class TextFileReader(iobase.NativeSourceReader,
                     coders.observable.ObservableMixin):
  """A reader for a text file source."""

  def __init__(self, source):
    super(TextFileReader, self).__init__()
    self.source = source
    self.start_offset = self.source.start_offset or 0
    self.end_offset = self.source.end_offset
    self.current_offset = self.start_offset

  def __enter__(self):
    if self.source.is_gcs_source:
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      self._file = gcsio.GcsIO().open(self.source.file_path, 'rb')
    else:
      self._file = open(self.source.file_path, 'rb')
    # Determine the real end_offset.
    # If not specified it will be the length of the file.
    if self.end_offset is None:
      self._file.seek(0, os.SEEK_END)
      self.end_offset = self._file.tell()

    if self.start_offset is None:
      self.start_offset = 0
      self.current_offset = self.start_offset
    if self.start_offset > 0:
      # Read one byte before. This operation will either consume a previous
      # newline if start_offset was at the beginning of a line or consume the
      # line if we were in the middle of it. Either way we get the read position
      # exactly where we wanted: at the begining of the first full line.
      self._file.seek(self.start_offset - 1)
      self.current_offset -= 1
      line = self._file.readline()
      self.notify_observers(line, is_encoded=True)
      self.current_offset += len(line)
    else:
      self._file.seek(self.start_offset)

    # Initializing range tracker after start and end offsets are finalized.
    self.range_tracker = range_trackers.OffsetRangeTracker(self.start_offset,
                                                           self.end_offset)

    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self._file.close()

  def __iter__(self):
    while True:
      if not self.range_tracker.try_claim(
          record_start=self.current_offset):
        # Reader has completed reading the set of records in its range. Note
        # that the end offset of the range may be smaller than the original
        # end offset defined when creating the reader due to reader accepting
        # a dynamic split request from the service.
        return
      line = self._file.readline()
      self.notify_observers(line, is_encoded=True)
      self.current_offset += len(line)
      if self.source.strip_trailing_newlines:
        line = line.rstrip('\n')
      yield self.source.coder.decode(line)

  def get_progress(self):
    return iobase.ReaderProgress(position=iobase.ReaderPosition(
        byte_offset=self.range_tracker.last_record_start))

  def request_dynamic_split(self, dynamic_split_request):
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
              '1). Requested: %r',
              dynamic_split_request)
          return
        split_position = iobase.ReaderPosition()
        split_position.byte_offset = (
            self.range_tracker.position_at_fraction(percent_complete))
      else:
        logging.warning(
            'TextReader requires either a position or a percentage of work to '
            'be complete to perform a dynamic split request. Requested: %r',
            dynamic_split_request)
        return

    if self.range_tracker.try_split(split_position.byte_offset):
      return iobase.DynamicSplitResultWithPosition(split_position)
    else:
      return


class TextMultiFileReader(iobase.NativeSourceReader):
  """A reader for a multi-file text source."""

  def __init__(self, source):
    self.source = source
    self.file_paths = ChannelFactory.glob(self.source.file_path)
    if not self.file_paths:
      raise RuntimeError(
          'No files found for path: %s' % self.source.file_path)

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
          path, strip_trailing_newlines=self.source.strip_trailing_newlines,
          coder=self.source.coder).reader() as reader:
        for line in reader:
          yield line


# -----------------------------------------------------------------------------
# TextFileWriter.


class TextFileWriter(iobase.NativeSinkWriter):
  """The sink writer for a TextFileSink."""

  def __init__(self, sink):
    self.sink = sink

  def __enter__(self):
    if self.sink.is_gcs_sink:
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.io import gcsio
      self._file = gcsio.GcsIO().open(self.sink.file_path, 'wb')
    else:
      self._file = open(self.sink.file_path, 'wb')
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self._file.close()

  def Write(self, line):
    self._file.write(self.sink.coder.encode(line))
    if self.sink.append_trailing_newlines:
      self._file.write('\n')
