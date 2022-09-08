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

"""File-based sink."""

# pytype: skip-file

import logging
import os
import re
import time
import uuid

from apache_beam.internal import util
from apache_beam.io import iobase
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import check_accessible
from apache_beam.transforms.display import DisplayDataItem

DEFAULT_SHARD_NAME_TEMPLATE = '-SSSSS-of-NNNNN'

__all__ = ['FileBasedSink']

_LOGGER = logging.getLogger(__name__)


class FileBasedSink(iobase.Sink):
  """A sink to a GCS or local files.

  To implement a file-based sink, extend this class and override
  either :meth:`.write_record()` or :meth:`.write_encoded_record()`.

  If needed, also overwrite :meth:`.open()` and/or :meth:`.close()` to customize
  the file handling or write headers and footers.

  The output of this write is a :class:`~apache_beam.pvalue.PCollection` of
  all written shards.
  """

  # Max number of threads to be used for renaming.
  _MAX_RENAME_THREADS = 64
  __hash__ = None  # type: ignore[assignment]

  def __init__(
      self,
      file_path_prefix,
      coder,
      file_name_suffix='',
      num_shards=0,
      shard_name_template=None,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO,
      *,
      max_records_per_shard=None,
      max_bytes_per_shard=None,
      skip_if_empty=False):
    """
     Raises:
      TypeError: if file path parameters are not a :class:`str` or
        :class:`~apache_beam.options.value_provider.ValueProvider`, or if
        **compression_type** is not member of
        :class:`~apache_beam.io.filesystem.CompressionTypes`.
      ValueError: if **shard_name_template** is not of expected
        format.
    """
    if not isinstance(file_path_prefix, (str, ValueProvider)):
      raise TypeError(
          'file_path_prefix must be a string or ValueProvider;'
          'got %r instead' % file_path_prefix)
    if not isinstance(file_name_suffix, (str, ValueProvider)):
      raise TypeError(
          'file_name_suffix must be a string or ValueProvider;'
          'got %r instead' % file_name_suffix)

    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError(
          'compression_type must be CompressionType object but '
          'was %s' % type(compression_type))
    if shard_name_template is None:
      shard_name_template = DEFAULT_SHARD_NAME_TEMPLATE
    elif shard_name_template == '':
      num_shards = 1
    if isinstance(file_path_prefix, str):
      file_path_prefix = StaticValueProvider(str, file_path_prefix)
    if isinstance(file_name_suffix, str):
      file_name_suffix = StaticValueProvider(str, file_name_suffix)
    self.file_path_prefix = file_path_prefix
    self.file_name_suffix = file_name_suffix
    self.num_shards = num_shards
    self.coder = coder
    self.shard_name_format = self._template_to_format(shard_name_template)
    self.shard_name_glob_format = self._template_to_glob_format(
        shard_name_template)
    self.compression_type = compression_type
    self.mime_type = mime_type
    self.max_records_per_shard = max_records_per_shard
    self.max_bytes_per_shard = max_bytes_per_shard
    self.skip_if_empty = skip_if_empty

  def display_data(self):
    return {
        'shards': DisplayDataItem(self.num_shards,
                                  label='Number of Shards').drop_if_default(0),
        'compression': DisplayDataItem(str(self.compression_type)),
        'file_pattern': DisplayDataItem(
            '{}{}{}'.format(
                self.file_path_prefix,
                self.shard_name_format,
                self.file_name_suffix),
            label='File Pattern')
    }

  @check_accessible(['file_path_prefix'])
  def open(self, temp_path):
    """Opens ``temp_path``, returning an opaque file handle object.

    The returned file handle is passed to ``write_[encoded_]record`` and
    ``close``.
    """
    writer = FileSystems.create(
        temp_path, self.mime_type, self.compression_type)
    if self.max_bytes_per_shard:
      self.byte_counter = _ByteCountingWriter(writer)
      return self.byte_counter
    else:
      return writer

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

  @check_accessible(['file_path_prefix', 'file_name_suffix'])
  def initialize_write(self):
    file_path_prefix = self.file_path_prefix.get()

    tmp_dir = self._create_temp_dir(file_path_prefix)
    FileSystems.mkdirs(tmp_dir)
    return tmp_dir

  def _create_temp_dir(self, file_path_prefix):
    base_path, last_component = FileSystems.split(file_path_prefix)
    if not last_component:
      # Trying to re-split the base_path to check if it's a root.
      new_base_path, _ = FileSystems.split(base_path)
      if base_path == new_base_path:
        raise ValueError(
            'Cannot create a temporary directory for root path '
            'prefix %s. Please specify a file path prefix with '
            'at least two components.' % file_path_prefix)
    path_components = [
        base_path, 'beam-temp-' + last_component + '-' + uuid.uuid1().hex
    ]
    return FileSystems.join(*path_components)

  @check_accessible(['file_path_prefix', 'file_name_suffix'])
  def open_writer(self, init_result, uid):
    # A proper suffix is needed for AUTO compression detection.
    # We also ensure there will be no collisions with uid and a
    # (possibly unsharded) file_path_prefix and a (possibly empty)
    # file_name_suffix.
    file_path_prefix = self.file_path_prefix.get()
    file_name_suffix = self.file_name_suffix.get()
    suffix = ('.' + os.path.basename(file_path_prefix) + file_name_suffix)
    writer_path = FileSystems.join(init_result, uid) + suffix
    return FileBasedSinkWriter(self, writer_path)

  @check_accessible(['file_path_prefix', 'file_name_suffix'])
  def _get_final_name(self, shard_num, num_shards):
    return ''.join([
        self.file_path_prefix.get(),
        self.shard_name_format %
        dict(shard_num=shard_num, num_shards=num_shards),
        self.file_name_suffix.get()
    ])

  @check_accessible(['file_path_prefix', 'file_name_suffix'])
  def _get_final_name_glob(self, num_shards):
    return ''.join([
        self.file_path_prefix.get(),
        self.shard_name_glob_format % dict(num_shards=num_shards),
        self.file_name_suffix.get()
    ])

  def pre_finalize(self, init_result, writer_results):
    num_shards = len(list(writer_results))
    dst_glob = self._get_final_name_glob(num_shards)
    dst_glob_files = [
        file_metadata.path for mr in FileSystems.match([dst_glob])
        for file_metadata in mr.metadata_list
    ]

    if dst_glob_files:
      _LOGGER.warning(
          'Deleting %d existing files in target path matching: %s',
          len(dst_glob_files),
          self.shard_name_glob_format)
      FileSystems.delete(dst_glob_files)

  def _check_state_for_finalize_write(self, writer_results, num_shards):
    """Checks writer output files' states.

    Returns:
      src_files, dst_files: Lists of files to rename. For each i, finalize_write
        should rename(src_files[i], dst_files[i]).
      delete_files: Src files to delete. These could be leftovers from an
        incomplete (non-atomic) rename operation.
      num_skipped: Tally of writer results files already renamed, such as from
        a previous run of finalize_write().
    """
    if not writer_results:
      return [], [], [], 0

    src_glob = FileSystems.join(FileSystems.split(writer_results[0])[0], '*')
    dst_glob = self._get_final_name_glob(num_shards)
    src_glob_files = set(
        file_metadata.path for mr in FileSystems.match([src_glob])
        for file_metadata in mr.metadata_list)
    dst_glob_files = set(
        file_metadata.path for mr in FileSystems.match([dst_glob])
        for file_metadata in mr.metadata_list)

    src_files = []
    dst_files = []
    delete_files = []
    num_skipped = 0
    for shard_num, src in enumerate(writer_results):
      final_name = self._get_final_name(shard_num, num_shards)
      dst = final_name
      src_exists = src in src_glob_files
      dst_exists = dst in dst_glob_files
      if not src_exists and not dst_exists:
        raise BeamIOError(
            'src and dst files do not exist. src: %s, dst: %s' % (src, dst))
      if not src_exists and dst_exists:
        _LOGGER.debug('src: %s -> dst: %s already renamed, skipping', src, dst)
        num_skipped += 1
        continue
      if (src_exists and dst_exists and
          FileSystems.checksum(src) == FileSystems.checksum(dst)):
        _LOGGER.debug('src: %s == dst: %s, deleting src', src, dst)
        delete_files.append(src)
        continue

      src_files.append(src)
      dst_files.append(dst)
    return src_files, dst_files, delete_files, num_skipped

  @check_accessible(['file_path_prefix'])
  def finalize_write(
      self, init_result, writer_results, unused_pre_finalize_results):
    writer_results = sorted(writer_results)
    num_shards = len(writer_results)

    src_files, dst_files, delete_files, num_skipped = (
        self._check_state_for_finalize_write(writer_results, num_shards))
    num_skipped += len(delete_files)
    FileSystems.delete(delete_files)
    num_shards_to_finalize = len(src_files)
    min_threads = min(num_shards_to_finalize, FileBasedSink._MAX_RENAME_THREADS)
    num_threads = max(1, min_threads)

    chunk_size = FileSystems.get_chunk_size(self.file_path_prefix.get())
    source_file_batch = [
        src_files[i:i + chunk_size]
        for i in range(0, len(src_files), chunk_size)
    ]
    destination_file_batch = [
        dst_files[i:i + chunk_size]
        for i in range(0, len(dst_files), chunk_size)
    ]

    if num_shards_to_finalize:
      _LOGGER.info(
          'Starting finalize_write threads with num_shards: %d (skipped: %d), '
          'batches: %d, num_threads: %d',
          num_shards_to_finalize,
          num_skipped,
          len(source_file_batch),
          num_threads)
      start_time = time.time()

      # Use a thread pool for renaming operations.
      def _rename_batch(batch):
        """_rename_batch executes batch rename operations."""
        source_files, destination_files = batch
        exceptions = []
        try:
          FileSystems.rename(source_files, destination_files)
          return exceptions
        except BeamIOError as exp:
          if exp.exception_details is None:
            raise
          for (src, dst), exception in exp.exception_details.items():
            if exception:
              _LOGGER.error(
                  ('Exception in _rename_batch. src: %s, '
                   'dst: %s, err: %s'),
                  src,
                  dst,
                  exception)
              exceptions.append(exception)
            else:
              _LOGGER.debug('Rename successful: %s -> %s', src, dst)
          return exceptions

      exception_batches = util.run_using_threadpool(
          _rename_batch,
          list(zip(source_file_batch, destination_file_batch)),
          num_threads)

      all_exceptions = [
          e for exception_batch in exception_batches for e in exception_batch
      ]
      if all_exceptions:
        raise Exception(
            'Encountered exceptions in finalize_write: %s' % all_exceptions)

      for final_name in dst_files:
        yield final_name

      _LOGGER.info(
          'Renamed %d shards in %.2f seconds.',
          num_shards_to_finalize,
          time.time() - start_time)
    else:
      _LOGGER.warning(
          'No shards found to finalize. num_shards: %d, skipped: %d',
          num_shards,
          num_skipped)

    try:
      FileSystems.delete([init_result])
    except IOError:
      # May have already been removed.
      pass

  @staticmethod
  def _template_replace_num_shards(shard_name_template):
    match = re.search('N+', shard_name_template)
    if match:
      shard_name_template = shard_name_template.replace(
          match.group(0), '%%(num_shards)0%dd' % len(match.group(0)))
    return shard_name_template

  @staticmethod
  def _template_to_format(shard_name_template):
    if not shard_name_template:
      return ''
    match = re.search('S+', shard_name_template)
    if match is None:
      raise ValueError(
          "Shard number pattern S+ not found in shard_name_template: %s" %
          shard_name_template)
    shard_name_format = shard_name_template.replace(
        match.group(0), '%%(shard_num)0%dd' % len(match.group(0)))
    return FileBasedSink._template_replace_num_shards(shard_name_format)

  @staticmethod
  def _template_to_glob_format(shard_name_template):
    if not shard_name_template:
      return ''
    match = re.search('S+', shard_name_template)
    if match is None:
      raise ValueError(
          "Shard number pattern S+ not found in shard_name_template: %s" %
          shard_name_template)
    shard_name_format = shard_name_template.replace(match.group(0), '*')
    return FileBasedSink._template_replace_num_shards(shard_name_format)

  def __eq__(self, other):
    # TODO: Clean up workitem_test which uses this.
    # pylint: disable=unidiomatic-typecheck
    return type(self) == type(other) and self.__dict__ == other.__dict__


class FileBasedSinkWriter(iobase.Writer):
  """The writer for FileBasedSink.
  """
  def __init__(self, sink, temp_shard_path):
    self.sink = sink
    self.temp_shard_path = temp_shard_path
    self.temp_handle = self.sink.open(temp_shard_path)
    self.num_records_written = 0

  def write(self, value):
    self.num_records_written += 1
    self.sink.write_record(self.temp_handle, value)

  def at_capacity(self):
    return (
        self.sink.max_records_per_shard and
        self.num_records_written >= self.sink.max_records_per_shard
    ) or (
        self.sink.max_bytes_per_shard and
        self.sink.byte_counter.bytes_written >= self.sink.max_bytes_per_shard)

  def close(self):
    self.sink.close(self.temp_handle)
    return self.temp_shard_path


class _ByteCountingWriter:
  def __init__(self, writer):
    self.writer = writer
    self.bytes_written = 0

  def write(self, bs):
    self.bytes_written += len(bs)
    self.writer.write(bs)

  def flush(self):
    self.writer.flush()

  def close(self):
    self.writer.close()
