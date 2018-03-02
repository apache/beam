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

from __future__ import absolute_import

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
      ~exceptions.TypeError: if file path parameters are not a :class:`str` or
        :class:`~apache_beam.options.value_provider.ValueProvider`, or if
        **compression_type** is not member of
        :class:`~apache_beam.io.filesystem.CompressionTypes`.
      ~exceptions.ValueError: if **shard_name_template** is not of expected
        format.
    """
    if not isinstance(file_path_prefix, (basestring, ValueProvider)):
      raise TypeError('file_path_prefix must be a string or ValueProvider;'
                      'got %r instead' % file_path_prefix)
    if not isinstance(file_name_suffix, (basestring, ValueProvider)):
      raise TypeError('file_name_suffix must be a string or ValueProvider;'
                      'got %r instead' % file_name_suffix)

    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    if shard_name_template is None:
      shard_name_template = DEFAULT_SHARD_NAME_TEMPLATE
    elif shard_name_template == '':
      num_shards = 1
    if isinstance(file_path_prefix, basestring):
      file_path_prefix = StaticValueProvider(str, file_path_prefix)
    if isinstance(file_name_suffix, basestring):
      file_name_suffix = StaticValueProvider(str, file_name_suffix)
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

  @check_accessible(['file_path_prefix'])
  def open(self, temp_path):
    """Opens ``temp_path``, returning an opaque file handle object.

    The returned file handle is passed to ``write_[encoded_]record`` and
    ``close``.
    """
    return FileSystems.create(temp_path, self.mime_type, self.compression_type)

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
        raise ValueError('Cannot create a temporary directory for root path '
                         'prefix %s. Please specify a file path prefix with '
                         'at least two components.',
                         file_path_prefix)
    path_components = [base_path,
                       'beam-temp-' + last_component + '-' + uuid.uuid1().hex]
    return FileSystems.join(*path_components)

  @check_accessible(['file_path_prefix', 'file_name_suffix'])
  def open_writer(self, init_result, uid):
    # A proper suffix is needed for AUTO compression detection.
    # We also ensure there will be no collisions with uid and a
    # (possibly unsharded) file_path_prefix and a (possibly empty)
    # file_name_suffix.
    file_path_prefix = self.file_path_prefix.get()
    file_name_suffix = self.file_name_suffix.get()
    suffix = (
        '.' + os.path.basename(file_path_prefix) + file_name_suffix)
    return FileBasedSinkWriter(self, os.path.join(init_result, uid) + suffix)

  @check_accessible(['file_path_prefix', 'file_name_suffix'])
  def finalize_write(self, init_result, writer_results):
    file_path_prefix = self.file_path_prefix.get()
    file_name_suffix = self.file_name_suffix.get()
    writer_results = sorted(writer_results)
    num_shards = len(writer_results)
    min_threads = min(num_shards, FileBasedSink._MAX_RENAME_THREADS)
    num_threads = max(1, min_threads)

    source_files = []
    destination_files = []
    chunk_size = FileSystems.get_chunk_size(file_path_prefix)
    for shard_num, shard in enumerate(writer_results):
      final_name = ''.join([
          file_path_prefix, self.shard_name_format % dict(
              shard_num=shard_num, num_shards=num_shards), file_name_suffix
      ])
      source_files.append(shard)
      destination_files.append(final_name)

    source_file_batch = [source_files[i:i + chunk_size]
                         for i in range(0, len(source_files),
                                        chunk_size)]
    destination_file_batch = [destination_files[i:i + chunk_size]
                              for i in range(0, len(destination_files),
                                             chunk_size)]

    logging.info(
        'Starting finalize_write threads with num_shards: %d, '
        'batches: %d, num_threads: %d',
        num_shards, len(source_file_batch), num_threads)
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
        for (src, dest), exception in exp.exception_details.iteritems():
          if exception:
            logging.warning('Rename not successful: %s -> %s, %s', src, dest,
                            exception)
            should_report = True
            if isinstance(exception, IOError):
              # May have already been copied.
              try:
                if FileSystems.exists(dest):
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
        _rename_batch, zip(source_file_batch, destination_file_batch),
        num_threads)

    all_exceptions = [e for exception_batch in exception_batches
                      for e in exception_batch]
    if all_exceptions:
      raise Exception(
        'Encountered exceptions in finalize_write: %s' % all_exceptions)

    for final_name in destination_files:
      yield final_name

    logging.info('Renamed %d shards in %.2f seconds.', num_shards,
                 time.time() - start_time)

    try:
      FileSystems.delete([init_result])
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


class FileBasedSinkWriter(iobase.Writer):
  """The writer for FileBasedSink.
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
