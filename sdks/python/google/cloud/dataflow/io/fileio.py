# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""File-based sources and sinks."""

from __future__ import absolute_import

import glob
import logging
import os
import re
import tempfile

from google.cloud.dataflow import coders
from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.io import range_trackers
from google.cloud.dataflow.utils import processes
from google.cloud.dataflow.utils import retry


__all__ = ['TextFileSource', 'TextFileSink']


# Retrying is needed because there are transient errors that can happen.
@retry.with_exponential_backoff(num_retries=4, retry_filter=lambda _: True)
def _gcs_file_copy(from_path, to_path, encoding=''):
  """Copy a local file to a GCS location with retries for transient errors."""
  if not encoding:
    command_args = ['gsutil', '-m', '-q', 'cp', from_path, to_path]
  else:
    encoding = 'Content-Type:' + encoding
    command_args = ['gsutil', '-m', '-q', '-h', encoding, 'cp', from_path,
                    to_path]
  logging.info('Executing command: %s', command_args)
  popen = processes.Popen(command_args, stdout=processes.PIPE,
                          stderr=processes.PIPE)
  stdoutdata, stderrdata = popen.communicate()
  if popen.returncode != 0:
    raise ValueError(
        'Failed to copy GCS file from %s to %s (stdout=%s, stderr=%s).' % (
            from_path, to_path, stdoutdata, stderrdata))


# -----------------------------------------------------------------------------
# TextFileSource, TextFileSink.


class TextFileSource(iobase.NativeSource):
  """A source for a GCS or local text file.

  Parses a text file as newline-delimited elements, by default assuming
  UTF-8 encoding.
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


class TextFileSink(iobase.NativeSink):
  """A sink to a GCS or local text file or files."""

  def __init__(self, file_path_prefix,
               append_trailing_newlines=True,
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               validate=True,
               coder=coders.ToStringCoder()):
    """Initialize a TextSink.

    Args:
      file_path_prefix: The file path to write to. The files written will begin
        with this prefix, followed by a shard identifier (see num_shards), and
        end in a common extension, if given by file_name_suffix. In most cases,
        only this argument is specified and num_shards, shard_name_template, and
        file_name_suffix use default values.
      append_trailing_newlines: indicate whether this sink should write an
          additional newline char after writing each element.
      file_name_suffix: Suffix for the files written.
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
      validate: Enable path validation on pipeline creation.
      coder: Coder used to encode each line.

    Raises:
      TypeError: if file_path is not a string.
      ValueError: if shard_name_template is not of expected format.
    """
    if not isinstance(file_path_prefix, basestring):
      raise TypeError(
          '%s: file_path_prefix must be a string; got %r instead' %
          (self.__class__.__name__, file_path_prefix))
    if not isinstance(file_name_suffix, basestring):
      raise TypeError(
          '%s: file_name_suffix must be a string; got %r instead' %
          (self.__class__.__name__, file_name_suffix))

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
    if shard_name_template not in (None, '', '-SSSSS-of-NNNNN'):
      raise ValueError(
          'The shard_name_template argument must be an empty string or the '
          'pattern -SSSSS-of-NNNNN instead of %s' % shard_name_template)
    self.shard_name_template = (
        shard_name_template if shard_name_template is not None
        else '-SSSSS-of-NNNNN')
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


class TextFileReader(iobase.NativeSourceReader):
  """A reader for a text file source."""

  def __init__(self, source):
    self.source = source
    self.start_offset = self.source.start_offset or 0
    self.end_offset = self.source.end_offset
    self.current_offset = self.start_offset

  def __enter__(self):
    if self.source.is_gcs_source:
      # pylint: disable=g-import-not-at-top
      from google.cloud.dataflow.io import gcsio
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
      if not self.range_tracker.try_return_record_at(
          is_at_split_point=True,
          record_start=self.current_offset):
        # Reader has completed reading the set of records in its range. Note
        # that the end offset of the range may be smaller than the original
        # end offset defined when creating the reader due to reader accepting
        # a dynamic split request from the service.
        return
      line = self._file.readline()
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
            self.range_tracker.get_position_for_fraction_consumed(
                percent_complete))
      else:
        logging.warning(
            'TextReader requires either a position or a percentage of work to '
            'be complete to perform a dynamic split request. Requested: %r',
            dynamic_split_request)
        return

    if self.range_tracker.try_split_at_position(split_position.byte_offset):
      return iobase.DynamicSplitResultWithPosition(split_position)
    else:
      return


class TextMultiFileReader(iobase.NativeSourceReader):
  """A reader for a multi-file text source."""

  def __init__(self, source):
    self.source = source
    if source.is_gcs_source:
      # pylint: disable=g-import-not-at-top
      from google.cloud.dataflow.io import gcsio
      self.file_paths = gcsio.GcsIO().glob(self.source.file_path)
    else:
      self.file_paths = glob.glob(self.source.file_path)
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
      # TODO(silviuc): Use the storage library instead of gsutil for writes.
      self.temp_path = os.path.join(tempfile.mkdtemp(), 'gcsfile')
      self._file = open(self.temp_path, 'wb')
    else:
      self._file = open(self.sink.file_path, 'wb')
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self._file.close()
    if hasattr(self, 'temp_path'):
      _gcs_file_copy(self.temp_path, self.sink.file_path, 'text/plain')

  def Write(self, line):
    self._file.write(self.sink.coder.encode(line))
    if self.sink.append_trailing_newlines:
      self._file.write('\n')
