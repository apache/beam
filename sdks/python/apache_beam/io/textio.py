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

"""A source and a sink for reading from and writing to text files."""

from __future__ import absolute_import

from apache_beam import coders
from apache_beam.io import filebasedsource
from apache_beam.io import fileio
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.transforms import PTransform

__all__ = ['ReadFromText', 'WriteToText']


class _TextSource(filebasedsource.FileBasedSource):
  """A source for reading text files.

  Parses a text file as newline-delimited elements. Supports newline delimiters
  '\n' and '\r\n.

  This implementation only supports reading text encoded using UTF-8 or
  ASCII.
  """

  DEFAULT_READ_BUFFER_SIZE = 8192

  def __init__(self, file_pattern, min_bundle_size,
               compression_type, strip_trailing_newlines, coder,
               buffer_size=DEFAULT_READ_BUFFER_SIZE):
    super(_TextSource, self).__init__(file_pattern, min_bundle_size,
                                      compression_type=compression_type)
    self._buffer = ''
    self._next_position_in_buffer = 0
    self._file = None
    self._strip_trailing_newlines = strip_trailing_newlines
    self._compression_type = compression_type
    self._coder = coder
    self._buffer_size = buffer_size

  def read_records(self, file_name, range_tracker):
    start_offset = range_tracker.start_position()

    self._file = self.open_file(file_name)
    try:
      if start_offset > 0:
        # Seeking to one position before the start index and ignoring the
        # current line. If start_position is at beginning if the line, that line
        # belongs to the current bundle, hence ignoring that is incorrect.
        # Seeking to one byte before prevents that.

        self._file.seek(start_offset - 1)
        sep_bounds = self._find_separator_bounds()
        if not sep_bounds:
          # Could not find a separator after (start_offset - 1). This means that
          # none of the records within the file belongs to the current source.
          return

        _, sep_end = sep_bounds
        self._buffer = self._buffer[sep_end:]
        next_record_start_position = start_offset -1 + sep_end
      else:
        next_record_start_position = 0

      while range_tracker.try_claim(next_record_start_position):
        record, num_bytes_to_next_record = self._read_record()
        yield self._coder.decode(record)
        if num_bytes_to_next_record < 0:
          break
        next_record_start_position += num_bytes_to_next_record
    finally:
      self._file.close()

  def _find_separator_bounds(self):
    # Determines the start and end positions within 'self._buffer' of the next
    # separator starting from 'self._next_position_in_buffer'.
    # Currently supports following separators.
    # * '\n'
    # * '\r\n'
    # This method may increase the size of buffer but it will not decrease the
    # size of it.

    current_pos = self._next_position_in_buffer

    while True:
      if current_pos >= len(self._buffer):
        # Ensuring that there are enough bytes to determine if there is a '\n'
        # at current_pos.
        if not self._try_to_ensure_num_bytes_in_buffer(current_pos + 1):
          return

      # Using find() here is more efficient than a linear scan of the byte
      # array.
      next_lf = self._buffer.find('\n', current_pos)
      if next_lf >= 0:
        if self._buffer[next_lf - 1] == '\r':
          return (next_lf - 1, next_lf + 1)
        else:
          return (next_lf, next_lf + 1)

      current_pos = len(self._buffer)

  def _try_to_ensure_num_bytes_in_buffer(self, num_bytes):
    # Tries to ensure that there are at least num_bytes bytes in the buffer.
    # Returns True if this can be fulfilled, returned False if this cannot be
    # fulfilled due to reaching EOF.
    while len(self._buffer) < num_bytes:
      read_data = self._file.read(self._buffer_size)
      if not read_data:
        return False

      self._buffer += read_data

    return True

  def _read_record(self):
    # Returns a tuple containing the current_record and number of bytes to the
    # next record starting from 'self._next_position_in_buffer'. If EOF is
    # reached, returns a tuple containing the current record and -1.

    if self._next_position_in_buffer > self._buffer_size:
      # Buffer is too large. Truncating it and adjusting
      # self._next_position_in_buffer.
      self._buffer = self._buffer[self._next_position_in_buffer:]
      self._next_position_in_buffer = 0

    record_start_position_in_buffer = self._next_position_in_buffer
    sep_bounds = self._find_separator_bounds()
    self._next_position_in_buffer = sep_bounds[1] if sep_bounds else len(
        self._buffer)

    if not sep_bounds:
      # Reached EOF. Bytes up to the EOF is the next record. Returning '-1' for
      # the starting position of the next record.
      return (self._buffer[record_start_position_in_buffer:], -1)

    if self._strip_trailing_newlines:
      # Current record should not contain the separator.
      return (self._buffer[record_start_position_in_buffer:sep_bounds[0]],
              sep_bounds[1] - record_start_position_in_buffer)
    else:
      # Current record should contain the separator.
      return (self._buffer[record_start_position_in_buffer:sep_bounds[1]],
              sep_bounds[1] - record_start_position_in_buffer)


class _TextSink(fileio.TextFileSink):
  # TODO: Move code from 'fileio.TextFileSink' to here.
  pass


class ReadFromText(PTransform):
  """A PTransform for reading text files.

  Parses a text file as newline-delimited elements, by default assuming
  UTF-8 encoding. Supports newline delimiters '\n' and '\r\n'.

  This implementation only supports reading text encoded using UTF-8 or ASCII.
  This does not support other encodings such as UTF-16 or UTF-32."""

  def __init__(self, file_pattern=None, min_bundle_size=0,
               compression_type=fileio.CompressionTypes.UNCOMPRESSED,
               strip_trailing_newlines=True,
               coder=coders.StrUtf8Coder(), **kwargs):
    """Initialize the ReadFromText transform.

    Args:
      file_pattern: The file path to read from as a local file path or a GCS
        gs:// path. The path can contain glob characters (*, ?, and [...]
        sets).
      min_bundle_size: Minimum size of bundles that should be generated when
                       splitting this source into bundles. See
                       ``FileBasedSource`` for more details.
      compression_type: Used to handle compressed input files. Should be an
                        object of type fileio.CompressionTypes.
      strip_trailing_newlines: Indicates whether this source should remove
                               the newline char in each line it reads before
                               decoding that line.
      coder: Coder used to decode each line.
    """

    super(ReadFromText, self).__init__(**kwargs)
    self._file_pattern = file_pattern
    self._min_bundle_size = min_bundle_size
    self._compression_type = compression_type
    self._strip_trailing_newlines = strip_trailing_newlines
    self._coder = coder

  def apply(self, pcoll):
    return pcoll | Read(_TextSource(
        self._file_pattern,
        self._min_bundle_size,
        self._compression_type,
        self._strip_trailing_newlines,
        self._coder))


class WriteToText(PTransform):
  """A PTransform for writing to text files."""

  def __init__(self,
               file_path_prefix,
               file_name_suffix='',
               append_trailing_newlines=True,
               num_shards=0,
               shard_name_template=None,
               coder=coders.ToStringCoder(),
               compression_type=fileio.CompressionTypes.NO_COMPRESSION,
              ):
    """Initialize a WriteToText PTransform.

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
    """

    self._file_path_prefix = file_path_prefix
    self._file_name_suffix = file_name_suffix
    self._append_trailing_newlines = append_trailing_newlines
    self._num_shards = num_shards
    self._shard_name_template = shard_name_template
    self._coder = coder
    self._compression_type = compression_type

  def apply(self, pcoll):
    return pcoll | Write(_TextSink(
        self._file_path_prefix, self._file_name_suffix,
        self._append_trailing_newlines, self._num_shards,
        self._shard_name_template, self._coder, self._compression_type))
