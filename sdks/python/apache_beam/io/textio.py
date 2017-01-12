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
from apache_beam.transforms.display import DisplayDataItem

__all__ = ['ReadFromText', 'WriteToText']


class _TextSource(filebasedsource.FileBasedSource):
  """A source for reading text files.

  Parses a text file as newline-delimited elements. Supports newline delimiters
  '\n' and '\r\n.

  This implementation only supports reading text encoded using UTF-8 or
  ASCII.
  """

  DEFAULT_READ_BUFFER_SIZE = 8192

  class ReadBuffer(object):
    # A buffer that gives the buffered data and next position in the
    # buffer that should be read.

    def __init__(self, data, position):
      self._data = data
      self._position = position

    @property
    def data(self):
      return self._data

    @data.setter
    def data(self, value):
      assert isinstance(value, bytes)
      self._data = value

    @property
    def position(self):
      return self._position

    @position.setter
    def position(self, value):
      assert isinstance(value, (int, long))
      if value > len(self._data):
        raise ValueError('Cannot set position to %d since it\'s larger than '
                         'size of data %d.', value, len(self._data))
      self._position = value

  def __init__(self,
               file_pattern,
               min_bundle_size,
               compression_type,
               strip_trailing_newlines,
               coder,
               buffer_size=DEFAULT_READ_BUFFER_SIZE,
               validate=True,
               skip_header_lines=0):
    super(_TextSource, self).__init__(file_pattern, min_bundle_size,
                                      compression_type=compression_type,
                                      validate=validate)

    self._strip_trailing_newlines = strip_trailing_newlines
    self._compression_type = compression_type
    self._coder = coder
    self._buffer_size = buffer_size
    if skip_header_lines < 0:
      raise ValueError('Cannot skip negative number of header lines: %d',
                       skip_header_lines)

    self._skip_header_lines = skip_header_lines

  def read_records(self, file_name, range_tracker):
    start_offset = range_tracker.start_position()
    read_buffer = _TextSource.ReadBuffer('', 0)

    with self.open_file(file_name) as file_to_read:
      current_position = self._skip_lines(
          file_to_read, read_buffer,
          self._skip_header_lines) if self._skip_header_lines else 0
      start_offset = max(start_offset, current_position)
      if start_offset > current_position:
        # Seeking to one position before the start index and ignoring the
        # current line. If start_position is at beginning if the line, that line
        # belongs to the current bundle, hence ignoring that is incorrect.
        # Seeking to one byte before prevents that.

        file_to_read.seek(start_offset - 1)
        sep_bounds = self._find_separator_bounds(file_to_read, read_buffer)
        if not sep_bounds:
          # Could not find a separator after (start_offset - 1). This means that
          # none of the records within the file belongs to the current source.
          return

        _, sep_end = sep_bounds
        read_buffer.data = read_buffer.data[sep_end:]
        next_record_start_position = start_offset -1 + sep_end
      else:
        next_record_start_position = current_position

      while range_tracker.try_claim(next_record_start_position):
        record, num_bytes_to_next_record = self._read_record(file_to_read,
                                                             read_buffer)

        # For compressed text files that use an unsplittable OffsetRangeTracker
        # with infinity as the end position, above 'try_claim()' invocation
        # would pass for an empty record at the end of file that is not
        # followed by a new line character. Since such a record is at the last
        # position of a file, it should not be a part of the considered range.
        # We do this check to ignore such records.
        if len(record) == 0 and num_bytes_to_next_record < 0:
          break

        yield self._coder.decode(record)
        if num_bytes_to_next_record < 0:
          break
        next_record_start_position += num_bytes_to_next_record

  def _find_separator_bounds(self, file_to_read, read_buffer):
    # Determines the start and end positions within 'read_buffer.data' of the
    # next separator starting from position 'read_buffer.position'.
    # Currently supports following separators.
    # * '\n'
    # * '\r\n'
    # This method may increase the size of buffer but it will not decrease the
    # size of it.

    current_pos = read_buffer.position

    while True:
      if current_pos >= len(read_buffer.data):
        # Ensuring that there are enough bytes to determine if there is a '\n'
        # at current_pos.
        if not self._try_to_ensure_num_bytes_in_buffer(
            file_to_read, read_buffer, current_pos + 1):
          return

      # Using find() here is more efficient than a linear scan of the byte
      # array.
      next_lf = read_buffer.data.find('\n', current_pos)
      if next_lf >= 0:
        if next_lf > 0 and read_buffer.data[next_lf - 1] == '\r':
          # Found a '\r\n'. Accepting that as the next separator.
          return (next_lf - 1, next_lf + 1)
        else:
          # Found a '\n'. Accepting that as the next separator.
          return (next_lf, next_lf + 1)

      current_pos = len(read_buffer.data)

  def _try_to_ensure_num_bytes_in_buffer(
      self, file_to_read, read_buffer, num_bytes):
    # Tries to ensure that there are at least num_bytes bytes in the buffer.
    # Returns True if this can be fulfilled, returned False if this cannot be
    # fulfilled due to reaching EOF.
    while len(read_buffer.data) < num_bytes:
      read_data = file_to_read.read(self._buffer_size)
      if not read_data:
        return False

      read_buffer.data += read_data

    return True

  def _skip_lines(self, file_to_read, read_buffer, num_lines):
    if file_to_read.tell() > 0:
      file_to_read.seek(0)
    position = 0
    for _ in range(num_lines):
      _, num_bytes_to_next_record = self._read_record(file_to_read, read_buffer)
      if num_bytes_to_next_record < 0:
        # We reached end of file. It is OK to just break here
        # because subsequent _read_record will return same result.
        break
      position += num_bytes_to_next_record
    return position

  def _read_record(self, file_to_read, read_buffer):
    # Returns a tuple containing the current_record and number of bytes to the
    # next record starting from 'self._next_position_in_buffer'. If EOF is
    # reached, returns a tuple containing the current record and -1.

    if read_buffer.position > self._buffer_size:
      # read_buffer is too large. Truncating and adjusting it.
      read_buffer.data = read_buffer.data[read_buffer.position:]
      read_buffer.position = 0

    record_start_position_in_buffer = read_buffer.position
    sep_bounds = self._find_separator_bounds(file_to_read, read_buffer)
    read_buffer.position = sep_bounds[1] if sep_bounds else len(
        read_buffer.data)

    if not sep_bounds:
      # Reached EOF. Bytes up to the EOF is the next record. Returning '-1' for
      # the starting position of the next record.
      return (read_buffer.data[record_start_position_in_buffer:], -1)

    if self._strip_trailing_newlines:
      # Current record should not contain the separator.
      return (read_buffer.data[record_start_position_in_buffer:sep_bounds[0]],
              sep_bounds[1] - record_start_position_in_buffer)
    else:
      # Current record should contain the separator.
      return (read_buffer.data[record_start_position_in_buffer:sep_bounds[1]],
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
  def __init__(
      self,
      file_pattern=None,
      min_bundle_size=0,
      compression_type=fileio.CompressionTypes.AUTO,
      strip_trailing_newlines=True,
      coder=coders.StrUtf8Coder(),
      validate=True,
      skip_header_lines=0,
      **kwargs):
    """Initialize the ReadFromText transform.

    Args:
      file_pattern: The file path to read from as a local file path or a GCS
        gs:// path. The path can contain glob characters (*, ?, and [...]
        sets).
      min_bundle_size: Minimum size of bundles that should be generated when
                       splitting this source into bundles. See
                       ``FileBasedSource`` for more details.
      compression_type: Used to handle compressed input files. Typical value
          is CompressionTypes.AUTO, in which case the underlying file_path's
          extension will be used to detect the compression.
      strip_trailing_newlines: Indicates whether this source should remove
                               the newline char in each line it reads before
                               decoding that line.
      validate: flag to verify that the files exist during the pipeline
                creation time.
      skip_header_lines: Number of header lines to skip. Same number is skipped
                         from each source file. Must be 0 or higher. Large
                         number of skipped lines might impact performance.
      coder: Coder used to decode each line.
    """

    super(ReadFromText, self).__init__(**kwargs)
    self._strip_trailing_newlines = strip_trailing_newlines
    self._source = _TextSource(file_pattern, min_bundle_size, compression_type,
                               strip_trailing_newlines, coder,
                               validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)

  def display_data(self):
    return {'source_dd': self._source,
            'strip_newline': DisplayDataItem(self._strip_trailing_newlines,
                                             label='Strip Trailing New Lines')}


class WriteToText(PTransform):
  """A PTransform for writing to text files."""

  def __init__(self,
               file_path_prefix,
               file_name_suffix='',
               append_trailing_newlines=True,
               num_shards=0,
               shard_name_template=None,
               coder=coders.ToStringCoder(),
               compression_type=fileio.CompressionTypes.AUTO,
               header=None):
    r"""Initialize a WriteToText PTransform.

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
      header: String to write at beginning of file as a header. If not None and
          append_trailing_newlines is set, '\n' will be added.
    """

    self._append_trailing_newlines = append_trailing_newlines
    self._sink = _TextSink(file_path_prefix, file_name_suffix,
                           append_trailing_newlines, num_shards,
                           shard_name_template, coder, compression_type, header)

  def expand(self, pcoll):
    return pcoll | Write(self._sink)

  def display_data(self):
    return {'sink_dd': self._sink,
            'append_newline': DisplayDataItem(
                self._append_trailing_newlines,
                label='Append Trailing New Lines')}
