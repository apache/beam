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

# pytype: skip-file

import logging
from functools import partial
from typing import Any
from typing import Optional

from apache_beam import typehints
from apache_beam.coders import coders
from apache_beam.io import filebasedsink
from apache_beam.io import filebasedsource
from apache_beam.io import iobase
from apache_beam.io.filebasedsource import ReadAllFiles
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem

__all__ = [
    'ReadFromText',
    'ReadFromTextWithFilename',
    'ReadAllFromText',
    'ReadAllFromTextContinuously',
    'WriteToText'
]

_LOGGER = logging.getLogger(__name__)


class _TextSource(filebasedsource.FileBasedSource):
  r"""A source for reading text files.

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
      assert isinstance(value, int)
      if value > len(self._data):
        raise ValueError(
            'Cannot set position to %d since it\'s larger than '
            'size of data %d.' % (value, len(self._data)))
      self._position = value

    def reset(self):
      self.data = b''
      self.position = 0

  def __init__(self,
               file_pattern,
               min_bundle_size,
               compression_type,
               strip_trailing_newlines,
               coder,  # type: coders.Coder
               buffer_size=DEFAULT_READ_BUFFER_SIZE,
               validate=True,
               skip_header_lines=0,
               header_processor_fns=(None, None),
               delimiter=None,
               escapechar=None):
    """Initialize a _TextSource

    Args:
      header_processor_fns (tuple): a tuple of a `header_matcher` function
        and a `header_processor` function. The `header_matcher` should
        return `True` for all lines at the start of the file that are part
        of the file header and `False` otherwise. These header lines will
        not be yielded when reading records and instead passed into
        `header_processor` to be handled. If `skip_header_lines` and a
        `header_matcher` are both provided, the value of `skip_header_lines`
        lines will be skipped and the header will be processed from
        there.
      delimiter (bytes) Optional: delimiter to split records.
        Must not self-overlap, because self-overlapping delimiters cause
        ambiguous parsing.
      escapechar (bytes) Optional: a single byte to escape the records
        delimiter, can also escape itself.
    Raises:
      ValueError: if skip_lines is negative.

    Please refer to documentation in class `ReadFromText` for the rest
    of the arguments.
    """
    super().__init__(
        file_pattern,
        min_bundle_size,
        compression_type=compression_type,
        validate=validate)

    self._strip_trailing_newlines = strip_trailing_newlines
    self._compression_type = compression_type
    self._coder = coder
    self._buffer_size = buffer_size
    if skip_header_lines < 0:
      raise ValueError(
          'Cannot skip negative number of header lines: %d' % skip_header_lines)
    elif skip_header_lines > 10:
      _LOGGER.warning(
          'Skipping %d header lines. Skipping large number of header '
          'lines might significantly slow down processing.')
    self._skip_header_lines = skip_header_lines
    self._header_matcher, self._header_processor = header_processor_fns
    if delimiter is not None:
      if not isinstance(delimiter, bytes) or len(delimiter) == 0:
        raise ValueError('Delimiter must be a non-empty bytes sequence.')
      if self._is_self_overlapping(delimiter):
        raise ValueError('Delimiter must not self-overlap.')
    self._delimiter = delimiter
    if escapechar is not None:
      if not (isinstance(escapechar, bytes) and len(escapechar) == 1):
        raise ValueError(
            "escapechar must be bytes of size 1: '%s'" % escapechar)
    self._escapechar = escapechar

  def display_data(self):
    parent_dd = super().display_data()
    parent_dd['strip_newline'] = DisplayDataItem(
        self._strip_trailing_newlines, label='Strip Trailing New Lines')
    parent_dd['buffer_size'] = DisplayDataItem(
        self._buffer_size, label='Buffer Size')
    parent_dd['coder'] = DisplayDataItem(self._coder.__class__, label='Coder')
    return parent_dd

  def read_records(self, file_name, range_tracker):
    start_offset = range_tracker.start_position()
    read_buffer = _TextSource.ReadBuffer(b'', 0)

    next_record_start_position = -1

    def split_points_unclaimed(stop_position):
      return (
          0 if stop_position <= next_record_start_position else
          iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)

    range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

    with self.open_file(file_name) as file_to_read:
      position_after_processing_header_lines = (
          self._process_header(file_to_read, read_buffer))
      start_offset = max(start_offset, position_after_processing_header_lines)
      if start_offset > position_after_processing_header_lines:
        # Seeking to one delimiter length before the start index and ignoring
        # the current line. If start_position is at beginning of the line, that
        # line belongs to the current bundle, hence ignoring that is incorrect.
        # Seeking to one delimiter before prevents that.

        if self._delimiter is not None and start_offset >= len(self._delimiter):
          required_position = start_offset - len(self._delimiter)
        else:
          required_position = start_offset - 1

        if self._escapechar is not None:
          # Need more bytes to check if the delimiter is escaped.
          # Seek until the first escapechar if any.
          while required_position > 0:
            file_to_read.seek(required_position - 1)
            if file_to_read.read(1) == self._escapechar:
              required_position -= 1
            else:
              break

        file_to_read.seek(required_position)
        read_buffer.reset()
        sep_bounds = self._find_separator_bounds(file_to_read, read_buffer)
        if not sep_bounds:
          # Could not find a delimiter after required_position. This means that
          # none of the records within the file belongs to the current source.
          return

        _, sep_end = sep_bounds
        read_buffer.data = read_buffer.data[sep_end:]
        next_record_start_position = required_position + sep_end
      else:
        next_record_start_position = position_after_processing_header_lines

      while range_tracker.try_claim(next_record_start_position):
        record, num_bytes_to_next_record = self._read_record(file_to_read,
                                                             read_buffer)
        # For compressed text files that use an unsplittable OffsetRangeTracker
        # with infinity as the end position, above 'try_claim()' invocation
        # would pass for an empty record at the end of file that is not
        # followed by a new line character. Since such a record is at the last
        # position of a file, it should not be a part of the considered range.
        # We do this check to ignore such records.
        if len(record) == 0 and num_bytes_to_next_record < 0:  # pylint: disable=len-as-condition
          break

        # Record delimiter must be larger than zero bytes.
        assert num_bytes_to_next_record != 0
        if num_bytes_to_next_record > 0:
          next_record_start_position += num_bytes_to_next_record

        yield self._coder.decode(record)
        if num_bytes_to_next_record < 0:
          break

  def _process_header(self, file_to_read, read_buffer):
    # Returns a tuple containing the position in file after processing header
    # records and a list of decoded header lines that match
    # 'header_matcher'.
    header_lines = []
    position = self._skip_lines(
        file_to_read, read_buffer,
        self._skip_header_lines) if self._skip_header_lines else 0
    if self._header_matcher:
      while True:
        record, num_bytes_to_next_record = self._read_record(file_to_read,
                                                             read_buffer)
        decoded_line = self._coder.decode(record)
        if not self._header_matcher(decoded_line):
          # We've read past the header section at this point, so go back a line.
          file_to_read.seek(position)
          read_buffer.reset()
          break
        header_lines.append(decoded_line)
        if num_bytes_to_next_record < 0:
          break
        position += num_bytes_to_next_record

      if self._header_processor:
        self._header_processor(header_lines)

    return position

  def _find_separator_bounds(self, file_to_read, read_buffer):
    # Determines the start and end positions within 'read_buffer.data' of the
    # next delimiter starting from position 'read_buffer.position'.
    # Use the custom delimiter to be used in place of
    # the default ones ('\n' or '\r\n')'
    # This method may increase the size of buffer but it will not decrease the
    # size of it.

    current_pos = read_buffer.position

    # b'\n' use as default
    delimiter = self._delimiter or b'\n'
    delimiter_len = len(delimiter)

    while True:
      if current_pos >= len(read_buffer.data) - delimiter_len + 1:
        # Ensuring that there are enough bytes to determine
        # at current_pos.
        if not self._try_to_ensure_num_bytes_in_buffer(
            file_to_read, read_buffer, current_pos + delimiter_len):
          return

      # Using find() here is more efficient than a linear scan
      # of the byte array.
      next_delim = read_buffer.data.find(delimiter, current_pos)

      if next_delim >= 0:
        if (self._delimiter is None and
            read_buffer.data[next_delim - 1:next_delim] == b'\r'):
          if self._escapechar is not None and self._is_escaped(read_buffer,
                                                               next_delim - 1):
            # Accept '\n' as a default delimiter, because '\r' is escaped.
            return (next_delim, next_delim + 1)
          else:
            # Accept both '\r\n' and '\n' as a default delimiter.
            return (next_delim - 1, next_delim + 1)
        else:
          if self._escapechar is not None and self._is_escaped(read_buffer,
                                                               next_delim):
            # Skip an escaped delimiter.
            current_pos = next_delim + delimiter_len + 1
            continue
          else:
            # Found a delimiter. Accepting that as the next delimiter.
            return (next_delim, next_delim + delimiter_len)

      elif self._delimiter is not None:
        # Corner case: custom delimiter is truncated at the end of the buffer.
        next_delim = read_buffer.data.find(
            delimiter[0], len(read_buffer.data) - delimiter_len + 1)
        if next_delim >= 0:
          # Delimiters longer than 1 byte may cross the buffer boundary.
          # Defer full matching till the next iteration.
          current_pos = next_delim
          continue

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
    """Skip num_lines from file_to_read, return num_lines+1 start position."""
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
    # next record starting from 'read_buffer.position'. If EOF is
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
      # Current record should not contain the delimiter.
      return (
          read_buffer.data[record_start_position_in_buffer:sep_bounds[0]],
          sep_bounds[1] - record_start_position_in_buffer)
    else:
      # Current record should contain the delimiter.
      return (
          read_buffer.data[record_start_position_in_buffer:sep_bounds[1]],
          sep_bounds[1] - record_start_position_in_buffer)

  @staticmethod
  def _is_self_overlapping(delimiter):
    # A delimiter self-overlaps if it has a prefix that is also its suffix.
    for i in range(1, len(delimiter)):
      if delimiter[0:i] == delimiter[len(delimiter) - i:]:
        return True
    return False

  def _is_escaped(self, read_buffer, position):
    # Returns True if byte at position is preceded with an odd number
    # of escapechar bytes or False if preceded by 0 or even escapes
    # (the even number means that all the escapes are escaped themselves).
    escape_count = 0
    for current_pos in reversed(range(0, position)):
      if read_buffer.data[current_pos:current_pos + 1] != self._escapechar:
        break
      escape_count += 1
    return escape_count % 2 == 1

  def output_type_hint(self):
    try:
      return self._coder.to_type_hint()
    except NotImplementedError:
      return Any


class _TextSourceWithFilename(_TextSource):
  def read_records(self, file_name, range_tracker):
    records = super().read_records(file_name, range_tracker)
    for record in records:
      yield (file_name, record)

  def output_type_hint(self):
    return typehints.KV[str, super().output_type_hint()]


class _TextSink(filebasedsink.FileBasedSink):
  """A sink to a GCS or local text file or files."""

  def __init__(self,
               file_path_prefix,
               file_name_suffix='',
               append_trailing_newlines=True,
               num_shards=0,
               shard_name_template=None,
               coder=coders.ToBytesCoder(),  # type: coders.Coder
               compression_type=CompressionTypes.AUTO,
               header=None,
               footer=None,
               *,
               max_records_per_shard=None,
               max_bytes_per_shard=None,
               skip_if_empty=False):
    """Initialize a _TextSink.

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
        the shard number and shard count. When constructing a filename for a
        particular shard number, the upper-case letters 'S' and 'N' are
        replaced with the 0-padded shard number and shard count respectively.
        This argument can be '' in which case it behaves as if num_shards was
        set to 1 and only one file will be generated. The default pattern used
        is '-SSSSS-of-NNNNN' if None is passed as the shard_name_template.
      coder: Coder used to encode each line.
      compression_type: Used to handle compressed output files. Typical value
        is CompressionTypes.AUTO, in which case the final file path's
        extension (as determined by file_path_prefix, file_name_suffix,
        num_shards and shard_name_template) will be used to detect the
        compression.
      header: String to write at beginning of file as a header. If not None and
        append_trailing_newlines is set, '\n' will be added.
      footer: String to write at the end of file as a footer. If not None and
        append_trailing_newlines is set, '\n' will be added.
      max_records_per_shard: Maximum number of records to write to any
        individual shard.
      max_bytes_per_shard: Target maximum number of bytes to write to any
        individual shard. This may be exceeded slightly, as a new shard is
        created once this limit is hit, but the remainder of a given record, a
        subsequent newline, and a footer may cause the actual shard size
        to exceed this value.  This also tracks the uncompressed,
        not compressed, size of the shard.
      skip_if_empty: Don't write any shards if the PCollection is empty.

    Returns:
      A _TextSink object usable for writing.
    """
    super().__init__(
        file_path_prefix,
        file_name_suffix=file_name_suffix,
        num_shards=num_shards,
        shard_name_template=shard_name_template,
        coder=coder,
        mime_type='text/plain',
        compression_type=compression_type,
        max_records_per_shard=max_records_per_shard,
        max_bytes_per_shard=max_bytes_per_shard,
        skip_if_empty=skip_if_empty)
    self._append_trailing_newlines = append_trailing_newlines
    self._header = header
    self._footer = footer

  def open(self, temp_path):
    file_handle = super().open(temp_path)
    if self._header is not None:
      file_handle.write(coders.ToBytesCoder().encode(self._header))
      if self._append_trailing_newlines:
        file_handle.write(b'\n')
    return file_handle

  def close(self, file_handle):
    if self._footer is not None:
      file_handle.write(coders.ToBytesCoder().encode(self._footer))
      if self._append_trailing_newlines:
        file_handle.write(b'\n')
    super().close(file_handle)

  def display_data(self):
    dd_parent = super().display_data()
    dd_parent['append_newline'] = DisplayDataItem(
        self._append_trailing_newlines, label='Append Trailing New Lines')
    return dd_parent

  def write_encoded_record(self, file_handle, encoded_value):
    """Writes a single encoded record."""
    file_handle.write(encoded_value)
    if self._append_trailing_newlines:
      file_handle.write(b'\n')


def _create_text_source(
    file_pattern=None,
    min_bundle_size=None,
    compression_type=None,
    strip_trailing_newlines=None,
    coder=None,
    validate=False,
    skip_header_lines=None,
    delimiter=None,
    escapechar=None):
  return _TextSource(
      file_pattern=file_pattern,
      min_bundle_size=min_bundle_size,
      compression_type=compression_type,
      strip_trailing_newlines=strip_trailing_newlines,
      coder=coder,
      validate=validate,
      skip_header_lines=skip_header_lines,
      delimiter=delimiter,
      escapechar=escapechar)


class ReadAllFromText(PTransform):
  """A ``PTransform`` for reading a ``PCollection`` of text files.

   Reads a ``PCollection`` of text files or file patterns and produces a
   ``PCollection`` of strings.

  Parses a text file as newline-delimited elements, by default assuming
  UTF-8 encoding. Supports newline delimiters '\\n' and '\\r\\n'.

  If `with_filename` is ``True`` the output will include the file name. This is
  similar to ``ReadFromTextWithFilename`` but this ``PTransform`` can be placed
  anywhere in the pipeline.

  This implementation only supports reading text encoded using UTF-8 or ASCII.
  This does not support other encodings such as UTF-16 or UTF-32.

  This implementation is only tested with batch pipeline. In streaming,
  reading may happen with delay due to the limitation in ReShuffle involved.
  """
  DEFAULT_DESIRED_BUNDLE_SIZE = 64 * 1024 * 1024  # 64MB

  def __init__(
      self,
      min_bundle_size=0,
      desired_bundle_size=DEFAULT_DESIRED_BUNDLE_SIZE,
      compression_type=CompressionTypes.AUTO,
      strip_trailing_newlines=True,
      validate=False,
      coder=coders.StrUtf8Coder(),  # type: coders.Coder
      skip_header_lines=0,
      with_filename=False,
      delimiter=None,
      escapechar=None,
      **kwargs):
    """Initialize the ``ReadAllFromText`` transform.

    Args:
      min_bundle_size: Minimum size of bundles that should be generated when
        splitting this source into bundles. See ``FileBasedSource`` for more
        details.
      desired_bundle_size: Desired size of bundles that should be generated when
        splitting this source into bundles. See ``FileBasedSource`` for more
        details.
      compression_type: Used to handle compressed input files. Typical value
        is ``CompressionTypes.AUTO``, in which case the underlying file_path's
        extension will be used to detect the compression.
      strip_trailing_newlines: Indicates whether this source should remove
        the newline char in each line it reads before decoding that line.
      validate: flag to verify that the files exist during the pipeline
        creation time.
      skip_header_lines: Number of header lines to skip. Same number is skipped
        from each source file. Must be 0 or higher. Large number of skipped
        lines might impact performance.
      coder: Coder used to decode each line.
      with_filename: If True, returns a Key Value with the key being the file
        name and the value being the actual data. If False, it only returns
        the data.
      delimiter (bytes) Optional: delimiter to split records.
        Must not self-overlap, because self-overlapping delimiters cause
        ambiguous parsing.
      escapechar (bytes) Optional: a single byte to escape the records
        delimiter, can also escape itself.
    """
    super().__init__(**kwargs)
    self._source_from_file = partial(
        _create_text_source,
        min_bundle_size=min_bundle_size,
        compression_type=compression_type,
        strip_trailing_newlines=strip_trailing_newlines,
        validate=validate,
        coder=coder,
        skip_header_lines=skip_header_lines,
        delimiter=delimiter,
        escapechar=escapechar)
    self._desired_bundle_size = desired_bundle_size
    self._min_bundle_size = min_bundle_size
    self._compression_type = compression_type
    self._with_filename = with_filename
    self._read_all_files = ReadAllFiles(
        True,
        self._compression_type,
        self._desired_bundle_size,
        self._min_bundle_size,
        self._source_from_file,
        self._with_filename)

  def expand(self, pvalue):
    return pvalue | 'ReadAllFiles' >> self._read_all_files


class ReadAllFromTextContinuously(ReadAllFromText):
  """A ``PTransform`` for reading text files in given file patterns.
  This PTransform acts as a Source and produces continuously a ``PCollection``
  of strings.

  For more details, see ``ReadAllFromText`` for text parsing settings;
  see ``apache_beam.io.fileio.MatchContinuously`` for watching settings.

  ReadAllFromTextContinuously is experimental.  No backwards-compatibility
  guarantees. Due to the limitation on Reshuffle, current implementation does
  not scale.
  """
  _ARGS_FOR_MATCH = (
      'interval',
      'has_deduplication',
      'start_timestamp',
      'stop_timestamp',
      'match_updated_files',
      'apply_windowing')
  _ARGS_FOR_READ = (
      'min_bundle_size',
      'desired_bundle_size',
      'compression_type',
      'strip_trailing_newlines',
      'validate',
      'coder',
      'skip_header_lines',
      'with_filename',
      'delimiter',
      'escapechar')

  def __init__(self, file_pattern, **kwargs):
    """Initialize the ``ReadAllFromTextContinuously`` transform.

    Accepts args for constructor args of both :class:`ReadAllFromText` and
    :class:`~apache_beam.io.fileio.MatchContinuously`.
    """
    kwargs_for_match = {
        k: v
        for (k, v) in kwargs.items() if k in self._ARGS_FOR_MATCH
    }
    kwargs_for_read = {
        k: v
        for (k, v) in kwargs.items() if k in self._ARGS_FOR_READ
    }
    kwargs_additinal = {
        k: v
        for (k, v) in kwargs.items()
        if k not in self._ARGS_FOR_MATCH and k not in self._ARGS_FOR_READ
    }
    super().__init__(**kwargs_for_read, **kwargs_additinal)
    self._file_pattern = file_pattern
    self._kwargs_for_match = kwargs_for_match

  def expand(self, pbegin):
    # Importing locally to prevent circular dependency issues.
    from apache_beam.io.fileio import MatchContinuously

    # TODO(BEAM-14497) always reshuffle once gbk always trigger works.
    return (
        pbegin
        | MatchContinuously(self._file_pattern, **self._kwargs_for_match)
        | 'ReadAllFiles' >> self._read_all_files._disable_reshuffle())


class ReadFromText(PTransform):
  r"""A :class:`~apache_beam.transforms.ptransform.PTransform` for reading text
  files.

  Parses a text file as newline-delimited elements, by default assuming
  ``UTF-8`` encoding. Supports newline delimiters ``\n`` and ``\r\n``
  or specified delimiter .

  This implementation only supports reading text encoded using ``UTF-8`` or
  ``ASCII``.
  This does not support other encodings such as ``UTF-16`` or ``UTF-32``.
  """

  _source_class = _TextSource

  def __init__(
      self,
      file_pattern=None,
      min_bundle_size=0,
      compression_type=CompressionTypes.AUTO,
      strip_trailing_newlines=True,
      coder=coders.StrUtf8Coder(),  # type: coders.Coder
      validate=True,
      skip_header_lines=0,
      delimiter=None,
      escapechar=None,
      **kwargs):
    """Initialize the :class:`ReadFromText` transform.

    Args:
      file_pattern (str): The file path to read from as a local file path or a
        GCS ``gs://`` path. The path can contain glob characters
        (``*``, ``?``, and ``[...]`` sets).
      min_bundle_size (int): Minimum size of bundles that should be generated
        when splitting this source into bundles. See
        :class:`~apache_beam.io.filebasedsource.FileBasedSource` for more
        details.
      compression_type (str): Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      strip_trailing_newlines (bool): Indicates whether this source should
        remove the newline char in each line it reads before decoding that line.
      validate (bool): flag to verify that the files exist during the pipeline
        creation time.
      skip_header_lines (int): Number of header lines to skip. Same number is
        skipped from each source file. Must be 0 or higher. Large number of
        skipped lines might impact performance.
      coder (~apache_beam.coders.coders.Coder): Coder used to decode each line.
      delimiter (bytes) Optional: delimiter to split records.
        Must not self-overlap, because self-overlapping delimiters cause
        ambiguous parsing.
      escapechar (bytes) Optional: a single byte to escape the records
        delimiter, can also escape itself.
    """

    super().__init__(**kwargs)
    self._source = self._source_class(
        file_pattern,
        min_bundle_size,
        compression_type,
        strip_trailing_newlines,
        coder,
        validate=validate,
        skip_header_lines=skip_header_lines,
        delimiter=delimiter,
        escapechar=escapechar)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source).with_output_types(
        self._source.output_type_hint())


class ReadFromTextWithFilename(ReadFromText):
  r"""A :class:`~apache_beam.io.textio.ReadFromText` for reading text
  files returning the name of the file and the content of the file.

  This class extend ReadFromText class just setting a different
  _source_class attribute.
  """

  _source_class = _TextSourceWithFilename


class WriteToText(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for writing to
  text files."""

  def __init__(
      self,
      file_path_prefix,  # type: str
      file_name_suffix='',
      append_trailing_newlines=True,
      num_shards=0,
      shard_name_template=None,  # type: Optional[str]
      coder=coders.ToBytesCoder(),  # type: coders.Coder
      compression_type=CompressionTypes.AUTO,
      header=None,
      footer=None,
      *,
      max_records_per_shard=None,
      max_bytes_per_shard=None,
      skip_if_empty=False):
    r"""Initialize a :class:`WriteToText` transform.

    Args:
      file_path_prefix (str): The file path to write to. The files written will
        begin with this prefix, followed by a shard identifier (see
        **num_shards**), and end in a common extension, if given by
        **file_name_suffix**. In most cases, only this argument is specified and
        **num_shards**, **shard_name_template**, and **file_name_suffix** use
        default values.
      file_name_suffix (str): Suffix for the files written.
      append_trailing_newlines (bool): indicate whether this sink should write
        an additional newline char after writing each element.
      num_shards (int): The number of files (shards) used for output.
        If not set, the service will decide on the optimal number of shards.
        Constraining the number of shards is likely to reduce
        the performance of a pipeline.  Setting this value is not recommended
        unless you require a specific number of output files.
      shard_name_template (str): A template string containing placeholders for
        the shard number and shard count. Currently only ``''`` and
        ``'-SSSSS-of-NNNNN'`` are patterns accepted by the service.
        When constructing a filename for a particular shard number, the
        upper-case letters ``S`` and ``N`` are replaced with the ``0``-padded
        shard number and shard count respectively.  This argument can be ``''``
        in which case it behaves as if num_shards was set to 1 and only one file
        will be generated. The default pattern used is ``'-SSSSS-of-NNNNN'``.
      coder (~apache_beam.coders.coders.Coder): Coder used to encode each line.
      compression_type (str): Used to handle compressed output files.
        Typical value is :class:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        final file path's extension (as determined by **file_path_prefix**,
        **file_name_suffix**, **num_shards** and **shard_name_template**) will
        be used to detect the compression.
      header (str): String to write at beginning of file as a header.
        If not :data:`None` and **append_trailing_newlines** is set, ``\n`` will
        be added.
      footer (str): String to write at the end of file as a footer.
        If not :data:`None` and **append_trailing_newlines** is set, ``\n`` will
        be added.
      max_records_per_shard: Maximum number of records to write to any
        individual shard.
      max_bytes_per_shard: Target maximum number of bytes to write to any
        individual shard. This may be exceeded slightly, as a new shard is
        created once this limit is hit, but the remainder of a given record, a
        subsequent newline, and a footer may cause the actual shard size
        to exceed this value.  This also tracks the uncompressed,
        not compressed, size of the shard.
      skip_if_empty: Don't write any shards if the PCollection is empty.
    """

    self._sink = _TextSink(
        file_path_prefix,
        file_name_suffix,
        append_trailing_newlines,
        num_shards,
        shard_name_template,
        coder,
        compression_type,
        header,
        footer,
        max_records_per_shard=max_records_per_shard,
        max_bytes_per_shard=max_bytes_per_shard,
        skip_if_empty=skip_if_empty)

  def expand(self, pcoll):
    return pcoll | Write(self._sink)
