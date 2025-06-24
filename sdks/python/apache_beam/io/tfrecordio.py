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

"""TFRecord sources and sinks."""

# pytype: skip-file

import codecs
import logging
import struct
from functools import partial

import crcmod

from apache_beam import coders
from apache_beam.io import filebasedsink
from apache_beam.io.filebasedsource import FileBasedSource
from apache_beam.io.filebasedsource import ReadAllFiles
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.transforms import PTransform

__all__ = ['ReadFromTFRecord', 'ReadAllFromTFRecord', 'WriteToTFRecord']

_LOGGER = logging.getLogger(__name__)


def _default_crc32c_fn(value):
  """Calculates crc32c of a bytes object using 
  either snappy or google-crc32c or crcmod."""

  if not _default_crc32c_fn.fn:
    try:
      import snappy  # pylint: disable=import-error
      # Support multiple versions of python-snappy:
      # https://github.com/andrix/python-snappy/pull/53
      if getattr(snappy, '_crc32c', None):
        _default_crc32c_fn.fn = snappy._crc32c  # pylint: disable=protected-access
      elif getattr(snappy, '_snappy', None):
        _default_crc32c_fn.fn = snappy._snappy._crc32c  # pylint: disable=protected-access
    except ImportError:
      pass

  if not _default_crc32c_fn.fn:
    try:
      import google_crc32c  # pylint: disable=import-error

      if getattr(google_crc32c, 'value', None):
        _default_crc32c_fn.fn = google_crc32c.value  # pylint: disable=protected-access
    except ImportError:
      pass

    if not _default_crc32c_fn.fn:
      _LOGGER.warning(
          'Couldn\'t find python-snappy or google-crc32c so the '
          'implementation of _TFRecordUtil._masked_crc32c is not as fast '
          'as it could be.')
      _default_crc32c_fn.fn = crcmod.predefined.mkPredefinedCrcFun('crc-32c')
  return _default_crc32c_fn.fn(value)


_default_crc32c_fn.fn = None  # type: ignore


class _TFRecordUtil(object):
  """Provides basic TFRecord encoding/decoding with consistency checks.

  For detailed TFRecord format description see:
    https://www.tensorflow.org/versions/r1.11/api_guides/python/python_io#TFRecords_Format_Details

  Note that masks and length are represented in LittleEndian order.
  """
  @classmethod
  def _masked_crc32c(cls, value, crc32c_fn=_default_crc32c_fn):
    """Compute a masked crc32c checksum for a value.

    Args:
      value: A bytes object for which we compute the crc.
      crc32c_fn: A function that can compute a crc32c.
        This is a performance hook that also helps with testing. Callers are
        not expected to make use of it directly.
    Returns:
      Masked crc32c checksum.
    """

    crc = crc32c_fn(value)
    return (((crc >> 15) | (crc << 17)) + 0xa282ead8) & 0xffffffff

  @staticmethod
  def encoded_num_bytes(record):
    """Return the number of bytes consumed by a record in its encoded form."""
    # 16 = 8 (Length) + 4 (crc of length) + 4 (crc of data)
    return len(record) + 16

  @classmethod
  def write_record(cls, file_handle, value):
    """Encode a value as a TFRecord.

    Args:
      file_handle: The file to write to.
      value: A bytes object representing content of the record.
    """
    encoded_length = struct.pack(b'<Q', len(value))
    file_handle.write(
        b''.join([
            encoded_length,
            struct.pack(b'<I', cls._masked_crc32c(encoded_length)),
            value,
            struct.pack(b'<I', cls._masked_crc32c(value))
        ]))

  @classmethod
  def read_record(cls, file_handle):
    """Read a record from a TFRecords file.

    Args:
      file_handle: The file to read from.
    Returns:
      None if EOF is reached; the paylod of the record otherwise.
    Raises:
      ValueError: If file appears to not be a valid TFRecords file.
    """
    buf_length_expected = 12
    buf = file_handle.read(buf_length_expected)
    if not buf:
      return None  # EOF Reached.

    # Validate all length related payloads.
    if len(buf) != buf_length_expected:
      raise ValueError(
          'Not a valid TFRecord. Fewer than %d bytes: %s' %
          (buf_length_expected, codecs.encode(buf, 'hex')))
    length, length_mask_expected = struct.unpack('<QI', buf)
    length_mask_actual = cls._masked_crc32c(buf[:8])
    if length_mask_actual != length_mask_expected:
      raise ValueError(
          'Not a valid TFRecord. Mismatch of length mask: %s' %
          codecs.encode(buf, 'hex'))

    # Validate all data related payloads.
    buf_length_expected = length + 4
    buf = file_handle.read(buf_length_expected)
    if len(buf) != buf_length_expected:
      raise ValueError(
          'Not a valid TFRecord. Fewer than %d bytes: %s' %
          (buf_length_expected, codecs.encode(buf, 'hex')))
    data, data_mask_expected = struct.unpack('<%dsI' % length, buf)
    data_mask_actual = cls._masked_crc32c(data)
    if data_mask_actual != data_mask_expected:
      raise ValueError(
          'Not a valid TFRecord. Mismatch of data mask: %s' %
          codecs.encode(buf, 'hex'))

    # All validation checks passed.
    return data


class _TFRecordSource(FileBasedSource):
  """A File source for reading files of TFRecords.

  For detailed TFRecords format description see:
    https://www.tensorflow.org/versions/r1.11/api_guides/python/python_io#TFRecords_Format_Details
  """
  def __init__(self, file_pattern, coder, compression_type, validate):
    """Initialize a TFRecordSource.  See ReadFromTFRecord for details."""
    super().__init__(
        file_pattern=file_pattern,
        compression_type=compression_type,
        splittable=False,
        validate=validate)
    self._coder = coder

  def read_records(self, file_name, offset_range_tracker):
    if offset_range_tracker.start_position():
      raise ValueError(
          'Start position not 0:%s' % offset_range_tracker.start_position())

    current_offset = offset_range_tracker.start_position()
    with self.open_file(file_name) as file_handle:
      while True:
        if not offset_range_tracker.try_claim(current_offset):
          raise RuntimeError('Unable to claim position: %s' % current_offset)
        record = _TFRecordUtil.read_record(file_handle)
        if record is None:
          return  # Reached EOF
        else:
          current_offset += _TFRecordUtil.encoded_num_bytes(record)
          yield self._coder.decode(record)


def _create_tfrecordio_source(
    file_pattern=None, coder=None, compression_type=None):
  # We intentionally disable validation for ReadAll pattern so that reading does
  # not fail for globs (elements) that are empty.
  return _TFRecordSource(file_pattern, coder, compression_type, validate=False)


class ReadAllFromTFRecord(PTransform):
  """A ``PTransform`` for reading a ``PCollection`` of TFRecord files."""
  def __init__(
      self,
      coder=coders.BytesCoder(),
      compression_type=CompressionTypes.AUTO,
      with_filename=False):
    """Initialize the ``ReadAllFromTFRecord`` transform.

    Args:
      coder: Coder used to decode each record.
      compression_type: Used to handle compressed input files. Default value
          is CompressionTypes.AUTO, in which case the file_path's extension will
          be used to detect the compression.
      with_filename: If True, returns a Key Value with the key being the file
        name and the value being the actual data. If False, it only returns
        the data.
    """
    super().__init__()
    source_from_file = partial(
        _create_tfrecordio_source,
        compression_type=compression_type,
        coder=coder)
    # Desired and min bundle sizes do not matter since TFRecord files are
    # unsplittable.
    self._read_all_files = ReadAllFiles(
        splittable=False,
        compression_type=compression_type,
        desired_bundle_size=0,
        min_bundle_size=0,
        source_from_file=source_from_file,
        with_filename=with_filename)

  def expand(self, pvalue):
    return pvalue | 'ReadAllFiles' >> self._read_all_files


class ReadFromTFRecord(PTransform):
  """Transform for reading TFRecord sources."""
  def __init__(
      self,
      file_pattern,
      coder=coders.BytesCoder(),
      compression_type=CompressionTypes.AUTO,
      validate=True):
    """Initialize a ReadFromTFRecord transform.

    Args:
      file_pattern: A file glob pattern to read TFRecords from.
      coder: Coder used to decode each record.
      compression_type: Used to handle compressed input files. Default value
          is CompressionTypes.AUTO, in which case the file_path's extension will
          be used to detect the compression.
      validate: Boolean flag to verify that the files exist during the pipeline
          creation time.

    Returns:
      A ReadFromTFRecord transform object.
    """
    super().__init__()
    self._source = _TFRecordSource(
        file_pattern, coder, compression_type, validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)


class _TFRecordSink(filebasedsink.FileBasedSink):
  """Sink for writing TFRecords files.

  For detailed TFRecord format description see:
    https://www.tensorflow.org/versions/r1.11/api_guides/python/python_io#TFRecords_Format_Details
  """
  def __init__(
      self,
      file_path_prefix,
      coder,
      file_name_suffix,
      num_shards,
      shard_name_template,
      compression_type,
      triggering_frequency=60):
    """Initialize a TFRecordSink. See WriteToTFRecord for details."""

    super().__init__(
        file_path_prefix=file_path_prefix,
        coder=coder,
        file_name_suffix=file_name_suffix,
        num_shards=num_shards,
        shard_name_template=shard_name_template,
        mime_type='application/octet-stream',
        compression_type=compression_type,
        triggering_frequency=triggering_frequency)

  def write_encoded_record(self, file_handle, value):
    _TFRecordUtil.write_record(file_handle, value)


class WriteToTFRecord(PTransform):
  """Transform for writing to TFRecord sinks."""
  def __init__(
      self,
      file_path_prefix,
      coder=coders.BytesCoder(),
      file_name_suffix='',
      num_shards=0,
      shard_name_template=None,
      compression_type=CompressionTypes.AUTO,
      triggering_frequency=None):
    """Initialize WriteToTFRecord transform.

    Args:
      file_path_prefix: The file path to write to. The files written will begin
        with this prefix, followed by a shard identifier (see num_shards), and
        end in a common extension, if given by file_name_suffix.
      coder: Coder used to encode each record.
      file_name_suffix: Suffix for the files written.
      num_shards: The number of files (shards) used for output. If not set, the
        default value will be used.
        In streaming if not set, the service will write a file per bundle.
      shard_name_template: A template string containing placeholders for
        the shard number and shard count. Currently only ``''``,
        ``'-SSSSS-of-NNNNN'``, ``'-W-SSSSS-of-NNNNN'`` and
        ``'-V-SSSSS-of-NNNNN'`` are patterns accepted by the service.
        When constructing a filename for a particular shard number, the
        upper-case letters ``S`` and ``N`` are replaced with the ``0``-padded
        shard number and shard count respectively.  This argument can be ``''``
        in which case it behaves as if num_shards was set to 1 and only one file
        will be generated. The default pattern used is ``'-SSSSS-of-NNNNN'`` for
        bounded PCollections and for ``'-W-SSSSS-of-NNNNN'`` unbounded 
        PCollections.
        W is used for windowed shard naming and is replaced with 
        ``[window.start, window.end)``
        V is used for windowed shard naming and is replaced with 
        ``[window.start.to_utc_datetime().strftime("%Y-%m-%dT%H-%M-%S"), 
        window.end.to_utc_datetime().strftime("%Y-%m-%dT%H-%M-%S")``
      compression_type: Used to handle compressed output files. Typical value
          is CompressionTypes.AUTO, in which case the file_path's extension will
          be used to detect the compression.
      triggering_frequency: (int) Every triggering_frequency duration, a window 
        will be triggered and all bundles in the window will be written.
        If set it overrides user windowing. Mandatory for GlobalWindow.

    Returns:
      A WriteToTFRecord transform object.
    """
    super().__init__()
    self._sink = _TFRecordSink(
        file_path_prefix,
        coder,
        file_name_suffix,
        num_shards,
        shard_name_template,
        compression_type,
        triggering_frequency)

  def expand(self, pcoll):
    if (not pcoll.is_bounded and self._sink.shard_name_template
        == filebasedsink.DEFAULT_SHARD_NAME_TEMPLATE):
      self._sink.shard_name_template = (
          filebasedsink.DEFAULT_WINDOW_SHARD_NAME_TEMPLATE)
      self._sink.shard_name_format = self._sink._template_to_format(
          self._sink.shard_name_template)
      self._sink.shard_name_glob_format = self._sink._template_to_glob_format(
          self._sink.shard_name_template)

    return pcoll | Write(self._sink)
