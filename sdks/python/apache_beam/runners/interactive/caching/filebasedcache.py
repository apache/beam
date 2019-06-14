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
import urllib
import warnings

from apache_beam import coders
from apache_beam.io import avroio
from apache_beam.io import parquetio
from apache_beam.io import textio
from apache_beam.io import tfrecordio
from apache_beam.io.filesystems import FileSystems
from apache_beam.runners.interactive.caching import PCollectionCache
from apache_beam.runners.interactive.caching.datatype_inference import \
    infer_element_type

try:  # Python 3
  unquote_to_bytes = urllib.parse.unquote_to_bytes
  quote = urllib.parse.quote
except AttributeError:  # Python 2
  # pylint: disable=deprecated-urllib-function
  unquote_to_bytes = urllib.unquote
  quote = urllib.quote

__all__ = [
    "FileBasedCache",
    "TextBasedCache",
    "SafeTextBasedCache",
    "TFRecordBasedCache",
    "ParquetBasedCache",
    "AvroBasedCache",
    "SafeFastPrimitivesCoder",
]


class FileBasedCache(PCollectionCache):

  def __init__(self, location, if_exists="error", **writer_kwargs):
    self.location = location
    self._writer_kwargs = writer_kwargs
    self._num_writes = 0
    self._coder_was_provided = "coder" in writer_kwargs

    def check_if_exists():
      if if_exists == "overwrite":
        self.clear()
      elif if_exists == "error":
        existing_files = self._existing_file_paths()
        if existing_files:
          raise IOError("The following cache files already exist: {}.".format(
              existing_files))
      else:
        raise ValueError(
            '`if_exists` must be set to either "error" or "overwrite".')

    check_if_exists()

  @property
  def timestamp(self):
    timestamp = 0
    for path in self._existing_file_paths():
      timestamp = max(timestamp, FileSystems.last_updated(path))
    return timestamp

  def reader(self, **reader_kwargs):
    kwargs = {
        k: v
        for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    kwargs.update(reader_kwargs)
    return self._reader_class(self._file_pattern, **kwargs)

  def writer(self):
    writer = self._writer_class(self._file_path_prefix, **self._writer_kwargs)

    if self._infer_coder:
      writer.expand = patch_writer_expand(writer.expand, writer,
                                          self._writer_kwargs)

    self._num_writes += 1
    return writer

  def read(self, **reader_kwargs):
    source = self.reader(**reader_kwargs)._source
    range_tracker = source.get_range_tracker(None, None)
    for element in source.read(range_tracker):
      yield element

  def write(self, elements):
    writer = self.writer()
    if self._infer_coder:
      # TODO(ostrokach): We might want to infer the element type from the first
      # N elements, rather than reading the entire iterator.
      elements = list(elements)
      element_type = infer_element_type(elements)
      register_coder(writer, self._writer_kwargs, element_type)
    handle = writer._sink.open(self._file_path_prefix)
    try:
      for element in elements:
        writer._sink.write_record(handle, element)
    finally:
      self._num_writes += 1
      writer._sink.close(handle)

  def clear(self):
    FileSystems.delete(self._existing_file_paths())
    if not self._coder_was_provided and "coder" in self._writer_kwargs:
      del self._writer_kwargs["coder"]

  @property
  def _file_path_prefix(self):
    return self.location + "-{:03d}".format(self._num_writes)

  @property
  def _file_pattern(self):
    return self.location + '*'

  def _existing_file_paths(self):
    match = FileSystems.match([self._file_pattern])
    assert len(match) == 1
    return [metadata.path for metadata in match[0].metadata_list]

  @property
  def _infer_coder(self):
    return (not self._writer_kwargs.get("coder") and
            "coder" in self._reader_passthrough_arguments)


class TextBasedCache(FileBasedCache):

  _reader_class = textio.ReadFromText
  _writer_class = textio.WriteToText
  _reader_passthrough_arguments = {"coder", "compression_type"}

  def __init__(self, location, **writer_kwargs):
    warnings.warn("TextBasedCache is not reliable and should not be used.")
    super(TextBasedCache, self).__init__(location, **writer_kwargs)


class SafeTextBasedCache(FileBasedCache):

  _reader_class = textio.ReadFromText
  _writer_class = textio.WriteToText
  _reader_passthrough_arguments = {"coder", "compression_type"}

  def __init__(self, location, **writer_kwargs):
    writer_kwargs["coder"] = SafeFastPrimitivesCoder()
    super(SafeTextBasedCache, self).__init__(location, **writer_kwargs)


class TFRecordBasedCache(FileBasedCache):

  _reader_class = tfrecordio.ReadFromTFRecord
  _writer_class = tfrecordio.WriteToTFRecord
  _reader_passthrough_arguments = {"coder", "compression_type"}


class ParquetBasedCache(FileBasedCache):

  _reader_class = parquetio.ReadFromParquet
  _writer_class = parquetio.WriteToParquet
  _reader_passthrough_arguments = {}


class AvroBasedCache(FileBasedCache):

  _reader_class = avroio.ReadFromAvro
  _writer_class = avroio.WriteToAvro
  _reader_passthrough_arguments = {"use_fastavro"}


def register_coder(writer, writer_kwargs, element_type):
  coder = coders.registry.get_coder(element_type)
  writer_kwargs["coder"] = coder
  writer._sink.coder = coder


def patch_writer_expand(expand_fn, writer, writer_kwargs):

  def expand(pcoll):
    register_coder(writer, writer_kwargs, pcoll.element_type)
    return expand_fn(pcoll)

  return expand


class SafeFastPrimitivesCoder(coders.Coder):
  """This class add an quote/unquote step to escape special characters."""

  # pylint: disable=deprecated-urllib-function

  def encode(self, value):
    return quote(
        coders.coders.FastPrimitivesCoder().encode(value)).encode('utf-8')

  def decode(self, value):
    return coders.coders.FastPrimitivesCoder().decode(unquote_to_bytes(value))
